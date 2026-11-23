use std::{os::unix::fs::PermissionsExt, path::Path, process::Command};

use sqlx::{Connection, Executor, SqliteConnection, sqlite::SqliteConnectOptions};

use super::{Paths, backup, cleanup};
use crate::privileged_proc_proof::{ProofError, ProofResult};

const CHILD: &str = "isolated_state_backup::backup_runtime_tests::backup_runtime_fixture_child";
const PLANTED: &str = "planted-backup-runtime-secret";

fn paths(root: &Path) -> Paths {
    Paths {
        root: root.to_path_buf(),
        database: root.join("state.db"),
        config: root.join("config.toml"),
    }
}

async fn database(paths: &Paths) -> SqliteConnection {
    std::fs::create_dir(&paths.root).unwrap();
    std::fs::set_permissions(&paths.root, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(&paths.config, b"fixture").unwrap();
    std::fs::set_permissions(&paths.config, std::fs::Permissions::from_mode(0o600)).unwrap();
    let options = paths
        .database
        .to_str()
        .unwrap()
        .parse::<SqliteConnectOptions>()
        .unwrap()
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    connection.execute("PRAGMA journal_mode=WAL").await.unwrap();
    connection
        .execute("CREATE TABLE evidence(id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    connection
        .execute("INSERT INTO evidence DEFAULT VALUES")
        .await
        .unwrap();
    connection
}

fn line(result: Result<String, super::Failure>) -> (String, i32) {
    match result {
        Ok(line) => (line, 0),
        Err(failure) => (
            format!(
                "{} result=rejected status={} cause={}",
                failure.command, failure.status, failure.cause
            ),
            i32::from(failure.status),
        ),
    }
}

#[tokio::test]
#[ignore]
async fn backup_runtime_fixture_child() {
    let Some(root) = std::env::var_os("T050_BACKUP_RUNTIME_ROOT") else {
        return;
    };
    let mode = std::env::var("T050_BACKUP_RUNTIME_MODE").unwrap();
    let paths = paths(Path::new(&root));
    let connection = database(&paths).await;
    let result = match mode.as_str() {
        "success" => {
            let result = backup::run_with_proof_fixture(paths, Ok(ProofResult::Clean)).await;
            connection.close().await.unwrap();
            result
        }
        "cleanup" => {
            let created =
                backup::run_with_proof_fixture(paths.clone(), Ok(ProofResult::Clean)).await;
            connection.close().await.unwrap();
            match created {
                Err(failure) => Err(failure),
                Ok(_) => {
                    let namespace = super::fs::open_dir(&paths.namespace()).unwrap();
                    let hash = backup::load_manifest(&namespace).unwrap().snapshot_hash;
                    drop(namespace);
                    cleanup::cleanup_fixture(&paths, hash).map_err(|cause| {
                        super::Failure::rejected("cleanup-isolated-t050-backup", cause)
                    })
                }
            }
        }
        "process" | "listener" | "unknown" => {
            let proof = match mode.as_str() {
                "process" => ProofResult::Process,
                "listener" => ProofResult::Listener,
                "unknown" => ProofResult::Unknown,
                _ => unreachable!(),
            };
            let result = backup::run_with_proof_fixture(paths, Ok(proof)).await;
            connection.close().await.unwrap();
            result
        }
        "no-generation" => {
            let result = backup::run_with_proof_fixture(paths, Err(ProofError)).await;
            connection.close().await.unwrap();
            result
        }
        _ => unreachable!(),
    };
    let (output, status) = line(result);
    println!("{output}");
    std::process::exit(status);
}

fn run(root: &Path, mode: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", CHILD, "--nocapture"])
        .env_remove("RUST_TEST_THREADS")
        .env("T050_BACKUP_RUNTIME_ROOT", root)
        .env("T050_BACKUP_RUNTIME_MODE", mode)
        .output()
        .unwrap()
}

fn fixed_line(output: &std::process::Output) -> &str {
    std::str::from_utf8(&output.stdout)
        .unwrap()
        .lines()
        .find(|line| {
            line.starts_with("backup-isolated-t050-state ")
                || line.starts_with("cleanup-isolated-t050-backup ")
        })
        .unwrap_or_else(|| {
            panic!(
                "child emitted no fixed record: exit={:?} stdout_bytes={} stderr_bytes={}",
                output.status.code(),
                output.stdout.len(),
                output.stderr.len()
            )
        })
}

#[test]
fn process_backup_success_and_cleanup_are_real_and_fixed() {
    let success = tempfile::tempdir().unwrap();
    let output = run(&success.path().join("state"), "success");
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(fixed_line(&output).starts_with("backup-isolated-t050-state result=created status=0 "));
    assert!(success.path().join("state/.t050-backup-v1").is_dir());

    let cleanup = tempfile::tempdir().unwrap();
    let output = run(&cleanup.path().join("state"), "cleanup");
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        fixed_line(&output)
            .starts_with("cleanup-isolated-t050-backup result=cleaned status=0 count=")
    );
    assert!(!cleanup.path().join("state/.t050-backup-v1").exists());
}

#[test]
fn process_runtime_and_no_generation_refuse_without_residue() {
    for mode in ["process", "listener", "unknown", "no-generation"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("state");
        let output = run(&root, mode);
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(
            fixed_line(&output),
            "backup-isolated-t050-state result=rejected status=3 cause=runtime"
        );
        assert!(output.stderr.is_empty());
        assert!(!root.join(".t050-backup-v1").exists());
        assert!(!root.join(".runtime-admission.lock").exists());
    }
}

#[test]
fn process_backup_refusal_is_redacted() {
    let temp = tempfile::Builder::new().prefix(PLANTED).tempdir().unwrap();
    let output = run(&temp.path().join(PLANTED), "unknown");
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(
        fixed_line(&output),
        "backup-isolated-t050-state result=rejected status=3 cause=runtime"
    );
    assert!(!fixed_line(&output).contains(PLANTED));
    assert!(output.stderr.is_empty());
}
