use std::{
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Output,
};

use super::{Fixture, durable_rows};

// Include database/sidecar bytes and permissions; never follow hostile links.
#[derive(Debug, PartialEq, Eq)]
struct Entry {
    path: PathBuf,
    mode: u32,
    device: u64,
    inode: u64,
    links: u64,
    bytes: Vec<u8>,
}

fn tree(root: &Path) -> Vec<Entry> {
    fn visit(root: &Path, path: &Path, entries: &mut Vec<Entry>) {
        let meta = std::fs::symlink_metadata(path).unwrap();
        let bytes = if meta.is_file() {
            std::fs::read(path).unwrap()
        } else if meta.file_type().is_symlink() {
            std::fs::read_link(path)
                .unwrap()
                .as_os_str()
                .as_encoded_bytes()
                .to_vec()
        } else {
            Vec::new()
        };
        entries.push(Entry {
            path: path.strip_prefix(root).unwrap().to_owned(),
            mode: meta.mode(),
            device: meta.dev(),
            inode: meta.ino(),
            links: meta.nlink(),
            bytes,
        });
        if meta.is_dir() {
            for child in std::fs::read_dir(path).unwrap() {
                visit(root, &child.unwrap().path(), entries);
            }
        }
    }
    let mut entries = Vec::new();
    visit(root, root, &mut entries);
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    entries
}

fn refused(output: Output, command: &str, diagnostic: Option<(&str, &str)>, case: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(3),
        "case={case} stdout={stdout} stderr={stderr}"
    );
    let mut expected = String::new();
    if let Some((stage, cause)) = diagnostic {
        expected.push_str(&format!(
            "{command} stage={stage} cause={cause} counts=failures=1\n"
        ));
    }
    expected.push_str(&format!("{command} result=rejected status=3\n"));
    assert_eq!(stdout, expected, "case={case}");
    assert!(stderr.is_empty(), "case={case} stderr={stderr}");
}

const PREPARE: &str = "prepare-isolated-legacy-fixture-migration-manifest";
const MIGRATE: &str = "migrate-isolated-legacy-fixture";

fn prepare_refused(f: &Fixture, stage: &str, cause: &str, case: &str) {
    let before = tree(&f.root);
    let output = f
        .prepare()
        .env("T050_FIXTURE_MIGRATION_PREFLIGHT_DIAGNOSTIC", "1")
        .output()
        .unwrap();
    // Compare before interpreting status, so a classification failure cannot mask writes.
    assert!(tree(&f.root) == before, "fixture changed: case={case}");
    refused(output, PREPARE, Some((stage, cause)), case);
}

pub(super) async fn publication_and_sqlite_open() {
    let publication = Fixture::new().await;
    let rows = durable_rows(&publication.db).await;
    std::fs::set_permissions(
        &publication.manifest,
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    prepare_refused(
        &publication,
        "publication",
        "manifest-target",
        "manifest-mode",
    );
    assert_eq!(durable_rows(&publication.db).await, rows);

    let sqlite = Fixture::new().await;
    let rows = durable_rows(&sqlite.db).await;
    let wal = sqlite.db.with_extension("db-wal");
    std::fs::create_dir(&wal).unwrap();
    prepare_refused(&sqlite, "sqlite-open", "sidecar-auth", "wal-directory");
    std::fs::remove_dir(wal).unwrap();
    assert_eq!(durable_rows(&sqlite.db).await, rows);
}

pub(super) async fn hostile_targets_and_live_interlock() {
    let f = Fixture::new().await;
    let rows = durable_rows(&f.db).await;
    let guard =
        guigu_agent_bridge::runtime_admission::Guard::acquire(&f.db, &f.config, "startup").unwrap();
    let before = tree(&f.root);
    refused(f.run().output().unwrap(), MIGRATE, None, "live-admission");
    assert!(tree(&f.root) == before, "fixture changed");
    assert_eq!(durable_rows(&f.db).await, rows);
    drop(guard);
    std::fs::write(&f.evidence, b"hostile").unwrap();
    std::fs::set_permissions(&f.evidence, std::fs::Permissions::from_mode(0o644)).unwrap();
    let before = tree(&f.root);
    refused(f.run().output().unwrap(), MIGRATE, None, "evidence-mode");
    assert!(tree(&f.root) == before, "fixture changed");
    assert_eq!(durable_rows(&f.db).await, rows);

    let f = Fixture::new().await;
    let rows = durable_rows(&f.db).await;
    let outside = tempfile::tempdir().unwrap();
    let destination = outside.path().join("outside");
    std::fs::write(&destination, b"unchanged").unwrap();
    let outside_before = tree(outside.path());
    std::fs::remove_file(&f.manifest).unwrap();
    std::os::unix::fs::symlink(&destination, &f.manifest).unwrap();
    prepare_refused(&f, "publication", "manifest-target", "manifest-symlink");
    assert!(
        tree(outside.path()) == outside_before,
        "outside target changed"
    );
    assert_eq!(durable_rows(&f.db).await, rows);

    let f = Fixture::new().await;
    let rows = durable_rows(&f.db).await;
    std::fs::remove_file(&f.manifest).unwrap();
    let wal = f.db.with_extension("db-wal");
    assert!(!wal.exists());
    std::os::unix::fs::symlink(&destination, &wal).unwrap();
    prepare_refused(&f, "sqlite-open", "sidecar-auth", "wal-symlink");
    assert!(
        tree(outside.path()) == outside_before,
        "outside target changed"
    );
    std::fs::remove_file(wal).unwrap();
    assert_eq!(durable_rows(&f.db).await, rows);
}

#[tokio::test]
async fn both_sidecars_reject_hostile_types_before_discovery() {
    for suffix in ["db-wal", "db-shm"] {
        for kind in ["directory", "symlink", "writable"] {
            let f = Fixture::new_unprepared().await;
            let rows = durable_rows(&f.db).await;
            let outside = tempfile::tempdir().unwrap();
            let destination = outside.path().join("private-sentinel");
            std::fs::write(&destination, b"not-a-database-secret").unwrap();
            let outside_before = tree(outside.path());
            let sidecar = f.db.with_extension(suffix);
            match kind {
                "directory" => std::fs::create_dir(&sidecar).unwrap(),
                "symlink" => std::os::unix::fs::symlink(&destination, &sidecar).unwrap(),
                _ => {
                    std::fs::write(&sidecar, b"hostile-sidecar").unwrap();
                    std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o666))
                        .unwrap();
                }
            }
            prepare_refused(
                &f,
                "sqlite-open",
                "sidecar-auth",
                &format!("{suffix}-{kind}"),
            );
            assert!(
                tree(outside.path()) == outside_before,
                "outside target changed"
            );
            if kind == "directory" {
                std::fs::remove_dir(sidecar).unwrap();
            } else {
                std::fs::remove_file(sidecar).unwrap();
            }
            assert_eq!(durable_rows(&f.db).await, rows);
        }
    }
}

#[tokio::test]
async fn publication_refusal_precedes_unusable_database_discovery() {
    let f = Fixture::new_unprepared().await;
    std::fs::write(&f.manifest, b"private-manifest-sentinel").unwrap();
    std::fs::set_permissions(&f.manifest, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::write(&f.db, b"not-a-database-secret").unwrap();
    prepare_refused(
        &f,
        "publication",
        "manifest-target",
        "publication-before-discovery",
    );
}

#[tokio::test]
async fn maintenance_contention_keeps_generic_exclusion_and_never_opens_database() {
    let f = Fixture::new_unprepared().await;
    use std::os::fd::AsRawFd;
    let guard = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(f.root.join(".state.db.offline-migration.lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(guard.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    std::fs::write(&f.db, b"not-a-database-secret").unwrap();
    let before = tree(&f.root);
    let output = f.prepare().output().unwrap();
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(
        output.stdout,
        b"maintenance-exclusion result=busy-or-unresolved\n"
    );
    assert!(output.stderr.is_empty());
    assert!(tree(&f.root) == before, "fixture changed");
}

#[tokio::test]
async fn deferred_discovery_still_refuses_unresolved_state_without_publication() {
    for case in ["unknown-schema", "pending-operation"] {
        let f = Fixture::new_unprepared().await;
        let rows = durable_rows(&f.db).await;
        if case == "unknown-schema" {
            let mut c = sqlx::SqliteConnection::connect_with(
                &sqlx::sqlite::SqliteConnectOptions::new().filename(&f.db),
            )
            .await
            .unwrap();
            sqlx::query("UPDATE _sqlx_migrations SET checksum=X'00' WHERE version=1")
                .execute(&mut c)
                .await
                .unwrap();
            c.close().await.unwrap();
        } else {
            std::fs::write(
                f.root.join(".state.db.offline-operation.pending"),
                b"pending-sentinel",
            )
            .unwrap();
        }
        prepare_refused(&f, "preflight", "maintenance-discovery", case);
        assert_eq!(durable_rows(&f.db).await, rows);
        assert!(!f.manifest.exists());
    }
}

use sqlx::Connection;

#[tokio::test]
async fn unresolved_maintenance_preserves_stale_admission_evidence() {
    use std::{
        process::Stdio,
        time::{Duration, Instant},
    };

    let f = Fixture::new().await;
    let rows = durable_rows(&f.db).await;
    let pause = f.root.join("precommit");
    let mut child = f
        .run()
        .env("T050_FIXTURE_MIGRATION_TEST_PRECOMMIT", &pause)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !pause.exists() && Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let parked = pause.exists();
    if child.try_wait().unwrap().is_none() {
        child.kill().unwrap();
    }
    let status = child.wait().unwrap();
    assert!(parked, "child did not reach the precommit boundary");
    assert!(!status.success());
    assert_eq!(durable_rows(&f.db).await, rows);
    assert!(f.root.join(".runtime-admission.lock").exists());
    std::fs::write(
        f.root.join(".state.db.offline-operation.pending"),
        b"pending-sentinel",
    )
    .unwrap();
    prepare_refused(
        &f,
        "preflight",
        "maintenance-discovery",
        "pending-with-stale-admission",
    );
    assert_eq!(durable_rows(&f.db).await, rows);
}
