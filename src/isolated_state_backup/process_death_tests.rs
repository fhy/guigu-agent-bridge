use std::os::unix::fs::PermissionsExt;

use sqlx::Connection;

use super::{Paths, backup, cleanup, fs, restore};

fn paths(root: impl Into<std::path::PathBuf>) -> Paths {
    let root = root.into();
    Paths {
        database: root.join("state.db"),
        root,
        config: std::path::PathBuf::from("unused"),
    }
}
fn decode(name: &str) -> [u8; 32] {
    let value = std::env::var(name).unwrap();
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).unwrap();
    }
    bytes
}
async fn fixture() -> (tempfile::TempDir, Paths, [u8; 32], [u8; 32]) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("state");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let paths = paths(root);
    let options = paths
        .database
        .to_str()
        .unwrap()
        .parse::<sqlx::sqlite::SqliteConnectOptions>()
        .unwrap()
        .create_if_missing(true);
    let mut connection = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE evidence(id INTEGER PRIMARY KEY)")
        .execute(&mut connection)
        .await
        .unwrap();
    backup::create_fixture(paths.clone()).await.unwrap();
    connection.close().await.unwrap();
    let namespace = fs::open_dir(&paths.namespace()).unwrap();
    let snapshot = backup::load_manifest(&namespace).unwrap().snapshot_hash;
    drop(namespace);
    let current = backup::source_set(&paths).unwrap().hash;
    (temp, paths, snapshot, current)
}
fn command(test: &str, paths: &Paths, snapshot: [u8; 32], point: &str) -> std::process::Command {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", test, "--nocapture"])
        .env("T050_BACKUP_CRASH_ROOT", &paths.root)
        .env("T050_BACKUP_CRASH_SNAPSHOT", backup::hex(snapshot))
        .env("T050_BACKUP_CRASH_POINT", point);
    command
}

#[tokio::test]
async fn restore_worker() {
    let Ok(root) = std::env::var("T050_BACKUP_CRASH_ROOT") else {
        return;
    };
    let _ = restore::restore_fixture(
        &paths(root),
        decode("T050_BACKUP_CRASH_SNAPSHOT"),
        decode("T050_BACKUP_CRASH_CURRENT"),
    )
    .await;
}
#[tokio::test]
async fn cleanup_worker() {
    let Ok(root) = std::env::var("T050_BACKUP_CRASH_ROOT") else {
        return;
    };
    let _ = cleanup::cleanup_fixture(&paths(root), decode("T050_BACKUP_CRASH_SNAPSHOT"));
}

#[tokio::test]
async fn forward_post_rename_pre_ack_process_death_is_adopted() {
    let (_temp, paths, snapshot, current) = fixture().await;
    let output = command(
        "isolated_state_backup::process_death_tests::restore_worker",
        &paths,
        snapshot,
        "restore-after-quarantine-rename",
    )
    .env("T050_BACKUP_CRASH_CURRENT", backup::hex(current))
    .output()
    .unwrap();
    assert_eq!(output.status.code(), Some(86));
    assert!(
        restore::restore_fixture(&paths, snapshot, current)
            .await
            .unwrap()
            .contains("result=committed")
    );
}

#[tokio::test]
async fn replacement_creation_and_publication_process_deaths_are_adopted() {
    for point in [
        "restore-after-new-write",
        "restore-after-new-fsync",
        "restore-before-publish-rename",
        "restore-after-publish-rename",
        "restore-after-publish-fsync",
        "restore-before-publish-ack",
    ] {
        let (_temp, paths, snapshot, current) = fixture().await;
        let output = command(
            "isolated_state_backup::process_death_tests::restore_worker",
            &paths,
            snapshot,
            point,
        )
        .env("T050_BACKUP_CRASH_CURRENT", backup::hex(current))
        .output()
        .unwrap();
        assert_eq!(output.status.code(), Some(86), "{point}");
        assert!(
            restore::restore_fixture(&paths, snapshot, current)
                .await
                .unwrap()
                .contains("result=committed"),
            "{point}"
        );
    }
}

#[tokio::test]
async fn rollback_post_rename_pre_ack_process_death_is_adopted() {
    for point in [
        "rollback-after-new-unlink",
        "rollback-after-new-fsync",
        "rollback-before-new-ack",
        "rollback-after-restore-rename",
    ] {
        let (_temp, paths, snapshot, current) = fixture().await;
        let output = command(
            "isolated_state_backup::process_death_tests::restore_worker",
            &paths,
            snapshot,
            point,
        )
        .env("T050_BACKUP_CRASH_CURRENT", backup::hex(current))
        .env("T050_BACKUP_FAIL_POINT", "restore-before-publish-rename")
        .output()
        .unwrap();
        assert_eq!(output.status.code(), Some(86), "{point}");
        assert_eq!(
            restore::restore_fixture(&paths, snapshot, current).await,
            Err("pre-publication"),
            "{point}"
        );
        assert_eq!(backup::source_set(&paths).unwrap().hash, current, "{point}");
    }
}

#[tokio::test]
async fn cleanup_and_witness_process_deaths_are_adopted() {
    for point in [
        "cleanup-after-leaf-unlink",
        "witness-after-namespace-unlink",
    ] {
        let (_temp, paths, snapshot, _) = fixture().await;
        let output = command(
            "isolated_state_backup::process_death_tests::cleanup_worker",
            &paths,
            snapshot,
            point,
        )
        .output()
        .unwrap();
        assert_eq!(output.status.code(), Some(86), "{point}");
        assert!(
            cleanup::cleanup_fixture(&paths, snapshot)
                .unwrap()
                .contains("status=0"),
            "{point}"
        );
        assert!(!paths.namespace().exists(), "{point}");
    }
}
