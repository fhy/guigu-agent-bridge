use std::os::unix::fs::PermissionsExt;

use sqlx::Connection;

use super::{Paths, backup, cleanup, fault, fs, restore};

async fn fixture(restored: bool) -> (tempfile::TempDir, Paths, [u8; 32]) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("state");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let paths = Paths {
        database: root.join("state.db"),
        root,
        config: temp.path().join("config"),
    };
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
    sqlx::query("PRAGMA wal_autocheckpoint=0")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE evidence(id INTEGER PRIMARY KEY)")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO evidence DEFAULT VALUES")
        .execute(&mut connection)
        .await
        .unwrap();
    backup::create_fixture(paths.clone()).await.unwrap();
    connection.close().await.unwrap();
    let namespace = fs::open_dir(&paths.namespace()).unwrap();
    let snapshot = backup::load_manifest(&namespace).unwrap().snapshot_hash;
    drop(namespace);
    if restored {
        let current = backup::source_set(&paths).unwrap().hash;
        restore::restore_fixture(&paths, snapshot, current)
            .await
            .unwrap();
    }
    (temp, paths, snapshot)
}

#[tokio::test]
async fn both_cleanup_kinds_cover_manifest_quarantine_durability_and_ack() {
    for restored in [false, true] {
        for point in [
            "cleanup-after-manifest-quarantine",
            "cleanup-after-manifest-fsync",
            "cleanup-before-manifest-ack",
        ] {
            let (_temp, paths, snapshot) = fixture(restored).await;
            fault::once(point);
            assert_eq!(
                cleanup::cleanup_fixture(&paths, snapshot),
                Err("cleanup"),
                "{restored} {point}"
            );
            assert!(
                cleanup::cleanup_fixture(&paths, snapshot)
                    .unwrap()
                    .contains("status=0")
            );
            assert!(!paths.namespace().exists());
        }
    }
}

#[tokio::test]
async fn every_applicable_leaf_covers_unlink_parent_fsync_and_ack() {
    for (restored, leaves) in [(false, 2usize), (true, 3usize)] {
        for point in [
            "cleanup-after-artifact-unlink",
            "cleanup-after-leaf-fsync",
            "cleanup-before-leaf-ack",
        ] {
            for occurrence in 1..=leaves {
                let (_temp, paths, snapshot) = fixture(restored).await;
                fault::nth(point, occurrence);
                assert_eq!(
                    cleanup::cleanup_fixture(&paths, snapshot),
                    Err("cleanup"),
                    "{restored} {point} {occurrence}"
                );
                assert!(
                    cleanup::cleanup_fixture(&paths, snapshot)
                        .unwrap()
                        .contains("status=0")
                );
                assert!(!paths.namespace().exists());
            }
        }
    }
}
