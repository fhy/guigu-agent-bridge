use std::os::unix::fs::PermissionsExt;

use sqlx::Connection;

use super::{Paths, backup, codec::MANIFEST, fault, fs};

fn paths(temp: &tempfile::TempDir) -> Paths {
    let root = temp.path().join("state");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    Paths {
        database: root.join("state.db"),
        root,
        config: temp.path().join("config"),
    }
}

async fn database(paths: &Paths) -> sqlx::SqliteConnection {
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
    connection
}

fn assert_no_manifest(paths: &Paths, point: &str) {
    if paths.namespace().exists() {
        let namespace = fs::open_dir(&paths.namespace()).unwrap();
        assert!(fs::leaf_absent(&namespace, MANIFEST).unwrap(), "{point}");
    }
}

#[tokio::test]
async fn snapshot_publication_boundaries_roll_back_without_enablement() {
    let cases = [
        ("before-publication-metadata", 1),
        ("after-publication-create", 1),
        ("after-snapshot-write", 1),
        ("after-snapshot-file-fsync", 1),
        ("after-publication-rename", 1),
        ("after-publication-parent-fsync", 1),
    ];
    for (point, occurrence) in cases {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        let connection = database(&paths).await;
        fault::nth(point, occurrence);
        assert!(
            backup::create_fixture(paths.clone()).await.is_err(),
            "{point}"
        );
        assert_no_manifest(&paths, point);
        connection.close().await.unwrap();
    }
}

#[tokio::test]
async fn manifest_publication_boundaries_roll_back_without_enablement() {
    let cases = [
        ("before-publication-metadata", 2),
        ("after-publication-create", 2),
        ("after-leaf-write", 1),
        ("after-leaf-fsync", 1),
        ("after-publication-rename", 2),
        ("after-publication-parent-fsync", 2),
    ];
    for (point, occurrence) in cases {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        let connection = database(&paths).await;
        fault::nth(point, occurrence);
        assert!(
            backup::create_fixture(paths.clone()).await.is_err(),
            "{point}"
        );
        assert_no_manifest(&paths, point);
        connection.close().await.unwrap();
    }
}

#[tokio::test]
async fn publication_rollback_boundaries_report_exact_cleanup_ambiguity() {
    for point in [
        "before-publication-rollback-unlink",
        "after-publication-rollback-unlink",
        "after-publication-rollback-fsync",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        let connection = database(&paths).await;
        fault::once("after-snapshot-write");
        fault::action(point, || fault::once(point));
        let error = backup::create_fixture(paths.clone()).await.unwrap_err();
        assert_eq!(error.cause, "cleanup", "{point}");
        assert_no_manifest(&paths, point);
        connection.close().await.unwrap();
    }
}
