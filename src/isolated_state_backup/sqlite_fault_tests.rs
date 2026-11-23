use std::os::unix::fs::PermissionsExt;

use sqlx::Connection;

use super::{Paths, backup, cleanup, fault};

fn fixture_paths(temp: &tempfile::TempDir) -> Paths {
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
    connection
}

#[tokio::test]
async fn closed_wal_main_only_snapshot_is_supported() {
    let temp = tempfile::tempdir().unwrap();
    let paths = fixture_paths(&temp);
    database(&paths).await.close().await.unwrap();
    assert!(!paths.root.join("state.db-wal").exists());
    assert!(!paths.root.join("state.db-shm").exists());

    let (source, retained) = backup::source_set_retained(&paths).unwrap();
    let destination_path = paths.root.join("diagnostic-snapshot");
    let destination = std::fs::File::create(&destination_path).unwrap();
    super::sqlite::online_backup(&paths, &source, &retained, &destination)
        .await
        .unwrap();
    drop(destination);
    std::fs::remove_file(destination_path).unwrap();

    let created = backup::create_fixture(paths.clone()).await.unwrap();
    assert!(created.contains("result=created"));
    let namespace = super::fs::open_dir(&paths.namespace()).unwrap();
    let expected = backup::load_manifest(&namespace).unwrap().snapshot_hash;
    drop(namespace);
    cleanup::cleanup_fixture(&paths, expected).unwrap();
}

#[tokio::test]
async fn retained_main_replacement_at_s0_s1_s3_s5_s7_is_invariant() {
    for point in [
        "sqlite-s0-retained",
        "sqlite-s1-open",
        "sqlite-s3-transaction",
        "sqlite-s5-copy",
        "sqlite-s7-close",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let paths = fixture_paths(&temp);
        let connection = database(&paths).await;
        let main = paths.database.clone();
        let saved = paths.root.join("saved-main");
        fault::action(point, move || {
            std::fs::rename(&main, &saved).unwrap();
            std::fs::write(&main, b"foreign-main").unwrap();
            std::fs::set_permissions(&main, std::fs::Permissions::from_mode(0o600)).unwrap();
        });
        assert!(
            backup::create_fixture(paths.clone()).await.is_err(),
            "{point}"
        );
        assert_eq!(std::fs::read(&paths.database).unwrap(), b"foreign-main");
        connection.close().await.unwrap_or(());
    }
}

#[tokio::test]
async fn shm_lifecycle_success_and_replacement_refusal_are_real_sqlite_cases() {
    let temp = tempfile::tempdir().unwrap();
    let paths = fixture_paths(&temp);
    let connection = database(&paths).await;
    let created = backup::create_fixture(paths.clone()).await.unwrap();
    assert!(created.contains("result=created"));
    connection.close().await.unwrap();
    let namespace = super::fs::open_dir(&paths.namespace()).unwrap();
    let expected = backup::load_manifest(&namespace).unwrap().snapshot_hash;
    drop(namespace);
    cleanup::cleanup_fixture(&paths, expected).unwrap();

    for point in [
        "sqlite-s1-open",
        "sqlite-s3-transaction",
        "sqlite-s5-copy",
        "sqlite-s7-close",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let paths = fixture_paths(&temp);
        let connection = database(&paths).await;
        let shm = paths.root.join("state.db-shm");
        assert!(shm.exists());
        let saved = paths.root.join("saved-shm");
        fault::action(point, move || {
            std::fs::rename(&shm, &saved).unwrap();
            std::fs::write(&shm, b"foreign-shm").unwrap();
            std::fs::set_permissions(&shm, std::fs::Permissions::from_mode(0o600)).unwrap();
        });
        assert!(
            backup::create_fixture(paths.clone()).await.is_err(),
            "{point}"
        );
        assert_eq!(
            std::fs::read(paths.root.join("state.db-shm")).unwrap(),
            b"foreign-shm"
        );
        connection.close().await.unwrap_or(());
    }
}
