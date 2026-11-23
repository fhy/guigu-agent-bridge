use std::os::unix::fs::PermissionsExt;

use sqlx::{Connection, Executor, SqliteConnection, sqlite::SqliteConnectOptions};

use super::{
    Paths, backup, cleanup,
    codec::{Identity, Manifest, RestorePhase, RestoreRecord},
    restore, rollback,
};

fn paths(temp: &tempfile::TempDir) -> Paths {
    let root = temp.path().join("state");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = temp.path().join("config.toml");
    std::fs::write(&config, b"fixture").unwrap();
    Paths {
        database: root.join("state.db"),
        root,
        config,
    }
}

async fn wal_database(paths: &Paths) -> SqliteConnection {
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
        .execute("PRAGMA wal_autocheckpoint=0")
        .await
        .unwrap();
    connection
        .execute("CREATE TABLE evidence(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        .await
        .unwrap();
    connection
        .execute("INSERT INTO evidence(value) VALUES ('committed')")
        .await
        .unwrap();
    connection
}

fn identity(seed: u64) -> Identity {
    Identity {
        device: 1,
        inode: seed,
        size: seed + 3,
        uid: 1000,
        gid: 1000,
        mode: 0o600,
    }
}

#[test]
fn canonical_manifest_rejects_malformed_and_noncanonical_bytes() {
    let value = Manifest {
        operator_uid: 1000,
        operator_gid: 1000,
        namespace: identity(1),
        source_main: identity(2),
        source_wal: Some(identity(3)),
        source_shm: None,
        source_set_hash: [4; 32],
        source_bytes: 20,
        snapshot: identity(5),
        snapshot_hash: [6; 32],
        sqlite_version: 3049001,
        page_size: 4096,
        page_count: 2,
        user_version: 7,
        schema_hash: [8; 32],
        snapshot_id: [9; 16],
        created_nanos: 10,
    };
    let bytes = value.encode();
    assert_eq!(Manifest::decode(&bytes).unwrap(), value);
    for index in [0, 8, bytes.len() - 1] {
        let mut changed = bytes.clone();
        changed[index] ^= 1;
        assert!(Manifest::decode(&changed).is_err());
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(Manifest::decode(&trailing).is_err());
}

#[test]
fn restore_record_binds_phase_bitmap_and_predecessor() {
    let value = RestoreRecord {
        sequence: 4,
        predecessor: [1; 32],
        manifest: [2; 32],
        snapshot: [3; 32],
        expected_current: [4; 32],
        phase: RestorePhase::IntentMain,
        bitmap: 3,
        original_main: identity(10),
        original_wal: Some(identity(11)),
        original_shm: None,
        replacement: Some(identity(12)),
    };
    let bytes = value.encode();
    assert_eq!(RestoreRecord::decode(&bytes).unwrap(), value);
    let mut changed = bytes;
    changed[147] ^= 1;
    assert!(RestoreRecord::decode(&changed).is_err());
}

#[tokio::test]
async fn wal_committed_data_is_in_standalone_snapshot_and_verify_is_repeatable() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let paths = paths(&temp);
    let mut connection = wal_database(&paths).await;
    connection.execute("BEGIN IMMEDIATE").await.unwrap();
    connection
        .execute("INSERT INTO evidence(value) VALUES ('uncommitted')")
        .await
        .unwrap();
    backup::create_fixture(paths.clone()).await.unwrap();
    let first = backup::verify_fixture(&paths).await.unwrap();
    let second = backup::verify_fixture(&paths).await.unwrap();
    assert_eq!(first, second);
    let mut snapshot =
        SqliteConnection::connect(paths.namespace().join("snapshot.sqlite3").to_str().unwrap())
            .await
            .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM evidence")
        .fetch_one(&mut snapshot)
        .await
        .unwrap();
    assert_eq!(count, 1);
    snapshot.close().await.unwrap();
    connection.execute("ROLLBACK").await.unwrap();
    connection.close().await.unwrap();
}

#[tokio::test]
async fn backup_restore_cleanup_round_trip_preserves_old_quarantine_until_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let paths = paths(&temp);
    let connection = wal_database(&paths).await;
    backup::create_fixture(paths.clone()).await.unwrap();
    connection.close().await.unwrap();
    let manifest = {
        let namespace = super::fs::open_dir(&paths.namespace()).unwrap();
        backup::load_manifest(&namespace).unwrap()
    };
    let current = backup::source_set(&paths).unwrap().hash;
    let restored = restore::restore_fixture(&paths, manifest.snapshot_hash, current)
        .await
        .unwrap();
    assert!(restored.contains("result=committed"));
    assert!(paths.namespace().join("pre-restore.main").is_file());
    let cleaned = cleanup::cleanup_fixture(&paths, manifest.snapshot_hash)
        .unwrap_or_else(|error| panic!("cleanup={error} tree={:?}", tree_names(&paths.root)));
    assert!(cleaned.contains("result=cleaned"));
    assert!(!paths.namespace().exists());
    let mut current_db = SqliteConnection::connect(paths.database.to_str().unwrap())
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM evidence")
        .fetch_one(&mut current_db)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn wrong_hash_refuses_with_tree_invariance() {
    let temp = tempfile::tempdir().unwrap();
    let paths = paths(&temp);
    let connection = wal_database(&paths).await;
    backup::create_fixture(paths.clone()).await.unwrap();
    connection.close().await.unwrap();
    let before = tree(&paths.root);
    assert_eq!(cleanup::cleanup_fixture(&paths, [0; 32]), Err("manifest"));
    assert_eq!(tree(&paths.root), before);
}

#[tokio::test]
async fn backup_only_cleanup_is_hash_bound_and_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let paths = paths(&temp);
    let connection = wal_database(&paths).await;
    backup::create_fixture(paths.clone()).await.unwrap();
    connection.close().await.unwrap();
    let namespace = super::fs::open_dir(&paths.namespace()).unwrap();
    let expected = backup::load_manifest(&namespace).unwrap().snapshot_hash;
    drop(namespace);
    assert!(
        cleanup::cleanup_fixture(&paths, expected)
            .unwrap()
            .contains("result=cleaned")
    );
    assert!(
        cleanup::cleanup_fixture(&paths, expected)
            .unwrap()
            .contains("already-absent")
    );
}

#[tokio::test]
async fn preexisting_namespace_refuses_without_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let paths = paths(&temp);
    wal_database(&paths).await.close().await.unwrap();
    std::fs::create_dir(paths.namespace()).unwrap();
    std::fs::set_permissions(paths.namespace(), std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(paths.namespace().join("foreign"), b"outside").unwrap();
    let before = tree(&paths.root);
    assert_eq!(
        backup::create_fixture(paths.clone())
            .await
            .unwrap_err()
            .cause,
        "target"
    );
    assert_eq!(tree(&paths.root), before);
}

#[test]
fn retained_hash_rejects_hardlinks_and_sparse_oversize() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let directory = super::fs::open_dir(temp.path()).unwrap();
    std::fs::write(temp.path().join("leaf"), b"content").unwrap();
    std::fs::hard_link(temp.path().join("leaf"), temp.path().join("other")).unwrap();
    assert!(super::fs::open_leaf(&directory, "leaf", false).is_err());
    std::fs::remove_file(temp.path().join("other")).unwrap();
    let oversized = std::fs::File::create(temp.path().join("large")).unwrap();
    oversized.set_len(super::MAX_FILE + 1).unwrap();
    assert!(super::fs::open_leaf(&directory, "large", false).is_err());
}

#[test]
fn never_quarantined_main_is_preacknowledged_without_rename() {
    let temp = tempfile::tempdir().unwrap();
    let paths = paths(&temp);
    std::fs::write(&paths.database, b"original").unwrap();
    std::fs::create_dir(paths.namespace()).unwrap();
    std::fs::set_permissions(paths.namespace(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let root = super::fs::open_dir(&paths.root).unwrap();
    let namespace = super::fs::open_dir(&paths.namespace()).unwrap();
    let mut main = super::fs::open_leaf(&root, "state.db", false).unwrap();
    let (_, expected) = super::fs::hash(&mut main).unwrap();
    rollback::restore_shape_fixture(&root, &namespace, false, expected).unwrap();
    assert_eq!(std::fs::read(&paths.database).unwrap(), b"original");
    assert!(!paths.namespace().join("pre-restore.main").exists());
}

#[tokio::test]
async fn fixed_cli_rejects_overrides_and_duplicate_arguments() {
    let binary = std::ffi::OsString::from("bridge");
    let result = super::dispatch(vec![
        binary.clone(),
        "backup-isolated-t050-state".into(),
        "--root".into(),
        "/tmp".into(),
    ])
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!((result.status, result.cause), (3, "identity"));
    let result = super::dispatch(vec![
        binary,
        "cleanup-isolated-t050-backup".into(),
        "--expected-backup-sha256".into(),
        "00".repeat(32).into(),
        "--expected-backup-sha256".into(),
        "00".repeat(32).into(),
    ])
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!((result.status, result.cause), (3, "identity"));
}

fn tree(root: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let mut values = Vec::new();
    fn walk(root: &std::path::Path, at: &std::path::Path, out: &mut Vec<(String, Vec<u8>)>) {
        let mut entries = std::fs::read_dir(at)
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.push((
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                    std::fs::read(path).unwrap(),
                ));
            }
        }
    }
    walk(root, root, &mut values);
    values
}

fn tree_names(root: &std::path::Path) -> Vec<String> {
    let mut values = Vec::new();
    fn walk(root: &std::path::Path, at: &std::path::Path, out: &mut Vec<String>) {
        let mut entries = std::fs::read_dir(at)
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            out.push(
                path.strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            );
            if path.is_dir() {
                walk(root, &path, out);
            }
        }
    }
    walk(root, root, &mut values);
    values
}
