use std::os::unix::fs::PermissionsExt;

use sqlx::Connection;

use super::{
    Paths, backup, cleanup,
    codec::{Identity, RestorePhase, RestoreRecord},
    fault, fs, restore, rollback,
};

fn fixture_paths(temp: &tempfile::TempDir) -> Paths {
    let root = temp.path().join("state");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    Paths {
        database: root.join("state.db"),
        root,
        config: temp.path().join("config.toml"),
    }
}

async fn wal_database(paths: &Paths) -> sqlx::SqliteConnection {
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
async fn real_wal_replacement_at_each_sqlite_observation_refuses() {
    for point in [
        "sqlite-s1-open",
        "sqlite-s3-transaction",
        "sqlite-s5-copy",
        "sqlite-s7-close",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let paths = fixture_paths(&temp);
        let connection = wal_database(&paths).await;
        let wal = paths.root.join("state.db-wal");
        assert!(wal.is_file());
        let saved = paths.root.join("saved-wal");
        let replacement = b"replacement-wal".to_vec();
        let wal_for_action = wal.clone();
        fault::action(point, move || {
            std::fs::rename(&wal_for_action, &saved).unwrap();
            std::fs::write(&wal_for_action, &replacement).unwrap();
            std::fs::set_permissions(&wal_for_action, std::fs::Permissions::from_mode(0o600))
                .unwrap();
        });
        let error = backup::create_fixture(paths.clone()).await.unwrap_err();
        assert!(
            matches!(error.cause, "identity" | "sqlite" | "cleanup"),
            "{point}: {}",
            error.cause
        );
        assert_eq!(std::fs::read(&wal).unwrap(), b"replacement-wal");
        connection.close().await.unwrap_or(());
    }
}

#[tokio::test]
async fn namespace_lock_and_publication_faults_have_checked_outcomes() {
    for point in [
        "after-namespace-mkdir",
        "after-namespace-reopen",
        "after-namespace-metadata",
        "after-namespace-file-fsync",
        "after-lock-create",
        "after-lock-flock",
        "after-lock-parent-fsync",
        "after-publication-create",
        "after-leaf-write",
        "after-leaf-fsync",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let paths = fixture_paths(&temp);
        let connection = wal_database(&paths).await;
        fault::once(point);
        assert!(
            backup::create_fixture(paths.clone()).await.is_err(),
            "{point}"
        );
        if paths.namespace().exists() {
            let namespace = fs::open_dir(&paths.namespace()).unwrap();
            assert!(
                fs::leaf_absent(&namespace, super::codec::MANIFEST).unwrap(),
                "{point} published a manifest"
            );
        }
        connection.close().await.unwrap();
    }
}

#[tokio::test]
async fn cleanup_refuses_snapshot_and_lock_replacements_without_deleting_them() {
    for replace_lock in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let paths = fixture_paths(&temp);
        let connection = wal_database(&paths).await;
        backup::create_fixture(paths.clone()).await.unwrap();
        connection.close().await.unwrap();
        let namespace = fs::open_dir(&paths.namespace()).unwrap();
        let expected = backup::load_manifest(&namespace).unwrap().snapshot_hash;
        drop(namespace);
        let name = if replace_lock {
            super::codec::LOCK
        } else {
            super::codec::SNAPSHOT
        };
        let leaf = paths.namespace().join(name);
        let saved = paths
            .namespace()
            .join(format!("saved-{}", name.replace('.', "-")));
        let point = if replace_lock {
            "cleanup-before-lock-delete"
        } else {
            "cleanup-before-leaf-delete"
        };
        fault::action(point, move || {
            std::fs::rename(&leaf, saved).unwrap();
            std::fs::write(&leaf, b"foreign-replacement").unwrap();
            std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o600)).unwrap();
        });
        assert_eq!(
            cleanup::cleanup_fixture(&paths, expected),
            Err("cleanup"),
            "replace_lock={replace_lock}"
        );
        assert_eq!(
            std::fs::read(paths.namespace().join(name)).unwrap(),
            b"foreign-replacement"
        );
    }
}

#[tokio::test]
async fn first_metadata_replacements_are_never_cleanup_authority() {
    let temp = tempfile::tempdir().unwrap();
    let paths = fixture_paths(&temp);
    let connection = wal_database(&paths).await;
    let fixed = paths.namespace();
    let saved = paths.root.join("saved-namespace");
    fault::action("before-namespace-metadata", move || {
        std::fs::rename(&fixed, saved).unwrap();
        std::fs::create_dir(&fixed).unwrap();
        std::fs::set_permissions(&fixed, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(fixed.join("foreign"), b"namespace-replacement").unwrap();
    });
    assert!(backup::create_fixture(paths.clone()).await.is_err());
    assert_eq!(
        std::fs::read(paths.namespace().join("foreign")).unwrap(),
        b"namespace-replacement"
    );
    connection.close().await.unwrap();

    let temp = tempfile::tempdir().unwrap();
    let paths = fixture_paths(&temp);
    let connection = wal_database(&paths).await;
    let namespace = paths.namespace();
    fault::action("before-publication-metadata", move || {
        let entry = std::fs::read_dir(&namespace)
            .unwrap()
            .map(Result::unwrap)
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("snapshot.tmp.")
            })
            .unwrap();
        let leaf = entry.path();
        std::fs::rename(&leaf, namespace.join("saved-publication")).unwrap();
        std::fs::write(&leaf, b"publication-replacement").unwrap();
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o600)).unwrap();
    });
    assert!(backup::create_fixture(paths.clone()).await.is_err());
    let replacement = std::fs::read_dir(paths.namespace())
        .unwrap()
        .map(Result::unwrap)
        .find(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("snapshot.tmp.")
        })
        .unwrap();
    assert_eq!(
        std::fs::read(replacement.path()).unwrap(),
        b"publication-replacement"
    );
    connection.close().await.unwrap();
}

fn restore_record(identity: Identity) -> RestoreRecord {
    RestoreRecord {
        sequence: 0,
        predecessor: [0; 32],
        manifest: [1; 32],
        snapshot: [2; 32],
        expected_current: [3; 32],
        phase: RestorePhase::Prepared,
        bitmap: 0,
        original_main: identity,
        original_wal: None,
        original_shm: None,
        replacement: None,
    }
}

#[test]
fn real_rename_and_fsync_faults_are_adopted_forward_and_reverse() {
    let components = [
        (
            "state.db-shm",
            "pre-restore.shm",
            RestorePhase::IntentShm,
            restore::SHM,
        ),
        (
            "state.db-wal",
            "pre-restore.wal",
            RestorePhase::IntentWal,
            restore::WAL,
        ),
        (
            "state.db",
            "pre-restore.main",
            RestorePhase::IntentMain,
            restore::MAIN,
        ),
    ];
    for (fixed, quarantine, phase, bit) in components {
        for point in [
            "restore-after-quarantine-rename",
            "restore-after-quarantine-fsync",
            "restore-before-quarantine-ack",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let paths = fixture_paths(&temp);
            std::fs::write(paths.root.join(fixed), b"original").unwrap();
            std::fs::set_permissions(
                paths.root.join(fixed),
                std::fs::Permissions::from_mode(0o600),
            )
            .unwrap();
            let namespace_path = paths.root.join("ns");
            std::fs::create_dir(&namespace_path).unwrap();
            std::fs::set_permissions(&namespace_path, std::fs::Permissions::from_mode(0o700))
                .unwrap();
            let root = fs::open_dir(&paths.root).unwrap();
            let namespace = fs::open_dir(&namespace_path).unwrap();
            let mut main = fs::open_leaf(&root, fixed, false).unwrap();
            let (_, identity) = fs::hash(&mut main).unwrap();
            let mut record = restore_record(identity);
            super::journal::initial(&namespace, &record).unwrap();
            fault::once(point);
            assert!(
                restore::quarantine_fixture(
                    &paths,
                    &namespace,
                    &mut record,
                    restore::QuarantineFixture {
                        fixed,
                        quarantine,
                        phase,
                        bit,
                        expected: identity,
                    },
                )
                .is_err()
            );
            restore::quarantine_fixture(
                &paths,
                &namespace,
                &mut record,
                restore::QuarantineFixture {
                    fixed,
                    quarantine,
                    phase,
                    bit,
                    expected: identity,
                },
            )
            .unwrap();
            assert_eq!(
                std::fs::read(namespace_path.join(quarantine)).unwrap(),
                b"original"
            );
        }
    }
    for (fixed, quarantine, _, _) in components {
        for point in [
            "rollback-after-restore-rename",
            "rollback-after-restore-fsync",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let paths = fixture_paths(&temp);
            let namespace_path = paths.root.join("ns");
            std::fs::create_dir(&namespace_path).unwrap();
            std::fs::set_permissions(&namespace_path, std::fs::Permissions::from_mode(0o700))
                .unwrap();
            std::fs::write(namespace_path.join(quarantine), b"original").unwrap();
            std::fs::set_permissions(
                namespace_path.join(quarantine),
                std::fs::Permissions::from_mode(0o600),
            )
            .unwrap();
            let root = fs::open_dir(&paths.root).unwrap();
            let namespace = fs::open_dir(&namespace_path).unwrap();
            let mut leaf = fs::open_leaf(&namespace, quarantine, false).unwrap();
            let (_, identity) = fs::hash(&mut leaf).unwrap();
            fault::once(point);
            assert!(
                rollback::restore_shape_named_fixture(
                    &root, &namespace, fixed, quarantine, true, identity
                )
                .is_err()
            );
            rollback::restore_shape_named_fixture(
                &root, &namespace, fixed, quarantine, true, identity,
            )
            .unwrap();
            assert_eq!(std::fs::read(paths.root.join(fixed)).unwrap(), b"original");
        }
    }
}

#[tokio::test]
async fn committed_restore_cleanup_rejects_journal_and_lock_replacements() {
    for replace_lock in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let paths = fixture_paths(&temp);
        let connection = wal_database(&paths).await;
        backup::create_fixture(paths.clone()).await.unwrap();
        connection.close().await.unwrap();
        let namespace = fs::open_dir(&paths.namespace()).unwrap();
        let manifest = backup::load_manifest(&namespace).unwrap();
        drop(namespace);
        let current = backup::source_set(&paths).unwrap().hash;
        restore::restore_fixture(&paths, manifest.snapshot_hash, current)
            .await
            .unwrap();
        let name = if replace_lock {
            super::codec::LOCK
        } else {
            super::codec::RESTORE_JOURNAL
        };
        let leaf = paths.namespace().join(name);
        let saved = paths
            .namespace()
            .join(format!("saved-restore-{replace_lock}"));
        let point = if replace_lock {
            "cleanup-before-lock-delete"
        } else {
            "cleanup-before-restore-journal-delete"
        };
        fault::action(point, move || {
            std::fs::rename(&leaf, saved).unwrap();
            std::fs::write(&leaf, b"foreign-restore-replacement").unwrap();
            std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o600)).unwrap();
        });
        assert!(cleanup::cleanup_fixture(&paths, manifest.snapshot_hash).is_err());
        assert_eq!(
            std::fs::read(paths.namespace().join(name)).unwrap(),
            b"foreign-restore-replacement"
        );
    }
}
