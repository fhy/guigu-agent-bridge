use std::os::unix::fs::PermissionsExt;

use super::{
    Paths,
    cleanup_codec::{Kind, Phase, Record as CleanupRecord},
    cleanup_journal,
    codec::{Identity, RestorePhase, RestoreRecord},
    fs, journal, restore,
};

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
fn cleanup_record(namespace: Identity, leaf: Identity) -> CleanupRecord {
    CleanupRecord {
        sequence: 0,
        predecessor: [0; 32],
        kind: Kind::Backup,
        phase: Phase::Ready,
        bitmap: 0,
        expected_snapshot: [1; 32],
        manifest_digest: [2; 32],
        namespace,
        snapshot: leaf,
        manifest: leaf,
        old_main: None,
        old_wal: None,
        old_shm: None,
    }
}
fn write(path: &std::path::Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn forward_quarantine_adopts_before_and_after_shapes_and_rejects_ambiguity() {
    for after in [false, true] {
        let temp = tempfile::TempDir::new().unwrap();
        let paths = paths(&temp);
        let ns = paths.root.join("ns");
        std::fs::create_dir(&ns).unwrap();
        std::fs::set_permissions(&ns, std::fs::Permissions::from_mode(0o700)).unwrap();
        write(&paths.database, b"old");
        let root = fs::open_dir(&paths.root).unwrap();
        let namespace = fs::open_dir(&ns).unwrap();
        let mut file = fs::open_leaf(&root, "state.db", false).unwrap();
        let (_, id) = fs::hash(&mut file).unwrap();
        let mut record = restore_record(id);
        journal::initial(&namespace, &record).unwrap();
        restore::next(&namespace, &mut record, RestorePhase::IntentMain, 0).unwrap();
        if after {
            fs::rename_noreplace(&root, "state.db", &namespace, "pre-restore.main").unwrap();
            root.sync_all().unwrap();
            namespace.sync_all().unwrap();
        }
        restore::quarantine_fixture(
            &paths,
            &namespace,
            &mut record,
            "state.db",
            "pre-restore.main",
            RestorePhase::IntentMain,
            restore::MAIN,
            id,
        )
        .unwrap();
        assert_eq!(record.bitmap & restore::MAIN, restore::MAIN);
    }
    let temp = tempfile::TempDir::new().unwrap();
    let paths = paths(&temp);
    let ns = paths.root.join("ns");
    std::fs::create_dir(&ns).unwrap();
    std::fs::set_permissions(&ns, std::fs::Permissions::from_mode(0o700)).unwrap();
    write(&paths.database, b"old");
    write(&ns.join("pre-restore.main"), b"other");
    let root = fs::open_dir(&paths.root).unwrap();
    let namespace = fs::open_dir(&ns).unwrap();
    let mut f = fs::open_leaf(&root, "state.db", false).unwrap();
    let (_, id) = fs::hash(&mut f).unwrap();
    let mut record = restore_record(id);
    journal::initial(&namespace, &record).unwrap();
    assert!(
        restore::quarantine_fixture(
            &paths,
            &namespace,
            &mut record,
            "state.db",
            "pre-restore.main",
            RestorePhase::IntentMain,
            restore::MAIN,
            id
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&paths.database).unwrap(), b"old");
    assert_eq!(
        std::fs::read(ns.join("pre-restore.main")).unwrap(),
        b"other"
    );
}

#[test]
fn cleanup_codec_and_three_leaf_adoption_are_canonical() {
    let temp = tempfile::TempDir::new().unwrap();
    let paths = paths(&temp);
    let namespace = fs::open_dir(&paths.root).unwrap();
    write(&paths.database, b"x");
    let mut f = fs::open_leaf(&namespace, "state.db", false).unwrap();
    let (_, leaf) = fs::hash(&mut f).unwrap();
    let ns = fs::dir_identity(&namespace.metadata().unwrap()).unwrap();
    let mut record = cleanup_record(ns, leaf);
    let bytes = record.encode();
    assert_eq!(CleanupRecord::decode(&bytes).unwrap(), record);
    cleanup_journal::initial(&namespace, &record).unwrap();
    cleanup_journal::transition(&namespace, &mut record, Phase::IntentManifest, 0).unwrap();
    assert_eq!(
        cleanup_journal::adopt(&namespace, Kind::Backup).unwrap(),
        Some(record)
    );
}

#[test]
fn source_set_rejects_sidecar_owner_mode_drift_without_content_change() {
    let temp = tempfile::TempDir::new().unwrap();
    let paths = paths(&temp);
    write(&paths.database, b"main");
    write(&paths.root.join("state.db-wal"), b"wal");
    std::fs::set_permissions(
        paths.root.join("state.db-wal"),
        std::fs::Permissions::from_mode(0o640),
    )
    .unwrap();
    let before = std::fs::read(paths.root.join("state.db-wal")).unwrap();
    assert!(super::backup::source_set(&paths).is_err());
    assert_eq!(
        std::fs::read(paths.root.join("state.db-wal")).unwrap(),
        before
    );
}

#[test]
fn reverse_restore_adopts_pre_and_post_rename_and_rejects_both_present() {
    for already_restored in [false, true] {
        let temp = tempfile::TempDir::new().unwrap();
        let paths = paths(&temp);
        let ns = paths.root.join("ns");
        std::fs::create_dir(&ns).unwrap();
        std::fs::set_permissions(&ns, std::fs::Permissions::from_mode(0o700)).unwrap();
        write(&ns.join("pre-restore.main"), b"old");
        let root = fs::open_dir(&paths.root).unwrap();
        let namespace = fs::open_dir(&ns).unwrap();
        let mut file = fs::open_leaf(&namespace, "pre-restore.main", false).unwrap();
        let (_, identity) = fs::hash(&mut file).unwrap();
        if already_restored {
            fs::rename_noreplace(&namespace, "pre-restore.main", &root, "state.db").unwrap();
            root.sync_all().unwrap();
        }
        super::rollback::restore_shape_fixture(&root, &namespace, true, identity).unwrap();
        assert_eq!(std::fs::read(&paths.database).unwrap(), b"old");
        assert!(fs::leaf_absent(&namespace, "pre-restore.main").unwrap());
    }

    let temp = tempfile::TempDir::new().unwrap();
    let paths = paths(&temp);
    let ns = paths.root.join("ns");
    std::fs::create_dir(&ns).unwrap();
    std::fs::set_permissions(&ns, std::fs::Permissions::from_mode(0o700)).unwrap();
    write(&paths.database, b"old");
    write(&ns.join("pre-restore.main"), b"old");
    let root = fs::open_dir(&paths.root).unwrap();
    let namespace = fs::open_dir(&ns).unwrap();
    let mut file = fs::open_leaf(&root, "state.db", false).unwrap();
    let (_, identity) = fs::hash(&mut file).unwrap();
    assert!(super::rollback::restore_shape_fixture(&root, &namespace, true, identity).is_err());
    assert_eq!(std::fs::read(&paths.database).unwrap(), b"old");
    assert_eq!(std::fs::read(ns.join("pre-restore.main")).unwrap(), b"old");
}
