use guigu_agent_bridge::offline_identity::store_generation;

#[test]
fn fingerprint_detects_byte_changes_nul_tails_and_inode_replacement() {
    let root = tempfile::Builder::new()
        .prefix("r092-generation-")
        .tempdir_in("/tmp")
        .unwrap();
    let file = root.path().join("state");
    std::fs::write(&file, b"prefix\0original").unwrap();
    let original = store_generation(root.path()).unwrap();
    assert_eq!(original, store_generation(root.path()).unwrap());
    std::fs::write(&file, b"prefix\0modified").unwrap();
    let changed = store_generation(root.path()).unwrap();
    assert_ne!(original.inventory_digest, changed.inventory_digest);
    assert_eq!(original.locator_digest, changed.locator_digest);
    let replacement = root.path().join("replacement");
    std::fs::write(&replacement, b"prefix\0modified").unwrap();
    std::fs::rename(&replacement, &file).unwrap();
    assert_ne!(
        changed.inventory_digest,
        store_generation(root.path()).unwrap().inventory_digest
    );
}

#[test]
fn missing_empty_symlink_and_hardlinked_store_refuse() {
    let root = tempfile::Builder::new()
        .prefix("r092-generation-refuse-")
        .tempdir_in("/tmp")
        .unwrap();
    assert!(store_generation(&root.path().join("missing")).is_err());
    assert!(store_generation(root.path()).is_err());
    let file = root.path().join("state");
    std::fs::write(&file, b"synthetic").unwrap();
    let link = root.path().join("link");
    std::os::unix::fs::symlink(&file, &link).unwrap();
    assert!(store_generation(root.path()).is_err());
    std::fs::remove_file(&link).unwrap();
    std::fs::hard_link(&file, &link).unwrap();
    assert!(store_generation(root.path()).is_err());
}
