use std::os::unix::fs::PermissionsExt;

use super::{codec::LOCK, fault, fs, fs::OwnershipFailure};

fn root() -> (tempfile::TempDir, std::fs::File) {
    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let root = fs::open_dir(temp.path()).unwrap();
    (temp, root)
}

#[test]
fn namespace_pre_identity_boundaries_are_ambiguous_and_preserve_residue() {
    let cases = [
        "after-namespace-mkdir",
        "after-namespace-reopen",
        "before-namespace-metadata",
    ];
    for (index, point) in cases.into_iter().enumerate() {
        let (_temp, root) = root();
        let name = format!("namespace-{index}");
        fault::once(point);
        assert_eq!(
            fs::create_namespace_fixture(&root, &name).map(|_| ()),
            Err(OwnershipFailure::Ambiguous),
            "{point}"
        );
        assert!(!fs::leaf_absent(&root, &name).unwrap(), "{point}");
    }
}

#[test]
fn namespace_retained_identity_boundaries_clean_exact_owned_inode() {
    let cases = [
        "after-namespace-metadata",
        "after-namespace-ownership",
        "after-namespace-file-fsync",
        "after-namespace-parent-fsync",
    ];
    for (index, point) in cases.into_iter().enumerate() {
        let (_temp, root) = root();
        let name = format!("namespace-{index}");
        fault::once(point);
        assert_eq!(
            fs::create_namespace_fixture(&root, &name).map(|_| ()),
            Err(OwnershipFailure::Clean),
            "{point}"
        );
        assert!(fs::leaf_absent(&root, &name).unwrap(), "{point}");
    }
}

#[test]
fn namespace_replacement_is_ambiguous_and_foreign_tree_is_invariant() {
    let (_temp, root) = root();
    let root_path = std::path::PathBuf::from(format!(
        "/proc/self/fd/{}",
        std::os::fd::AsRawFd::as_raw_fd(&root)
    ));
    let fixed = root_path.join("namespace");
    let saved = root_path.join("owned");
    fault::action("after-namespace-metadata", move || {
        std::fs::rename(&fixed, &saved).unwrap();
        std::fs::create_dir(&fixed).unwrap();
        std::fs::set_permissions(&fixed, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(fixed.join("foreign"), b"unchanged").unwrap();
    });
    fault::once("after-namespace-metadata");
    assert_eq!(
        fs::create_namespace_fixture(&root, "namespace").map(|_| ()),
        Err(OwnershipFailure::Ambiguous)
    );
    assert_eq!(
        std::fs::read(root_path.join("namespace/foreign")).unwrap(),
        b"unchanged"
    );
}

fn namespace() -> (tempfile::TempDir, std::fs::File) {
    let (temp, root) = root();
    let (namespace, _) = fs::create_namespace_fixture(&root, "namespace").unwrap();
    (temp, namespace)
}

#[test]
fn lock_retained_identity_boundaries_clean_exact_owned_inode() {
    let cases = [
        "after-lock-create",
        "after-lock-flock",
        "after-lock-file-fsync",
        "after-lock-parent-fsync",
    ];
    for point in cases {
        let (_temp, namespace) = namespace();
        fault::once(point);
        assert_eq!(
            fs::OperationLock::acquire(&namespace).map(|_| ()),
            Err(OwnershipFailure::Clean),
            "{point}"
        );
        assert!(fs::leaf_absent(&namespace, LOCK).unwrap(), "{point}");
    }
}

#[test]
fn lock_first_metadata_failure_is_ambiguous_and_preserves_residue() {
    let (_temp, namespace) = namespace();
    fault::once("before-lock-metadata");
    assert_eq!(
        fs::OperationLock::acquire(&namespace).map(|_| ()),
        Err(OwnershipFailure::Ambiguous)
    );
    assert!(!fs::leaf_absent(&namespace, LOCK).unwrap());
}

#[test]
fn lock_replacement_is_ambiguous_and_foreign_leaf_is_invariant() {
    let (_temp, namespace) = namespace();
    let dir = std::path::PathBuf::from(format!(
        "/proc/self/fd/{}",
        std::os::fd::AsRawFd::as_raw_fd(&namespace)
    ));
    let fixed = dir.join(LOCK);
    let saved = dir.join("owned-lock");
    fault::action("after-lock-create", move || {
        std::fs::rename(&fixed, &saved).unwrap();
        std::fs::write(&fixed, b"foreign-lock").unwrap();
        std::fs::set_permissions(&fixed, std::fs::Permissions::from_mode(0o600)).unwrap();
    });
    fault::once("after-lock-create");
    assert_eq!(
        fs::OperationLock::acquire(&namespace).map(|_| ()),
        Err(OwnershipFailure::Ambiguous)
    );
    assert_eq!(std::fs::read(dir.join(LOCK)).unwrap(), b"foreign-lock");
}
