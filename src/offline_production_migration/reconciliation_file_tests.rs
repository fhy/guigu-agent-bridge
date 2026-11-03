use super::*;
use std::os::unix::fs::symlink;
use std::path::PathBuf;

fn fixture() -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("protected");
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o750)).unwrap();
    let path = directory.join("record.json");
    fs::write(&path, b"original").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    (root, path)
}

#[test]
fn unchanged_file_is_read() {
    let (_root, path) = fixture();
    assert_eq!(protected_bytes(&path).unwrap(), b"original");
}

#[test]
fn final_file_replacement_is_refused() {
    let (_root, path) = fixture();
    let result = protected_bytes_checked(&path, || {
        fs::rename(&path, path.with_extension("old")).unwrap();
        fs::write(&path, b"original").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    });
    assert!(matches!(result, Err(Failure::ManualRequired)));
}

#[test]
fn ancestor_replacement_is_refused() {
    let (root, path) = fixture();
    let result = protected_bytes_checked(&path, || {
        let parent = path.parent().unwrap();
        fs::rename(parent, root.path().join("retained")).unwrap();
        fs::create_dir(parent).unwrap();
        fs::write(&path, b"original").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    });
    assert!(matches!(result, Err(Failure::ManualRequired)));
}

#[test]
fn final_symlink_replacement_is_refused() {
    let (_root, path) = fixture();
    let result = protected_bytes_checked(&path, || {
        let retained = path.with_extension("old");
        fs::rename(&path, &retained).unwrap();
        symlink(retained, &path).unwrap();
    });
    assert!(matches!(result, Err(Failure::ManualRequired)));
}

#[test]
fn same_length_in_place_change_is_refused() {
    let (_root, path) = fixture();
    let result = protected_bytes_checked(&path, || {
        fs::write(&path, b"modified").unwrap();
    });
    assert!(matches!(result, Err(Failure::ManualRequired)));
}

#[test]
fn ancestor_mode_change_is_refused() {
    let (_root, path) = fixture();
    let result = protected_bytes_checked(&path, || {
        fs::set_permissions(path.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
    });
    assert!(matches!(result, Err(Failure::ManualRequired)));
}

#[test]
fn ancestor_symlink_is_refused() {
    let (root, path) = fixture();
    let retained = root.path().join("retained");
    fs::rename(path.parent().unwrap(), &retained).unwrap();
    symlink(retained, path.parent().unwrap()).unwrap();
    assert!(matches!(
        protected_bytes(&path),
        Err(Failure::ManualRequired)
    ));
}

#[test]
fn grant_policy_does_not_trust_user_owned_ancestors() {
    let (_root, path) = fixture();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(matches!(
        protected_bytes_with_policy(&path, true, || {}),
        Err(Failure::ManualRequired)
    ));
}

#[test]
fn publication_is_no_replace_and_idempotent() {
    let (root, _) = fixture();
    let path = root.path().join("protected/published.json");
    publish(&path, &vec!["original"]).unwrap();
    let identity = fs::metadata(&path).unwrap().ino();
    publish(&path, &vec!["original"]).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().ino(), identity);
    assert!(matches!(
        publish(&path, &vec!["replacement"]),
        Err(Failure::ManualRequired)
    ));
    assert_eq!(fs::read(&path).unwrap(), br#"["original"]"#);
}

#[test]
fn publication_refuses_destination_created_before_rename() {
    let (root, _) = fixture();
    let path = root.path().join("protected/published.json");
    let result = publish_checked(&path, &vec!["original"], || {
        fs::write(&path, b"competing").unwrap();
    });
    assert!(matches!(result, Err(Failure::ManualRequired)));
    assert_eq!(fs::read(&path).unwrap(), b"competing");
}

#[test]
fn publication_refuses_replaced_parent_without_publishing() {
    let (root, _) = fixture();
    let path = root.path().join("protected/published.json");
    let retained = root.path().join("retained");
    let result = publish_checked(&path, &vec!["original"], || {
        fs::rename(path.parent().unwrap(), &retained).unwrap();
        fs::create_dir(path.parent().unwrap()).unwrap();
    });
    assert!(matches!(result, Err(Failure::ManualRequired)));
    assert!(!path.exists());
    assert!(!retained.join("published.json").exists());
}
