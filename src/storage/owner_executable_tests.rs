use super::*;
use std::{fs, os::unix::fs::PermissionsExt};

fn fixture() -> (tempfile::TempDir, std::path::PathBuf, Executable) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("executable");
    fs::write(&path, b"initial executable contents").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    let pinned = Executable::pin(File::open(&path).unwrap(), Instant::now() + BUDGET).unwrap();
    // ext4 timestamps can share one kernel tick, even with nanosecond fields.
    std::thread::sleep(Duration::from_millis(25));
    (root, path, pinned)
}

#[test]
fn repeated_ctime_only_changes_revalidate_without_adopting_content() {
    let (_root, path, mut pinned) = fixture();
    let original = pinned.digest;
    for _ in 0..3 {
        let previous = pinned.stamp.clone();
        std::thread::sleep(Duration::from_millis(25));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        assert_ne!(previous.changed, Stamp::read(&pinned.file).unwrap().changed);
        assert!(
            pinned
                .validate(File::open(&path).unwrap(), Instant::now() + BUDGET)
                .is_some()
        );
        assert_eq!(original, pinned.digest);
        // Cached path does not hash, so an expired hash deadline is harmless.
        assert!(
            pinned
                .validate(File::open(&path).unwrap(), Instant::now())
                .is_some()
        );
    }
}

#[test]
fn same_size_same_mtime_content_mutation_is_rejected_and_not_cached() {
    let (_root, path, mut pinned) = fixture();
    let mtime = fs::metadata(&path).unwrap().modified().unwrap();
    let original = pinned.stamp.clone();
    fs::write(&path, b"changed executable contents").unwrap();
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(mtime))
        .unwrap();
    assert!(
        pinned
            .validate(File::open(&path).unwrap(), Instant::now() + BUDGET)
            .is_none()
    );
    assert_eq!(pinned.stamp, original);
}

#[test]
fn permission_and_attribute_changes_refuse_without_rebaselining() {
    let (_root, path, mut pinned) = fixture();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o750)).unwrap();
    assert!(
        pinned
            .validate(File::open(&path).unwrap(), Instant::now() + BUDGET)
            .is_none()
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    let key = c"user.identity-test";
    assert_eq!(
        unsafe {
            libc::fsetxattr(
                pinned.file.as_raw_fd(),
                key.as_ptr(),
                b"changed".as_ptr().cast(),
                7,
                0,
            )
        },
        0
    );
    assert!(
        pinned
            .validate(File::open(&path).unwrap(), Instant::now() + BUDGET)
            .is_none()
    );
}

#[test]
fn path_replacement_does_not_replace_pinned_running_object() {
    let (root, path, mut pinned) = fixture();
    let moved = root.path().join("moved");
    fs::rename(&path, &moved).unwrap();
    fs::write(&path, b"initial executable contents").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        pinned
            .validate(File::open(&path).unwrap(), Instant::now() + BUDGET)
            .is_none()
    );
    // /proc/self/exe still refers to the old object, as represented by moved here.
    assert!(
        pinned
            .validate(File::open(&moved).unwrap(), Instant::now() + BUDGET)
            .is_some()
    );
    fs::remove_file(moved).unwrap();
    assert!(
        pinned
            .validate(pinned.file.try_clone().unwrap(), Instant::now() + BUDGET)
            .is_some()
    );
}

#[test]
fn raced_or_unreadable_capture_and_exhausted_hash_budget_refuse() {
    let (_root, path, mut pinned) = fixture();
    let before = pinned.stamp.clone();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    // The read started under a prior stamp; changing it before completion is a race.
    assert!(digest(&pinned.file, &before, Instant::now() + BUDGET).is_none());
    assert!(
        pinned
            .validate(File::open(&path).unwrap(), Instant::now())
            .is_none()
    );
    let write_only = File::options().write(true).open(&path).unwrap();
    assert!(Executable::pin(write_only, Instant::now() + BUDGET).is_none());
    let mut changed = before.clone();
    changed.authority.0 = changed.authority.0.wrapping_add(1);
    assert!(!before.same_identity(&changed));
    changed = before.clone();
    changed.authority.1 = changed.authority.1.wrapping_add(1);
    assert!(!before.same_identity(&changed));
}

#[tokio::test]
async fn actual_running_object_uses_legacy_format_and_domain_drift_refuses() {
    let mut owner = RuntimeExecutable::capture().await.unwrap();
    let fingerprint = owner.fingerprint().to_owned();
    assert_eq!(
        Some(fingerprint.clone()),
        crate::storage::RuntimeOwnerEvidence::fingerprint()
    );
    assert_eq!(
        owner.validate().await.as_deref(),
        Some(fingerprint.as_str())
    );
    owner.cgroup.push_str("/drift");
    assert!(owner.validate().await.is_none());
    assert!(owner.validate().await.is_none());
}

#[tokio::test]
async fn cancelled_validation_does_not_lose_shared_stop_identity() {
    let owner = RuntimeExecutable::capture().await.unwrap();
    let sibling = owner.clone();
    assert!(owner.same_cache(&sibling));
    let lock = owner.inner.lock().unwrap();
    let task = tokio::spawn(async move { sibling.validate().await });
    tokio::task::yield_now().await;
    task.abort();
    drop(lock);
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(owner.validate().await.as_deref(), Some(owner.fingerprint()));
}
