use super::publication::*;
use super::*;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;

struct Roots {
    _temp: tempfile::TempDir,
    artifacts: File,
    helpers: File,
    artifact_path: PathBuf,
    helper_path: PathBuf,
    uid: u32,
    gid: u32,
}

impl Roots {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let artifact_path = temp.path().join("artifacts");
        let helper_path = temp.path().join("helpers");
        std::fs::create_dir(&artifact_path).unwrap();
        std::fs::create_dir(&helper_path).unwrap();
        std::fs::set_permissions(&artifact_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(&helper_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        Self {
            artifacts: File::open(&artifact_path).unwrap(),
            helpers: File::open(&helper_path).unwrap(),
            artifact_path,
            helper_path,
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            _temp: temp,
        }
    }

    fn absent(&self) -> bool {
        !self.artifact_path.join("policy.v1").exists()
            && !self.artifact_path.join("artifacts.v1").exists()
            && !self.helper_path.join("t050-proc-proof").exists()
    }
}

fn set_test_capability(file: &File) -> Result<(), ()> {
    let value = b"present";
    if unsafe {
        libc::fsetxattr(
            file.as_raw_fd(),
            capability_name().as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    } != 0
    {
        return Err(());
    }
    Ok(())
}

fn test_capability_present(path: &Path) -> bool {
    let file = File::open(path).unwrap();
    (unsafe {
        libc::fgetxattr(
            file.as_raw_fd(),
            capability_name().as_ptr(),
            std::ptr::null_mut(),
            0,
        )
    }) >= 0
}

fn publish(roots: &Roots) -> Result<(), ()> {
    let mut transaction = PublicationTransaction::new(&roots.artifacts, &roots.helpers);
    let result = (|| {
        let policy = transaction.create(LeafKind::Policy, 0o644)?;
        transaction
            .file_mut(policy)?
            .write_all(b"policy")
            .map_err(|_| ())?;
        transaction.boundary("policy:after-write")?;
        transaction.sync_file(policy, "policy:after-file-fsync")?;
        transaction.promote(policy, roots.uid, roots.gid, 0o644)?;

        let helper = transaction.create(LeafKind::Helper, 0o750)?;
        transaction
            .file_mut(helper)?
            .write_all(b"helper")
            .map_err(|_| ())?;
        transaction.boundary("helper:after-copy")?;
        transaction.boundary("helper:after-fchown")?;
        transaction.boundary("helper:after-fchmod")?;
        set_test_capability(transaction.file_mut(helper)?)?;
        transaction.boundary("helper:after-capability")?;
        transaction.sync_file(helper, "helper:after-file-fsync")?;
        transaction.promote(helper, roots.uid, roots.gid, 0o750)?;
        transaction.boundary("helper:after-revalidation")?;
        transaction.sync_parent(LeafKind::Helper, "helper:after-parent-fsync")?;
        transaction.sync_parent(LeafKind::Policy, "artifacts:before-manifest-fsync")?;

        let manifest = transaction.create(LeafKind::Manifest, 0o644)?;
        transaction
            .file_mut(manifest)?
            .write_all(b"manifest")
            .map_err(|_| ())?;
        transaction.boundary("manifest:after-write")?;
        transaction.sync_file(manifest, "manifest:after-file-fsync")?;
        transaction.promote(manifest, roots.uid, roots.gid, 0o644)?;
        transaction.sync_parent(LeafKind::Manifest, "manifest:after-parent-fsync")?;
        transaction.commit(manifest)
    })();
    let cleanup = transaction.finish(result.is_ok());
    result.and(cleanup)
}

#[test]
fn every_publication_boundary_rolls_back_owned_leaves_in_reverse() {
    for fault in [
        "policy:after-create",
        "policy:after-write",
        "policy:after-file-fsync",
        "policy:after-metadata",
        "policy:after-promotion",
        "helper:after-create",
        "helper:after-copy",
        "helper:after-fchown",
        "helper:after-fchmod",
        "helper:after-capability",
        "helper:after-file-fsync",
        "helper:after-metadata",
        "helper:after-promotion",
        "helper:after-revalidation",
        "helper:after-parent-fsync",
        "artifacts:before-manifest-fsync",
        "manifest:after-create",
        "manifest:after-write",
        "manifest:after-file-fsync",
        "manifest:after-metadata",
        "manifest:after-promotion",
        "manifest:after-parent-fsync",
        "manifest:before-commit",
    ] {
        let roots = Roots::new();
        take_publication_events();
        set_publication_faults(Some(fault));
        assert!(publish(&roots).is_err(), "fault={fault}");
        set_publication_faults(None);
        assert!(roots.absent(), "fault={fault}");
        let cleanup = take_publication_events()
            .into_iter()
            .filter(|event| event.ends_with("cleanup-fsync"))
            .collect::<Vec<_>>();
        let mut rank = cleanup.iter().filter_map(|event| match event.as_str() {
            "manifest:cleanup-fsync" => Some(3),
            "helper:cleanup-fsync" => Some(2),
            "policy:cleanup-fsync" => Some(1),
            _ => None,
        });
        if let Some(mut previous) = rank.next() {
            for current in rank {
                assert!(previous > current, "fault={fault} cleanup={cleanup:?}");
                previous = current;
            }
        }
    }
}

#[test]
fn successful_manifest_commit_disarms_transaction() {
    let roots = Roots::new();
    set_publication_faults(None);
    assert!(publish(&roots).is_ok());
    assert!(roots.artifact_path.join("policy.v1").is_file());
    assert!(roots.artifact_path.join("artifacts.v1").is_file());
    assert!(roots.helper_path.join("t050-proc-proof").is_file());
}

#[test]
fn replacement_ambiguity_preserves_foreign_leaf_and_stops_reverse_cleanup() {
    for promote_helper in [false, true] {
        let roots = Roots::new();
        let mut transaction = PublicationTransaction::new(&roots.artifacts, &roots.helpers);
        let policy = transaction.create(LeafKind::Policy, 0o644).unwrap();
        transaction
            .file_mut(policy)
            .unwrap()
            .write_all(b"owned-policy")
            .unwrap();
        transaction
            .promote(policy, roots.uid, roots.gid, 0o644)
            .unwrap();
        let helper = transaction.create(LeafKind::Helper, 0o750).unwrap();
        transaction
            .file_mut(helper)
            .unwrap()
            .write_all(b"owned-helper")
            .unwrap();
        if promote_helper {
            transaction
                .promote(helper, roots.uid, roots.gid, 0o750)
                .unwrap();
        }

        let helper_path = roots.helper_path.join("t050-proc-proof");
        let displaced = roots.helper_path.join("owned-displaced");
        std::fs::rename(&helper_path, &displaced).unwrap();
        std::fs::write(&helper_path, b"foreign-helper").unwrap();
        std::fs::set_permissions(&helper_path, std::fs::Permissions::from_mode(0o750)).unwrap();
        set_test_capability(&File::open(&helper_path).unwrap()).unwrap();

        assert!(transaction.finish(false).is_err());
        assert_eq!(std::fs::read(&helper_path).unwrap(), b"foreign-helper");
        assert!(test_capability_present(&helper_path));
        assert_eq!(std::fs::read(&displaced).unwrap(), b"owned-helper");
        assert_eq!(
            std::fs::read(roots.artifact_path.join("policy.v1")).unwrap(),
            b"owned-policy"
        );
    }
}

#[test]
fn cleanup_faults_are_reported_and_never_continue_to_older_leaves() {
    for fault in [
        "helper:cleanup-inspect",
        "helper:cleanup-capability",
        "helper:cleanup-unlink",
        "helper:cleanup-fsync",
    ] {
        let roots = Roots::new();
        let mut transaction = PublicationTransaction::new(&roots.artifacts, &roots.helpers);
        let policy = transaction.create(LeafKind::Policy, 0o644).unwrap();
        transaction
            .file_mut(policy)
            .unwrap()
            .write_all(b"policy")
            .unwrap();
        transaction
            .promote(policy, roots.uid, roots.gid, 0o644)
            .unwrap();
        let helper = transaction.create(LeafKind::Helper, 0o750).unwrap();
        transaction
            .file_mut(helper)
            .unwrap()
            .write_all(b"helper")
            .unwrap();
        transaction
            .promote(helper, roots.uid, roots.gid, 0o750)
            .unwrap();
        set_test_capability(transaction.file_mut(helper).unwrap()).unwrap();

        set_publication_faults(Some(fault));
        assert!(transaction.finish(false).is_err(), "fault={fault}");
        set_publication_faults(None);
        assert!(
            roots.artifact_path.join("policy.v1").is_file(),
            "fault={fault}"
        );
        let helper_path = roots.helper_path.join("t050-proc-proof");
        if fault != "helper:cleanup-fsync" {
            assert!(helper_path.exists(), "fault={fault}");
            assert_eq!(
                test_capability_present(&helper_path),
                fault == "helper:cleanup-inspect",
                "fault={fault}"
            );
        } else {
            assert!(!helper_path.exists(), "fault={fault}");
        }
    }
}
