use super::*;
use std::os::unix::fs::MetadataExt;

#[cfg(test)]
thread_local! {
    static PUBLICATION_FAULTS: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    static PUBLICATION_EVENTS: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LeafKind {
    Policy,
    Helper,
    Manifest,
}

impl LeafKind {
    fn name(self) -> &'static std::ffi::CStr {
        match self {
            Self::Policy => c"policy.v1",
            Self::Helper => c"t050-proc-proof",
            Self::Manifest => c"artifacts.v1",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Policy => "policy",
            Self::Helper => "helper",
            Self::Manifest => "manifest",
        }
    }
}

#[derive(Clone, Copy)]
struct LeafIdentity {
    device: u64,
    inode: u64,
}

impl LeafIdentity {
    fn from(metadata: &std::fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }

    fn matches(self, metadata: &std::fs::Metadata) -> bool {
        self.device == metadata.dev() && self.inode == metadata.ino()
    }
}

struct OwnedLeaf {
    kind: LeafKind,
    file: File,
    identity: Option<LeafIdentity>,
}

pub(super) struct PublicationTransaction<'a> {
    artifact_dir: &'a File,
    helper_dir: &'a File,
    leaves: Vec<OwnedLeaf>,
    committed: bool,
}

impl<'a> PublicationTransaction<'a> {
    pub(super) fn new(artifact_dir: &'a File, helper_dir: &'a File) -> Self {
        Self {
            artifact_dir,
            helper_dir,
            leaves: Vec::with_capacity(3),
            committed: false,
        }
    }

    pub(super) fn create(&mut self, kind: LeafKind, mode: u32) -> Result<usize, ()> {
        let parent = self.parent(kind);
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                kind.name().as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                mode,
            )
        };
        if fd < 0 {
            return Err(());
        }
        self.leaves.push(OwnedLeaf {
            kind,
            file: unsafe { File::from_raw_fd(fd) },
            identity: None,
        });
        publication_fault(&format!("{}:after-create", kind.label()))?;
        Ok(self.leaves.len() - 1)
    }

    pub(super) fn file_mut(&mut self, index: usize) -> Result<&mut File, ()> {
        self.leaves
            .get_mut(index)
            .map(|leaf| &mut leaf.file)
            .ok_or(())
    }

    pub(super) fn boundary(&self, label: &str) -> Result<(), ()> {
        publication_fault(label)
    }

    pub(super) fn sync_file(&self, index: usize, label: &str) -> Result<(), ()> {
        self.leaves
            .get(index)
            .ok_or(())?
            .file
            .sync_all()
            .map_err(|_| ())?;
        publication_event(label);
        publication_fault(label)
    }

    pub(super) fn promote(
        &mut self,
        index: usize,
        uid: u32,
        gid: u32,
        mode: u32,
    ) -> Result<(), ()> {
        let leaf = self.leaves.get_mut(index).ok_or(())?;
        let metadata = leaf.file.metadata().map_err(|_| ())?;
        publication_fault(&format!("{}:after-metadata", leaf.kind.label()))?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.uid() != uid
            || metadata.gid() != gid
            || metadata.mode() & 0o7777 != mode
        {
            return Err(());
        }
        leaf.identity = Some(LeafIdentity::from(&metadata));
        publication_fault(&format!("{}:after-promotion", leaf.kind.label()))
    }

    pub(super) fn sync_parent(&self, kind: LeafKind, label: &str) -> Result<(), ()> {
        self.parent(kind).sync_all().map_err(|_| ())?;
        publication_event(label);
        publication_fault(label)
    }

    pub(super) fn commit(&mut self, manifest: usize) -> Result<(), ()> {
        let leaf = self.leaves.get(manifest).ok_or(())?;
        if leaf.kind != LeafKind::Manifest {
            return Err(());
        }
        verify_owned(self.parent(LeafKind::Manifest), leaf)?;
        publication_fault("manifest:before-commit")?;
        self.committed = true;
        Ok(())
    }

    pub(super) fn finish(mut self, published: bool) -> Result<(), ()> {
        if published && self.committed {
            return Ok(());
        }
        self.rollback()
    }

    fn rollback(&mut self) -> Result<(), ()> {
        while let Some(leaf) = self.leaves.last() {
            let parent = self.parent(leaf.kind);
            verify_owned(parent, leaf)?;
            publication_fault(&format!("{}:cleanup-inspect", leaf.kind.label()))?;
            if leaf.kind == LeafKind::Helper {
                remove_helper_capability(&leaf.file)?;
                leaf.file.sync_all().map_err(|_| ())?;
                publication_fault("helper:cleanup-capability")?;
                verify_owned(parent, leaf)?;
            }
            publication_fault(&format!("{}:cleanup-unlink", leaf.kind.label()))?;
            if unsafe { libc::unlinkat(parent.as_raw_fd(), leaf.kind.name().as_ptr(), 0) } != 0 {
                return Err(());
            }
            parent.sync_all().map_err(|_| ())?;
            publication_event(&format!("{}:cleanup-fsync", leaf.kind.label()));
            publication_fault(&format!("{}:cleanup-fsync", leaf.kind.label()))?;
            self.leaves.pop();
        }
        Ok(())
    }

    fn parent(&self, kind: LeafKind) -> &File {
        match kind {
            LeafKind::Helper => self.helper_dir,
            LeafKind::Policy | LeafKind::Manifest => self.artifact_dir,
        }
    }
}

fn verify_owned(parent: &File, leaf: &OwnedLeaf) -> Result<(), ()> {
    let retained = leaf.file.metadata().map_err(|_| ())?;
    let retained_identity = LeafIdentity::from(&retained);
    if leaf
        .identity
        .is_some_and(|identity| !identity.matches(&retained))
    {
        return Err(());
    }
    let mut current = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            leaf.kind.name().as_ptr(),
            current.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(());
    }
    let current = unsafe { current.assume_init() };
    if current.st_dev != retained_identity.device || current.st_ino != retained_identity.inode {
        return Err(());
    }
    Ok(())
}

fn capability_name() -> &'static std::ffi::CStr {
    #[cfg(test)]
    {
        c"user.t050-capability"
    }
    #[cfg(not(test))]
    {
        c"security.capability"
    }
}

fn remove_helper_capability(file: &File) -> Result<(), ()> {
    let name = capability_name();
    let size = unsafe { libc::fgetxattr(file.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if size >= 0 {
        if unsafe { libc::fremovexattr(file.as_raw_fd(), name.as_ptr()) } != 0 {
            return Err(());
        }
    } else if std::io::Error::last_os_error().raw_os_error() != Some(libc::ENODATA) {
        return Err(());
    }
    let absent =
        unsafe { libc::fgetxattr(file.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if absent >= 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ENODATA) {
        return Err(());
    }
    Ok(())
}

fn publication_fault(label: &str) -> Result<(), ()> {
    #[cfg(test)]
    if PUBLICATION_FAULTS.with(|faults| {
        faults
            .borrow()
            .as_deref()
            .is_some_and(|faults| faults.split(',').any(|fault| fault == label))
    }) {
        return Err(());
    }
    let _ = label;
    Ok(())
}

fn publication_event(label: &str) {
    #[cfg(test)]
    PUBLICATION_EVENTS.with(|events| events.borrow_mut().push(label.to_owned()));
    let _ = label;
}

#[cfg(test)]
pub(super) fn set_publication_faults(faults: Option<&str>) {
    PUBLICATION_FAULTS.with(|value| *value.borrow_mut() = faults.map(str::to_owned));
}

#[cfg(test)]
pub(super) fn take_publication_events() -> Vec<String> {
    PUBLICATION_EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

#[cfg(test)]
mod tests {
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
            std::fs::set_permissions(&artifact_path, std::fs::Permissions::from_mode(0o755))
                .unwrap();
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
}
