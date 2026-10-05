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

pub(super) fn capability_name() -> &'static std::ffi::CStr {
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
