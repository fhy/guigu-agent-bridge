use super::*;
use std::os::unix::fs::OpenOptionsExt;

#[cfg(test)]
thread_local! {
    static FSYNC_EVENTS: std::cell::RefCell<Vec<&'static str>> = const { std::cell::RefCell::new(Vec::new()) };
}

const DIRECTORY_MODE: u32 = 0o755;
const LOCK_MODE: u32 = 0o600;

#[derive(Clone, Copy)]
struct Identity {
    device: u64,
    inode: u64,
}

impl Identity {
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

struct CreatedDirectory {
    parent: File,
    name: CString,
    identity: Identity,
}

pub(super) struct ProvisionLayout {
    artifact_dir: File,
    lock: File,
    lock_identity: Identity,
    created: Vec<CreatedDirectory>,
    finished: bool,
}

impl ProvisionLayout {
    pub(super) fn production() -> Result<Self, ()> {
        Self::at_roots(Path::new("/etc"), Path::new("/usr/libexec"), 0)
    }

    pub(super) fn at_roots(
        etc_root: &Path,
        libexec_root: &Path,
        expected_uid: u32,
    ) -> Result<Self, ()> {
        let etc = trusted_root(etc_root, expected_uid)?;
        let libexec = trusted_root(libexec_root, expected_uid)?;
        let mut created = Vec::new();
        let directories = (|| {
            let bridge_etc = directory(&etc, "guigu-agent-bridge", expected_uid, &mut created)?;
            let artifact_dir =
                directory(&bridge_etc, "t050-proc-proof", expected_uid, &mut created)?;
            directory(&libexec, "guigu-agent-bridge", expected_uid, &mut created)?;
            Ok(artifact_dir)
        })();
        let artifact_dir = match directories {
            Ok(directory) => directory,
            Err(()) => {
                cleanup_directories(&mut created)?;
                return Err(());
            }
        };
        let (lock, lock_identity) = match create_lock(&artifact_dir, expected_uid) {
            Ok(lock) => lock,
            Err(()) => {
                cleanup_directories(&mut created)?;
                return Err(());
            }
        };
        Ok(Self {
            artifact_dir,
            lock,
            lock_identity,
            created,
            finished: false,
        })
    }

    pub(super) fn finish(mut self, retain_directories: bool) -> Result<(), ()> {
        if cleanup_lock(&self.artifact_dir, &self.lock, self.lock_identity).is_err() {
            self.finished = true;
            return Err(());
        }
        if !retain_directories && cleanup_directories(&mut self.created).is_err() {
            self.finished = true;
            return Err(());
        }
        self.finished = true;
        Ok(())
    }
}

impl Drop for ProvisionLayout {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if cleanup_lock(&self.artifact_dir, &self.lock, self.lock_identity).is_ok() {
            let _ = cleanup_directories(&mut self.created);
        }
    }
}

fn trusted_root(path: &Path, expected_uid: u32) -> Result<File, ()> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| ())?;
    validate_directory(&file, expected_uid)?;
    Ok(file)
}

fn validate_directory(file: &File, expected_uid: u32) -> Result<Identity, ()> {
    let metadata = file.metadata().map_err(|_| ())?;
    if !metadata.is_dir()
        || metadata.uid() != expected_uid
        || metadata.gid() != expected_uid
        || metadata.mode() & 0o7777 != DIRECTORY_MODE
    {
        return Err(());
    }
    Ok(Identity::from(&metadata))
}

fn open_directory(parent: &File, name: &CString) -> Result<File, ()> {
    #[repr(C)]
    struct How {
        flags: u64,
        mode: u64,
        resolve: u64,
    }
    let how = How {
        flags: (libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) as u64,
        mode: 0,
        resolve: 0x08 | 0x04 | 0x02,
    };
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            parent.as_raw_fd(),
            name.as_ptr(),
            &how,
            std::mem::size_of::<How>(),
        )
    };
    if fd < 0 {
        return Err(());
    }
    Ok(unsafe { File::from_raw_fd(fd as RawFd) })
}

fn directory(
    parent: &File,
    name: &str,
    expected_uid: u32,
    created: &mut Vec<CreatedDirectory>,
) -> Result<File, ()> {
    let name = CString::new(name).map_err(|_| ())?;
    let mut invocation_created = false;
    let directory = match open_directory(parent, &name) {
        Ok(directory) => directory,
        Err(()) => {
            if unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), DIRECTORY_MODE) } != 0 {
                return Err(());
            }
            invocation_created = true;
            match open_directory(parent, &name) {
                Ok(directory) => directory,
                Err(()) => return Err(()),
            }
        }
    };
    let metadata = directory.metadata().map_err(|_| ())?;
    let identity = Identity::from(&metadata);
    if invocation_created {
        let retained_parent = match parent.try_clone() {
            Ok(parent) => parent,
            Err(_) => {
                let _ = remove_created_directory(parent, &name, identity);
                return Err(());
            }
        };
        created.push(CreatedDirectory {
            parent: retained_parent,
            name,
            identity,
        });
    }
    if !metadata.is_dir()
        || metadata.uid() != expected_uid
        || metadata.gid() != expected_uid
        || metadata.mode() & 0o7777 != DIRECTORY_MODE
    {
        return Err(());
    }
    if invocation_created {
        sync_parent(parent, "mkdir")?;
    }
    Ok(directory)
}

fn create_lock(parent: &File, expected_uid: u32) -> Result<(File, Identity), ()> {
    let name = c".provision.lock";
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            LOCK_MODE,
        )
    };
    if fd < 0 {
        return Err(());
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let validated = (|| {
        let metadata = file.metadata().map_err(|_| ())?;
        if !metadata.is_file()
            || metadata.uid() != expected_uid
            || metadata.gid() != expected_uid
            || metadata.mode() & 0o7777 != LOCK_MODE
            || metadata.nlink() != 1
            || unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0
        {
            return Err(());
        }
        sync_parent(parent, "lock-create")?;
        Ok(Identity::from(&metadata))
    })();
    let identity = match validated {
        Ok(identity) => identity,
        Err(()) => {
            let _ = remove_created_lock(parent, &file);
            return Err(());
        }
    };
    Ok((file, identity))
}

fn remove_created_lock(parent: &File, lock: &File) -> Result<(), ()> {
    let retained = Identity::from(&lock.metadata().map_err(|_| ())?);
    let current = current_identity(parent, c".provision.lock")?;
    if current.device != retained.device || current.inode != retained.inode {
        return Err(());
    }
    if unsafe { libc::unlinkat(parent.as_raw_fd(), c".provision.lock".as_ptr(), 0) } != 0 {
        return Err(());
    }
    sync_parent(parent, "lock-create-rollback")
}

fn current_identity(parent: &File, name: &std::ffi::CStr) -> Result<Identity, ()> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(());
    }
    let metadata = unsafe { metadata.assume_init() };
    Ok(Identity {
        device: metadata.st_dev,
        inode: metadata.st_ino,
    })
}

fn cleanup_lock(parent: &File, lock: &File, expected: Identity) -> Result<(), ()> {
    let retained = lock.metadata().map_err(|_| ())?;
    let current = current_identity(parent, c".provision.lock")?;
    if !expected.matches(&retained)
        || current.device != expected.device
        || current.inode != expected.inode
    {
        return Err(());
    }
    if unsafe { libc::unlinkat(parent.as_raw_fd(), c".provision.lock".as_ptr(), 0) } != 0 {
        return Err(());
    }
    sync_parent(parent, "lock-unlink")
}

fn cleanup_directories(created: &mut Vec<CreatedDirectory>) -> Result<(), ()> {
    while let Some(directory) = created.pop() {
        remove_created_directory(&directory.parent, &directory.name, directory.identity)?;
    }
    Ok(())
}

fn remove_created_directory(
    parent: &File,
    name: &std::ffi::CStr,
    identity: Identity,
) -> Result<(), ()> {
    let current = current_identity(parent, name)?;
    if current.device != identity.device || current.inode != identity.inode {
        return Err(());
    }
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) } != 0 {
        return Err(());
    }
    sync_parent(parent, "directory-unlink")
}

fn sync_parent(parent: &File, event: &'static str) -> Result<(), ()> {
    parent.sync_all().map_err(|_| ())?;
    #[cfg(test)]
    FSYNC_EVENTS.with(|events| events.borrow_mut().push(event));
    let _ = event;
    Ok(())
}

#[cfg(test)]
pub(super) fn take_fsync_events() -> Vec<&'static str> {
    FSYNC_EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}
