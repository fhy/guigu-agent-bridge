use super::*;
use std::os::unix::fs::OpenOptionsExt;

#[cfg(test)]
thread_local! {
    static FSYNC_EVENTS: std::cell::RefCell<Vec<&'static str>> = const { std::cell::RefCell::new(Vec::new()) };
    static CONSTRUCTION_FAULT: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
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

include!("bootstrap_pending.rs");

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
        let directories: Result<File, ConstructionFailure> = (|| {
            let bridge_etc = directory(
                &etc,
                "guigu-agent-bridge",
                "etc/guigu-agent-bridge",
                expected_uid,
                &mut created,
            )?;
            let artifact_dir = directory(
                &bridge_etc,
                "t050-proc-proof",
                "etc/guigu-agent-bridge/t050-proc-proof",
                expected_uid,
                &mut created,
            )?;
            directory(
                &libexec,
                "guigu-agent-bridge",
                "libexec/guigu-agent-bridge",
                expected_uid,
                &mut created,
            )?;
            Ok(artifact_dir)
        })();
        let artifact_dir = match directories {
            Ok(directory) => directory,
            Err(ConstructionFailure::Clean) => {
                cleanup_directories(&mut created)?;
                return Err(());
            }
            Err(ConstructionFailure::Ambiguous) => return Err(()),
        };
        let (lock, lock_identity) = match create_lock(&artifact_dir, expected_uid) {
            Ok(lock) => lock,
            Err(ConstructionFailure::Clean) => {
                cleanup_directories(&mut created)?;
                return Err(());
            }
            Err(ConstructionFailure::Ambiguous) => return Err(()),
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
    fault_label: &str,
    expected_uid: u32,
    created: &mut Vec<CreatedDirectory>,
) -> Result<File, ConstructionFailure> {
    let name = CString::new(name).map_err(|_| ConstructionFailure::Clean)?;
    let directory = match open_directory(parent, &name) {
        Ok(directory) => directory,
        Err(()) => {
            if unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), DIRECTORY_MODE) } != 0 {
                return Err(ConstructionFailure::Clean);
            }
            let mut pending = PendingDirectory {
                parent,
                name,
                identity: None,
                armed: true,
            };
            if construction_fault(&format!("{fault_label}:after-mkdir")).is_err() {
                return Err(pending.fail());
            }
            let directory = match open_directory(parent, &pending.name) {
                Ok(directory) => directory,
                Err(()) => return Err(pending.fail()),
            };
            if construction_fault(&format!("{fault_label}:after-reopen")).is_err() {
                return Err(pending.fail());
            }
            let metadata = match directory.metadata() {
                Ok(metadata) => metadata,
                Err(_) => return Err(pending.fail()),
            };
            pending.identity = Some(Identity::from(&metadata));
            if construction_fault(&format!("{fault_label}:after-metadata")).is_err() {
                return Err(pending.fail());
            }
            let retained_parent = match parent.try_clone() {
                Ok(parent) => parent,
                Err(_) => return Err(pending.fail()),
            };
            if construction_fault(&format!("{fault_label}:after-clone")).is_err() {
                return Err(pending.fail());
            }
            if !metadata.is_dir()
                || metadata.uid() != expected_uid
                || metadata.gid() != expected_uid
                || metadata.mode() & 0o7777 != DIRECTORY_MODE
            {
                return Err(pending.fail());
            }
            created.push(CreatedDirectory {
                parent: retained_parent,
                name: pending.name.clone(),
                identity: pending.identity.expect("pending identity is set"),
            });
            pending.armed = false;
            if sync_parent(parent, "mkdir").is_err()
                || construction_fault(&format!("{fault_label}:after-fsync")).is_err()
            {
                return Err(ConstructionFailure::Clean);
            }
            return Ok(directory);
        }
    };
    let metadata = directory
        .metadata()
        .map_err(|_| ConstructionFailure::Clean)?;
    if !metadata.is_dir()
        || metadata.uid() != expected_uid
        || metadata.gid() != expected_uid
        || metadata.mode() & 0o7777 != DIRECTORY_MODE
    {
        return Err(ConstructionFailure::Clean);
    }
    Ok(directory)
}

fn create_lock(parent: &File, expected_uid: u32) -> Result<(File, Identity), ConstructionFailure> {
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
        return Err(ConstructionFailure::Clean);
    }
    let mut pending = PendingLock {
        parent,
        file: Some(unsafe { File::from_raw_fd(fd) }),
        identity: None,
        armed: true,
    };
    if construction_fault("lock:after-open").is_err() {
        return Err(pending.fail());
    }
    let metadata = match pending
        .file
        .as_ref()
        .expect("pending lock exists")
        .metadata()
    {
        Ok(metadata) => metadata,
        Err(_) => return Err(pending.fail()),
    };
    pending.identity = Some(Identity::from(&metadata));
    if construction_fault("lock:after-metadata").is_err() {
        return Err(pending.fail());
    }
    let file = pending.file.as_ref().expect("pending lock exists");
    if !metadata.is_file()
        || metadata.uid() != expected_uid
        || metadata.gid() != expected_uid
        || metadata.mode() & 0o7777 != LOCK_MODE
        || metadata.nlink() != 1
        || unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0
    {
        return Err(pending.fail());
    }
    if construction_fault("lock:after-flock").is_err() {
        return Err(pending.fail());
    }
    if sync_parent(parent, "lock-create").is_err() {
        return Err(pending.fail());
    }
    if construction_fault("lock:after-fsync").is_err() {
        return Err(pending.fail());
    }
    Ok(pending.promote())
}

include!("bootstrap_cleanup.rs");
