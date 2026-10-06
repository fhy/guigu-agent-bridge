use std::{
    ffi::CString,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
    path::Path,
};

use sha2::{Digest, Sha256};

use super::{MAX_FILE, codec::Identity};

const RESOLVE: u64 = 0x08 | 0x04 | 0x02;

#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

pub(crate) fn identity(metadata: &std::fs::Metadata) -> Result<Identity, ()> {
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() > MAX_FILE {
        return Err(());
    }
    Ok(Identity {
        device: metadata.dev(),
        inode: metadata.ino(),
        size: metadata.len(),
        uid: metadata.uid(),
        gid: metadata.gid(),
        mode: metadata.mode() & 0o7777,
    })
}

pub(crate) fn dir_identity(metadata: &std::fs::Metadata) -> Result<Identity, ()> {
    if !metadata.is_dir() || metadata.mode() & 0o022 != 0 {
        return Err(());
    }
    Ok(Identity {
        device: metadata.dev(),
        inode: metadata.ino(),
        size: metadata.len(),
        uid: metadata.uid(),
        gid: metadata.gid(),
        mode: metadata.mode() & 0o7777,
    })
}

pub(crate) fn same(left: Identity, right: Identity) -> bool {
    left.device == right.device
        && left.inode == right.inode
        && left.size == right.size
        && left.uid == right.uid
        && left.gid == right.gid
        && left.mode == right.mode
}

pub(crate) fn same_object(left: Identity, right: Identity) -> bool {
    left.device == right.device
        && left.inode == right.inode
        && left.uid == right.uid
        && left.gid == right.gid
        && left.mode == right.mode
}

pub(crate) fn open_dir(path: &Path) -> Result<File, ()> {
    let bytes = CString::new(path.as_os_str().as_bytes()).map_err(|_| ())?;
    let fd = unsafe {
        libc::open(
            bytes.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(());
    }
    let file = unsafe { File::from_raw_fd(fd) };
    dir_identity(&file.metadata().map_err(|_| ())?)?;
    Ok(file)
}

pub(crate) fn open_leaf(dir: &File, name: &str, write: bool) -> Result<File, ()> {
    let name = CString::new(name).map_err(|_| ())?;
    let flags = libc::O_CLOEXEC | if write { libc::O_RDWR } else { libc::O_RDONLY };
    let how = OpenHow {
        flags: flags as u64,
        mode: 0,
        resolve: RESOLVE,
    };
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            dir.as_raw_fd(),
            name.as_ptr(),
            &how,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd < 0 {
        return Err(());
    }
    let file = unsafe { File::from_raw_fd(fd as i32) };
    identity(&file.metadata().map_err(|_| ())?)?;
    Ok(file)
}

pub(crate) fn create_leaf(dir: &File, name: &str, mode: u32) -> Result<File, ()> {
    let name = CString::new(name).map_err(|_| ())?;
    let how = OpenHow {
        flags: (libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC) as u64,
        mode: mode as u64,
        resolve: RESOLVE,
    };
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            dir.as_raw_fd(),
            name.as_ptr(),
            &how,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd < 0 {
        Err(())
    } else {
        Ok(unsafe { File::from_raw_fd(fd as i32) })
    }
}

pub(crate) fn hash(file: &mut File) -> Result<([u8; 32], Identity), ()> {
    file.seek(SeekFrom::Start(0)).map_err(|_| ())?;
    let before = identity(&file.metadata().map_err(|_| ())?)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let count = file.read(&mut buffer).map_err(|_| ())?;
        if count == 0 {
            break;
        }
        total = total.checked_add(count as u64).ok_or(())?;
        if total > MAX_FILE || total > before.size {
            return Err(());
        }
        digest.update(&buffer[..count]);
    }
    let after = identity(&file.metadata().map_err(|_| ())?)?;
    if total != before.size || !same(before, after) {
        return Err(());
    }
    Ok((digest.finalize().into(), after))
}

pub(crate) fn read_small(file: &mut File) -> Result<Vec<u8>, ()> {
    file.seek(SeekFrom::Start(0)).map_err(|_| ())?;
    let before = identity(&file.metadata().map_err(|_| ())?)?;
    if before.size > 4096 {
        return Err(());
    }
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes).map_err(|_| ())?;
    let after = identity(&file.metadata().map_err(|_| ())?)?;
    if bytes.len() as u64 != before.size || !same(before, after) {
        return Err(());
    }
    Ok(bytes)
}

pub(crate) fn write_sync(file: &mut File, bytes: &[u8]) -> Result<Identity, ()> {
    file.write_all(bytes).map_err(|_| ())?;
    file.sync_data().map_err(|_| ())?;
    identity(&file.metadata().map_err(|_| ())?)
}

pub(crate) fn rename_noreplace(
    from_dir: &File,
    from: &str,
    to_dir: &File,
    to: &str,
) -> Result<(), ()> {
    let from = CString::new(from).map_err(|_| ())?;
    let to = CString::new(to).map_err(|_| ())?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            from_dir.as_raw_fd(),
            from.as_ptr(),
            to_dir.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 { Ok(()) } else { Err(()) }
}

pub(crate) fn unlink(dir: &File, name: &str) -> Result<(), ()> {
    let name = CString::new(name).map_err(|_| ())?;
    if unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) } == 0 {
        Ok(())
    } else {
        Err(())
    }
}

pub(crate) fn remove_dir(dir: &File, name: &str) -> Result<(), ()> {
    let name = CString::new(name).map_err(|_| ())?;
    if unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) } == 0 {
        Ok(())
    } else {
        Err(())
    }
}

pub(crate) fn leaf_absent(dir: &File, name: &str) -> Result<bool, ()> {
    let name = CString::new(name).map_err(|_| ())?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            dir.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        return Ok(false);
    }
    if std::io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT) {
        Ok(true)
    } else {
        Err(())
    }
}

pub(crate) fn create_namespace(root: &File, name: &str, uid: u32, gid: u32) -> Result<File, ()> {
    if !leaf_absent(root, name)? {
        return Err(());
    }
    let name = CString::new(name).map_err(|_| ())?;
    if unsafe { libc::mkdirat(root.as_raw_fd(), name.as_ptr(), 0o700) } != 0 {
        return Err(());
    }
    let outcome = (|| {
        let dir = openat_dir(root, name.as_c_str())?;
        let created = dir.metadata().map_err(|_| ())?;
        if !created.is_dir() {
            return Err(());
        }
        if unsafe { libc::fchown(dir.as_raw_fd(), uid, gid) } != 0
            || unsafe { libc::fchmod(dir.as_raw_fd(), 0o700) } != 0
        {
            return Err(());
        }
        dir.sync_all().map_err(|_| ())?;
        root.sync_all().map_err(|_| ())?;
        Ok(dir)
    })();
    if outcome.is_err() {
        let pending = openat_dir(root, name.as_c_str())?;
        let metadata = pending.metadata().map_err(|_| ())?;
        if !metadata.is_dir() {
            return Err(());
        }
        drop(pending);
        remove_dir(root, name.to_str().map_err(|_| ())?)?;
        root.sync_all().map_err(|_| ())?;
    }
    outcome
}

fn openat_dir(parent: &File, name: &std::ffi::CStr) -> Result<File, ()> {
    let how = OpenHow {
        flags: (libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) as u64,
        mode: 0,
        resolve: RESOLVE,
    };
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            parent.as_raw_fd(),
            name.as_ptr(),
            &how,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd < 0 {
        Err(())
    } else {
        Ok(unsafe { File::from_raw_fd(fd as i32) })
    }
}

pub(crate) fn open_child_dir(parent: &File, name: &str) -> Result<File, ()> {
    let name = CString::new(name).map_err(|_| ())?;
    let file = openat_dir(parent, name.as_c_str())?;
    dir_identity(&file.metadata().map_err(|_| ())?)?;
    Ok(file)
}

pub(crate) struct OperationLock {
    _file: File,
    dir: File,
    identity: Identity,
    armed: bool,
}
impl OperationLock {
    pub(crate) fn acquire(dir: &File) -> Result<Self, ()> {
        let file = create_leaf(dir, super::codec::LOCK, 0o600)?;
        let pending = identity(&file.metadata().map_err(|_| ())?);
        let outcome = pending.and_then(|identity| {
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(());
            }
            dir.sync_all().map_err(|_| ())?;
            Ok(identity)
        });
        let identity = match outcome {
            Ok(identity) => identity,
            Err(()) => {
                let observed = identity(&file.metadata().map_err(|_| ())?)?;
                let current = open_leaf(dir, super::codec::LOCK, false)?;
                let current_identity = identity(&current.metadata().map_err(|_| ())?)?;
                if !same_object(observed, current_identity) {
                    return Err(());
                }
                unlink(dir, super::codec::LOCK)?;
                dir.sync_all().map_err(|_| ())?;
                return Err(());
            }
        };
        Ok(Self {
            _file: file,
            dir: dir.try_clone().map_err(|_| ())?,
            identity,
            armed: true,
        })
    }
    pub(crate) fn acquire_resume(dir: &File) -> Result<Self, ()> {
        let file = open_leaf(dir, super::codec::LOCK, true)?;
        let identity = identity(&file.metadata().map_err(|_| ())?)?;
        let uid = unsafe { libc::geteuid() };
        let gid = unsafe { libc::getegid() };
        if identity.uid != uid || identity.gid != gid || identity.mode != 0o600 {
            return Err(());
        }
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(());
        }
        Ok(Self {
            _file: file,
            dir: dir.try_clone().map_err(|_| ())?,
            identity,
            armed: true,
        })
    }
    pub(crate) fn preserve(mut self) {
        self.armed = false;
    }
    pub(crate) fn finish(mut self) -> Result<(), ()> {
        self.remove()?;
        self.armed = false;
        Ok(())
    }
    fn remove(&self) -> Result<(), ()> {
        let mut current = open_leaf(&self.dir, super::codec::LOCK, false)?;
        let (_, observed) = hash(&mut current)?;
        if !same(self.identity, observed) {
            return Err(());
        }
        unlink(&self.dir, super::codec::LOCK)?;
        self.dir.sync_all().map_err(|_| ())
    }
}
impl Drop for OperationLock {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.remove();
        }
    }
}
