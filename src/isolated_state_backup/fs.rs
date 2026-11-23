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

pub(crate) use super::lock::OperationLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnershipFailure {
    Clean,
    Ambiguous,
}

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

pub(crate) fn same_inode(left: Identity, right: Identity) -> bool {
    left.device == right.device && left.inode == right.inode
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
    super::fault::hit("after-leaf-write")?;
    file.sync_data().map_err(|_| ())?;
    super::fault::hit("after-leaf-fsync")?;
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
        super::fault::hit("after-leaf-unlink")?;
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

pub(crate) fn create_namespace(
    root: &File,
    name: &str,
    uid: u32,
    gid: u32,
) -> Result<(File, Identity), OwnershipFailure> {
    if !leaf_absent(root, name).map_err(|_| OwnershipFailure::Clean)? {
        return Err(OwnershipFailure::Clean);
    }
    let name = CString::new(name).map_err(|_| OwnershipFailure::Clean)?;
    if unsafe { libc::mkdirat(root.as_raw_fd(), name.as_ptr(), 0o700) } != 0 {
        return Err(OwnershipFailure::Clean);
    }
    super::fault::hit("after-namespace-mkdir").map_err(|_| OwnershipFailure::Ambiguous)?;
    let dir = openat_dir(root, name.as_c_str()).map_err(|_| OwnershipFailure::Ambiguous)?;
    super::fault::hit("after-namespace-reopen").map_err(|_| OwnershipFailure::Ambiguous)?;
    super::fault::hit("before-namespace-metadata").map_err(|_| OwnershipFailure::Ambiguous)?;
    let retained = dir_identity(&dir.metadata().map_err(|_| OwnershipFailure::Ambiguous)?)
        .map_err(|_| OwnershipFailure::Ambiguous)?;
    let outcome = (|| {
        super::fault::hit("after-namespace-metadata")?;
        if unsafe { libc::fchown(dir.as_raw_fd(), uid, gid) } != 0
            || unsafe { libc::fchmod(dir.as_raw_fd(), 0o700) } != 0
        {
            return Err(());
        }
        super::fault::hit("after-namespace-ownership")?;
        dir.sync_all().map_err(|_| ())?;
        super::fault::hit("after-namespace-file-fsync")?;
        root.sync_all().map_err(|_| ())?;
        super::fault::hit("after-namespace-parent-fsync")?;
        let current = open_child_dir(root, name.to_str().map_err(|_| ())?)?;
        let observed = dir_identity(&current.metadata().map_err(|_| ())?)?;
        if !same_inode(retained, observed) {
            return Err(());
        }
        dir_identity(&dir.metadata().map_err(|_| ())?)
    })();
    if outcome.is_err() {
        let leaf = name.to_str().map_err(|_| OwnershipFailure::Ambiguous)?;
        return match remove_dir_exact(root, leaf, retained)
            .and_then(|()| root.sync_all().map_err(|_| ()))
        {
            Ok(()) => Err(OwnershipFailure::Clean),
            Err(()) => Err(OwnershipFailure::Ambiguous),
        };
    }
    Ok((dir, outcome.expect("checked success")))
}

#[cfg(test)]
pub(crate) fn create_namespace_fixture(
    root: &File,
    name: &str,
) -> Result<(File, Identity), OwnershipFailure> {
    create_namespace(root, name, unsafe { libc::geteuid() }, unsafe {
        libc::getegid()
    })
}

pub(crate) fn remove_dir_exact(root: &File, name: &str, expected: Identity) -> Result<(), ()> {
    let current = open_child_dir(root, name)?;
    let observed = dir_identity(&current.metadata().map_err(|_| ())?)?;
    if !same_inode(expected, observed) {
        return Err(());
    }
    drop(current);
    remove_dir(root, name)
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
