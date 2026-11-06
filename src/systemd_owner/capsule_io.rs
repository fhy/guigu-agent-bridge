use super::{FileBinding, refused};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

pub(super) fn proc_start(pid: u32) -> std::io::Result<u64> {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))?
        .rsplit_once(") ")
        .and_then(|(_, rest)| rest.split_whitespace().nth(19))
        .and_then(|value| value.parse().ok())
        .ok_or_else(refused)
}

pub(super) fn file_binding(path: &Path) -> std::io::Result<FileBinding> {
    let path = path.canonicalize()?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(refused());
    }
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 65536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    if file.metadata()?.len() != metadata.len()
        || (file.metadata()?.mtime(), file.metadata()?.mtime_nsec())
            != (metadata.mtime(), metadata.mtime_nsec())
    {
        return Err(refused());
    }
    Ok(FileBinding {
        path,
        device: metadata.dev(),
        inode: metadata.ino(),
        digest: format!("{:x}", digest.finalize()),
    })
}

fn private_parent(path: &Path) -> std::io::Result<()> {
    if !path.is_absolute() {
        return Err(refused());
    }
    let parent = path.parent().ok_or_else(refused)?;
    if parent.canonicalize()? != parent {
        return Err(refused());
    }
    for ancestor in parent.ancestors() {
        let metadata = std::fs::symlink_metadata(ancestor)?;
        if !metadata.is_dir() || metadata.mode() & 0o022 != 0 {
            return Err(refused());
        }
    }
    let metadata = parent.metadata()?;
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
        return Err(refused());
    }
    Ok(())
}

pub(super) fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    private_parent(path)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

pub(super) fn read_private(path: &Path) -> std::io::Result<Vec<u8>> {
    private_parent(path)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.mode() & 0o777 != 0o600
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.len() > 65536
    {
        return Err(refused());
    }
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err(refused());
    }
    Ok(bytes)
}
