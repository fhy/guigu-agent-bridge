use std::{
    fs::{File, OpenOptions},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
};

use sha2::{Digest, Sha256};

use super::parent::CollectorRefused;

pub(super) struct PinnedExecutable {
    pub file: File,
    pub digest: String,
    pub device: u64,
    pub inode: u64,
}

pub(super) fn pin() -> Result<PinnedExecutable, CollectorRefused> {
    let path = std::env::current_exe().map_err(|_| CollectorRefused)?;
    pin_path(&path)
}

pub(super) fn pin_path(path: &std::path::Path) -> Result<PinnedExecutable, CollectorRefused> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| CollectorRefused)?;
    let before = file.metadata().map_err(|_| CollectorRefused)?;
    if !before.is_file() {
        return Err(CollectorRefused);
    }
    let mut hash = Sha256::new();
    std::io::copy(&mut file, &mut hash).map_err(|_| CollectorRefused)?;
    let after = file.metadata().map_err(|_| CollectorRefused)?;
    let stamp = |metadata: &std::fs::Metadata| {
        (
            metadata.dev(),
            metadata.ino(),
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        )
    };
    if stamp(&before) != stamp(&after) {
        return Err(CollectorRefused);
    }
    Ok(PinnedExecutable {
        file,
        digest: format!("{:x}", hash.finalize()),
        device: before.dev(),
        inode: before.ino(),
    })
}
