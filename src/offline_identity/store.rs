use std::{
    fs,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::CollectorRefused;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreGeneration {
    pub locator_digest: String,
    pub inventory_digest: String,
    pub files: usize,
}

pub fn store_generation(root: &Path) -> Result<StoreGeneration, CollectorRefused> {
    if !root.is_absolute() || fs::canonicalize(root).map_err(|_| CollectorRefused)? != root {
        return Err(CollectorRefused);
    }
    let metadata = fs::symlink_metadata(root).map_err(|_| CollectorRefused)?;
    if !metadata.is_dir() || metadata.uid() != unsafe { libc::geteuid() } {
        return Err(CollectorRefused);
    }
    let mut pending = vec![root.to_path_buf()];
    let mut entries = Vec::new();
    let mut files = 0;
    while let Some(path) = pending.pop() {
        if entries.len() >= 10000 {
            return Err(CollectorRefused);
        }
        let before = fs::symlink_metadata(&path).map_err(|_| CollectorRefused)?;
        if before.uid() != metadata.uid() || before.file_type().is_symlink() {
            return Err(CollectorRefused);
        }
        let digest = if before.is_dir() {
            for entry in fs::read_dir(&path).map_err(|_| CollectorRefused)? {
                if pending.len() + entries.len() >= 10000 {
                    return Err(CollectorRefused);
                }
                pending.push(entry.map_err(|_| CollectorRefused)?.path());
            }
            None
        } else if before.is_file() && before.nlink() == 1 {
            files += 1;
            Some(file_digest(&path, &before)?)
        } else {
            return Err(CollectorRefused);
        };
        if stamp(&fs::symlink_metadata(&path).map_err(|_| CollectorRefused)?) != stamp(&before) {
            return Err(CollectorRefused);
        }
        entries.push((
            path.strip_prefix(root)
                .map_err(|_| CollectorRefused)?
                .to_path_buf(),
            stamp(&before),
            digest,
        ));
    }
    if files == 0 {
        return Err(CollectorRefused);
    }
    entries.sort();
    for (relative, expected, _) in &entries {
        let current = fs::symlink_metadata(root.join(relative)).map_err(|_| CollectorRefused)?;
        if stamp(&current) != *expected {
            return Err(CollectorRefused);
        }
    }
    let locator = serde_json::to_vec(&(root, metadata.dev(), metadata.ino()))
        .map_err(|_| CollectorRefused)?;
    let inventory = serde_json::to_vec(&entries).map_err(|_| CollectorRefused)?;
    Ok(StoreGeneration {
        locator_digest: format!("{:x}", Sha256::digest(locator)),
        inventory_digest: format!("{:x}", Sha256::digest(inventory)),
        files,
    })
}

fn file_digest(path: &Path, before: &fs::Metadata) -> Result<String, CollectorRefused> {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| CollectorRefused)?;
    if stamp(&file.metadata().map_err(|_| CollectorRefused)?) != stamp(before) {
        return Err(CollectorRefused);
    }
    let mut hash = Sha256::new();
    std::io::copy(&mut file, &mut hash).map_err(|_| CollectorRefused)?;
    if stamp(&file.metadata().map_err(|_| CollectorRefused)?) != stamp(before) {
        return Err(CollectorRefused);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn stamp(metadata: &fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64, u32) {
    (
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
        metadata.mode(),
    )
}
