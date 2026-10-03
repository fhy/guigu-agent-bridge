use std::{
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const O_NOFOLLOW: i32 = 0o400000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileIdentity {
    path: String,
    device: u64,
    inode: u64,
    mode: u32,
    digest: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u8,
    kind: String,
    pid: u32,
    starttime: u64,
    executable: FileIdentity,
    config: FileIdentity,
    database: FileIdentity,
    nonce: String,
}

pub struct Guard {
    path: PathBuf,
    nonce: String,
    device: u64,
    inode: u64,
}

fn identity(path: &Path) -> std::io::Result<FileIdentity> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let canonical = path.canonicalize()?;
    if canonical != path {
        return Err(std::io::Error::other("path is not canonical"));
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW)
        .open(path)?;
    let before = file.metadata()?;
    if !before.is_file() {
        return Err(std::io::Error::other("identity is not a regular file"));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    if before.dev() != after.dev() || before.ino() != after.ino() || before.mode() != after.mode() {
        return Err(std::io::Error::other("identity changed while reading"));
    }
    Ok(FileIdentity {
        path: canonical.to_string_lossy().into_owned(),
        device: before.dev(),
        inode: before.ino(),
        mode: before.mode(),
        digest: format!("{:x}", Sha256::digest(bytes)),
    })
}

fn process_starttime(pid: u32) -> std::io::Result<u64> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let close = text
        .rfind(')')
        .ok_or_else(|| std::io::Error::other("malformed proc stat"))?;
    text[close + 1..]
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| std::io::Error::other("missing proc starttime"))?
        .parse()
        .map_err(|_| std::io::Error::other("invalid proc starttime"))
}

fn read_record(path: &Path) -> std::io::Result<Record> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.mode() & 0o777 != 0o600 || metadata.nlink() != 1 {
        return Err(std::io::Error::other("invalid admission lock"));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let record: Record = serde_json::from_slice(&bytes)
        .map_err(|_| std::io::Error::other("malformed admission lock"))?;
    if record.version != 1
        || !matches!(record.kind.as_str(), "startup" | "reconcile")
        || record.pid == 0
        || record.starttime == 0
        || uuid::Uuid::parse_str(&record.nonce).is_err()
    {
        return Err(std::io::Error::other("invalid admission lock record"));
    }
    Ok(record)
}

fn fsync_parent(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

impl Guard {
    pub fn acquire(database: &Path, config: &Path, kind: &str) -> std::io::Result<Self> {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

        let database = identity(database)?;
        let config = identity(config)?;
        let executable = identity(&std::env::current_exe()?.canonicalize()?)?;
        let parent = Path::new(&database.path)
            .parent()
            .ok_or_else(|| std::io::Error::other("database has no parent"))?
            .to_path_buf();
        let path = parent.join(".runtime-admission.lock");
        let nonce = uuid::Uuid::now_v7().to_string();
        let record = Record {
            version: 1,
            kind: kind.to_owned(),
            pid: std::process::id(),
            starttime: process_starttime(std::process::id())?,
            executable,
            config,
            database,
            nonce: nonce.clone(),
        };
        let bytes = serde_json::to_vec(&record).map_err(std::io::Error::other)?;

        let validate_stale = |stale: &Record| -> std::io::Result<()> {
            match process_starttime(stale.pid) {
                Ok(_) => return Err(std::io::Error::other("admission owner may be live")),
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(_) => return Err(std::io::Error::other("admission owner is unknown")),
            }
            if stale.executable != record.executable
                || stale.config != record.config
                || stale.database != record.database
            {
                return Err(std::io::Error::other("stale lock identity drift"));
            }
            Ok(())
        };

        let quarantines = std::fs::read_dir(&parent)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|candidate| {
                candidate.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .starts_with(".runtime-admission.stale.")
                })
            })
            .collect::<Vec<_>>();
        if quarantines.len() > 1 || (!quarantines.is_empty() && path.exists()) {
            return Err(std::io::Error::other("ambiguous admission quarantine"));
        }
        if let Some(quarantine) = quarantines.first() {
            let stale = read_record(quarantine)?;
            validate_stale(&stale)?;
            std::fs::rename(quarantine, &path)?;
            fsync_parent(&parent)?;
        }

        let create = || -> std::io::Result<Self> {
            let mut options = OpenOptions::new();
            options
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(O_NOFOLLOW);
            let mut file = options.open(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            let metadata = file.metadata()?;
            fsync_parent(&parent)?;
            Ok(Self {
                path: path.clone(),
                nonce: nonce.clone(),
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        };
        match create() {
            Ok(guard) => Ok(guard),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let stale = read_record(&path)?;
                validate_stale(&stale)?;
                let quarantine = parent.join(format!(".runtime-admission.stale.{}", stale.nonce));
                std::fs::rename(&path, &quarantine)?;
                fsync_parent(&parent)?;
                let guard = create()?;
                std::fs::remove_file(&quarantine)?;
                fsync_parent(&parent)?;
                Ok(guard)
            }
            Err(error) => Err(error),
        }
    }

    pub fn revalidate(&self) -> std::io::Result<()> {
        use std::os::unix::fs::MetadataExt;

        let record = read_record(&self.path)?;
        let metadata = std::fs::symlink_metadata(&self.path)?;
        if record.nonce != self.nonce
            || metadata.dev() != self.device
            || metadata.ino() != self.inode
        {
            return Err(std::io::Error::other("admission lock identity changed"));
        }
        Ok(())
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.revalidate().is_ok()
            && std::fs::remove_file(&self.path).is_ok()
            && let Some(parent) = self.path.parent()
        {
            let _ = fsync_parent(parent);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (PathBuf, PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("t050-lock-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let database = root.join("state.db");
        let config = root.join("config.toml");
        std::fs::write(&database, b"database").unwrap();
        std::fs::write(&config, b"config").unwrap();
        (root, database, config)
    }

    #[test]
    fn live_owner_is_rejected_and_release_is_inode_bound() {
        let (root, database, config) = fixture();
        let guard = Guard::acquire(&database, &config, "startup").unwrap();
        assert!(Guard::acquire(&database, &config, "reconcile").is_err());
        guard.revalidate().unwrap();
        drop(guard);
        assert!(!root.join(".runtime-admission.lock").exists());
    }

    #[test]
    fn explicit_not_found_owner_is_quarantined_and_reclaimed() {
        let (root, database, config) = fixture();
        let guard = Guard::acquire(&database, &config, "startup").unwrap();
        let path = root.join(".runtime-admission.lock");
        let mut record = read_record(&path).unwrap();
        record.pid = u32::MAX;
        std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        std::mem::forget(guard);
        let replacement = Guard::acquire(&database, &config, "reconcile").unwrap();
        replacement.revalidate().unwrap();
        assert_eq!(
            std::fs::read_dir(&root)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().contains(".stale."))
                .count(),
            0
        );
    }
}
