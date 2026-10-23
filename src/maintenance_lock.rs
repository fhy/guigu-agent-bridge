use std::{
    fs::{File, OpenOptions},
    io,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::Path,
};

pub struct MaintenanceLock {
    _file: File,
}

impl MaintenanceLock {
    pub fn acquire(database: &Path) -> io::Result<Self> {
        let resolved = match std::fs::canonicalize(database) {
            Ok(path) => {
                if std::fs::metadata(&path)?.nlink() != 1 {
                    return Err(io::Error::other("database alias"));
                }
                path
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if std::fs::symlink_metadata(database).is_ok() {
                    return Err(io::Error::other("database alias"));
                }
                database.to_path_buf()
            }
            Err(error) => return Err(error),
        };
        let database = &resolved;
        let parent = database
            .parent()
            .ok_or_else(|| io::Error::other("parent"))?;
        let parent = std::fs::canonicalize(parent)?;
        let name = database
            .file_name()
            .ok_or_else(|| io::Error::other("database name"))?;
        let lock_path = parent.join(format!(
            ".{}.offline-migration.lock",
            name.to_string_lossy()
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lock_path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(io::Error::other("lock type"));
        }
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let current = std::fs::symlink_metadata(lock_path)?;
        if current.dev() != metadata.dev() || current.ino() != metadata.ino() {
            return Err(io::Error::other("lock changed"));
        }
        Ok(Self { _file: file })
    }
}
