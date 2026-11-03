use super::{Failure, reconciliation_records::Grant};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::{
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        io::{AsRawFd, FromRawFd},
    },
    path::{Component, Path},
};

pub(super) fn load<T>(path: &Path) -> Result<T, Failure>
where
    T: DeserializeOwned + Serialize,
{
    let bytes = protected_bytes(path)?;
    let value = serde_json::from_slice(&bytes).map_err(|_| Failure::ManualRequired)?;
    if serde_json::to_vec(&value).map_err(|_| Failure::ManualRequired)? != bytes {
        return Err(Failure::ManualRequired);
    }
    Ok(value)
}

pub(super) fn load_grant(path: &Path) -> Result<Grant, Failure> {
    #[cfg(test)]
    if std::env::var_os("R094_GRANT_PATH").as_deref() == Some(path.as_os_str()) {
        return load(path);
    }
    let bytes = protected_bytes_with_policy(path, true, || {})?;
    let grant: Grant = serde_json::from_slice(&bytes).map_err(|_| Failure::ManualRequired)?;
    if serde_json::to_vec(&grant).map_err(|_| Failure::ManualRequired)? != bytes {
        return Err(Failure::ManualRequired);
    }
    Ok(grant)
}

pub(super) fn protected_bytes(path: &Path) -> Result<Vec<u8>, Failure> {
    protected_bytes_checked(path, || {})
}

fn protected_bytes_checked(path: &Path, after_read: impl FnOnce()) -> Result<Vec<u8>, Failure> {
    protected_bytes_with_policy(path, false, after_read)
}

fn protected_bytes_with_policy(
    path: &Path,
    grant: bool,
    after_read: impl FnOnce(),
) -> Result<Vec<u8>, Failure> {
    let ancestors = PinnedAncestors::open(path, grant)?;
    let mut file = ancestors.open_file(path, libc::O_RDONLY, 0)?;
    let metadata = file.metadata().map_err(|_| Failure::ManualRequired)?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != if grant { 0 } else { unsafe { libc::geteuid() } }
        || if grant {
            metadata.permissions().mode() & 0o777 != 0o644
        } else {
            metadata.permissions().mode() & 0o077 != 0
        }
    {
        return Err(Failure::ManualRequired);
    }
    let opened = file.metadata().map_err(|_| Failure::ManualRequired)?;
    if metadata_stamp(&opened) != metadata_stamp(&metadata) {
        return Err(Failure::ManualRequired);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| Failure::Preflight)?;
    after_read();
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(0))
        .map_err(|_| Failure::Preflight)?;
    let mut reread = Vec::new();
    file.read_to_end(&mut reread)
        .map_err(|_| Failure::Preflight)?;
    if reread != bytes {
        return Err(Failure::ManualRequired);
    }
    ancestors.recheck(path, grant)?;
    let final_metadata = fs::symlink_metadata(path).map_err(|_| Failure::ManualRequired)?;
    if metadata_stamp(&final_metadata) != metadata_stamp(&opened)
        || metadata_stamp(&file.metadata().map_err(|_| Failure::ManualRequired)?)
            != metadata_stamp(&opened)
    {
        return Err(Failure::ManualRequired);
    }
    Ok(bytes)
}

fn metadata_stamp(metadata: &fs::Metadata) -> [i128; 10] {
    [
        metadata.dev().into(),
        metadata.ino().into(),
        metadata.len().into(),
        metadata.nlink().into(),
        metadata.uid().into(),
        metadata.mode().into(),
        metadata.mtime().into(),
        metadata.mtime_nsec().into(),
        metadata.ctime().into(),
        metadata.ctime_nsec().into(),
    ]
}

struct PinnedAncestors {
    directories: Vec<File>,
    identities: Vec<[u64; 4]>,
}

impl PinnedAncestors {
    fn open(path: &Path, grant: bool) -> Result<Self, Failure> {
        if !path.is_absolute() {
            return Err(Failure::ManualRequired);
        }
        let mut directories = vec![
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open("/")
                .map_err(|_| Failure::ManualRequired)?,
        ];
        for component in path.parent().ok_or(Failure::ManualRequired)?.components() {
            let Component::Normal(name) = component else {
                if component == Component::RootDir {
                    continue;
                }
                return Err(Failure::ManualRequired);
            };
            let directory = open_at(
                directories.last().ok_or(Failure::ManualRequired)?,
                name,
                libc::O_RDONLY | libc::O_DIRECTORY,
                0,
            )?;
            directories.push(directory);
        }
        let mut identities = Vec::new();
        for directory in &directories {
            let metadata = directory.metadata().map_err(|_| Failure::ManualRequired)?;
            let mode = metadata.mode();
            if !metadata.is_dir()
                || if grant {
                    metadata.uid() != 0 || mode & 0o022 != 0
                } else {
                    metadata.uid() != 0 && metadata.uid() != unsafe { libc::geteuid() }
                        || mode & 0o002 != 0 && !(metadata.uid() == 0 && mode & 0o1000 != 0)
                }
            {
                return Err(Failure::ManualRequired);
            }
            identities.push(directory_stamp(&metadata));
        }
        Ok(Self {
            directories,
            identities,
        })
    }

    fn open_file(&self, path: &Path, flags: i32, mode: u32) -> Result<File, Failure> {
        open_at(
            self.directories.last().ok_or(Failure::ManualRequired)?,
            path.file_name().ok_or(Failure::ManualRequired)?,
            flags,
            mode,
        )
    }

    fn recheck(&self, path: &Path, grant: bool) -> Result<(), Failure> {
        let current = Self::open(path, grant)?;
        if self.directories.len() != current.directories.len() {
            return Err(Failure::ManualRequired);
        }
        for ((original, actual), identity) in self
            .directories
            .iter()
            .zip(&current.directories)
            .zip(&self.identities)
        {
            let original = original.metadata().map_err(|_| Failure::ManualRequired)?;
            let actual = actual.metadata().map_err(|_| Failure::ManualRequired)?;
            if directory_stamp(&original) != *identity || directory_stamp(&actual) != *identity {
                return Err(Failure::ManualRequired);
            }
        }
        Ok(())
    }
}

fn directory_stamp(metadata: &fs::Metadata) -> [u64; 4] {
    [
        metadata.dev(),
        metadata.ino(),
        metadata.uid().into(),
        metadata.mode().into(),
    ]
}

fn open_at(
    directory: &File,
    name: &std::ffi::OsStr,
    flags: i32,
    mode: u32,
) -> Result<File, Failure> {
    let name =
        std::ffi::CString::new(name.as_encoded_bytes()).map_err(|_| Failure::ManualRequired)?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            mode,
        )
    };
    if descriptor < 0 {
        return Err(Failure::ManualRequired);
    }
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

pub(super) fn publish<T: Serialize>(path: &Path, value: &T) -> Result<(), Failure> {
    publish_checked(path, value, || {})
}

fn publish_checked<T: Serialize>(
    path: &Path,
    value: &T,
    before_rename: impl FnOnce(),
) -> Result<(), Failure> {
    let bytes = serde_json::to_vec(value).map_err(|_| Failure::Preflight)?;
    let ancestors = PinnedAncestors::open(path, false)?;
    if path.exists() {
        return if protected_bytes(path)? == bytes {
            Ok(())
        } else {
            Err(Failure::ManualRequired)
        };
    }
    let parent = path.parent().ok_or(Failure::Preflight)?;
    let temporary = parent.join(format!(
        ".{}.pending",
        path.file_name()
            .ok_or(Failure::Preflight)?
            .to_string_lossy()
    ));
    let mut file = ancestors.open_file(
        &temporary,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        0o600,
    )?;
    file.write_all(&bytes).map_err(|_| Failure::Preflight)?;
    file.sync_all().map_err(|_| Failure::Preflight)?;
    before_rename();
    ancestors.recheck(path, false)?;
    let directory = ancestors
        .directories
        .last()
        .ok_or(Failure::ManualRequired)?;
    let source = std::ffi::CString::new(
        temporary
            .file_name()
            .ok_or(Failure::Preflight)?
            .as_encoded_bytes(),
    )
    .map_err(|_| Failure::Preflight)?;
    let target = std::ffi::CString::new(
        path.file_name()
            .ok_or(Failure::Preflight)?
            .as_encoded_bytes(),
    )
    .map_err(|_| Failure::Preflight)?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory.as_raw_fd(),
            source.as_ptr(),
            directory.as_raw_fd(),
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result != 0 {
        return Err(
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST) {
                Failure::ManualRequired
            } else {
                Failure::Preflight
            },
        );
    }
    directory.sync_all().map_err(|_| Failure::Preflight)?;
    ancestors.recheck(path, false)?;
    if protected_bytes(path)? != bytes {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

#[cfg(test)]
#[path = "reconciliation_file_tests.rs"]
mod protected_file_tests;
