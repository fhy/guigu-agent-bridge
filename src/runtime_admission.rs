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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProofBinding {
    pub nonce: [u8; 16],
    pub mode: u8,
    pub pid: u32,
    pub starttime: u64,
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProofIdentity {
    pub path: Vec<u8>,
    pub device: u64,
    pub inode: u64,
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
    let mut digest = Sha256::new();
    #[cfg(debug_assertions)]
    let synthetic_executable = std::env::var_os("T050_RECONCILE_TEST_PROC_ROOT").is_some()
        && std::env::current_exe()?.canonicalize()? == path;
    #[cfg(not(debug_assertions))]
    let synthetic_executable = false;
    if synthetic_executable {
        digest.update(before.dev().to_be_bytes());
        digest.update(before.ino().to_be_bytes());
        digest.update(before.len().to_be_bytes());
        digest.update(before.mode().to_be_bytes());
    } else {
        let mut buffer = [0u8; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
    }
    let after = file.metadata()?;
    if before.dev() != after.dev() || before.ino() != after.ino() || before.mode() != after.mode() {
        return Err(std::io::Error::other("identity changed while reading"));
    }
    Ok(FileIdentity {
        path: canonical.to_string_lossy().into_owned(),
        device: before.dev(),
        inode: before.ino(),
        mode: before.mode(),
        digest: format!("{:x}", digest.finalize()),
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
        || !matches!(
            record.kind.as_str(),
            "startup"
                | "reconcile"
                | "fixture-prepare"
                | "fixture-migrate"
                | "backup"
                | "backup-verify"
                | "restore"
                | "owner-binding-remediation"
        )
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

        reject_restore_interlock(database, kind)?;
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

    pub fn proof_binding(&self) -> std::io::Result<ProofBinding> {
        self.revalidate()?;
        let record = read_record(&self.path)?;
        let nonce = uuid::Uuid::parse_str(&record.nonce)
            .map_err(|_| std::io::Error::other("invalid admission nonce"))?;
        Ok(ProofBinding {
            nonce: *nonce.as_bytes(),
            mode: match record.kind.as_str() {
                "fixture-prepare" => 1,
                "fixture-migrate" => 2,
                "backup" => 3,
                "backup-verify" => 4,
                "restore" => 5,
                "reconcile" => return Err(std::io::Error::other("unsupported proof mode")),
                _ => return Err(std::io::Error::other("invalid admission mode")),
            },
            pid: record.pid,
            starttime: record.starttime,
            device: self.device,
            inode: self.inode,
        })
    }

    pub fn verify_proof_identities(
        &self,
        binary: &ProofIdentity,
        config: &ProofIdentity,
        database_root: &ProofIdentity,
    ) -> std::io::Result<()> {
        verify_proof_identities(&self.path, binary, config, database_root)
    }
}

fn reject_restore_interlock(database: &Path, kind: &str) -> std::io::Result<()> {
    let root = database
        .parent()
        .ok_or_else(|| std::io::Error::other("database has no parent"))?;
    let namespace = root.join(".t050-backup-v1");
    let markers = [
        "restore.journal.v1",
        "restore.journal.next",
        "restore.journal.prev",
        "cleanup.restore.journal.v1",
        "cleanup.restore.journal.next",
        "cleanup.restore.journal.prev",
        "cleanup.backup.journal.v1",
        "cleanup.backup.journal.next",
        "cleanup.backup.journal.prev",
        "snapshot.manifest.quarantine",
        "restore.new.sqlite3",
        "pre-restore.main",
        "pre-restore.wal",
        "pre-restore.shm",
    ];
    for marker in markers {
        if kind == "restore"
            && (marker.starts_with("restore.") || marker.starts_with("pre-restore."))
        {
            continue;
        }
        match std::fs::symlink_metadata(namespace.join(marker)) {
            Ok(_) => return Err(std::io::Error::other("restore interlock is active")),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(_) => return Err(std::io::Error::other("restore interlock is ambiguous")),
        }
    }
    for witness in [
        ".t050-backup-cleanup-owner.v1",
        ".t050-backup-cleanup-owner.next",
        ".t050-backup-cleanup-owner.prev",
    ] {
        match std::fs::symlink_metadata(root.join(witness)) {
            Ok(_) => return Err(std::io::Error::other("cleanup interlock is active")),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(_) => return Err(std::io::Error::other("cleanup interlock is ambiguous")),
        }
    }
    Ok(())
}

pub fn verify_proof_binding(path: &Path, expected: ProofBinding) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let record = read_record(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    let nonce = uuid::Uuid::parse_str(&record.nonce)
        .map_err(|_| std::io::Error::other("invalid admission nonce"))?;
    let mode = match record.kind.as_str() {
        "fixture-prepare" => 1,
        "fixture-migrate" => 2,
        "backup" => 3,
        "backup-verify" => 4,
        "restore" => 5,
        "reconcile" => return Err(std::io::Error::other("unsupported proof mode")),
        _ => return Err(std::io::Error::other("invalid admission mode")),
    };
    if *nonce.as_bytes() != expected.nonce
        || mode != expected.mode
        || record.pid != expected.pid
        || record.starttime != expected.starttime
        || metadata.dev() != expected.device
        || metadata.ino() != expected.inode
    {
        return Err(std::io::Error::other("admission proof binding changed"));
    }
    Ok(())
}

pub fn verify_proof_identities(
    path: &Path,
    binary: &ProofIdentity,
    config: &ProofIdentity,
    database_root: &ProofIdentity,
) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let record = read_record(path)?;
    let database_parent = Path::new(&record.database.path)
        .parent()
        .ok_or_else(|| std::io::Error::other("database has no parent"))?;
    let matches = |record: &FileIdentity, expected: &ProofIdentity| {
        record.path.as_bytes() == expected.path
            && record.device == expected.device
            && record.inode == expected.inode
    };
    if !matches(&record.executable, binary)
        || !matches(&record.config, config)
        || database_parent.as_os_str().as_bytes() != database_root.path
    {
        return Err(std::io::Error::other(
            "admission identity does not match policy",
        ));
    }
    let metadata = std::fs::metadata(database_parent)?;
    use std::os::unix::fs::MetadataExt;
    if metadata.dev() != database_root.device || metadata.ino() != database_root.inode {
        return Err(std::io::Error::other("database root identity changed"));
    }
    Ok(())
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

include!("runtime_admission_tests.rs");
