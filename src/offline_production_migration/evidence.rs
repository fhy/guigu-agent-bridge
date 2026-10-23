use super::{Failure, Request};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Provenance {
    pub source_commit: String,
    pub package: String,
    pub version: String,
    pub publication: String,
    pub review: String,
    pub package_sha256: String,
    pub executable_sha256: String,
    pub executable: std::path::PathBuf,
    pub elf_sha256: String,
    pub migration_set_sha256: String,
    pub launcher_hold: LauncherHold,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LauncherHold {
    pub operation_id: String,
    pub controller: String,
    pub prior_state: String,
    pub prior_show_sha256: String,
    pub executable: std::path::PathBuf,
    pub config: std::path::PathBuf,
    pub manual_launchers_excluded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Identity {
    pub device: u64,
    pub inode: u64,
}

impl Identity {
    pub fn of(path: &Path) -> Result<Self, Failure> {
        canonical(path)?;
        let meta = fs::metadata(path).map_err(|_| Failure::Preflight)?;
        if !meta.is_file() || meta.nlink() != 1 {
            return Err(Failure::Preflight);
        }
        Ok(Self {
            device: meta.dev(),
            inode: meta.ino(),
        })
    }
}

pub(super) fn canonical(path: &Path) -> Result<(), Failure> {
    if !path.is_absolute() || fs::canonicalize(path).map_err(|_| Failure::Preflight)? != path {
        return Err(Failure::Preflight);
    }
    Ok(())
}

pub(super) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn hash_file(path: &Path) -> Result<String, Failure> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| Failure::Preflight)?;
    hash_handle(&mut file)
}

fn hash_handle(file: &mut File) -> Result<String, Failure> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| Failure::Preflight)?;
    let mut hash = Sha256::new();
    std::io::copy(file, &mut hash).map_err(|_| Failure::Preflight)?;
    Ok(format!("{:x}", hash.finalize()))
}

pub(super) fn migration_digest() -> String {
    digest(&serde_json::to_vec(&crate::storage::migration_contract()).expect("migration contract"))
}

pub(super) struct Evidence {
    executable: File,
    executable_stamp: (u64, i64, i64, i64, i64),
    executable_identity: Identity,
    pub provenance_digest: String,
    pub config_digest: String,
    pub database_identity: Identity,
    pub listeners: Vec<std::net::SocketAddr>,
}

fn hexadecimal(value: &str, length: usize) -> bool {
    value.len() == length
        && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        && value.bytes().any(|byte| byte != b'0')
}

impl Evidence {
    pub fn read(req: &Request) -> Result<Self, Failure> {
        for path in [
            &req.config,
            &req.database,
            &req.manifest_dir,
            &req.release_provenance,
            &req.executable,
        ] {
            canonical(path)?;
        }
        for path in [&req.manifest_dir, &req.release_provenance] {
            if fs::metadata(path)
                .map_err(|_| Failure::Preflight)?
                .permissions()
                .mode()
                & 0o077
                != 0
            {
                return Err(Failure::Preflight);
            }
        }
        let bytes = fs::read(&req.release_provenance).map_err(|_| Failure::Preflight)?;
        let provenance: Provenance =
            serde_json::from_slice(&bytes).map_err(|_| Failure::Preflight)?;
        let mut executable = File::open("/proc/self/exe").map_err(|_| Failure::Preflight)?;
        let meta = executable.metadata().map_err(|_| Failure::Preflight)?;
        let executable_identity = Identity {
            device: meta.dev(),
            inode: meta.ino(),
        };
        let mut elf = [0u8; 64];
        executable
            .read_exact(&mut elf)
            .map_err(|_| Failure::Preflight)?;
        let executable_hash = hash_handle(&mut executable)?;
        validate(&provenance, req, &elf, &executable_hash)?;
        if Identity::of(&req.executable)? != executable_identity {
            return Err(Failure::ManualRequired);
        }
        let config_digest = hash_file(&req.config)?;
        let listeners = super::configuration::listeners(req)?;
        if hash_file(&req.config)? != config_digest {
            return Err(Failure::ManualRequired);
        }
        Ok(Self {
            executable_stamp: stamp(&meta),
            executable,
            executable_identity,
            provenance_digest: digest(&bytes),
            config_digest,
            database_identity: Identity::of(&req.database)?,
            listeners,
        })
    }

    pub fn recheck(&mut self, req: &Request) -> Result<(), Failure> {
        for path in [
            &req.config,
            &req.database,
            &req.manifest_dir,
            &req.release_provenance,
            &req.executable,
        ] {
            canonical(path)?;
        }
        if hash_file(&req.release_provenance)? != self.provenance_digest
            || hash_file(&req.config)? != self.config_digest
            || Identity::of(&req.database)? != self.database_identity
            || Identity::of(&req.executable)? != self.executable_identity
            || stamp(
                &self
                    .executable
                    .metadata()
                    .map_err(|_| Failure::ManualRequired)?,
            ) != self.executable_stamp
            || stamp(&fs::metadata(&req.executable).map_err(|_| Failure::ManualRequired)?)
                != self.executable_stamp
        {
            return Err(Failure::ManualRequired);
        }
        Ok(())
    }
}

fn validate(
    provenance: &Provenance,
    req: &Request,
    elf: &[u8],
    executable_hash: &str,
) -> Result<(), Failure> {
    if !elf.starts_with(b"\x7fELF")
        || provenance.elf_sha256 != digest(elf)
        || !hexadecimal(&provenance.source_commit, 40)
        || provenance.publication.is_empty()
        || provenance.review.is_empty()
        || !hexadecimal(&provenance.package_sha256, 64)
        || provenance.package != "guigu-agent-bridge"
        || provenance.version != env!("CARGO_PKG_VERSION")
        || provenance.executable != req.executable
        || provenance.executable_sha256 != executable_hash
        || provenance.migration_set_sha256 != migration_digest()
        || provenance.launcher_hold.operation_id != req.operation_id
        || provenance.launcher_hold.controller.is_empty()
        || provenance.launcher_hold.prior_state != "inactive/dead/MainPID=0"
        || !hexadecimal(&provenance.launcher_hold.prior_show_sha256, 64)
        || provenance.launcher_hold.executable != req.executable
        || provenance.launcher_hold.config != req.config
        || !provenance.launcher_hold.manual_launchers_excluded
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

fn stamp(meta: &fs::Metadata) -> (u64, i64, i64, i64, i64) {
    (
        meta.len(),
        meta.mtime(),
        meta.mtime_nsec(),
        meta.ctime(),
        meta.ctime_nsec(),
    )
}
