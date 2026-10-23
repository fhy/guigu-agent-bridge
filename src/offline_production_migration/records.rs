use super::{
    Failure, Request,
    evidence::{Evidence, Identity, digest},
    verification::Facts,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Operation {
    pub operation_id: String,
    pub database: PathBuf,
    pub config: PathBuf,
    pub manifest_dir: PathBuf,
    pub database_identity: Identity,
    pub config_digest: String,
    pub provenance_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Backup {
    pub operation: Operation,
    pub sha256: String,
    pub before: Facts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub operation: Operation,
    pub backup_digest: String,
    pub migration_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Complete {
    pub intent: Intent,
    pub after: Facts,
}

pub(super) fn anchor(database: &Path) -> PathBuf {
    database.with_file_name(format!(
        ".{}.offline-operation.json",
        database.file_name().unwrap_or_default().to_string_lossy()
    ))
}

pub(super) fn operation(req: &Request, evidence: &Evidence) -> Operation {
    Operation {
        operation_id: req.operation_id.clone(),
        database: req.database.clone(),
        config: req.config.clone(),
        manifest_dir: req.manifest_dir.clone(),
        database_identity: evidence.database_identity.clone(),
        config_digest: evidence.config_digest.clone(),
        provenance_digest: evidence.provenance_digest.clone(),
    }
}

pub(super) fn load<T: DeserializeOwned>(path: &Path) -> Result<T, Failure> {
    super::evidence::canonical(path).map_err(|_| Failure::ManualRequired)?;
    let bytes = fs::read(path).map_err(|_| Failure::ManualRequired)?;
    serde_json::from_slice(&bytes).map_err(|_| Failure::ManualRequired)
}

pub(super) fn sync_dir(path: &Path) -> Result<(), Failure> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| Failure::Preflight)
}

pub(super) fn publish<T: Serialize>(path: &Path, value: &T) -> Result<(), Failure> {
    let temporary = path.with_extension("pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temporary)
        .map_err(|_| Failure::ManualRequired)?;
    file.write_all(&serde_json::to_vec(value).map_err(|_| Failure::Preflight)?)
        .and_then(|()| file.sync_all())
        .map_err(|_| Failure::Preflight)?;
    fs::hard_link(&temporary, path).map_err(|_| Failure::ManualRequired)?;
    sync_dir(path.parent().ok_or(Failure::Preflight)?)?;
    fs::remove_file(temporary).map_err(|_| Failure::Preflight)?;
    sync_dir(path.parent().ok_or(Failure::Preflight)?)
}

pub(super) fn encoded_digest<T: Serialize>(value: &T) -> Result<String, Failure> {
    Ok(digest(
        &serde_json::to_vec(value).map_err(|_| Failure::Preflight)?,
    ))
}

pub(super) fn no_residue(req: &Request) -> Result<(), Failure> {
    if anchor(&req.database).exists()
        || anchor(&req.database).with_extension("pending").exists()
        || fs::read_dir(&req.manifest_dir)
            .map_err(|_| Failure::Preflight)?
            .next()
            .is_some()
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

pub(super) fn no_pending(req: &Request) -> Result<(), Failure> {
    if present(&anchor(&req.database).with_extension("pending"))? {
        return Err(Failure::ManualRequired);
    }
    for entry in fs::read_dir(&req.manifest_dir).map_err(|_| Failure::ManualRequired)? {
        if entry
            .map_err(|_| Failure::ManualRequired)?
            .file_name()
            .to_string_lossy()
            .ends_with(".pending")
        {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

pub(crate) fn startup_allowed(database: &Path) -> Result<(), Failure> {
    let path = anchor(database);
    if present(&path.with_extension("pending"))? {
        return Err(Failure::ManualRequired);
    }
    if !present(&path)? {
        return Ok(());
    }
    let operation: Operation = load(&path)?;
    if operation.database != database || operation.database_identity != Identity::of(database)? {
        return Err(Failure::ManualRequired);
    }
    let directory = &operation.manifest_dir;
    for entry in fs::read_dir(directory).map_err(|_| Failure::ManualRequired)? {
        if entry
            .map_err(|_| Failure::ManualRequired)?
            .file_name()
            .to_string_lossy()
            .ends_with(".pending")
        {
            return Err(Failure::ManualRequired);
        }
    }
    if fs::metadata(directory)
        .map_err(|_| Failure::ManualRequired)?
        .permissions()
        .mode()
        & 0o077
        != 0
    {
        return Err(Failure::ManualRequired);
    }
    let complete: Complete = load(&directory.join("complete.json"))?;
    let intent: Intent = load(&directory.join("migration_intent.json"))?;
    let backup: Backup = load(&directory.join("backup_published.json"))?;
    if complete.intent != intent
        || intent.operation != operation
        || backup.operation != operation
        || intent.backup_digest != encoded_digest(&backup)?
        || complete.after.version != 20
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

fn present(path: &Path) -> Result<bool, Failure> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(Failure::ManualRequired),
    }
}
