use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::output::OutputSpoolError;
use crate::storage::{OutputSegment, PendingDelivery};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    pub(super) format: u32,
    pub(super) set_id: String,
    pub(super) total_bytes: u64,
    pub(super) total_hash: String,
    pub(super) segments: Vec<ManifestSegment>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct ManifestSegment {
    pub(super) index: u64,
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) hash: String,
    pub(super) name: String,
}

pub(super) fn prepare_root(root: &Path) -> Result<(), OutputSpoolError> {
    if root.exists() {
        validate_dir(root)?;
    } else {
        fs::create_dir(root)?;
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        sync_dir(root.parent().ok_or(OutputSpoolError::Identity)?)?;
    }
    for name in ["sets", "quarantine"] {
        let path = root.join(name);
        if !path.exists() {
            fs::create_dir(&path)?;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
            sync_dir(root)?;
        }
        validate_dir(&path)?;
    }
    Ok(())
}

pub(super) fn prepare_set(
    root: &Path,
    set_id: &str,
) -> Result<std::path::PathBuf, OutputSpoolError> {
    let set_dir = root.join("sets").join(set_id);
    match fs::create_dir(&set_dir) {
        Ok(()) => {
            fs::set_permissions(&set_dir, fs::Permissions::from_mode(0o700))?;
            sync_dir(set_dir.parent().ok_or(OutputSpoolError::Identity)?)?;
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => validate_dir(&set_dir)?,
        Err(error) => return Err(error.into()),
    }
    Ok(set_dir)
}

pub(super) fn publish_file(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let temp = dir.join(format!(".{name}.tmp"));
    let target = dir.join(name);
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)
    {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if read_exact_file(&temp)? != bytes {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "conflicting durable temp",
                ));
            }
        }
        Err(error) => return Err(error),
    }
    match fs::hard_link(&temp, &target) {
        Ok(()) => {
            fs::remove_file(&temp)?;
            sync_dir(dir)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let existing = read_exact_file(&target)?;
            fs::remove_file(&temp)?;
            sync_dir(dir)?;
            if existing == bytes {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "conflicting durable output",
                ))
            }
        }
        Err(error) => {
            let _ = fs::remove_file(&temp);
            Err(error)
        }
    }
}

pub(super) fn read_segment(
    root: &Path,
    segment: &OutputSegment,
) -> Result<Vec<u8>, OutputSpoolError> {
    let path = root
        .join("sets")
        .join(&segment.set_id)
        .join(&segment.relative_name);
    let bytes = read_exact_file(&path)?;
    if hash(&bytes) != segment.content_hash
        || bytes.len() as i64 != segment.byte_end - segment.byte_start
    {
        return Err(OutputSpoolError::Identity);
    }
    Ok(bytes)
}

pub(super) fn read_delivery(
    root: &Path,
    delivery: &PendingDelivery,
) -> Result<Vec<u8>, OutputSpoolError> {
    validate_manifest(root, delivery)?;
    let bytes = read_segment(root, &delivery.spool)?;
    let start = usize::try_from(delivery.part.byte_start - delivery.spool.byte_start)
        .map_err(|_| OutputSpoolError::Identity)?;
    let end = usize::try_from(delivery.part.byte_end - delivery.spool.byte_start)
        .map_err(|_| OutputSpoolError::Identity)?;
    let slice = bytes
        .get(start..end)
        .ok_or(OutputSpoolError::Identity)?
        .to_vec();
    if hash(&slice) != delivery.part.content_hash {
        return Err(OutputSpoolError::Identity);
    }
    Ok(slice)
}

fn validate_manifest(root: &Path, delivery: &PendingDelivery) -> Result<(), OutputSpoolError> {
    let segment = &delivery.spool;
    let set_dir = root.join("sets").join(&segment.set_id);
    let bytes = read_exact_file(&set_dir.join("manifest-v1.json"))?;
    let manifest: Manifest =
        serde_json::from_slice(&bytes).map_err(|_| OutputSpoolError::Identity)?;
    if manifest.format != 1
        || manifest.set_id != segment.set_id
        || manifest.total_bytes != delivery.total_bytes as u64
        || manifest.total_hash != delivery.total_hash
        || serde_json::to_vec(&manifest).map_err(|_| OutputSpoolError::Identity)? != bytes
    {
        return Err(OutputSpoolError::Identity);
    }
    validate_tree(&set_dir, &manifest)?;
    let index = usize::try_from(segment.index).map_err(|_| OutputSpoolError::Identity)?;
    let item = manifest
        .segments
        .get(index)
        .ok_or(OutputSpoolError::Identity)?;
    if item.index != segment.index as u64
        || item.start != segment.byte_start as u64
        || item.end != segment.byte_end as u64
        || item.hash != segment.content_hash
        || item.name != segment.relative_name
    {
        return Err(OutputSpoolError::Identity);
    }
    Ok(())
}

fn validate_tree(set_dir: &Path, manifest: &Manifest) -> Result<(), OutputSpoolError> {
    let mut expected = BTreeSet::from(["manifest-v1.json".to_owned()]);
    expected.extend(manifest.segments.iter().map(|item| item.name.clone()));
    let actual = fs::read_dir(set_dir)?
        .map(|entry| {
            entry
                .map_err(OutputSpoolError::Io)?
                .file_name()
                .into_string()
                .map_err(|_| OutputSpoolError::Identity)
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if actual != expected {
        return Err(OutputSpoolError::Identity);
    }
    let mut aggregate = Sha256::new();
    let mut start = 0_u64;
    for (index, item) in manifest.segments.iter().enumerate() {
        if item.index != index as u64
            || item.start != start
            || item.end < item.start
            || item.name != format!("segment-{index}.bin")
        {
            return Err(OutputSpoolError::Identity);
        }
        let content = read_exact_file(&set_dir.join(&item.name))?;
        if content.len() as u64 != item.end - item.start || hash(&content) != item.hash {
            return Err(OutputSpoolError::Identity);
        }
        aggregate.update(&content);
        start = item.end;
    }
    if start != manifest.total_bytes || format!("{:x}", aggregate.finalize()) != manifest.total_hash
    {
        return Err(OutputSpoolError::Identity);
    }
    Ok(())
}

pub(super) fn cleanup(
    root: &Path,
    set_id: &str,
    segments: &[OutputSegment],
) -> Result<(), OutputSpoolError> {
    let set_dir = root.join("sets").join(set_id);
    if !set_dir.exists() {
        return Ok(());
    }
    validate_dir(&set_dir)?;
    for segment in segments {
        remove_file(&set_dir, &segment.relative_name)?;
    }
    remove_file(&set_dir, "manifest-v1.json")?;
    match fs::remove_dir(&set_dir) {
        Ok(()) => sync_dir(set_dir.parent().ok_or(OutputSpoolError::Identity)?)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn remove_file(dir: &Path, name: &str) -> Result<(), OutputSpoolError> {
    match fs::remove_file(dir.join(name)) {
        Ok(()) => sync_dir(dir)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn read_exact_file(path: &Path) -> io::Result<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(0x20000)
        .open(path)?;
    let before = file.metadata()?;
    if !before.is_file() || before.nlink() != 1 || before.permissions().mode() & 0o777 != 0o600 {
        return Err(io::Error::other("unsafe durable output file"));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    let path_meta = fs::symlink_metadata(path)?;
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || after.dev() != path_meta.dev()
        || after.ino() != path_meta.ino()
    {
        return Err(io::Error::other("durable output file changed"));
    }
    Ok(bytes)
}

pub(super) fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn validate_dir(path: &Path) -> Result<(), OutputSpoolError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.permissions().mode() & 0o777 != 0o700 {
        Err(OutputSpoolError::Identity)
    } else {
        Ok(())
    }
}

pub(super) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn stable_txn(set: &str, index: i64, content_hash: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"guigu-acp-output-v1\0");
    hash.update(set);
    hash.update(index.to_be_bytes());
    hash.update(content_hash.as_bytes());
    format!("t051-{:x}", hash.finalize())
}

pub(super) fn utf8_ranges(
    bytes: &[u8],
    max: usize,
) -> Result<Vec<(usize, usize)>, OutputSpoolError> {
    let text = std::str::from_utf8(bytes).map_err(|_| OutputSpoolError::Identity)?;
    if bytes.is_empty() {
        return Ok(vec![(0, 0)]);
    }
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < bytes.len() {
        let mut end = (start + max).min(bytes.len());
        while end > start && !text.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            return Err(OutputSpoolError::Identity);
        }
        ranges.push((start, end));
        start = end;
    }
    Ok(ranges)
}
