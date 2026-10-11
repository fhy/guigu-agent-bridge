use crate::{
    Refusal, Result,
    identity::FileId,
    paths::OwnedDir,
    publication::{Anonymous, Published},
};
use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotReference {
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub sha256: String,
}
impl SnapshotReference {
    pub fn capture(identity: &FileId) -> Self {
        Self {
            device: identity.object.dev,
            inode: identity.object.ino,
            size: identity.len,
            uid: identity.object.uid,
            gid: identity.object.gid,
            mode: identity.object.mode,
            sha256: identity.sha256.clone(),
        }
    }
    pub fn verify(&self, file: &File) -> Result<()> {
        let identity = FileId::capture(file)?;
        identity.validate(self.uid, self.gid, 0o600)?;
        if Self::capture(&identity) != *self {
            return Err(Refusal::Identity);
        }
        Ok(())
    }
}
/// The source must be a closed, authenticated SQLite image (including completed
/// WAL checkpoint); this is not a live raw-copy substitute for SQLite backup.
pub fn publish_closed(
    dir: &OwnedDir,
    source: &File,
    expected: &FileId,
) -> Result<(Published, SnapshotReference)> {
    let mut magic = [0u8; 16];
    use std::os::fd::AsRawFd;
    if unsafe { libc::pread(source.as_raw_fd(), magic.as_mut_ptr().cast(), 16, 0) } != 16
        || &magic != b"SQLite format 3\0"
    {
        return Err(Refusal::Contract);
    }
    let snapshot =
        Anonymous::copy(dir, source, expected, 0o600)?.publish(dir, "snapshot.sqlite3")?;
    let reference = SnapshotReference::capture(&snapshot.identity);
    reference.verify(&snapshot.file)?;
    let body = serde_json::to_vec(&reference).map_err(|_| Refusal::Contract)?;
    let mut record = b"T050N-SNAPSHOT-1\n".to_vec();
    record.extend_from_slice(&body);
    record.extend_from_slice(crate::digest(&body).as_bytes());
    Anonymous::bytes(dir, &record, 0o600)?.publish(dir, "snapshot.manifest.v1")?;
    Ok((snapshot, reference))
}
