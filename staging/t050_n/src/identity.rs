use crate::{Refusal, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
};

pub const MAX_BYTES: u64 = 1 << 30;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectId {
    pub dev: u64,
    pub ino: u64,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub kind: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileId {
    pub object: ObjectId,
    pub len: u64,
    pub nlink: u64,
    pub sha256: String,
}
impl ObjectId {
    pub fn capture(file: &File) -> Result<Self> {
        let m = file.metadata().map_err(|_| Refusal::Identity)?;
        Ok(Self {
            dev: m.dev(),
            ino: m.ino(),
            uid: m.uid(),
            gid: m.gid(),
            mode: m.mode() & 0o7777,
            kind: m.mode() & libc::S_IFMT,
        })
    }
    pub fn validate_dir(&self, uid: u32, gid: u32) -> Result<()> {
        if self.uid != uid || self.gid != gid || self.mode != 0o700 || self.kind != libc::S_IFDIR {
            return Err(Refusal::Permission);
        }
        Ok(())
    }
}
impl FileId {
    pub fn capture(file: &File) -> Result<Self> {
        let before = file.metadata().map_err(|_| Refusal::Identity)?;
        if !before.is_file() || before.len() > MAX_BYTES {
            return Err(Refusal::Length);
        }
        let object = ObjectId::capture(file)?;
        let mut total = 0u64;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let read = unsafe {
                libc::pread(
                    file.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    total as i64,
                )
            };
            if read < 0 {
                return Err(Refusal::Identity);
            }
            if read == 0 {
                break;
            }
            total = total.checked_add(read as u64).ok_or(Refusal::Length)?;
            if total > before.len() || total > MAX_BYTES {
                return Err(Refusal::Length);
            }
            hash.update(&buffer[..read as usize]);
        }
        let after = file.metadata().map_err(|_| Refusal::Identity)?;
        if total != before.len()
            || after.len() != before.len()
            || after.nlink() != before.nlink()
            || ObjectId::capture(file)? != object
            || before.mtime_nsec() != after.mtime_nsec()
            || before.mtime() != after.mtime()
            || before.ctime() != after.ctime()
            || before.ctime_nsec() != after.ctime_nsec()
        {
            return Err(Refusal::Identity);
        }
        Ok(Self {
            object,
            len: total,
            nlink: after.nlink(),
            sha256: format!("{:x}", hash.finalize()),
        })
    }
    pub fn validate(&self, uid: u32, gid: u32, mode: u32) -> Result<()> {
        if self.object.uid != uid
            || self.object.gid != gid
            || self.object.mode != mode
            || self.object.kind != libc::S_IFREG
            || self.nlink != 1
            || self.len > MAX_BYTES
            || !crate::contract::hex(&self.sha256, 64)
        {
            return Err(Refusal::Permission);
        }
        Ok(())
    }
    pub fn recheck(&self, file: &File) -> Result<()> {
        if Self::capture(file)? != *self {
            return Err(Refusal::Identity);
        }
        Ok(())
    }
}
pub struct RetainedImage {
    file: File,
    identity: FileId,
    poisoned: std::sync::atomic::AtomicBool,
}
impl RetainedImage {
    pub fn authenticate(file: File, expected: &FileId, uid: u32, gid: u32) -> Result<Self> {
        expected.validate(uid, gid, 0o700)?;
        expected.recheck(&file)?;
        let mut magic = [0u8; 4];
        if unsafe { libc::pread(file.as_raw_fd(), magic.as_mut_ptr().cast(), 4, 0) } != 4
            || magic != *b"\x7fELF"
        {
            return Err(Refusal::Identity);
        }
        let mut capability = [0u8; 24];
        let result = unsafe {
            libc::fgetxattr(
                file.as_raw_fd(),
                c"security.capability".as_ptr(),
                capability.as_mut_ptr().cast(),
                24,
            )
        };
        if result >= 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ENODATA) {
            return Err(Refusal::Permission);
        }
        Ok(Self {
            file,
            identity: expected.clone(),
            poisoned: std::sync::atomic::AtomicBool::new(false),
        })
    }
    pub fn file(&self) -> &File {
        &self.file
    }
    pub fn identity(&self) -> &FileId {
        &self.identity
    }
    pub fn recheck(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        if self.poisoned.load(Ordering::Acquire) {
            return Err(Refusal::Identity);
        }
        if self.identity.recheck(&self.file).is_err() {
            self.poisoned.store(true, Ordering::Release);
            return Err(Refusal::Identity);
        }
        Ok(())
    }
}

/// Read-only verification using original receipt/journal witnesses, never adoption.
pub fn verify_namespace(packet: &crate::contract::VerificationPacket) -> Result<()> {
    use crate::{
        contract::Leaf,
        journal::{Binding, Journal, Stage},
        paths::OwnedDir,
    };
    let receipt = &packet.receipt;
    receipt.validate()?;
    if receipt.spec.seed().uid != unsafe { libc::geteuid() }
        || receipt.spec.seed().gid != unsafe { libc::getegid() }
        || packet.records.is_empty()
        || packet.records.len() > 129
    {
        return Err(Refusal::NoAuthority);
    }
    let root = OwnedDir::open_absolute(
        receipt.spec.root(),
        receipt.spec.seed().uid,
        receipt.spec.seed().gid,
    )?;
    if root.identity() != &receipt.root {
        return Err(Refusal::Identity);
    }
    let journal = Journal::recover(
        root.subdir("journal")?,
        Binding {
            uuid: receipt.spec.seed().uuid.clone(),
            root: receipt.root.clone(),
            epoch: receipt.epoch.clone(),
            candidate: receipt.spec.seed().candidate.clone(),
        },
        &packet.records,
    )?;
    journal.disposition()?;
    if journal.last().map(|r| r.stage) != Some(Stage::PublicationCommitted)
        || journal.last().and_then(|r| r.receipt.as_ref()) != Some(&receipt.digest()?)
    {
        return Err(Refusal::PublicationUnknown);
    }
    let control = root.subdir("control")?;
    for (leaf, id) in [
        (Leaf::Lock, &receipt.lock),
        (Leaf::Contract, &receipt.contract),
        (Leaf::Policy, &receipt.policy),
        (Leaf::Manifest, &receipt.manifest),
    ] {
        let path = receipt.spec.path(leaf);
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or(Refusal::Path)?;
        control.verify_leaf(name, id)?;
    }
    root.subdir("config")?
        .verify_leaf("config.toml", &receipt.config)?;
    root.subdir("units")?
        .verify_leaf(&format!("{}.v1", receipt.spec.unit()), &receipt.unit)?;
    let images = root.subdir("images")?;
    for (index, name) in ["parent", "helper", "provisioner"].into_iter().enumerate() {
        images.verify_leaf(name, &receipt.images[index])?;
    }
    Ok(())
}

#[cfg(test)]
mod host_tests {
    use super::*;
    use crate::{
        BoundNamespace,
        contract::{NamespaceSeed, SourcePacket, UUID},
        paths::OwnedDir,
        publication::Anonymous,
    };
    use std::{fs, os::unix::fs::PermissionsExt};
    fn published() -> (tempfile::TempDir, BoundNamespace) {
        let temp = tempfile::tempdir().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let file = File::open(temp.path()).unwrap();
        let id = ObjectId::capture(&file).unwrap();
        let root = OwnedDir::authenticate(file, &id).unwrap();
        let source_dir = root.mkdir("sources").unwrap();
        let source = File::open("/usr/bin/true").unwrap();
        let expected = FileId::capture(&source).unwrap();
        let images: Vec<_> = ["parent", "helper", "provisioner"]
            .iter()
            .map(|name| {
                let p = Anonymous::copy(&source_dir, &source, &expected, 0o700)
                    .unwrap()
                    .publish(&source_dir, name)
                    .unwrap();
                RetainedImage::authenticate(p.file, &p.identity, id.uid, id.gid).unwrap()
            })
            .collect();
        let packet = SourcePacket {
            seed: NamespaceSeed {
                uuid: UUID.into(),
                candidate: "b".repeat(40),
                approval: "c".repeat(64),
                uid: id.uid,
                gid: id.gid,
            },
            epoch: "a".repeat(64),
            images: std::array::from_fn(|i| images[i].identity().clone()),
        };
        let mut host = BoundNamespace::from_allocation(&packet, root).unwrap();
        host.publish(&packet, [&images[0], &images[1], &images[2]])
            .unwrap();
        (temp, host)
    }
    #[test]
    fn host_manifest_last_and_single_reservation() {
        let (_temp, mut host) = published();
        host.receipt().unwrap().validate().unwrap();
        host.continuity().unwrap();
        host.reserve_operation().unwrap();
        assert!(host.reserve_operation().is_err());
        assert_eq!(
            host.journal
                .records()
                .iter()
                .filter(|r| r.stage == crate::journal::Stage::OperationReserved)
                .count(),
            1
        );
    }
    #[test]
    fn host_replacement_revokes_and_restored_name_never_readmits() {
        let (temp, mut host) = published();
        let name = format!("artifacts-{UUID}.v1");
        let path = temp.path().join("control").join(&name);
        let retained = temp.path().join("control/retained-original");
        fs::rename(&path, &retained).unwrap();
        let bytes = fs::read(&retained).unwrap();
        Anonymous::bytes(&host.control, &bytes, 0o600)
            .unwrap()
            .publish(&host.control, &name)
            .unwrap();
        assert_eq!(host.reserve_operation(), Err(Refusal::RevokedUnknown));
        let replacement = temp.path().join("control/retained-replacement");
        fs::rename(&path, &replacement).unwrap();
        fs::rename(&retained, &path).unwrap();
        assert!(host.reserve_operation().is_err());
        assert!(replacement.exists());
        assert!(path.exists());
        assert_eq!(
            host.journal.last().unwrap().stage,
            crate::journal::Stage::Revoked
        );
        assert_eq!(
            host.journal
                .records()
                .iter()
                .filter(|r| r.stage == crate::journal::Stage::OperationReserved)
                .count(),
            0
        );
    }
}
