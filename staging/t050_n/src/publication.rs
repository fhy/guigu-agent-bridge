use crate::{
    Refusal, Result,
    identity::{FileId, MAX_BYTES},
    paths::{OwnedDir, component},
};
use std::{
    fs::File,
    io::Write,
    os::fd::{AsRawFd, FromRawFd},
};

pub struct Anonymous {
    file: File,
    hash: String,
    len: u64,
    mode: u32,
}
pub struct Published {
    pub file: File,
    pub identity: FileId,
}
impl Anonymous {
    pub fn bytes(dir: &OwnedDir, bytes: &[u8], mode: u32) -> Result<Self> {
        if bytes.len() as u64 > MAX_BYTES || ![0o600, 0o700].contains(&mode) {
            return Err(Refusal::Length);
        }
        dir.recheck()?;
        let fd = unsafe {
            libc::openat(
                dir.file().as_raw_fd(),
                c".".as_ptr(),
                libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(Refusal::Unsupported);
        }
        let mut file = unsafe { File::from_raw_fd(fd) };
        file.write_all(bytes).map_err(|_| Refusal::Publication)?;
        if unsafe { libc::fchmod(file.as_raw_fd(), mode) } != 0 {
            return Err(Refusal::Permission);
        }
        file.sync_all().map_err(|_| Refusal::Publication)?;
        let id = FileId::capture(&file)?;
        if id.nlink != 0 || id.len != bytes.len() as u64 || id.sha256 != crate::digest(bytes) {
            return Err(Refusal::Identity);
        }
        Ok(Self {
            file,
            hash: id.sha256,
            len: id.len,
            mode,
        })
    }
    pub fn copy(dir: &OwnedDir, source: &File, expected: &FileId, mode: u32) -> Result<Self> {
        expected.recheck(source)?;
        let fd = unsafe {
            libc::openat(
                dir.file().as_raw_fd(),
                c".".as_ptr(),
                libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(Refusal::Unsupported);
        }
        let mut file = unsafe { File::from_raw_fd(fd) };
        let mut total = 0u64;
        let mut buffer = [0u8; 65536];
        loop {
            let read = unsafe {
                libc::pread(
                    source.as_raw_fd(),
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
            if total > expected.len || total > MAX_BYTES {
                return Err(Refusal::Length);
            }
            file.write_all(&buffer[..read as usize])
                .map_err(|_| Refusal::Publication)?;
        }
        if total != expected.len || ![0o600, 0o700].contains(&mode) {
            return Err(Refusal::Length);
        }
        expected.recheck(source)?;
        if unsafe { libc::fchmod(file.as_raw_fd(), mode) } != 0 {
            return Err(Refusal::Permission);
        }
        file.sync_all().map_err(|_| Refusal::Publication)?;
        let id = FileId::capture(&file)?;
        if id.nlink != 0 || id.sha256 != expected.sha256 {
            return Err(Refusal::Identity);
        }
        Ok(Self {
            file,
            hash: id.sha256,
            len: total,
            mode,
        })
    }
    pub fn descriptor(&self) -> &File {
        &self.file
    }
    pub fn publish(self, dir: &OwnedDir, name: &str) -> Result<Published> {
        let name = component(name)?;
        dir.recheck()?;
        let before = FileId::capture(&self.file)?;
        if before.nlink != 0 || before.sha256 != self.hash || before.len != self.len {
            return Err(Refusal::Identity);
        }
        let source = std::ffi::CString::new(format!("/proc/self/fd/{}", self.file.as_raw_fd()))
            .map_err(|_| Refusal::Path)?;
        if unsafe {
            libc::linkat(
                libc::AT_FDCWD,
                source.as_ptr(),
                dir.file().as_raw_fd(),
                name.as_ptr(),
                libc::AT_SYMLINK_FOLLOW,
            )
        } != 0
        {
            return Err(Refusal::Publication);
        }
        let identity = FileId::capture(&self.file)?;
        identity.validate(dir.identity().uid, dir.identity().gid, self.mode)?;
        if identity.object != before.object
            || identity.sha256 != self.hash
            || identity.len != self.len
        {
            return Err(Refusal::PublicationUnknown);
        }
        // Capture the final dentry before any reservation. The original anonymous
        // inode stays retained: equal bytes on another inode never authorize this FD.
        let file = dir.verify_leaf(name.to_str().map_err(|_| Refusal::Path)?, &identity)?;
        identity.recheck(&self.file)?;
        dir.sync()?;
        Ok(Published { file, identity })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u8,
    pub candidate: String,
    pub images: [FileId; 3],
    pub objects: Vec<FileId>,
}
impl Manifest {
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.version != 1
            || !crate::contract::hex(&self.candidate, 40)
            || self.objects.len() > 16
        {
            return Err(Refusal::Contract);
        }
        let body = serde_json::to_vec(self).map_err(|_| Refusal::Contract)?;
        let mut bytes = b"T050N-MANIFEST-1\n".to_vec();
        bytes.extend_from_slice(&body);
        bytes.push(b'\n');
        bytes.extend_from_slice(crate::digest(&bytes).as_bytes());
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 32768 || !bytes.starts_with(b"T050N-MANIFEST-1\n") {
            return Err(Refusal::Contract);
        }
        let split = bytes.len().checked_sub(64).ok_or(Refusal::Contract)?;
        if crate::digest(&bytes[..split]).as_bytes() != &bytes[split..] || bytes[split - 1] != b'\n'
        {
            return Err(Refusal::Identity);
        }
        let value: Self = serde_json::from_slice(&bytes[b"T050N-MANIFEST-1\n".len()..split - 1])
            .map_err(|_| Refusal::Contract)?;
        if value.encode()? != bytes {
            return Err(Refusal::Contract);
        }
        Ok(value)
    }
}

use crate::{contract, identity, journal, publication};
impl crate::BoundNamespace {
    pub(crate) fn publish_authority(&mut self, packet: &contract::SourcePacket) -> Result<()> {
        use contract::Leaf;
        use journal::{Outcome, Stage};
        let config_dir = self.root.subdir("config")?;
        let config = publication::Anonymous::bytes(&config_dir, &self.config_template(), 0o600)?
            .publish(&config_dir, "config.toml")?;
        let contract = self.publish_control(
            &format!("contract-{}.v1", self.spec.seed().uuid),
            &serde_json::to_vec(packet).map_err(|_| Refusal::Contract)?,
        )?;
        let policy = self.publish_control(
            &format!("policy-{}.v1", self.spec.seed().uuid),
            b"T050N-POLICY-1\nnamespace-only;matrix-disabled;descriptor-only;no-retry\n",
        )?;
        let unit_dir = self.root.subdir("units")?;
        let unit = publication::Anonymous::bytes(&unit_dir, self.spec.unit().as_bytes(), 0o600)?
            .publish(&unit_dir, &format!("{}.v1", self.spec.unit()))?;
        let ids: [identity::FileId; 3] = std::array::from_fn(|i| self.images[i].identity().clone());
        let manifest = publication::Manifest {
            version: 1,
            candidate: self.spec.seed().candidate.clone(),
            images: ids.clone(),
            objects: vec![
                config.identity.clone(),
                contract.identity.clone(),
                policy.identity.clone(),
                unit.identity.clone(),
            ],
        };
        self.journal.append(
            Stage::PublicationIntent,
            Outcome::None,
            Outcome::None,
            None,
            None,
        )?;
        let name = self.spec.path(Leaf::Manifest);
        let name = name
            .file_name()
            .and_then(|v| v.to_str())
            .ok_or(Refusal::Path)?;
        let published = self.publish_control(name, &manifest.encode()?)?;
        self.journal.append(
            Stage::ManifestLinked,
            Outcome::None,
            Outcome::None,
            None,
            None,
        )?;
        let receipt = contract::PublicationReceipt {
            spec: self.spec.clone(),
            root: self.root.identity().clone(),
            lock: self.lock.identity().clone(),
            epoch: self.lock.epoch().into(),
            images: ids,
            contract: contract.identity,
            config: config.identity.clone(),
            policy: policy.identity,
            unit: unit.identity,
            manifest: published.identity,
            generation: 1,
            predecessor: self.journal.tip()?,
        };
        self.commit_authority(config, receipt)
    }
    fn commit_authority(
        &mut self,
        config: Published,
        receipt: contract::PublicationReceipt,
    ) -> Result<()> {
        use journal::{Outcome, Stage};
        let bytes = serde_json::to_vec(&receipt).map_err(|_| Refusal::Contract)?;
        Anonymous::bytes(&self.root.subdir("evidence")?, &bytes, 0o600)?
            .publish(&self.root.subdir("evidence")?, "publication.v1")?;
        self.journal.append(
            Stage::PublicationCommitted,
            Outcome::None,
            Outcome::None,
            Some(receipt.digest()?),
            None,
        )?;
        self.config = Some(config);
        self.receipt = Some(receipt);
        self.admission()
    }
    fn config_template(&self) -> Vec<u8> {
        use contract::Leaf;
        format!("[bridge]\ndatabase = {:?}\nsession_root = {:?}\nhealth_bind = \"127.0.0.1:9392\"\n[transports.matrix]\nenabled = false\n[transports.a2a]\nenabled = true\nlisten = \"127.0.0.1:9391\"\nallow_private_plaintext = true\n",
            self.spec.path(Leaf::Database),self.spec.path(Leaf::State).join("sessions")).into_bytes()
    }
    fn publish_control(&self, name: &str, bytes: &[u8]) -> Result<publication::Published> {
        self.lock.recheck(&self.control)?;
        publication::Anonymous::bytes(&self.control, bytes, 0o600)?.publish(&self.control, name)
    }
}
