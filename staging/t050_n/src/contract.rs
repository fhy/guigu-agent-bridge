use crate::{Refusal, Result, identity::FileId};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

pub const UUID: &str = "6c7f2431-5a4a-4541-bc10-be51d3f25f94";
pub const PORTS: [u16; 2] = [9391, 9392];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceSeed {
    pub uuid: String,
    pub candidate: String,
    pub approval: String,
    pub uid: u32,
    pub gid: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceSpec {
    seed: NamespaceSeed,
    root: PathBuf,
}
impl NamespaceSpec {
    pub fn derive(seed: NamespaceSeed) -> Result<Self> {
        if seed.uuid != UUID || !hex(&seed.candidate, 40) || !hex(&seed.approval, 64) {
            return Err(Refusal::Contract);
        }
        let root = PathBuf::from(format!("/home/fhy/.local/share/guigu-t050-n/{}", seed.uuid));
        Ok(Self { seed, root })
    }
    pub fn seed(&self) -> &NamespaceSeed {
        &self.seed
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn path(&self, leaf: Leaf) -> PathBuf {
        self.root.join(leaf.relative(&self.seed.uuid))
    }
    pub fn unit(&self) -> String {
        format!("guigu-t050-n-{}.scope", self.seed.uuid)
    }
    pub fn ports(&self) -> [u16; 2] {
        PORTS
    }
    pub fn validate(&self) -> Result<()> {
        if Self::derive(self.seed.clone())? != *self {
            return Err(Refusal::Contract);
        }
        Ok(())
    }
    pub fn accepts(&self, path: &Path) -> bool {
        let Some(text) = path.to_str() else {
            return false;
        };
        !text.contains("//")
            && !text.split('/').any(|part| part == "." || part == "..")
            && !text.ends_with('/')
            && path.is_absolute()
            && path
                .components()
                .all(|c| matches!(c, Component::RootDir | Component::Normal(_)))
            && path.starts_with(&self.root)
    }
}
pub fn hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Leaf {
    Parent,
    Helper,
    Provisioner,
    State,
    Database,
    Config,
    Backup,
    Snapshot,
    SnapshotManifest,
    Policy,
    Manifest,
    Contract,
    Unit,
    Lock,
    Journal,
    Evidence,
}
impl Leaf {
    pub fn relative(self, uuid: &str) -> String {
        match self {
            Self::Parent => "images/parent".into(),
            Self::Helper => "images/helper".into(),
            Self::Provisioner => "images/provisioner".into(),
            Self::State => "state".into(),
            Self::Database => "state/state.db".into(),
            Self::Config => "config/config.toml".into(),
            Self::Backup => format!("state/.t050-n-backup-{uuid}"),
            Self::Snapshot => format!("state/.t050-n-backup-{uuid}/snapshot.sqlite3"),
            Self::SnapshotManifest => format!("state/.t050-n-backup-{uuid}/snapshot.manifest.v1"),
            Self::Policy => format!("control/policy-{uuid}.v1"),
            Self::Manifest => format!("control/artifacts-{uuid}.v1"),
            Self::Contract => format!("control/contract-{uuid}.v1"),
            Self::Unit => format!("units/guigu-t050-n-{uuid}.scope.v1"),
            Self::Lock => format!("control/namespace-{uuid}.lock"),
            Self::Journal => "journal".into(),
            Self::Evidence => "evidence".into(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationReceipt {
    pub spec: NamespaceSpec,
    pub root: crate::identity::ObjectId,
    pub lock: FileId,
    pub epoch: String,
    pub images: [FileId; 3],
    pub contract: FileId,
    pub config: FileId,
    pub policy: FileId,
    pub unit: FileId,
    pub manifest: FileId,
    pub generation: u8,
    pub predecessor: String,
}
impl PublicationReceipt {
    pub fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        if self.generation != 1 || !hex(&self.epoch, 64) || !hex(&self.predecessor, 64) {
            return Err(Refusal::Contract);
        }
        for image in &self.images {
            image.validate(self.spec.seed.uid, self.spec.seed.gid, 0o700)?;
        }
        for file in [
            &self.lock,
            &self.contract,
            &self.config,
            &self.policy,
            &self.unit,
            &self.manifest,
        ] {
            file.validate(self.spec.seed.uid, self.spec.seed.gid, 0o600)?;
        }
        self.root
            .validate_dir(self.spec.seed.uid, self.spec.seed.gid)
    }
    pub fn digest(&self) -> Result<String> {
        self.validate()?;
        Ok(crate::digest(
            &serde_json::to_vec(self).map_err(|_| Refusal::Contract)?,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePacket {
    pub seed: NamespaceSeed,
    pub epoch: String,
    pub images: [FileId; 3],
}
impl SourcePacket {
    pub fn validate(&self) -> Result<NamespaceSpec> {
        let spec = NamespaceSpec::derive(self.seed.clone())?;
        if !hex(&self.epoch, 64)
            || self.seed.uid != unsafe { libc::geteuid() }
            || self.seed.gid != unsafe { libc::getegid() }
        {
            return Err(Refusal::Contract);
        }
        for image in &self.images {
            image.validate(self.seed.uid, self.seed.gid, 0o700)?;
        }
        Ok(spec)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationPacket {
    pub receipt: PublicationReceipt,
    pub records: Vec<(crate::journal::Record, FileId)>,
}
