//! Standalone namespace authority; no legacy/production dispatch.
#![cfg(target_os = "linux")]

pub mod audit;
pub mod contract;
pub mod identity;
pub mod journal;
pub mod manager;
pub mod native;
pub mod owned_syscalls;
pub mod paths;
pub mod publication;
pub mod readiness;
pub mod secret_channel;
pub mod snapshot;
pub mod supervision;
pub mod syscall_paths;
pub mod teardown;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    Contract,
    Path,
    Identity,
    Permission,
    Length,
    Unsupported,
    Publication,
    PublicationUnknown,
    ExecutionUnknown,
    UnknownRetained,
    RevokedUnknown,
    NoAuthority,
    Journal,
    Lock,
    Process,
    Channel,
    Manager,
    Readiness,
    Timeout,
    Cancelled,
    Nonzero,
    Trace,
    Secret,
}
impl std::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.category())
    }
}
impl std::error::Error for Refusal {}
impl Refusal {
    pub const fn category(self) -> &'static str {
        match self {
            Self::Contract => "contract",
            Self::Path => "path-confinement",
            Self::Identity => "identity",
            Self::Permission => "permission",
            Self::Length => "length",
            Self::Unsupported => "unsupported-boundary",
            Self::Publication => "publication",
            Self::PublicationUnknown => "publication-unknown",
            Self::ExecutionUnknown => "execution-unknown",
            Self::UnknownRetained => "unknown-retained",
            Self::RevokedUnknown => "revoked-unknown",
            Self::NoAuthority => "NoAuthority",
            Self::Journal => "journal",
            Self::Lock => "lock-identity",
            Self::Process => "process-identity",
            Self::Channel => "channel-identity",
            Self::Manager => "manager-identity",
            Self::Readiness => "readiness",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Nonzero => "nonzero",
            Self::Trace => "trace-incomplete",
            Self::Secret => "secret-boundary",
        }
    }
}
pub type Result<T> = std::result::Result<T, Refusal>;

pub fn digest(bytes: &[u8]) -> String {
    use sha2::Digest;
    format!("{:x}", sha2::Sha256::digest(bytes))
}

/// Original allocation and publication authority. Recovery never constructs this owner.
pub struct BoundNamespace {
    pub(crate) spec: contract::NamespaceSpec,
    pub(crate) root: paths::OwnedDir,
    pub(crate) control: paths::OwnedDir,
    pub(crate) lock: paths::NamespaceLock,
    pub(crate) journal: journal::Journal,
    pub(crate) receipt: Option<contract::PublicationReceipt>,
    pub(crate) images: Vec<identity::RetainedImage>,
    pub(crate) config: Option<publication::Published>,
    pub(crate) reservation: Option<String>,
    pub(crate) scope: Option<manager::ScopeFacts>,
    pub(crate) source_owner: identity::FileId,
    refused: bool,
}
impl BoundNamespace {
    pub fn receipt(&self) -> Option<&contract::PublicationReceipt> {
        self.receipt.as_ref()
    }
    pub fn allocate(packet: &contract::SourcePacket) -> Result<Self> {
        let spec = packet.validate()?;
        let base = spec.root().parent().ok_or(Refusal::Contract)?;
        let parent = paths::OwnedDir::open_absolute(base, spec.seed().uid, spec.seed().gid)?;
        let root = parent.mkdir(&spec.seed().uuid)?;
        Self::from_allocation(packet, root)
    }
    pub(crate) fn from_allocation(
        packet: &contract::SourcePacket,
        root: paths::OwnedDir,
    ) -> Result<Self> {
        let spec = packet.validate()?;
        root.identity()
            .validate_dir(spec.seed().uid, spec.seed().gid)?;
        root.recheck()?;
        let control = root.mkdir("control")?;
        let lock = paths::NamespaceLock::create(
            &control,
            &format!("namespace-{}.lock", spec.seed().uuid),
            &packet.epoch,
        )?;
        let dir = root.mkdir("journal")?;
        let binding = journal::Binding {
            uuid: spec.seed().uuid.clone(),
            root: root.identity().clone(),
            epoch: packet.epoch.clone(),
            candidate: spec.seed().candidate.clone(),
        };
        let mut journal = journal::Journal::new(dir, binding);
        journal.append(
            journal::Stage::Allocated,
            journal::Outcome::None,
            journal::Outcome::None,
            None,
            None,
        )?;
        for name in ["images", "config", "state", "units", "evidence"] {
            root.mkdir(name)?;
        }
        Ok(Self {
            spec,
            root,
            control,
            lock,
            journal,
            receipt: None,
            images: Vec::new(),
            config: None,
            reservation: None,
            scope: None,
            source_owner: packet.images[2].clone(),
            refused: false,
        })
    }
    pub fn publish(
        &mut self,
        packet: &contract::SourcePacket,
        sources: [&identity::RetainedImage; 3],
    ) -> Result<()> {
        if self.receipt.is_some() || packet.validate()? != self.spec {
            return Err(Refusal::NoAuthority);
        }
        self.lock.recheck(&self.control)?;
        for (source, expected) in sources.iter().zip(&packet.images) {
            if source.identity() != expected {
                return Err(Refusal::Identity);
            }
            source.recheck()?;
        }
        self.journal.append(
            journal::Stage::Prepared,
            journal::Outcome::None,
            journal::Outcome::None,
            None,
            None,
        )?;
        let dir = self.root.subdir("images")?;
        for (source, name) in sources.into_iter().zip(["parent", "helper", "provisioner"]) {
            let p = publication::Anonymous::copy(&dir, source.file(), source.identity(), 0o700)?
                .publish(&dir, name)?;
            self.images.push(identity::RetainedImage::authenticate(
                p.file,
                &p.identity,
                self.spec.seed().uid,
                self.spec.seed().gid,
            )?);
        }
        self.publish_authority(packet)
    }
    pub fn continuity(&self) -> Result<()> {
        self.journal.not_revoked()?;
        if self.refused {
            return Err(Refusal::UnknownRetained);
        }
        self.root.recheck()?;
        self.lock.recheck(&self.control)?;
        let receipt = self.receipt.as_ref().ok_or(Refusal::PublicationUnknown)?;
        receipt.validate()?;
        self.root
            .subdir("config")?
            .verify_leaf("config.toml", &receipt.config)?;
        let dir = self.root.subdir("images")?;
        if self.images.len() != 3 {
            return Err(Refusal::Identity);
        }
        for (index, (image, name)) in self
            .images
            .iter()
            .zip(["parent", "helper", "provisioner"])
            .enumerate()
        {
            if image.identity() != &receipt.images[index] {
                return Err(Refusal::Identity);
            }
            image.recheck()?;
            dir.verify_leaf(name, image.identity())?;
        }
        for (leaf, id) in [
            (contract::Leaf::Contract, &receipt.contract),
            (contract::Leaf::Policy, &receipt.policy),
            (contract::Leaf::Manifest, &receipt.manifest),
        ] {
            let path = self.spec.path(leaf);
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .ok_or(Refusal::Path)?;
            self.control.verify_leaf(name, id)?;
        }
        self.root
            .subdir("units")?
            .verify_leaf(&format!("{}.v1", self.spec.unit()), &receipt.unit)?;
        Ok(())
    }
    pub fn reserve_operation(&mut self) -> Result<()> {
        self.admission()?;
        self.journal.disposition()?;
        self.journal.append(
            journal::Stage::OperationReserved,
            journal::Outcome::None,
            journal::Outcome::None,
            Some(
                self.receipt
                    .as_ref()
                    .ok_or(Refusal::PublicationUnknown)?
                    .digest()?,
            ),
            None,
        )?;
        self.reservation = Some(self.journal.tip()?);
        Ok(())
    }
    pub(crate) fn admission(&mut self) -> Result<()> {
        match self.continuity() {
            Ok(()) => Ok(()),
            Err(Refusal::RevokedUnknown) => Err(Refusal::RevokedUnknown),
            Err(error) => {
                self.refused = true;
                let Some(receipt) = &self.receipt else {
                    return Err(error);
                };
                let name = format!("artifacts-{}.v1", self.spec.seed().uuid);
                let observed = teardown::observe(&self.control, &name).ok().flatten();
                match teardown::revoke(
                    &mut self.journal,
                    &receipt.manifest,
                    observed,
                    journal::Reason::IdentityDrift,
                    journal::Outcome::Unknown,
                ) {
                    Ok(()) => Err(Refusal::RevokedUnknown),
                    Err(_) => Err(Refusal::UnknownRetained),
                }
            }
        }
    }
    pub fn launcher_witness<'a>(
        &'a self,
        owner: &'a supervision::ChildCertificate,
    ) -> Result<supervision::LaunchWitness<'a>> {
        self.continuity()?;
        let config = self.config.as_ref().ok_or(Refusal::PublicationUnknown)?;
        Ok(supervision::LaunchWitness {
            spec: &self.spec,
            root: &self.root,
            root_id: self.root.identity(),
            control: &self.control,
            lock: &self.lock,
            config: &config.file,
            config_id: &config.identity,
            owner,
        })
    }
}
