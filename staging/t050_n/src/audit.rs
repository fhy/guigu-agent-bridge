use crate::{Refusal, Result, contract::NamespaceSpec, identity::FileId};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, path::Path};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kind {
    Open,
    Stat,
    List,
    Readlink,
    Mutation,
    Manager,
    Exec,
    Signal,
    Adopt,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Label {
    Immutable,
    Namespace,
    Library,
    OwnedProcess,
    Denied,
    Unknown,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub sequence: u32,
    pub predecessor: String,
    pub kind: Kind,
    pub label: Label,
    pub issued: bool,
}
pub struct Immutable {
    pub file: File,
    pub identity: FileId,
    pub label: Label,
}
pub struct AuditBroker {
    spec: NamespaceSpec,
    immutable: BTreeMap<String, Immutable>,
    events: Vec<Event>,
    protected_requests: u64,
    protected_issued: u64,
    unknown: u64,
    children_started: u64,
    children_covered: u64,
    terminal: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Certificate {
    pub schema: String,
    pub uuid: String,
    pub source: String,
    pub images: [String; 3],
    pub contract: String,
    pub protected_reference: String,
    pub trace: String,
    pub observer: String,
    pub coverage: String,
    pub first: u32,
    pub last: u32,
    pub requested: u64,
    pub issued: u64,
    pub unknown: u64,
    pub owned_started: u64,
    pub owned_covered: u64,
    pub terminal_complete: bool,
}
impl Certificate {
    pub fn validate(&self) -> Result<()> {
        if self.schema != "T050N-NOACCESS-1"
            || self.uuid != crate::contract::UUID
            || self.requested != 0
            || self.issued != 0
            || self.unknown != 0
            || self.owned_started != self.owned_covered
            || !self.terminal_complete
            || self.first != 0
            || self.last < self.first
            || !crate::contract::hex(&self.source, 40)
            || !crate::contract::hex(&self.contract, 64)
            || !crate::contract::hex(&self.trace, 64)
            || self.images.iter().any(|v| !crate::contract::hex(v, 64))
            || self.coverage.is_empty()
            || self.observer != "owned-syscall-broker"
        {
            return Err(Refusal::Trace);
        }
        Ok(())
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let value = serde_json::to_value(self).map_err(|_| Refusal::Trace)?;
        serde_json::to_vec(&value).map_err(|_| Refusal::Trace)
    }
    pub fn sha256(&self) -> Result<String> {
        Ok(crate::digest(&self.canonical_bytes()?))
    }
}
impl AuditBroker {
    pub fn new(spec: NamespaceSpec) -> Result<Self> {
        spec.validate()?;
        Ok(Self {
            spec,
            immutable: BTreeMap::new(),
            events: Vec::new(),
            protected_requests: 0,
            protected_issued: 0,
            unknown: 0,
            children_started: 0,
            children_covered: 0,
            terminal: false,
        })
    }
    pub fn register(&mut self, path: &Path, file: File, identity: FileId) -> Result<()> {
        if !self.spec.accepts(path) {
            return Err(Refusal::Path);
        }
        identity.validate(
            self.spec.seed().uid,
            self.spec.seed().gid,
            identity.object.mode,
        )?;
        if ![0o600, 0o700].contains(&identity.object.mode) {
            return Err(Refusal::Permission);
        }
        identity.recheck(&file)?;
        self.insert(path, file, identity, Label::Immutable)
    }
    pub fn register_library(&mut self, alias: &Path, file: File, identity: FileId) -> Result<()> {
        let text = alias.to_str().ok_or(Refusal::Path)?;
        if !text.starts_with("/usr/lib/")
            && !text.starts_with("/lib/")
            && !text.starts_with("/lib64/")
            && text != "/etc/ld.so.cache"
        {
            return Err(Refusal::Path);
        }
        if text.contains("..")
            || text.contains("//")
            || identity.object.uid != 0
            || identity.object.mode & 0o022 != 0
            || identity.object.kind != libc::S_IFREG
            || identity.nlink != 1
        {
            return Err(Refusal::Permission);
        }
        identity.recheck(&file)?;
        self.insert(alias, file, identity, Label::Library)
    }
    fn insert(&mut self, path: &Path, file: File, identity: FileId, label: Label) -> Result<()> {
        let name = path.to_str().ok_or(Refusal::Path)?.to_owned();
        if self.immutable.contains_key(&name) {
            return Err(Refusal::Identity);
        }
        self.immutable.insert(
            name,
            Immutable {
                file,
                identity,
                label,
            },
        );
        Ok(())
    }
    pub fn open_immutable(&mut self, path: &Path, flags: i32) -> Result<File> {
        let key = path.to_str().ok_or(Refusal::Path)?;
        let allowed = libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_LARGEFILE;
        if flags & !allowed != libc::O_RDONLY {
            self.event(Kind::Open, Label::Denied, false)?;
            return Err(Refusal::NoAuthority);
        }
        let Some(entry) = self.immutable.get(key) else {
            self.protected_requests += 1;
            self.event(Kind::Open, Label::Denied, false)?;
            return Err(Refusal::Path);
        };
        entry.identity.recheck(&entry.file)?;
        let file = entry.file.try_clone().map_err(|_| Refusal::Identity)?;
        let label = entry.label;
        self.event(Kind::Open, label, true)?;
        Ok(file)
    }
    pub fn event(&mut self, kind: Kind, label: Label, issued: bool) -> Result<()> {
        if self.events.len() >= 65536 || self.terminal {
            return Err(Refusal::Trace);
        }
        if label == Label::Unknown {
            self.unknown += 1;
        }
        let predecessor = self.trace_digest()?;
        self.events.push(Event {
            sequence: self.events.len() as u32,
            predecessor,
            kind,
            label,
            issued,
        });
        Ok(())
    }
    pub fn start_child(&mut self) -> Result<()> {
        if self.terminal {
            return Err(Refusal::Trace);
        }
        self.children_started += 1;
        Ok(())
    }
    pub fn cover_child(&mut self) -> Result<()> {
        if self.children_covered >= self.children_started {
            return Err(Refusal::Trace);
        }
        self.children_covered += 1;
        Ok(())
    }
    pub fn finish(&mut self) {
        self.terminal = true;
    }
    pub fn trace_digest(&self) -> Result<String> {
        Ok(crate::digest(
            &serde_json::to_vec(&self.events).map_err(|_| Refusal::Trace)?,
        ))
    }
    pub fn certificate(
        &self,
        images: [String; 3],
        contract: String,
        coverage: &str,
    ) -> Result<Certificate> {
        let certificate = Certificate {
            schema: "T050N-NOACCESS-1".into(),
            uuid: self.spec.seed().uuid.clone(),
            source: self.spec.seed().candidate.clone(),
            images,
            contract,
            protected_reference: "governance/permanent-option-1; no protected metadata sampling"
                .into(),
            trace: self.trace_digest()?,
            observer: "owned-syscall-broker".into(),
            coverage: coverage.into(),
            first: 0,
            last: self.events.len() as u32,
            requested: self.protected_requests,
            issued: self.protected_issued,
            unknown: self.unknown,
            owned_started: self.children_started,
            owned_covered: self.children_covered,
            terminal_complete: self.terminal,
        };
        certificate.validate()?;
        Ok(certificate)
    }
    pub fn events(&self) -> &[Event] {
        &self.events
    }
}
