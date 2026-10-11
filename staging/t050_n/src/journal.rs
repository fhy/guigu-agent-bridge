use crate::{
    Refusal, Result,
    identity::{FileId, ObjectId},
    paths::OwnedDir,
    publication::Anonymous,
};
use serde::{Deserialize, Serialize};
use std::io::Read;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub uuid: String,
    pub root: ObjectId,
    pub epoch: String,
    pub candidate: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Stage {
    Allocated,
    Prepared,
    PublicationIntent,
    ManifestLinked,
    PublicationCommitted,
    OperationReserved,
    ChildIntent,
    ChildAuthenticated,
    ScopeAuthenticated,
    Running,
    Success,
    Failure,
    ExecutionUnknown,
    TeardownReserved,
    TeardownSuccess,
    TeardownFailure,
    TeardownUnknown,
    RevocationIntent,
    Revoked,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Outcome {
    None,
    Exit0,
    Nonzero,
    Interrupt,
    Terminate,
    Timeout,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Reason {
    None,
    IdentityDrift,
    Replacement,
    Missing,
    KnownRetirement,
    Crash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Revocation {
    pub original: FileId,
    pub replacement: Option<ObjectId>,
    pub reason: Reason,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub version: u8,
    pub sequence: u32,
    pub predecessor: String,
    pub binding: Binding,
    pub stage: Stage,
    pub original: Outcome,
    pub cleanup: Outcome,
    pub raw_status: Option<i32>,
    pub receipt: Option<String>,
    pub revocation: Option<Revocation>,
}
impl Record {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.version != 1
            || self.sequence > 128
            || !crate::contract::hex(&self.predecessor, 64)
            || !crate::contract::hex(&self.binding.epoch, 64)
            || !crate::contract::hex(&self.binding.candidate, 40)
            || self.binding.uuid != crate::contract::UUID
            || self
                .raw_status
                .is_some_and(|status| !(0..=65535).contains(&status))
            || self
                .receipt
                .as_ref()
                .is_some_and(|v| !crate::contract::hex(v, 64))
        {
            return Err(Refusal::Journal);
        }
        serde_json::to_vec(self).map_err(|_| Refusal::Journal)
    }
    pub fn hash(&self) -> Result<String> {
        Ok(crate::digest(&self.bytes()?))
    }
}
pub struct Journal {
    dir: OwnedDir,
    binding: Binding,
    records: Vec<Record>,
    witnesses: Vec<FileId>,
    read_only: bool,
}
impl Journal {
    pub fn new(dir: OwnedDir, binding: Binding) -> Self {
        Self {
            dir,
            binding,
            records: Vec::new(),
            witnesses: Vec::new(),
            read_only: false,
        }
    }
    pub fn records(&self) -> &[Record] {
        &self.records
    }
    pub fn last(&self) -> Option<&Record> {
        self.records.last()
    }
    pub fn tip(&self) -> Result<String> {
        self.last().map_or(Ok("0".repeat(64)), Record::hash)
    }
    pub fn append(
        &mut self,
        stage: Stage,
        original: Outcome,
        cleanup: Outcome,
        receipt: Option<String>,
        revocation: Option<Revocation>,
    ) -> Result<()> {
        self.append_status(stage, original, cleanup, receipt, revocation, None)
    }
    pub fn append_status(
        &mut self,
        stage: Stage,
        original: Outcome,
        cleanup: Outcome,
        receipt: Option<String>,
        revocation: Option<Revocation>,
        raw_status: Option<i32>,
    ) -> Result<()> {
        if self.read_only {
            return Err(Refusal::NoAuthority);
        }
        self.validate_history()?;
        if !transition(self.last().map(|v| v.stage), stage) {
            return Err(Refusal::NoAuthority);
        }
        if matches!(stage, Stage::RevocationIntent | Stage::Revoked) != revocation.is_some() {
            return Err(Refusal::Journal);
        }
        if stage == Stage::Revoked
            && self.last().and_then(|r| r.revocation.as_ref()) != revocation.as_ref()
        {
            return Err(Refusal::Journal);
        }
        let record = Record {
            version: 1,
            sequence: self.records.len() as u32,
            predecessor: self.tip()?,
            binding: self.binding.clone(),
            stage,
            original,
            cleanup,
            raw_status,
            receipt,
            revocation,
        };
        let published = Anonymous::bytes(&self.dir, &record.bytes()?, 0o600)?
            .publish(&self.dir, &name(record.sequence))?;
        self.witnesses.push(published.identity);
        self.records.push(record);
        Ok(())
    }
    pub fn validate_history(&self) -> Result<()> {
        self.dir.recheck()?;
        for (record, witness) in self.records.iter().zip(&self.witnesses) {
            self.dir
                .verify_leaf(&name(record.sequence), witness)
                .map_err(|_| Refusal::UnknownRetained)?;
        }
        Ok(())
    }
    pub fn disposition(&self) -> Result<()> {
        self.validate_history()?;
        match self.last().map(|r| r.stage) {
            Some(Stage::Revoked) => Err(Refusal::RevokedUnknown),
            Some(Stage::RevocationIntent) => Err(Refusal::UnknownRetained),
            Some(Stage::PublicationIntent | Stage::ManifestLinked) => {
                Err(Refusal::PublicationUnknown)
            }
            Some(
                Stage::OperationReserved
                | Stage::ChildIntent
                | Stage::ChildAuthenticated
                | Stage::ScopeAuthenticated
                | Stage::Running
                | Stage::ExecutionUnknown,
            ) => Err(Refusal::ExecutionUnknown),
            Some(Stage::TeardownReserved | Stage::TeardownUnknown) => Err(Refusal::UnknownRetained),
            Some(
                Stage::Success | Stage::Failure | Stage::TeardownSuccess | Stage::TeardownFailure,
            ) => Err(Refusal::NoAuthority),
            _ => Ok(()),
        }
    }
    /// Only the original live owner may continue; recovery never creates authority.
    pub fn not_revoked(&self) -> Result<()> {
        self.validate_history()?;
        match self.last().map(|record| record.stage) {
            Some(Stage::Revoked) => Err(Refusal::RevokedUnknown),
            Some(Stage::RevocationIntent) => Err(Refusal::UnknownRetained),
            _ if self.read_only => Err(Refusal::NoAuthority),
            _ => Ok(()),
        }
    }
    /// Restart is always read-only. The caller supplies original record witnesses,
    /// not newly captured current metadata; gaps, extras and drift cannot grant work.
    pub fn recover(dir: OwnedDir, binding: Binding, expected: &[(Record, FileId)]) -> Result<Self> {
        if expected.is_empty() {
            return Err(Refusal::UnknownRetained);
        }
        let mut journal = Self::new(dir, binding);
        for (record, id) in expected {
            if record.binding != journal.binding
                || record.sequence != journal.records.len() as u32
                || record.predecessor != journal.tip()?
                || !transition(journal.last().map(|v| v.stage), record.stage)
            {
                return Err(Refusal::UnknownRetained);
            }
            let mut file = journal.dir.verify_leaf(&name(record.sequence), id)?;
            if id.len > 32768 {
                return Err(Refusal::Length);
            }
            let mut bytes = Vec::new();
            file.by_ref()
                .take(32769)
                .read_to_end(&mut bytes)
                .map_err(|_| Refusal::Journal)?;
            id.recheck(&file)?;
            if record.bytes()? != bytes {
                return Err(Refusal::UnknownRetained);
            }
            journal.records.push(record.clone());
            journal.witnesses.push(id.clone());
        }
        if journal
            .dir
            .open(&name(journal.records.len() as u32), libc::O_RDONLY, 0)
            .is_ok()
        {
            return Err(Refusal::UnknownRetained);
        }
        journal.read_only = true;
        Ok(journal)
    }
    pub fn witnesses(&self) -> Vec<(Record, FileId)> {
        self.records
            .iter()
            .cloned()
            .zip(self.witnesses.clone())
            .collect()
    }
    pub fn binding(&self) -> &Binding {
        &self.binding
    }
}
fn name(sequence: u32) -> String {
    format!("{sequence:08}.receipt")
}
pub fn transition(from: Option<Stage>, to: Stage) -> bool {
    use Stage::*;
    if to == RevocationIntent && !matches!(from, None | Some(Revoked | RevocationIntent)) {
        return true;
    }
    matches!(
        (from, to),
        (None, Allocated)
            | (Some(Allocated), Prepared)
            | (Some(Prepared), PublicationIntent)
            | (Some(PublicationIntent), ManifestLinked)
            | (Some(ManifestLinked), PublicationCommitted)
            | (Some(PublicationCommitted), OperationReserved)
            | (Some(OperationReserved), ChildIntent)
            | (Some(ChildIntent), ChildAuthenticated)
            | (Some(ChildAuthenticated), ScopeAuthenticated)
            | (Some(ScopeAuthenticated), Running)
            | (Some(Running), Success | Failure)
            | (
                Some(
                    OperationReserved
                        | ChildIntent
                        | ChildAuthenticated
                        | ScopeAuthenticated
                        | Running
                ),
                ExecutionUnknown
            )
            | (
                Some(PublicationCommitted | Success | Failure | ExecutionUnknown | ManifestLinked),
                TeardownReserved
            )
            | (
                Some(TeardownReserved),
                TeardownSuccess | TeardownFailure | TeardownUnknown
            )
            | (Some(RevocationIntent), Revoked)
    )
}
