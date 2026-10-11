use crate::{
    Refusal, Result,
    contract::PublicationReceipt,
    identity::{FileId, ObjectId},
    journal::{Journal, Outcome, Reason, Revocation, Stage},
    paths::OwnedDir,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeardownTuple {
    pub publication: PublicationReceipt,
    pub reservation: String,
    pub unit: String,
    pub ports: [u16; 2],
    pub invocation: String,
    pub children: Vec<crate::supervision::ChildCertificate>,
    pub terminal: Outcome,
}
impl TeardownTuple {
    pub fn validate(&self, expected: &Self) -> Result<()> {
        self.publication.validate()?;
        if self != expected
            || self.unit != self.publication.spec.unit()
            || self.ports != self.publication.spec.ports()
            || !crate::contract::hex(&self.reservation, 64)
            || !crate::contract::hex(&self.invocation, 32)
            || self.children.len() > 2
        {
            return Err(Refusal::NoAuthority);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Disposition {
    pub original: Outcome,
    pub cleanup: Outcome,
    pub retained: &'static [&'static str],
}
pub const RETAINED: &[&str] = &[
    "publication",
    "replacement-if-any",
    "images",
    "contract",
    "policy",
    "unit-definition",
    "manifest",
    "lock",
    "journal",
    "certificate",
    "isolated-database",
    "snapshot",
];

pub fn revoke(
    journal: &mut Journal,
    original: &FileId,
    replacement: Option<ObjectId>,
    reason: Reason,
    outcome: Outcome,
) -> Result<()> {
    if reason == Reason::None {
        return Err(Refusal::Journal);
    }
    let record = Revocation {
        original: original.clone(),
        replacement,
        reason,
    };
    let cleanup = if reason == Reason::KnownRetirement {
        Outcome::Exit0
    } else {
        Outcome::Unknown
    };
    journal.append(
        Stage::RevocationIntent,
        outcome,
        cleanup,
        None,
        Some(record.clone()),
    )?;
    // Anonymous publication and both fsyncs occur inside append. A crash in between
    // leaves revocation-intent: unknown-retained, never cleanup/adoption authority.
    journal.append(Stage::Revoked, outcome, cleanup, None, Some(record))
}

pub fn observe(dir: &OwnedDir, name: &str) -> Result<Option<ObjectId>> {
    let name = crate::paths::component(name)?;
    dir.recheck()?;
    use std::os::fd::AsRawFd;
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            dir.file().as_raw_fd(),
            name.as_ptr(),
            status.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        return if std::io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT) {
            Ok(None)
        } else {
            Err(Refusal::Identity)
        };
    }
    let status = unsafe { status.assume_init() };
    Ok(Some(ObjectId {
        dev: status.st_dev,
        ino: status.st_ino,
        uid: status.st_uid,
        gid: status.st_gid,
        mode: status.st_mode & 0o7777,
        kind: status.st_mode & libc::S_IFMT,
    }))
}

/// No unlink/rename/quarantine is performed, even for an apparently exact leaf.
pub fn retire_publication(
    journal: &mut Journal,
    dir: &OwnedDir,
    name: &str,
    original: &FileId,
    outcome: Outcome,
) -> Result<Disposition> {
    journal.not_revoked()?;
    if journal.last().map(|record| record.stage) != Some(Stage::TeardownReserved) {
        return Err(Refusal::NoAuthority);
    }
    let observed = observe(dir, name)?;
    let reason = match &observed {
        None => Reason::Missing,
        Some(id) if id != &original.object => Reason::Replacement,
        Some(_) => match dir.verify_leaf(name, original) {
            Ok(_) => Reason::KnownRetirement,
            Err(_) => Reason::IdentityDrift,
        },
    };
    revoke(journal, original, observed, reason, outcome)?;
    if reason != Reason::KnownRetirement {
        return Err(Refusal::RevokedUnknown);
    }
    Ok(Disposition {
        original: outcome,
        cleanup: Outcome::Exit0,
        retained: RETAINED,
    })
}

impl crate::BoundNamespace {
    pub fn reserve_staging_child(
        &mut self,
        owner: &crate::supervision::ChildCertificate,
        nonce: &str,
        channels: &crate::secret_channel::WorkerChannels,
    ) -> Result<crate::supervision::OwnedChild> {
        use crate::contract::Leaf;
        self.admission()?;
        if owner.image != self.source_owner
            || self.journal.last().map(|r| r.stage) != Some(Stage::OperationReserved)
        {
            return Err(Refusal::NoAuthority);
        }
        self.journal
            .append(Stage::ChildIntent, Outcome::None, Outcome::None, None, None)?;
        let argv = vec![
            self.spec
                .path(Leaf::Parent)
                .to_str()
                .ok_or(Refusal::Path)?
                .into(),
            "--worker".into(),
            self.spec.seed().candidate.clone(),
            self.spec.seed().approval.clone(),
        ];
        let child = crate::supervision::OwnedChild::reserve_with_channels(
            &self.images[0],
            &argv,
            nonce,
            self.images[0].identity(),
            &self.launcher_witness(owner)?,
            Some(channels),
        )?;
        let bytes = serde_json::to_vec(child.certificate()).map_err(|_| Refusal::Contract)?;
        crate::publication::Anonymous::bytes(&self.root.subdir("evidence")?, &bytes, 0o600)?
            .publish(&self.root.subdir("evidence")?, "child.v1")?;
        self.journal.append(
            Stage::ChildAuthenticated,
            Outcome::None,
            Outcome::None,
            Some(crate::digest(&bytes)),
            None,
        )?;
        Ok(child)
    }
    pub fn grant_staging_child<C: crate::manager::Connection>(
        &mut self,
        child: &mut crate::supervision::OwnedChild,
        owner: &crate::supervision::ChildCertificate,
        manager: &mut crate::manager::Manager<C>,
    ) -> Result<()> {
        self.admission()?;
        if owner.image != self.source_owner
            || self.journal.last().map(|r| r.stage) != Some(Stage::ChildAuthenticated)
        {
            return Err(Refusal::NoAuthority);
        }
        child.authenticate_reservation(&self.images[0], &self.launcher_witness(owner)?)?;
        let scope = manager.attach(&[child.certificate().clone()])?;
        let hash = crate::digest(&serde_json::to_vec(&scope).map_err(|_| Refusal::Contract)?);
        self.scope = Some(scope);
        self.journal.append(
            Stage::ScopeAuthenticated,
            Outcome::None,
            Outcome::None,
            Some(hash),
            None,
        )?;
        self.journal
            .append(Stage::Running, Outcome::None, Outcome::None, None, None)?;
        child.grant(&self.images[0], &self.launcher_witness(owner)?)
    }
    pub fn record_result(&mut self, original: Outcome, raw_status: Option<i32>) -> Result<()> {
        let stage = if original == Outcome::Exit0 {
            Stage::Success
        } else {
            Stage::Failure
        };
        self.journal
            .append_status(stage, original, Outcome::None, None, None, raw_status)
    }
    pub fn teardown_owned<C: crate::manager::Connection>(
        &mut self,
        tuple: &TeardownTuple,
        child: &mut crate::supervision::OwnedChild,
        manager: &mut crate::manager::Manager<C>,
        prove_ports_free: impl FnOnce([u16; 2]) -> Result<()>,
    ) -> Result<Disposition> {
        self.admission()?;
        let scope = self.scope.as_ref().ok_or(Refusal::NoAuthority)?;
        let expected = TeardownTuple {
            publication: self.receipt.clone().ok_or(Refusal::PublicationUnknown)?,
            reservation: self.reservation.clone().ok_or(Refusal::ExecutionUnknown)?,
            unit: self.spec.unit(),
            ports: self.spec.ports(),
            invocation: scope.invocation.clone(),
            children: vec![child.certificate().clone()],
            terminal: self.journal.last().ok_or(Refusal::Journal)?.original,
        };
        tuple.validate(&expected)?;
        self.journal.append(
            Stage::TeardownReserved,
            tuple.terminal,
            Outcome::None,
            None,
            None,
        )?;
        let result = (|| {
            if !child.reaped() {
                child.stop(tuple.terminal, std::time::Duration::from_secs(10))?;
            }
            manager.retire(scope)?;
            prove_ports_free(self.spec.ports())?;
            retire_publication(
                &mut self.journal,
                &self.control,
                &format!("artifacts-{}.v1", self.spec.seed().uuid),
                &expected.publication.manifest,
                tuple.terminal,
            )
        })();
        if result.is_err() && self.journal.last().map(|r| r.stage) == Some(Stage::TeardownReserved)
        {
            self.journal.append(
                Stage::TeardownUnknown,
                tuple.terminal,
                Outcome::Unknown,
                None,
                None,
            )?;
        }
        result
    }
}
