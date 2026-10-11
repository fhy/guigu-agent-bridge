use guigu_t050_n::{
    Refusal,
    contract::UUID,
    identity::ObjectId,
    journal::{Binding, Journal, Outcome, Reason, Stage},
    paths::OwnedDir,
    publication::Anonymous,
    teardown::{retire_publication, revoke},
};
use std::{
    fs::{self, File},
    os::unix::fs::PermissionsExt,
};
#[path = "ownership_process.rs"]
mod owned_fixtures;

fn owned(path: &std::path::Path) -> OwnedDir {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    let file = File::open(path).unwrap();
    let identity = ObjectId::capture(&file).unwrap();
    OwnedDir::authenticate(file, &identity).unwrap()
}

fn journal(root: &OwnedDir) -> Journal {
    let dir = root.mkdir("journal").unwrap();
    let mut journal = Journal::new(
        dir,
        Binding {
            uuid: UUID.into(),
            root: root.identity().clone(),
            epoch: "a".repeat(64),
            candidate: "b".repeat(40),
        },
    );
    for stage in [
        Stage::Allocated,
        Stage::Prepared,
        Stage::PublicationIntent,
        Stage::ManifestLinked,
        Stage::PublicationCommitted,
        Stage::TeardownReserved,
    ] {
        journal
            .append(stage, Outcome::None, Outcome::None, None, None)
            .unwrap();
    }
    journal
}

#[test]
fn publication_replacement_revokes_without_unlink() {
    let temp = tempfile::tempdir().unwrap();
    let root = owned(temp.path());
    let mut journal = journal(&root);
    let original = Anonymous::bytes(&root, b"original", 0o600)
        .unwrap()
        .publish(&root, "publication")
        .unwrap();
    fs::rename(
        temp.path().join("publication"),
        temp.path().join("retained-original"),
    )
    .unwrap();
    let replacement = Anonymous::bytes(&root, b"replacement", 0o600)
        .unwrap()
        .publish(&root, "publication")
        .unwrap();
    assert_eq!(
        retire_publication(
            &mut journal,
            &root,
            "publication",
            &original.identity,
            Outcome::Nonzero
        ),
        Err(Refusal::RevokedUnknown)
    );
    root.verify_leaf("publication", &replacement.identity)
        .unwrap();
    root.verify_leaf("retained-original", &original.identity)
        .unwrap();
    assert_eq!(journal.disposition(), Err(Refusal::RevokedUnknown));
    assert_eq!(journal.not_revoked(), Err(Refusal::RevokedUnknown));
    let record = journal.last().unwrap();
    assert_eq!(record.original, Outcome::Nonzero);
    assert_eq!(record.cleanup, Outcome::Unknown);
    let revocation = record.revocation.as_ref().unwrap();
    assert_eq!(revocation.original, original.identity);
    assert_eq!(revocation.replacement, Some(replacement.identity.object));
    assert_eq!(revocation.reason, Reason::Replacement);
}

#[test]
fn retained_exact_publication_is_revoked_not_removed() {
    let temp = tempfile::tempdir().unwrap();
    let root = owned(temp.path());
    let mut journal = journal(&root);
    let original = Anonymous::bytes(&root, b"publication", 0o600)
        .unwrap()
        .publish(&root, "publication")
        .unwrap();
    let disposition = retire_publication(
        &mut journal,
        &root,
        "publication",
        &original.identity,
        Outcome::Nonzero,
    )
    .unwrap();
    assert_eq!(disposition.original, Outcome::Nonzero);
    assert_eq!(disposition.cleanup, Outcome::Exit0);
    assert!(disposition.retained.contains(&"publication"));
    root.verify_leaf("publication", &original.identity).unwrap();
    assert_eq!(journal.last().unwrap().cleanup, Outcome::Exit0);
    assert_eq!(journal.disposition(), Err(Refusal::RevokedUnknown));
    assert_eq!(
        retire_publication(
            &mut journal,
            &root,
            "publication",
            &original.identity,
            Outcome::Exit0
        ),
        Err(Refusal::RevokedUnknown)
    );
}

#[test]
fn revocation_intent_crash_is_unknown_retained() {
    let temp = tempfile::tempdir().unwrap();
    let root = owned(temp.path());
    let mut journal = journal(&root);
    let original = Anonymous::bytes(&root, b"publication", 0o600)
        .unwrap()
        .publish(&root, "publication")
        .unwrap();
    let revocation = guigu_t050_n::journal::Revocation {
        original: original.identity.clone(),
        replacement: None,
        reason: Reason::Missing,
    };
    journal
        .append(
            Stage::RevocationIntent,
            Outcome::Interrupt,
            Outcome::Unknown,
            None,
            Some(revocation),
        )
        .unwrap();
    let mut recovered = Journal::recover(
        root.subdir("journal").unwrap(),
        journal.binding().clone(),
        &journal.witnesses(),
    )
    .unwrap();
    assert_eq!(recovered.disposition(), Err(Refusal::UnknownRetained));
    assert_eq!(recovered.not_revoked(), Err(Refusal::UnknownRetained));
    assert_eq!(
        recovered.append(
            Stage::Revoked,
            Outcome::Interrupt,
            Outcome::Unknown,
            None,
            recovered.last().unwrap().revocation.clone()
        ),
        Err(Refusal::NoAuthority)
    );
    root.verify_leaf("publication", &original.identity).unwrap();
}

#[test]
fn revoked_receipt_is_exact_readonly_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let root = owned(temp.path());
    let mut journal = journal(&root);
    let original = Anonymous::bytes(&root, b"publication", 0o600)
        .unwrap()
        .publish(&root, "publication")
        .unwrap();
    revoke(
        &mut journal,
        &original.identity,
        Some(original.identity.object.clone()),
        Reason::IdentityDrift,
        Outcome::Terminate,
    )
    .unwrap();
    let mut recovered = Journal::recover(
        root.subdir("journal").unwrap(),
        journal.binding().clone(),
        &journal.witnesses(),
    )
    .unwrap();
    assert_eq!(recovered.disposition(), Err(Refusal::RevokedUnknown));
    assert_eq!(
        recovered.append(
            Stage::OperationReserved,
            Outcome::None,
            Outcome::None,
            None,
            None
        ),
        Err(Refusal::NoAuthority)
    );
    root.verify_leaf("publication", &original.identity).unwrap();
}

fn cuts(stages: &[Stage], expected: Refusal) {
    let temp = tempfile::tempdir().unwrap();
    let root = owned(temp.path());
    let binding = Binding {
        uuid: UUID.into(),
        root: root.identity().clone(),
        epoch: "a".repeat(64),
        candidate: "b".repeat(40),
    };
    let mut original = Journal::new(root.mkdir("journal").unwrap(), binding.clone());
    for stage in stages {
        original
            .append(*stage, Outcome::None, Outcome::None, None, None)
            .unwrap();
    }
    let mut recovery = Journal::recover(
        root.subdir("journal").unwrap(),
        binding,
        &original.witnesses(),
    )
    .unwrap();
    assert_eq!(recovery.disposition(), Err(expected));
    assert_eq!(
        recovery.append(
            Stage::RevocationIntent,
            Outcome::None,
            Outcome::None,
            None,
            None
        ),
        Err(Refusal::NoAuthority)
    );
}
#[test]
fn publication_crash_cuts() {
    let stages = [
        Stage::Allocated,
        Stage::Prepared,
        Stage::PublicationIntent,
        Stage::ManifestLinked,
    ];
    for end in 3..=4 {
        cuts(&stages[..end], Refusal::PublicationUnknown);
    }
}
#[test]
fn execution_restart_cuts() {
    let stages = [
        Stage::Allocated,
        Stage::Prepared,
        Stage::PublicationIntent,
        Stage::ManifestLinked,
        Stage::PublicationCommitted,
        Stage::OperationReserved,
        Stage::ChildIntent,
        Stage::ChildAuthenticated,
        Stage::ScopeAuthenticated,
        Stage::Running,
        Stage::ExecutionUnknown,
    ];
    for end in 6..=stages.len() {
        cuts(&stages[..end], Refusal::ExecutionUnknown);
    }
}
#[test]
fn teardown_crash_reservation() {
    let stages = [
        Stage::Allocated,
        Stage::Prepared,
        Stage::PublicationIntent,
        Stage::ManifestLinked,
        Stage::PublicationCommitted,
        Stage::TeardownReserved,
    ];
    cuts(&stages, Refusal::UnknownRetained);
}
#[test]
fn successful_owned_teardown() {
    use std::time::Duration;
    let c = owned_fixtures::Context::new();
    let image = c.image("/usr/bin/true", "image");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut child = c.reserve(&image);
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
    let descriptor = listener.as_raw_fd();
    let own = std::fs::metadata(format!("/proc/self/fd/{descriptor}")).unwrap();
    let inherited =
        std::fs::metadata(format!("/proc/{}/fd/{descriptor}", child.certificate().pid)).unwrap();
    assert_eq!(own.ino(), inherited.ino());
    let mut j = journal(&c.root);
    let publication = Anonymous::bytes(&c.root, b"authority", 0o600)
        .unwrap()
        .publish(&c.root, "authority")
        .unwrap();
    assert_eq!(
        child
            .stop(Outcome::Timeout, Duration::from_millis(50))
            .unwrap(),
        (Outcome::Timeout, Outcome::Exit0)
    );
    assert!(child.reaped());
    drop(listener);
    let free = std::net::TcpListener::bind(address).unwrap();
    drop(free);
    let result = retire_publication(
        &mut j,
        &c.root,
        "authority",
        &publication.identity,
        Outcome::Timeout,
    )
    .unwrap();
    assert_eq!(result.original, Outcome::Timeout);
    assert_eq!(result.cleanup, Outcome::Exit0);
    assert_eq!(j.disposition(), Err(Refusal::RevokedUnknown));
    c.root
        .verify_leaf("authority", &publication.identity)
        .unwrap();
}
