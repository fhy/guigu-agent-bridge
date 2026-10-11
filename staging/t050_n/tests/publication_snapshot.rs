use guigu_t050_n::{
    Refusal,
    identity::{FileId, ObjectId},
    paths::OwnedDir,
    publication::{Anonymous, Manifest},
    snapshot::{SnapshotReference, publish_closed},
    teardown::retire_publication,
};
use std::{
    fs::{self, File},
    os::unix::fs::PermissionsExt,
};
fn receipt() -> guigu_t050_n::contract::PublicationReceipt {
    use guigu_t050_n::contract::UUID;
    use guigu_t050_n::contract::{NamespaceSeed, NamespaceSpec, PublicationReceipt};
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    let spec = NamespaceSpec::derive(NamespaceSeed {
        uuid: UUID.into(),
        candidate: "a".repeat(40),
        approval: "b".repeat(64),
        uid,
        gid,
    })
    .unwrap();
    let id = |ino, mode| guigu_t050_n::identity::FileId {
        object: ObjectId {
            dev: 1,
            ino,
            uid,
            gid,
            mode,
            kind: libc::S_IFREG,
        },
        len: 3,
        nlink: 1,
        sha256: "a".repeat(64),
    };
    PublicationReceipt {
        spec,
        root: ObjectId {
            dev: 1,
            ino: 2,
            uid,
            gid,
            mode: 0o700,
            kind: libc::S_IFDIR,
        },
        lock: id(3, 0o600),
        epoch: "c".repeat(64),
        images: [id(4, 0o700), id(5, 0o700), id(6, 0o700)],
        config: id(7, 0o600),
        contract: id(8, 0o600),
        policy: id(9, 0o600),
        unit: id(10, 0o600),
        manifest: id(11, 0o600),
        generation: 1,
        predecessor: "d".repeat(64),
    }
}
#[test]
fn foreign_teardown_tuple() {
    use guigu_t050_n::journal::Outcome;
    use guigu_t050_n::teardown::TeardownTuple;
    let publication = receipt();
    let original = TeardownTuple {
        unit: publication.spec.unit(),
        ports: publication.spec.ports(),
        publication,
        reservation: "e".repeat(64),
        invocation: "f".repeat(32),
        children: vec![],
        terminal: Outcome::Exit0,
    };
    original.validate(&original).unwrap();
    for field in 0..6 {
        let mut bad = original.clone();
        match field {
            0 => bad.publication.root.ino += 1,
            1 => bad.unit = "unqueried-foreign.scope".into(),
            2 => bad.ports[0] = 9191,
            3 => bad.reservation = "a".repeat(64),
            4 => bad.publication.manifest.object.ino += 1,
            _ => bad.invocation = "b".repeat(32),
        };
        assert_eq!(bad.validate(&original), Err(Refusal::NoAuthority));
    }
}
fn root(temp: &tempfile::TempDir) -> OwnedDir {
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let f = File::open(temp.path()).unwrap();
    let id = ObjectId::capture(&f).unwrap();
    OwnedDir::authenticate(f, &id).unwrap()
}
#[test]
fn descriptor_publication_source_race() {
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    let source = Anonymous::bytes(&dir, b"authenticated", 0o600).unwrap();
    // A competing name is irrelevant: publication selects the retained anonymous FD.
    Anonymous::bytes(&dir, b"foreign", 0o600)
        .unwrap()
        .publish(&dir, "temporary-name")
        .unwrap();
    let published = source.publish(&dir, "final").unwrap();
    assert_eq!(
        published.identity.sha256,
        guigu_t050_n::digest(b"authenticated")
    );
    assert_eq!(published.identity.nlink, 1);
    dir.verify_leaf("final", &published.identity).unwrap();
}
#[test]
fn final_name_no_replace() {
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    let first = Anonymous::bytes(&dir, b"first", 0o600)
        .unwrap()
        .publish(&dir, "final")
        .unwrap();
    assert_eq!(
        Anonymous::bytes(&dir, b"second", 0o600)
            .unwrap()
            .publish(&dir, "final")
            .err(),
        Some(Refusal::Publication)
    );
    dir.verify_leaf("final", &first.identity).unwrap();
}
#[test]
fn manifest_version_checksum_drift() {
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    let p = Anonymous::bytes(&dir, b"image", 0o700)
        .unwrap()
        .publish(&dir, "image")
        .unwrap();
    let m = Manifest {
        version: 1,
        candidate: "a".repeat(40),
        images: std::array::from_fn(|_| p.identity.clone()),
        objects: vec![],
    };
    let bytes = m.encode().unwrap();
    assert_eq!(Manifest::decode(&bytes).unwrap(), m);
    for index in [0, 17, bytes.len() - 1] {
        let mut bad = bytes.clone();
        bad[index] ^= 1;
        assert!(Manifest::decode(&bad).is_err());
    }
    let mut bad = m.clone();
    bad.version = 2;
    assert!(bad.encode().is_err());
}
#[test]
fn snapshot_hash_reference_drift() {
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    let path = temp.path().join("source.sqlite3");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let pool = guigu_agent_bridge::storage::connect(&path).await.unwrap();
            guigu_agent_bridge::storage::migrate(&pool).await.unwrap();
            pool.close().await;
        });
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let source = File::open(&path).unwrap();
    let source_id = FileId::capture(&source).unwrap();
    let (snapshot, expected) = publish_closed(&dir, &source, &source_id).unwrap();
    expected.verify(&snapshot.file).unwrap();
    assert_ne!(snapshot.identity.object.ino, source_id.object.ino);
    for field in 0..7 {
        let mut bad = expected.clone();
        match field {
            0 => bad.device ^= 1,
            1 => bad.inode ^= 1,
            2 => bad.size += 1,
            3 => bad.uid ^= 1,
            4 => bad.gid ^= 1,
            5 => bad.mode = 0o644,
            _ => bad.sha256 = "f".repeat(64),
        }
        assert!(bad.verify(&snapshot.file).is_err());
    }
    assert_eq!(
        SnapshotReference::capture(&FileId::capture(&snapshot.file).unwrap()),
        expected
    );
}
#[test]
fn publication_receipt_missing() {
    use guigu_t050_n::{
        contract::UUID,
        journal::{Binding, Journal, Outcome, Stage},
    };
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    let mut j = Journal::new(
        dir.mkdir("journal").unwrap(),
        Binding {
            uuid: UUID.into(),
            root: dir.identity().clone(),
            epoch: "a".repeat(64),
            candidate: "b".repeat(40),
        },
    );
    for stage in [
        Stage::Allocated,
        Stage::Prepared,
        Stage::PublicationIntent,
        Stage::ManifestLinked,
    ] {
        j.append(stage, Outcome::None, Outcome::None, None, None)
            .unwrap();
    }
    assert_eq!(j.disposition(), Err(Refusal::PublicationUnknown));
}

use guigu_t050_n::{
    contract::UUID,
    journal::{Binding, Journal, Outcome, Stage},
};
fn retired_journal(root: &OwnedDir) -> Journal {
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
fn missing_publication_still_records_original_failure() {
    let temp = tempfile::tempdir().unwrap();
    let root = root(&temp);
    let mut journal = retired_journal(&root);
    let original = Anonymous::bytes(&root, b"publication", 0o600)
        .unwrap()
        .publish(&root, "publication")
        .unwrap();
    fs::rename(
        temp.path().join("publication"),
        temp.path().join("retained"),
    )
    .unwrap();
    assert_eq!(
        retire_publication(
            &mut journal,
            &root,
            "publication",
            &original.identity,
            Outcome::Timeout
        ),
        Err(Refusal::RevokedUnknown)
    );
    let record = journal.last().unwrap();
    assert_eq!(record.original, Outcome::Timeout);
    assert_eq!(record.cleanup, Outcome::Unknown);
    assert_eq!(record.revocation.as_ref().unwrap().replacement, None);
    root.verify_leaf("retained", &original.identity).unwrap();
}

#[test]
fn recovered_nonterminal_journal_cannot_reserve() {
    let temp = tempfile::tempdir().unwrap();
    let root = root(&temp);
    let binding = Binding {
        uuid: UUID.into(),
        root: root.identity().clone(),
        epoch: "a".repeat(64),
        candidate: "b".repeat(40),
    };
    let mut original = Journal::new(root.mkdir("journal").unwrap(), binding.clone());
    original
        .append(Stage::Allocated, Outcome::None, Outcome::None, None, None)
        .unwrap();
    let mut recovered = Journal::recover(
        root.subdir("journal").unwrap(),
        binding,
        &original.witnesses(),
    )
    .unwrap();
    assert_eq!(recovered.not_revoked(), Err(Refusal::NoAuthority));
    assert_eq!(
        recovered.append(Stage::Prepared, Outcome::None, Outcome::None, None, None),
        Err(Refusal::NoAuthority)
    );
}
