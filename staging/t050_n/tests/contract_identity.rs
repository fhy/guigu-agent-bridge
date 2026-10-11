use guigu_t050_n::{
    Refusal,
    contract::{Leaf, NamespaceSeed, NamespaceSpec, UUID},
    identity::{FileId, ObjectId, RetainedImage},
    paths::OwnedDir,
    publication::Anonymous,
};
use std::{
    fs::{self, File},
    os::unix::fs::PermissionsExt,
};

fn spec() -> NamespaceSpec {
    NamespaceSpec::derive(NamespaceSeed {
        uuid: UUID.into(),
        candidate: "a".repeat(40),
        approval: "b".repeat(64),
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    })
    .unwrap()
}
fn root(temp: &tempfile::TempDir) -> OwnedDir {
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let file = File::open(temp.path()).unwrap();
    OwnedDir::authenticate(
        file.try_clone().unwrap(),
        &ObjectId::capture(&file).unwrap(),
    )
    .unwrap()
}
fn image(dir: &OwnedDir, name: &str) -> RetainedImage {
    let source = File::open("/usr/bin/true").unwrap();
    let id = FileId::capture(&source).unwrap();
    let p = Anonymous::copy(dir, &source, &id, 0o700)
        .unwrap()
        .publish(dir, name)
        .unwrap();
    RetainedImage::authenticate(p.file, &p.identity, dir.identity().uid, dir.identity().gid)
        .unwrap()
}
#[test]
fn fresh_contract_roundtrip() {
    let s = spec();
    let encoded = serde_json::to_vec(&s).unwrap();
    let round: NamespaceSpec = serde_json::from_slice(&encoded).unwrap();
    round.validate().unwrap();
    assert_eq!(round, s);
    assert!(
        s.path(Leaf::Lock)
            .ends_with(format!("namespace-{UUID}.lock"))
    );
    assert_eq!(s.ports(), [9391, 9392]);
    assert!(s.unit().contains(UUID));
}
#[test]
fn image_byte_drift() {
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    let original = image(&dir, "image");
    fs::write(temp.path().join("image"), b"drift").unwrap();
    assert_eq!(original.recheck(), Err(Refusal::Identity));
}
#[test]
fn same_byte_inode_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    let original = image(&dir, "image");
    fs::rename(temp.path().join("image"), temp.path().join("retained")).unwrap();
    let other = image(&dir, "image");
    assert_eq!(other.identity().sha256, original.identity().sha256);
    assert_ne!(other.identity().object.ino, original.identity().object.ino);
    assert_eq!(
        dir.verify_leaf("image", original.identity()).err(),
        Some(Refusal::Identity)
    );
    original.recheck().unwrap();
}
#[test]
fn retained_fd_aba() {
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    let original = image(&dir, "image");
    fs::rename(temp.path().join("image"), temp.path().join("original")).unwrap();
    let other = image(&dir, "image");
    assert!(dir.verify_leaf("image", original.identity()).is_err());
    fs::rename(temp.path().join("image"), temp.path().join("replacement")).unwrap();
    fs::rename(temp.path().join("original"), temp.path().join("image")).unwrap();
    dir.verify_leaf("image", original.identity()).unwrap();
    other.recheck().unwrap();
    original.recheck().unwrap();
}
#[test]
fn lexical_escape() {
    let s = spec();
    for path in [
        s.root().join("../escape"),
        s.root().join("x/../../escape"),
        std::path::PathBuf::from("relative"),
        std::path::PathBuf::from("/foreign"),
    ] {
        assert!(!s.accepts(&path));
    }
    let mut seed = s.seed().clone();
    seed.uuid = "fresh-uuid-bypass".into();
    assert_eq!(NamespaceSpec::derive(seed).err(), Some(Refusal::Contract));
}
#[test]
fn symlink_magiclink_escape() {
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    std::os::unix::fs::symlink("/unqueried/foreign", temp.path().join("link")).unwrap();
    std::os::unix::fs::symlink("/proc/self/fd/0", temp.path().join("magic")).unwrap();
    assert!(dir.open("link", libc::O_RDONLY, 0).is_err());
    assert!(dir.open("magic", libc::O_RDONLY, 0).is_err());
    assert!(dir.open("../escape", libc::O_RDONLY, 0).is_err());
}
#[test]
fn image_multilink() {
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    let original = image(&dir, "image");
    fs::hard_link(temp.path().join("image"), temp.path().join("other")).unwrap();
    assert!(original.recheck().is_err());
    let now = FileId::capture(original.file()).unwrap();
    assert_eq!(
        now.validate(dir.identity().uid, dir.identity().gid, 0o700),
        Err(Refusal::Permission)
    );
}
#[test]
fn owner_mode_kind_mismatch() {
    let temp = tempfile::tempdir().unwrap();
    let dir = root(&temp);
    let original = image(&dir, "image");
    let expected = original.identity();
    for field in 0..5 {
        let mut bad = expected.clone();
        match field {
            0 => bad.object.uid = bad.object.uid.wrapping_add(1),
            1 => bad.object.gid = bad.object.gid.wrapping_add(1),
            2 => bad.object.mode = 0o755,
            3 => bad.object.kind = libc::S_IFDIR,
            _ => bad.nlink = 2,
        }
        assert!(
            bad.validate(dir.identity().uid, dir.identity().gid, 0o700)
                .is_err()
        );
    }
}

#[test]
fn entry_missing_descriptors_refuse_before_namespace_access() {
    use std::{os::unix::process::CommandExt, process::Command};
    let cases = [
        (
            env!("CARGO_BIN_EXE_guigu-t050-n-provisioner"),
            "--prepare",
            "identity",
        ),
        (
            env!("CARGO_BIN_EXE_guigu-t050-n-helper"),
            "--worker",
            "channel-identity",
        ),
        (
            env!("CARGO_BIN_EXE_guigu-t050-n-parent"),
            "--worker",
            "channel-identity",
        ),
    ];
    for (binary, mode, category) in cases {
        let mut command = Command::new(binary);
        command
            .args([mode, &"b".repeat(40), &"c".repeat(64)])
            .env_clear();
        unsafe {
            command.pre_exec(|| {
                for fd in 3..=6 {
                    libc::close(fd);
                }
                Ok(())
            });
        }
        let result = command.output().unwrap();
        assert_eq!(result.status.code(), Some(3));
        assert!(result.stdout.is_empty());
        let text = String::from_utf8(result.stderr).unwrap();
        assert!(text.contains(&format!("category={category}")), "{text}");
        assert!(!text.contains(&"b".repeat(40)));
        assert!(!text.contains("provider"));
    }
}
