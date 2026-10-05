use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_provision-isolated-proc-proof-fixture")
}

fn write_file(path: &Path, bytes: &[u8], mode: u32) {
    std::fs::write(path, bytes).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

struct Fixture {
    _temp: tempfile::TempDir,
    root: PathBuf,
    parent: PathBuf,
    helper: PathBuf,
    config: PathBuf,
    database: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input");
        std::fs::create_dir(&input).unwrap();
        let parent = input.join("parent");
        let helper = input.join("helper");
        let config = input.join("config.toml");
        let database = input.join("database");
        write_file(&parent, b"parent", 0o755);
        write_file(&helper, b"helper", 0o775);
        write_file(&config, b"not-read", 0o600);
        std::fs::create_dir(&database).unwrap();
        std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o700)).unwrap();
        let root = temp.path().join("fixture-root");
        Self {
            _temp: temp,
            root,
            parent,
            helper,
            config,
            database,
        }
    }

    fn provision(&self) -> Output {
        Command::new(binary())
            .args([
                "provision",
                "--root",
                self.root.to_str().unwrap(),
                "--parent",
                self.parent.to_str().unwrap(),
                "--helper",
                self.helper.to_str().unwrap(),
                "--config",
                self.config.to_str().unwrap(),
                "--database-root",
                self.database.to_str().unwrap(),
                "--generation",
                "7",
            ])
            .output()
            .unwrap()
    }
}

fn mode(path: &Path) -> u32 {
    std::fs::symlink_metadata(path)
        .unwrap()
        .permissions()
        .mode()
        & 0o7777
}

#[test]
fn production_stage_policy_refuses_source_path_overrides() {
    let fixture = Fixture::new();
    for flag in ["--parent", "--helper"] {
        let result = Command::new(binary())
            .args([
                "stage-policy",
                flag,
                fixture.helper.to_str().unwrap(),
                "--config",
                fixture.config.to_str().unwrap(),
                "--database-root",
                fixture.database.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(2), "flag={flag}");
        assert!(result.stdout.is_empty(), "flag={flag}");
        assert!(String::from_utf8_lossy(&result.stderr).starts_with("usage:"));
        assert!(!fixture.root.exists());
        assert_eq!(std::fs::read(&fixture.helper).unwrap(), b"helper");
        assert_eq!(mode(&fixture.helper), 0o775);
    }
}

#[test]
fn destination_helper_0775_is_rejected_without_mutation() {
    let fixture = Fixture::new();
    let destination = fixture
        .root
        .join("usr/libexec/guigu-agent-bridge/t050-proc-proof");
    std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
    write_file(&destination, b"pre-existing-destination", 0o775);

    let result = fixture.provision();
    assert_eq!(result.status.code(), Some(3));
    assert!(result.stdout.is_empty());
    assert!(result.stderr.is_empty());
    assert_eq!(
        std::fs::read(&destination).unwrap(),
        b"pre-existing-destination"
    );
    assert_eq!(mode(&destination), 0o775);
    assert_eq!(std::fs::read(&fixture.parent).unwrap(), b"parent");
    assert_eq!(std::fs::read(&fixture.helper).unwrap(), b"helper");
    assert_eq!(mode(&fixture.helper), 0o775);
    assert_eq!(std::fs::read(&fixture.config).unwrap(), b"not-read");
    assert!(fixture.database.is_dir());
    assert!(
        !fixture
            .root
            .join("etc/guigu-agent-bridge/t050-proc-proof")
            .exists()
    );
}
