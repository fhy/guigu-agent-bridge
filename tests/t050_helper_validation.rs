use std::{
    os::{
        fd::AsRawFd,
        unix::{fs::PermissionsExt, process::CommandExt},
    },
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

    fn stage(&self, output: &Path) -> Output {
        let output_file = std::fs::File::create(output).unwrap();
        let fd = output_file.as_raw_fd();
        let mut command = Command::new(binary());
        command.args([
            "stage-policy",
            "--parent",
            self.parent.to_str().unwrap(),
            "--helper",
            self.helper.to_str().unwrap(),
            "--config",
            self.config.to_str().unwrap(),
            "--database-root",
            self.database.to_str().unwrap(),
        ]);
        unsafe {
            command.pre_exec(move || {
                if libc::dup2(fd, 3) < 0 {
                    Err(std::io::Error::last_os_error())
                } else {
                    libc::fcntl(3, libc::F_SETFD, 0);
                    Ok(())
                }
            });
        }
        command.output().unwrap()
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

fn assert_common_sources_unchanged(fixture: &Fixture) {
    assert_eq!(std::fs::read(&fixture.parent).unwrap(), b"parent");
    assert_eq!(mode(&fixture.parent), 0o755);
    assert_eq!(std::fs::read(&fixture.config).unwrap(), b"not-read");
    assert_eq!(mode(&fixture.config), 0o600);
    assert!(fixture.database.is_dir());
    assert_eq!(mode(&fixture.database), 0o700);
    assert!(!fixture.root.exists());
}

#[test]
fn helper_source_rejections_are_fixed_empty_and_invariant() {
    for case in ["symlink", "nonregular", "non-executable"] {
        let fixture = Fixture::new();
        let outside = fixture._temp.path().join("outside-helper");
        match case {
            "symlink" => {
                std::fs::remove_file(&fixture.helper).unwrap();
                write_file(&outside, b"outside", 0o775);
                std::os::unix::fs::symlink(&outside, &fixture.helper).unwrap();
            }
            "nonregular" => {
                std::fs::remove_file(&fixture.helper).unwrap();
                std::fs::create_dir(&fixture.helper).unwrap();
                std::fs::set_permissions(&fixture.helper, std::fs::Permissions::from_mode(0o755))
                    .unwrap();
            }
            "non-executable" => {
                std::fs::set_permissions(&fixture.helper, std::fs::Permissions::from_mode(0o644))
                    .unwrap();
            }
            _ => unreachable!(),
        }

        let output_path = fixture._temp.path().join("policy-output");
        let result = fixture.stage(&output_path);
        assert_eq!(result.status.code(), Some(3), "case={case}");
        assert!(result.stdout.is_empty(), "case={case}");
        assert_eq!(
            result.stderr, b"stage-policy status=3 cause=helper\n",
            "case={case}"
        );
        assert!(
            std::fs::read(&output_path).unwrap().is_empty(),
            "case={case}"
        );
        assert_common_sources_unchanged(&fixture);

        match case {
            "symlink" => {
                assert!(
                    std::fs::symlink_metadata(&fixture.helper)
                        .unwrap()
                        .file_type()
                        .is_symlink()
                );
                assert_eq!(std::fs::read_link(&fixture.helper).unwrap(), outside);
                assert_eq!(std::fs::read(&outside).unwrap(), b"outside");
                assert_eq!(mode(&outside), 0o775);
            }
            "nonregular" => {
                assert!(fixture.helper.is_dir());
                assert_eq!(mode(&fixture.helper), 0o755);
            }
            "non-executable" => {
                assert_eq!(std::fs::read(&fixture.helper).unwrap(), b"helper");
                assert_eq!(mode(&fixture.helper), 0o644);
            }
            _ => unreachable!(),
        }
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
