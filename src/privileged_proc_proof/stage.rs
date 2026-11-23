use super::*;

pub const PRODUCTION_PARENT_SOURCE: &str =
    "/home/fhy/.local/lib/guigu-codex-test/guigu-agent-bridge";
pub const PRODUCTION_HELPER_SOURCE: &str =
    "/home/fhy/.cache/cargo-target/guigu-agent-bridge/debug/guigu-t050-proc-proof";

#[derive(Debug, Clone)]
pub struct FixturePaths {
    pub root: PathBuf,
    pub parent: PathBuf,
    pub helper: PathBuf,
    pub config: PathBuf,
    pub database_root: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub enum StageCause {
    Parent,
    Helper,
    Config,
    Database,
    Output,
}

impl StageCause {
    pub fn code(self) -> &'static str {
        match self {
            Self::Parent => "parent",
            Self::Helper => "helper",
            Self::Config => "config",
            Self::Database => "database",
            Self::Output => "output",
        }
    }
}

pub fn stage_policy(
    config: &Path,
    database_root: &Path,
    output_fd: RawFd,
) -> Result<[u8; 32], StageCause> {
    let paths = FixturePaths {
        root: PathBuf::new(),
        parent: PathBuf::from(PRODUCTION_PARENT_SOURCE),
        helper: PathBuf::from(PRODUCTION_HELPER_SOURCE),
        config: config.to_path_buf(),
        database_root: database_root.to_path_buf(),
    };
    stage_policy_for_sources(
        &paths,
        Path::new(PRODUCTION_PARENT_SOURCE),
        Path::new(PRODUCTION_HELPER_SOURCE),
        output_fd,
    )
}

fn stage_policy_for_sources(
    paths: &FixturePaths,
    expected_parent: &Path,
    expected_helper: &Path,
    output_fd: RawFd,
) -> Result<[u8; 32], StageCause> {
    if paths.parent != expected_parent {
        return Err(StageCause::Parent);
    }
    if paths.helper != expected_helper {
        return Err(StageCause::Helper);
    }
    let parent = metadata_identity(&paths.parent).map_err(|_| StageCause::Parent)?;
    let helper = source_helper_identity(&paths.helper).map_err(|_| StageCause::Helper)?;
    let config = metadata_identity(&paths.config).map_err(|_| StageCause::Config)?;
    let database = metadata_identity(&paths.database_root).map_err(|_| StageCause::Database)?;
    if parent.file_type != libc::S_IFREG || parent.mode != 0o755 {
        return Err(StageCause::Parent);
    }
    if helper.uid != parent.uid || helper.gid != parent.gid {
        return Err(StageCause::Helper);
    }
    if config.file_type != libc::S_IFREG {
        return Err(StageCause::Config);
    }
    if database.file_type != libc::S_IFDIR {
        return Err(StageCause::Database);
    }
    let policy = Policy {
        operator_uid: parent.uid,
        operator_gid: parent.gid,
        binary: parent,
        config,
        database_root: database,
        admission_lock: b".runtime-admission.lock".to_vec(),
        unit: b"guigu-codex-test.service".to_vec(),
        ports: [9291, 9292],
    };
    let bytes = policy.encode();
    let duplicate = unsafe { libc::dup(output_fd) };
    if duplicate < 0 {
        return Err(StageCause::Output);
    }
    let mut output = unsafe { File::from_raw_fd(duplicate) };
    output
        .write_all(&bytes)
        .and_then(|_| output.sync_all())
        .map_err(|_| StageCause::Output)?;
    Ok(Sha256::digest(&bytes).into())
}

fn source_helper_identity(path: &Path) -> Result<FileIdentity, ()> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::symlink_metadata(path).map_err(|_| ())?;
    let identity = FileIdentity {
        path: path.as_os_str().as_bytes().to_vec(),
        device: metadata.dev(),
        inode: metadata.ino(),
        file_type: metadata.mode() & libc::S_IFMT,
        mode: metadata.mode() & 0o7777,
        uid: metadata.uid(),
        gid: metadata.gid(),
    };
    if identity.file_type != libc::S_IFREG || identity.mode & 0o111 == 0 {
        return Err(());
    }
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::{fs::PermissionsExt, process::CommandExt};
    use std::process::Command;

    fn write_file(path: &Path, bytes: &[u8], mode: u32) {
        std::fs::write(path, bytes).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    #[test]
    #[ignore]
    fn stage_fixture_child() {
        let Some(parent) = std::env::var_os("T050_STAGE_PARENT") else {
            return;
        };
        let paths = FixturePaths {
            root: PathBuf::new(),
            parent: PathBuf::from(parent),
            helper: PathBuf::from(std::env::var_os("T050_STAGE_HELPER").unwrap()),
            config: PathBuf::from(std::env::var_os("T050_STAGE_CONFIG").unwrap()),
            database_root: PathBuf::from(std::env::var_os("T050_STAGE_DATABASE").unwrap()),
        };
        let expected_parent = PathBuf::from(std::env::var_os("T050_STAGE_EXPECT_PARENT").unwrap());
        let expected_helper = PathBuf::from(std::env::var_os("T050_STAGE_EXPECT_HELPER").unwrap());
        match stage_policy_for_sources(&paths, &expected_parent, &expected_helper, 3) {
            Ok(_) => std::process::exit(0),
            Err(cause) => {
                eprintln!("stage-policy status=3 cause={}", cause.code());
                std::process::exit(3);
            }
        }
    }

    fn run_child(
        paths: &FixturePaths,
        expected_parent: &Path,
        expected_helper: &Path,
        output: &Path,
    ) -> std::process::Output {
        let output_file = File::create(output).unwrap();
        let fd = output_file.as_raw_fd();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "privileged_proc_proof::stage::tests::stage_fixture_child",
                "--nocapture",
            ])
            .env("T050_STAGE_PARENT", &paths.parent)
            .env("T050_STAGE_HELPER", &paths.helper)
            .env("T050_STAGE_CONFIG", &paths.config)
            .env("T050_STAGE_DATABASE", &paths.database_root)
            .env("T050_STAGE_EXPECT_PARENT", expected_parent)
            .env("T050_STAGE_EXPECT_HELPER", expected_helper);
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

    #[test]
    fn fixed_equivalent_0775_source_is_process_success() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("parent");
        let helper = temp.path().join("helper");
        let config = temp.path().join("config");
        let database = temp.path().join("database");
        write_file(&parent, b"parent", 0o755);
        write_file(&helper, b"helper", 0o775);
        write_file(&config, b"not-read", 0o600);
        std::fs::create_dir(&database).unwrap();
        std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o700)).unwrap();
        let output_path = temp.path().join("policy-output");
        let paths = FixturePaths {
            root: temp.path().join("destination"),
            parent: parent.clone(),
            helper: helper.clone(),
            config,
            database_root: database,
        };
        let result = run_child(&paths, &parent, &helper, &output_path);
        assert_eq!(result.status.code(), Some(0));
        assert!(result.stderr.is_empty());
        assert_eq!(&std::fs::read(output_path).unwrap()[..8], b"GGPFP001");
        assert!(!paths.root.exists());
        assert_eq!(std::fs::read(&helper).unwrap(), b"helper");
        assert_eq!(
            std::fs::metadata(&helper).unwrap().permissions().mode() & 0o777,
            0o775
        );
    }

    #[test]
    fn source_path_owner_and_shape_refusals_are_process_bounded() {
        use std::os::unix::fs::MetadataExt;

        for case in [
            "foreign",
            "owner",
            "symlink",
            "nonregular",
            "non-executable",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let parent = temp.path().join("parent");
            let mut helper = temp.path().join("helper");
            let expected_helper = temp.path().join("expected-helper");
            let config = temp.path().join("config");
            let database = temp.path().join("database");
            write_file(&parent, b"parent", 0o755);
            write_file(&helper, b"helper", 0o775);
            write_file(&config, b"not-read", 0o600);
            std::fs::create_dir(&database).unwrap();
            std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o700)).unwrap();

            let mut expected = helper.clone();
            let outside = temp.path().join("outside");
            match case {
                "foreign" => {
                    write_file(&expected_helper, b"reviewed", 0o775);
                    expected = expected_helper.clone();
                }
                "owner" if unsafe { libc::geteuid() } == 0 => {
                    let helper_c = std::ffi::CString::new(helper.as_os_str().as_bytes()).unwrap();
                    assert_eq!(unsafe { libc::chown(helper_c.as_ptr(), 1, 1) }, 0);
                }
                "owner" => {
                    helper = PathBuf::from("/bin/true");
                    let metadata = std::fs::metadata(&helper).unwrap();
                    assert!(metadata.uid() != unsafe { libc::geteuid() as u32 });
                }
                "symlink" => {
                    std::fs::remove_file(&helper).unwrap();
                    write_file(&outside, b"outside", 0o775);
                    std::os::unix::fs::symlink(&outside, &helper).unwrap();
                }
                "nonregular" => {
                    std::fs::remove_file(&helper).unwrap();
                    std::fs::create_dir(&helper).unwrap();
                }
                "non-executable" => {
                    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o644))
                        .unwrap();
                }
                _ => {}
            }

            let helper_metadata = std::fs::symlink_metadata(&helper).unwrap();
            let helper_bytes = if helper_metadata.is_file() {
                Some(std::fs::read(&helper).unwrap())
            } else {
                None
            };
            let output_path = temp.path().join("policy-output");
            let paths = FixturePaths {
                root: temp.path().join("destination"),
                parent: parent.clone(),
                helper: helper.clone(),
                config: config.clone(),
                database_root: database.clone(),
            };
            let result = run_child(&paths, &parent, &expected, &output_path);
            assert_eq!(result.status.code(), Some(3), "case={case}");
            assert_eq!(
                result.stderr, b"stage-policy status=3 cause=helper\n",
                "case={case}"
            );
            assert!(
                std::fs::read(&output_path).unwrap().is_empty(),
                "case={case}"
            );
            assert!(!paths.root.exists(), "case={case}");
            assert_eq!(std::fs::read(&parent).unwrap(), b"parent", "case={case}");
            assert_eq!(std::fs::read(&config).unwrap(), b"not-read", "case={case}");
            let after = std::fs::symlink_metadata(&helper).unwrap();
            assert_eq!(
                after.file_type(),
                helper_metadata.file_type(),
                "case={case}"
            );
            assert_eq!(after.mode(), helper_metadata.mode(), "case={case}");
            assert_eq!(after.uid(), helper_metadata.uid(), "case={case}");
            assert_eq!(after.gid(), helper_metadata.gid(), "case={case}");
            if let Some(bytes) = helper_bytes {
                assert_eq!(std::fs::read(&helper).unwrap(), bytes, "case={case}");
            }
            if case == "symlink" {
                assert_eq!(std::fs::read_link(&helper).unwrap(), outside);
                assert_eq!(std::fs::read(&outside).unwrap(), b"outside");
            }
        }
    }
}
