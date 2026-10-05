use std::{
    os::{
        fd::AsRawFd,
        unix::{fs::PermissionsExt, process::CommandExt},
    },
    path::Path,
    process::Command,
};

fn file(path: &Path, bytes: &[u8], mode: u32) {
    std::fs::write(path, bytes).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn fixture() -> (tempfile::TempDir, Vec<String>) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("fixture-root");
    let input = temp.path().join("input");
    std::fs::create_dir(&input).unwrap();
    let parent = input.join("parent");
    let helper = input.join("helper");
    let config = input.join("config.toml");
    let database_root = input.join("database");
    file(&parent, b"parent", 0o755);
    file(&helper, b"helper", 0o755);
    file(&config, b"not-read", 0o600);
    std::fs::create_dir(&database_root).unwrap();
    std::fs::set_permissions(&database_root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let args = vec![
        "provision".into(),
        "--root".into(),
        root.display().to_string(),
        "--parent".into(),
        parent.display().to_string(),
        "--helper".into(),
        helper.display().to_string(),
        "--config".into(),
        config.display().to_string(),
        "--database-root".into(),
        database_root.display().to_string(),
        "--generation".into(),
        "7".into(),
    ];
    (temp, args)
}

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_provision-isolated-proc-proof-fixture")
}

#[test]
fn manifest_last_repeat_refusal_and_manifest_first_teardown_are_bounded() {
    let (temp, args) = fixture();
    let root = temp.path().join("fixture-root");
    assert!(
        Command::new(binary())
            .args(&args)
            .status()
            .unwrap()
            .success()
    );
    let artifacts = root.join("etc/guigu-agent-bridge/t050-proc-proof");
    let manifest = artifacts.join("artifacts.v1");
    let policy = artifacts.join("policy.v1");
    let helper = root.join("usr/libexec/guigu-agent-bridge/t050-proc-proof");
    assert_eq!(std::fs::read(&manifest).unwrap().len(), 256);
    assert_eq!(&std::fs::read(&manifest).unwrap()[..8], b"GGPFM001");
    assert_eq!(&std::fs::read(&policy).unwrap()[..8], b"GGPFP001");
    assert_eq!(
        std::fs::metadata(&helper).unwrap().permissions().mode() & 0o777,
        0o750
    );
    assert_eq!(
        Command::new(binary()).args(&args).status().unwrap().code(),
        Some(3)
    );
    assert!(
        Command::new(binary())
            .args(["teardown", "--root", root.to_str().unwrap()])
            .status()
            .unwrap()
            .success()
    );
    assert!(!manifest.exists() && !policy.exists() && !helper.exists());
}

#[test]
fn failure_points_roll_back_and_system_roots_are_refused() {
    for stage in [
        "after-policy",
        "after-helper",
        "before-manifest",
        "after-manifest",
    ] {
        let (temp, args) = fixture();
        let root = temp.path().join("fixture-root");
        assert_eq!(
            Command::new(binary())
                .args(&args)
                .env("T050_PROVISION_TEST_FAIL", stage)
                .status()
                .unwrap()
                .code(),
            Some(3)
        );
        assert!(
            !root
                .join("etc/guigu-agent-bridge/t050-proc-proof/artifacts.v1")
                .exists()
        );
        assert!(
            !root
                .join("etc/guigu-agent-bridge/t050-proc-proof/policy.v1")
                .exists()
        );
        assert!(
            !root
                .join("usr/libexec/guigu-agent-bridge/t050-proc-proof")
                .exists()
        );
    }
    let (temp, mut args) = fixture();
    let root_index = args.iter().position(|arg| arg == "--root").unwrap() + 1;
    args[root_index] = "/etc".into();
    assert_eq!(
        Command::new(binary()).args(&args).status().unwrap().code(),
        Some(3)
    );
    drop(temp);
}

#[test]
fn production_mode_has_fixed_interface_and_refuses_overrides() {
    assert_eq!(
        Command::new(binary())
            .args(["production-provision", "--root", "/tmp/override"])
            .status()
            .unwrap()
            .code(),
        Some(3)
    );
    assert_eq!(
        Command::new(binary())
            .args(["production-provision", "--parent-sha", "00"])
            .status()
            .unwrap()
            .code(),
        Some(3)
    );
    assert_eq!(
        Command::new(binary())
            .args(["production-teardown", "--root", "/tmp/override"])
            .status()
            .unwrap()
            .code(),
        Some(3)
    );
}

#[test]
fn hostile_preexisting_leaves_are_untouched() {
    let (temp, args) = fixture();
    let root = temp.path().join("fixture-root");
    let manifest = root.join("etc/guigu-agent-bridge/t050-proc-proof/artifacts.v1");
    std::fs::create_dir_all(manifest.parent().unwrap()).unwrap();
    let target = temp.path().join("outside");
    std::fs::write(&target, b"outside").unwrap();
    std::os::unix::fs::symlink(&target, &manifest).unwrap();
    assert_eq!(
        Command::new(binary()).args(&args).status().unwrap().code(),
        Some(3)
    );
    assert!(manifest.is_symlink());
    assert_eq!(std::fs::read(&target).unwrap(), b"outside");
}

#[test]
fn policy_staging_emits_only_canonical_bytes_and_is_repeatable() {
    let (temp, args) = fixture();
    let input = temp.path().join("input");
    let output1 = temp.path().join("policy-1");
    let output2 = temp.path().join("policy-2");
    let config = input.join("config.toml");
    let database = input.join("database");
    let stage = [
        "stage-policy",
        "--config",
        config.to_str().unwrap(),
        "--database-root",
        database.to_str().unwrap(),
    ];
    for output in [&output1, &output2] {
        let file = std::fs::File::create(output).unwrap();
        let fd = file.as_raw_fd();
        let mut command = Command::new(binary());
        command.args(stage);
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
        let status = command.status().unwrap();
        assert!(status.success());
    }
    assert_eq!(
        std::fs::read(&output1).unwrap(),
        std::fs::read(&output2).unwrap()
    );
    assert_eq!(&std::fs::read(&output1).unwrap()[..8], b"GGPFP001");
    assert!(!temp.path().join("fixture-root").exists());
    drop(args);
}

#[test]
fn policy_staging_refusals_are_fixed_empty_and_invariant() {
    for cause in ["config", "database", "output"] {
        let (temp, _) = fixture();
        let input = temp.path().join("input");
        let config = input.join("config.toml");
        let database = input.join("database");
        match cause {
            "config" => {
                std::fs::remove_file(&config).unwrap();
                std::fs::create_dir(&config).unwrap();
            }
            "database" => {
                std::fs::remove_dir(&database).unwrap();
                std::fs::write(&database, b"not-a-directory").unwrap();
            }
            "output" => {}
            _ => unreachable!(),
        }
        let config_before = if config.is_file() {
            Some(std::fs::read(&config).unwrap())
        } else {
            None
        };
        let output = temp.path().join("policy-output");
        let file = std::fs::File::create(&output).unwrap();
        let fd = file.as_raw_fd();
        let mut command = Command::new(binary());
        command.args([
            "stage-policy",
            "--config",
            config.to_str().unwrap(),
            "--database-root",
            database.to_str().unwrap(),
        ]);
        if cause != "output" {
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
        }
        let result = command.output().unwrap();
        assert_eq!(result.status.code(), Some(3));
        assert_eq!(
            result.stderr,
            format!("stage-policy status=3 cause={cause}\n").as_bytes()
        );
        assert!(std::fs::read(&output).unwrap().is_empty());
        assert!(!temp.path().join("fixture-root").exists());
        if let Some(before) = config_before {
            assert_eq!(std::fs::read(&config).unwrap(), before);
        }
    }
}
