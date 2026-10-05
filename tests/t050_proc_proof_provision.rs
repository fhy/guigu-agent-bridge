use std::{os::unix::fs::PermissionsExt, path::Path, process::Command};

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
