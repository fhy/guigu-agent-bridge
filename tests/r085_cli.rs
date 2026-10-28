use std::process::Command;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_guigu-agent-bridge")
}

#[test]
fn version_contract_is_standalone_and_rejects_extra_arguments() {
    let version = Command::new(binary()).arg("--version").output().unwrap();
    assert_eq!(version.status.code(), Some(0));
    assert_eq!(version.stdout, b"guigu-agent-bridge 0.2.24\n");
    assert!(version.stderr.is_empty());

    for extra in ["--bad", "unexpected"] {
        let output = Command::new(binary())
            .args(["--version", extra])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(output.stdout, b"--version result=invalid-arguments\n");
        assert!(output.stderr.is_empty());
    }
}
