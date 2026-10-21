use std::process::Command;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_guigu-agent-bridge")
}

#[test]
fn version_flag_prints_package_version_and_exits_successfully() {
    let output = Command::new(binary()).arg("--version").output().unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "guigu-agent-bridge 0.2.20"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn version_flag_rejects_extra_arguments() {
    let output = Command::new(binary())
        .args(["--version", "extra"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stdout).contains("--version result=invalid-arguments"));
}
