use std::process::Command;

#[test]
fn version_reports_package_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .arg("--version")
        .output()
        .expect("version command should start");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("version output is utf8"),
        "guigu-agent-bridge 0.2.24\n"
    );
    assert!(output.stderr.is_empty());
}
