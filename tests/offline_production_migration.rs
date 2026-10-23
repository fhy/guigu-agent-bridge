use std::process::Command;

#[test]
fn real_cli_rejects_invalid_arguments_without_startup() {
    let temporary = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .current_dir(temporary.path())
        .args(["offline-migrate-production", "--mode", "apply"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stdout).contains("invalid-arguments"));
    assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
}
