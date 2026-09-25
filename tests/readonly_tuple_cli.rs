use std::process::Command;

#[test]
fn select_preacceptance_cli_has_fixed_success_and_rejection_shapes() {
    let bin = env!("CARGO_BIN_EXE_guigu-agent-bridge");
    let rejected = Command::new(bin)
        .args([
            "select-preacceptance",
            "--database",
            "/definitely/missing.db",
        ])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&rejected.stdout),
        String::from_utf8_lossy(&rejected.stderr)
    );
    assert!(!combined.contains("/definitely/missing.db"));
    assert!(!combined.contains("SELECT"));
    assert!(!combined.contains("delivery"));

    let invalid = Command::new(bin)
        .args(["select-preacceptance", "--database"])
        .output()
        .unwrap();
    assert!(!invalid.status.success());
    let invalid_text = format!(
        "{}{}",
        String::from_utf8_lossy(&invalid.stdout),
        String::from_utf8_lossy(&invalid.stderr)
    );
    assert!(!invalid_text.contains("--database"));
}
