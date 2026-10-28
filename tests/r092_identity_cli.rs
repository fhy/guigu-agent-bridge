use std::process::Command;

fn arguments(root: &std::path::Path, mode: &str) -> Vec<String> {
    vec![
        "offline-capture-matrix-identity".into(),
        "--mode".into(),
        mode.into(),
        "--config".into(),
        root.join("missing-config").display().to_string(),
        "--database".into(),
        root.join("missing-db").display().to_string(),
        "--operation-id".into(),
        "synthetic-operation".into(),
        "--manifest-dir".into(),
        root.join("missing-audit").display().to_string(),
        "--release-provenance".into(),
        root.join("missing-provenance").display().to_string(),
        "--executable".into(),
        env!("CARGO_BIN_EXE_guigu-agent-bridge").into(),
    ]
}

#[test]
fn capture_cli_rejects_malformed_modes_and_duplicate_options_before_runtime() {
    let root = tempfile::Builder::new()
        .prefix("r092-identity-cli-")
        .tempdir_in("/tmp")
        .unwrap();
    let mut duplicate = arguments(root.path(), "capture");
    duplicate[3] = "--operation-id".into();
    for args in [
        vec!["offline-capture-matrix-identity".into()],
        arguments(root.path(), "apply"),
        arguments(root.path(), "resume"),
        duplicate,
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
            .args(args)
            .env_clear()
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains("offline-capture-matrix-identity result=invalid-arguments")
        );
    }
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn capture_and_verify_require_existing_audit_inputs_without_creating_any_state() {
    let root = tempfile::Builder::new()
        .prefix("r092-identity-cli-inputs-")
        .tempdir_in("/tmp")
        .unwrap();
    for mode in ["capture", "verify"] {
        let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
            .args(arguments(root.path(), mode))
            .env_clear()
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(5), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains("offline-capture-matrix-identity result=identity-archive")
        );
        assert!(output.stderr.is_empty());
    }
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
