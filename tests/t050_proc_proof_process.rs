use std::process::{Command, Stdio};

#[test]
fn unprovisioned_helper_fails_silently_without_privilege() {
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-t050-proc-proof"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}
