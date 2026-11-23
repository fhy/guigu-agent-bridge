use std::process::Command;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_provision-isolated-proc-proof-fixture")
}

#[test]
fn malformed_residue_cli_shapes_are_fixed_redacted_and_terminal() {
    let valid = "01".repeat(32);
    let uppercase = "AA".repeat(32);
    let planted = "planted-sensitive-argument";
    let cases = [
        vec![],
        vec!["--wrong"],
        vec!["--wrong", &valid],
        vec!["--expected-sha256"],
        vec!["--expected-sha256", planted],
        vec!["--expected-sha256", &uppercase],
        vec!["--expected-sha256", &valid, "--expected-sha256", &valid],
        vec!["--expected-sha256", &valid, planted],
    ];
    let expected = concat!(
        "production-remediate-isolated-unit-residue ",
        "result=rejected status=3 cause=arguments substage=none\n"
    );

    for arguments in cases {
        let output = Command::new(binary())
            .arg("production-remediate-isolated-unit-residue")
            .args(arguments)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(output.stdout, expected.as_bytes());
        assert!(output.stderr.is_empty());
        assert!(!String::from_utf8_lossy(&output.stdout).contains(planted));
    }
}
