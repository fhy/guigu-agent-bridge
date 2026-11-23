fn run_row(row: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "acp::output_tests::process_fixture_child",
            "--nocapture",
        ])
        .env("T051_PROCESS_ROW", row)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "row {row}: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if row == "redaction" {
        assert!(
            !output
                .stdout
                .windows(b"secret-token".len())
                .any(|v| v == b"secret-token")
        );
        assert!(
            !output
                .stderr
                .windows(b"secret-token".len())
                .any(|v| v == b"secret-token")
        );
    }
}

#[test]
fn row_01_chunk_shapes() {
    run_row("chunks")
}
#[test]
fn row_02_limit_and_spill() {
    run_row("limit")
}
#[test]
fn row_03_quota_contention() {
    run_row("quota")
}
#[test]
fn row_04_publication_recovery() {
    run_row("publication")
}
#[test]
fn row_05_hostile_artifacts() {
    run_row("artifacts")
}
#[test]
fn row_06_ordered_delivery_cleanup() {
    run_row("ordered")
}
#[test]
fn row_07_unknown_commit() {
    run_row("unknown")
}
#[test]
fn row_08_partial_stop() {
    run_row("partial")
}
#[test]
fn row_09_cleanup_restart() {
    run_row("cleanup")
}
#[test]
fn row_10_cancel_abandon() {
    run_row("cancel")
}
#[test]
fn row_11_migration_reopen() {
    run_row("migration")
}
#[test]
fn row_12_redaction_and_paths() {
    run_row("redaction")
}
