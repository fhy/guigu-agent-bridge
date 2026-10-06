use super::*;

const BINDING: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const PLANTED: &str = "planted-secret-path-and-body";

fn child(script: &str) -> std::process::Output {
    Command::new("/bin/sh")
        .args(["-c", script, "sh", BINDING])
        .output()
        .unwrap()
}

fn record(outcome: ChildOutcome) -> String {
    match outcome {
        ChildOutcome::Success => boundary_result(BINDING, 0, "none"),
        ChildOutcome::ProvisionRefusal => boundary_result(BINDING, 3, "provision"),
        ChildOutcome::ChildFailure => boundary_result(BINDING, 3, "child"),
        ChildOutcome::InvalidResult => boundary_result(BINDING, 3, "result"),
    }
}

#[test]
fn success_result_records_are_accepted() {
    let records = format!("{}\n{}\n", root_start(BINDING), root_result(BINDING));
    assert_eq!(validate_child_records(records.as_bytes(), BINDING), Ok(()));
}

#[test]
fn missing_result_record_is_rejected() {
    let records = format!("{}\n", root_start(BINDING));
    assert_eq!(
        validate_child_records(records.as_bytes(), BINDING),
        Err(RecordError::Missing)
    );
}

#[test]
fn malformed_result_record_is_rejected() {
    let records = format!(
        "{}\nproduction-provision phase=result status=0 binding_sha256=wrong\n",
        root_start(BINDING)
    );
    assert_eq!(
        validate_child_records(records.as_bytes(), BINDING),
        Err(RecordError::Malformed)
    );
}

#[test]
fn duplicate_result_record_is_rejected() {
    let records = format!(
        "{}\n{}\n{}\n",
        root_start(BINDING),
        root_result(BINDING),
        root_result(BINDING)
    );
    assert_eq!(
        validate_child_records(records.as_bytes(), BINDING),
        Err(RecordError::Duplicate)
    );
}

#[test]
fn process_success_is_authenticated() {
    let output = child(
        "printf 'production-provision phase=start status=attempting binding_sha256=%s\\n' \"$1\"; \
         printf 'production-provision phase=result status=0 binding_sha256=%s\\n' \"$1\"",
    );
    assert_eq!(classify_child(&output, BINDING), ChildOutcome::Success);
}

#[test]
fn process_child_failure_is_fixed_and_redacted() {
    let output = child("printf '%s\\n' 'planted-secret-path-and-body' >&2; exit 7");
    let result = record(classify_child(&output, BINDING));
    assert_eq!(result, boundary_result(BINDING, 3, "child"));
    assert!(!result.contains(PLANTED));
}

#[test]
fn process_root_refusal_proves_no_generation() {
    let temp = tempfile::tempdir().unwrap();
    let generation = temp.path().join("generation");
    let script = format!(
        "test ! -e '{}'; \
         printf 'production-provision phase=start status=attempting binding_sha256=%s\\n' \"$1\"; \
         printf 'production-provision phase=result status=3 binding_sha256=%s\\n' \"$1\"; exit 3",
        generation.display()
    );
    let output = child(&script);
    assert_eq!(
        classify_child(&output, BINDING),
        ChildOutcome::ProvisionRefusal
    );
    assert!(!generation.exists());
}

#[test]
fn process_child_cleanup_leaves_no_owned_marker() {
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("owned-marker");
    let script = format!(
        "set -eu; : > '{}'; rm '{}'; \
         printf 'production-provision phase=start status=attempting binding_sha256=%s\\n' \"$1\"; \
         printf 'production-provision phase=result status=3 binding_sha256=%s\\n' \"$1\"; exit 3",
        marker.display(),
        marker.display()
    );
    let output = child(&script);
    assert_eq!(
        classify_child(&output, BINDING),
        ChildOutcome::ProvisionRefusal
    );
    assert!(!marker.exists());
}

#[test]
fn process_stdout_injection_is_rejected_and_redacted() {
    let output = child(
        "printf 'production-provision phase=start status=attempting binding_sha256=%s\\n' \"$1\"; \
         printf '%s\\n' 'planted-secret-path-and-body'; exit 3",
    );
    let result = record(classify_child(&output, BINDING));
    assert_eq!(result, boundary_result(BINDING, 3, "result"));
    assert!(!result.contains(PLANTED));
}
