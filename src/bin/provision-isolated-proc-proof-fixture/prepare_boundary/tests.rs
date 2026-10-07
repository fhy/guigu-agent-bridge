use std::{process::Command, time::Duration};

use super::*;

const TERMINAL_OK: &str =
    "prepare-isolated-legacy-fixture-migration-manifest result=prepared status=0\n";
const TERMINAL_REFUSED: &str =
    "prepare-isolated-legacy-fixture-migration-manifest result=rejected status=3\n";

fn shell(script: &str) -> Command {
    let mut command = Command::new("/bin/sh");
    command.args(["-c", script]);
    command
}

fn result(script: &str) -> ChildResult {
    capture(shell(script), Duration::from_secs(2)).unwrap()
}

#[test]
fn success_is_framed_reaped_and_authenticated() {
    let child = result(&format!("printf '%s' '{TERMINAL_OK}'"));
    assert_eq!(child.status.and_then(|status| status.code()), Some(0));
    assert_eq!(child.reap, "complete");
    assert_eq!(classify(&child).status, 0);
    assert_eq!(classify(&child).cause, "none");
    assert_eq!(classify(&child).prepare_stage, "none");
    assert_eq!(classify(&child).prepare_cause, "none");
}

#[test]
fn prepare_refusal_is_distinct_from_channel_failure() {
    let child = result(&format!("printf '%s' '{TERMINAL_REFUSED}'; exit 3"));
    assert_eq!(child.status.and_then(|status| status.code()), Some(3));
    assert_eq!(classify(&child).cause, "prepare");
    assert_eq!(classify(&child).stdout, StreamShape::Valid);
    assert_eq!(classify(&child).prepare_stage, "unknown");
    assert_eq!(classify(&child).prepare_cause, "unknown");
}

#[test]
fn exit_three_without_authenticated_diagnostic_is_unknown_matrix() {
    for (name, script, shape) in [
        (
            "terminal-only",
            format!("printf '%s' '{TERMINAL_REFUSED}'; exit 3"),
            StreamShape::Valid,
        ),
        ("missing", "exit 3".to_owned(), StreamShape::Missing),
        (
            "malformed",
            "printf 'prepare-isolated-legacy-fixture-migration-manifest result=planted-secret-body status=3\\n'; exit 3".to_owned(),
            StreamShape::Malformed,
        ),
        (
            "duplicate",
            format!("printf '%s%s' '{TERMINAL_REFUSED}' '{TERMINAL_REFUSED}'; exit 3"),
            StreamShape::Duplicate,
        ),
        (
            "overflow",
            "head -c 20000 /dev/zero | tr '\\0' x; printf '\\n'; exit 3".to_owned(),
            StreamShape::Overflow,
        ),
    ] {
        let classification = classify(&result(&script));
        assert_eq!(classification.child, "exit-3", "{name}");
        assert_eq!(classification.stdout, shape, "{name}");
        assert_eq!(classification.prepare_stage, "unknown", "{name}");
        assert_eq!(classification.prepare_cause, "unknown", "{name}");
        let line = result_line("00", &classification);
        assert!(
            line.contains("prepare_stage=unknown prepare_cause=unknown"),
            "{name}"
        );
        assert!(!line.contains("planted-secret-body"), "{name}");
    }
}

#[test]
fn missing_malformed_and_duplicate_records_are_fixed() {
    let missing = classify(&result("exit 3"));
    assert_eq!(missing.cause, "result");
    assert_eq!(missing.stdout, StreamShape::Missing);

    let malformed = classify(&result(
        "printf 'prepare-isolated-legacy-fixture-migration-manifest result=planted-secret-body status=3\\n'; exit 3",
    ));
    assert_eq!(malformed.cause, "result");
    assert_eq!(malformed.stdout, StreamShape::Malformed);

    let duplicate = classify(&result(&format!(
        "printf '%s%s' '{TERMINAL_REFUSED}' '{TERMINAL_REFUSED}'; exit 3"
    )));
    assert_eq!(duplicate.cause, "result");
    assert_eq!(duplicate.stdout, StreamShape::Duplicate);

    let no_newline = classify(&result(
        "printf 'prepare-isolated-legacy-fixture-migration-manifest result=rejected status=3'; exit 3",
    ));
    assert_eq!(no_newline.stdout, StreamShape::Malformed);
}

#[test]
fn stderr_exit_and_signal_are_classified_without_content() {
    let stderr = classify(&result("printf planted-secret-body >&2; exit 3"));
    assert_eq!(stderr.cause, "stderr");
    assert_eq!(stderr.stderr, "nonempty");

    let exit = classify(&result(&format!("printf '%s' '{TERMINAL_OK}'; exit 7")));
    assert_eq!(exit.cause, "child");
    assert_eq!(exit.child, "exit-other");

    let signal = classify(&result("kill -TERM $$"));
    assert_eq!(signal.cause, "result");
    assert_eq!(signal.child, "signal");
}

#[test]
fn timeout_kills_reaps_and_drains_the_child() {
    let child = capture(shell("sleep 5"), Duration::from_millis(20)).unwrap();
    assert_eq!(child.reap, "timeout-reaped");
    assert!(child.status.is_none());
    assert_eq!(classify(&child).cause, "timeout");
}

#[test]
fn bounded_drainers_report_stdout_and_stderr_overflow() {
    let stdout = classify(&result(
        "head -c 20000 /dev/zero | tr '\\0' x; printf '\\n'; exit 3",
    ));
    assert_eq!(stdout.stdout, StreamShape::Overflow);
    assert_eq!(stdout.cause, "result");

    let stderr = classify(&result("head -c 20000 /dev/zero >&2; exit 3"));
    assert_eq!(stderr.stderr, "overflow");
    assert_eq!(stderr.cause, "stderr");
}

#[test]
fn synthetic_cleanup_and_diagnostics_leave_no_residue() {
    let root = tempfile::tempdir().unwrap();
    let marker = root.path().join("owned-marker");
    let script = format!(
        "touch '{0}'; rm '{0}'; printf 'prepare-isolated-legacy-fixture-migration-manifest stage=eligibility cause=durable-facts counts=failures=1\\n'; printf '%s' '{TERMINAL_REFUSED}'; exit 3",
        marker.display()
    );
    let classification = classify(&result(&script));
    assert_eq!(classification.cause, "prepare");
    assert_eq!(classification.stdout, StreamShape::Valid);
    assert_eq!(classification.prepare_stage, "eligibility");
    assert_eq!(classification.prepare_cause, "durable-facts");
    assert!(!marker.exists());
}

#[test]
fn fixed_generic_and_eligibility_aggregate_diagnostics_are_valid() {
    for diagnostic in [
        "stage=preflight cause=arguments counts=failures=1",
        "stage=preflight cause=binary-path counts=failures=1",
        "stage=sqlite-open cause=sidecar-auth counts=failures=1",
    ] {
        let generic = classify(&result(&format!(
            "printf 'prepare-isolated-legacy-fixture-migration-manifest {diagnostic}\\n'; printf '%s' '{TERMINAL_REFUSED}'; exit 3"
        )));
        assert_eq!(generic.stdout, StreamShape::Valid, "{diagnostic}");
        assert_eq!(generic.cause, "prepare", "{diagnostic}");
        assert_eq!(
            generic.prepare_stage,
            diagnostic
                .split_whitespace()
                .next()
                .unwrap()
                .strip_prefix("stage=")
                .unwrap(),
            "{diagnostic}"
        );
        assert_eq!(
            generic.prepare_cause,
            diagnostic
                .split_whitespace()
                .nth(1)
                .unwrap()
                .strip_prefix("cause=")
                .unwrap(),
            "{diagnostic}"
        );
    }

    let nominal =
        "counts=owners=2,continuations=1,sessions=0,children=0,leases=1,admissions=1,work=0";
    let mut diagnostics = vec![
        "stage=eligibility cause=open-sessions counts=owners=2,continuations=1,sessions=1,children=0,leases=1,admissions=1,work=0".to_owned(),
        "stage=eligibility cause=database-facts counts=owners=0,continuations=0,sessions=0,children=0,leases=0,admissions=0,work=0".to_owned(),
    ];
    for cause in [
        "owner-identity",
        "owner-ordering",
        "continuation-state",
        "child-reap",
        "lease-binding",
        "admission-binding",
        "delivery-binding",
        "delivery-topology",
        "terminal-event",
        "response-receipt",
        "stopping-references",
        "workspace-claim",
        "projection-outbox",
    ] {
        diagnostics.push(format!("stage=eligibility cause={cause} {nominal}"));
    }
    for diagnostic in diagnostics {
        let child = classify(&result(&format!(
            "printf 'prepare-isolated-legacy-fixture-migration-manifest {diagnostic}\\n'; printf '%s' '{TERMINAL_REFUSED}'; exit 3"
        )));
        assert_eq!(child.stdout, StreamShape::Valid, "{diagnostic}");
        assert_eq!(child.cause, "prepare", "{diagnostic}");
        assert_eq!(child.prepare_stage, "eligibility", "{diagnostic}");
        assert_eq!(
            child.prepare_cause,
            diagnostic
                .split_whitespace()
                .nth(1)
                .unwrap()
                .strip_prefix("cause=")
                .unwrap(),
            "{diagnostic}"
        );
    }
}

#[test]
fn eligibility_retains_the_last_authenticated_specific_cause() {
    let child = classify(&result(&format!(
        "printf 'prepare-isolated-legacy-fixture-migration-manifest stage=eligibility cause=durable-facts counts=failures=1\\n'; printf 'prepare-isolated-legacy-fixture-migration-manifest stage=eligibility cause=open-sessions counts=owners=2,continuations=1,sessions=1,children=0,leases=1,admissions=1,work=0\\n'; printf '%s' '{TERMINAL_REFUSED}'; exit 3"
    )));
    assert_eq!(child.status, 3);
    assert_eq!(child.cause, "prepare");
    assert_eq!(child.stdout, StreamShape::Valid);
    assert_eq!(child.prepare_stage, "eligibility");
    assert_eq!(child.prepare_cause, "open-sessions");
}

#[test]
fn hostile_diagnostic_grammar_is_malformed_and_fail_closed() {
    let hostile = [
        "stage=unknown cause=root-path counts=failures=1",
        "stage=preflight cause=unknown counts=failures=1",
        "stage=preflight counts=failures=1",
        "stage=preflight cause=root-path",
        "cause=root-path stage=preflight counts=failures=1",
        "stage=preflight counts=failures=1 cause=root-path",
        "stage=preflight cause=root-path counts=failures=1 extra=planted",
        "stage=preflight cause=root-path counts=failures=2",
        "stage=preflight cause=root-path counts=failures=01",
        "stage=eligibility cause=open-sessions counts=sessions=1,owners=2,continuations=1,children=0,leases=1,admissions=1,work=0",
        "stage=eligibility cause=open-sessions counts=owners=2,continuations=1,sessions=01,children=0,leases=1,admissions=1,work=0",
        "stage=eligibility cause=open-sessions counts=owners=2,continuations=1,sessions=1,children=0,leases=1,admissions=1,work=0,planted=1",
        "stage=eligibility cause=owner-topology counts=owners=2,continuations=1,sessions=1,children=0,leases=1,admissions=1,work=0",
        "stage=preflight cause=root-path counts=failures=1 planted-ascii",
    ];
    for diagnostic in hostile {
        let child = classify(&result(&format!(
            "printf 'prepare-isolated-legacy-fixture-migration-manifest {diagnostic}\\n'; printf '%s' '{TERMINAL_OK}'"
        )));
        assert_eq!(child.status, 3, "{diagnostic}");
        assert_eq!(child.cause, "result", "{diagnostic}");
        assert_eq!(child.stdout, StreamShape::Malformed, "{diagnostic}");
        assert_eq!(child.prepare_stage, "unknown", "{diagnostic}");
        assert_eq!(child.prepare_cause, "unknown", "{diagnostic}");
        let line = result_line("00", &child);
        assert!(
            line.contains("prepare_stage=unknown prepare_cause=unknown"),
            "{diagnostic}"
        );
        assert!(!line.contains(diagnostic), "{diagnostic}");
    }

    let valid_but_impossible_success = classify(&result(&format!(
        "printf 'prepare-isolated-legacy-fixture-migration-manifest stage=preflight cause=root-path counts=failures=1\\n'; printf '%s' '{TERMINAL_OK}'"
    )));
    assert_eq!(valid_but_impossible_success.status, 3);
    assert_eq!(valid_but_impossible_success.cause, "result");
    assert_eq!(valid_but_impossible_success.stdout, StreamShape::Valid);
}

#[test]
fn invalid_program_is_spawn_failure_and_binding_is_redacted() {
    let args = vec![COMMAND.to_owned(), "planted-secret-body".to_owned()];
    let digest = binding(&args);
    assert_eq!(digest.len(), 64);
    assert!(!digest.contains("planted-secret-body"));
    assert!(run_command(Command::new("/definitely/absent"), &args, Duration::ZERO).is_err());

    let classification = Classification {
        status: 3,
        cause: "result",
        child: "exit-3",
        stdout: StreamShape::Malformed,
        stderr: "empty",
        reap: "complete",
        prepare_stage: "unknown",
        prepare_cause: "unknown",
    };
    let line = result_line(&digest, &classification);
    assert!(line.contains(
        "cause=result child=exit-3 stdout=malformed stderr=empty reap=complete prepare_stage=unknown prepare_cause=unknown"
    ));
    assert!(!line.contains("planted-secret-body"));
}

#[test]
fn cli_requires_the_exact_prepare_command() {
    assert!(run_cli(std::iter::empty()).is_err());
    assert!(run_cli(["other-command".to_owned()].into_iter()).is_err());
}
