const PREFIX: &str = "prepare-isolated-legacy-fixture-migration-manifest stage=";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Diagnostic {
    pub(super) stage: &'static str,
    pub(super) cause: &'static str,
}

fn canonical(value: &str, values: &'static [&'static str]) -> Option<&'static str> {
    values.iter().copied().find(|candidate| *candidate == value)
}

fn canonical_stage(value: &str) -> Option<&'static str> {
    canonical(
        value,
        &[
            "preflight",
            "publication",
            "sqlite-open",
            "eligibility",
            "sqlite-read",
            "sqlite-close",
            "postflight",
        ],
    )
}

fn canonical_cause(value: &str) -> Option<&'static str> {
    canonical(
        value,
        &[
            "root-path",
            "root-contract",
            "arguments",
            "config-path",
            "database-path",
            "binary-path",
            "path-identity",
            "admission-lock",
            "unit-state",
            "runtime-process",
            "runtime-listener",
            "manifest-target",
            "evidence-target",
            "manifest-serialization",
            "marker",
            "manifest",
            "sidecar-auth",
            "connect",
            "sidecar-revalidation",
            "begin",
            "durable-facts",
            "pre-snapshot",
            "post-snapshot",
            "sibling-receipts",
            "rollback",
            "connection",
            "sidecar-replacement-write",
            "sidecar-replacement-mode",
            "runtime-absence",
            "config-identity",
            "database-identity",
            "binary-identity",
            "database-facts",
            "owner-topology",
            "owner-identity",
            "owner-ordering",
            "continuation-topology",
            "continuation-state",
            "open-sessions",
            "child-reap",
            "lease-binding",
            "lease-topology",
            "admission-binding",
            "admission-topology",
            "delivery-binding",
            "delivery-topology",
            "terminal-event",
            "response-receipt",
            "stopping-references",
            "work-queue",
            "workspace-claim",
            "projection-outbox",
            "runtime-unknown-procfs-permission",
            "runtime-unknown-procfs-io",
            "runtime-unknown-procfs-parse",
            "runtime-unknown-identity-permission",
            "runtime-unknown-identity-io",
            "runtime-unknown-identity-parse",
            "runtime-unknown-deployment-metadata-permission",
            "runtime-unknown-deployment-metadata-io",
            "runtime-unknown-deployment-metadata-parse",
            "runtime-unknown-process-executable-link-deployment-owner-permission",
            "runtime-unknown-process-executable-link-deployment-owner-io",
            "runtime-unknown-process-executable-link-deployment-owner-parse",
            "runtime-unknown-process-executable-link-privileged-kernel-owner-permission",
            "runtime-unknown-process-executable-link-privileged-kernel-owner-io",
            "runtime-unknown-process-executable-link-privileged-kernel-owner-parse",
            "runtime-unknown-process-executable-link-privileged-userspace-owner-permission",
            "runtime-unknown-process-executable-link-privileged-userspace-owner-io",
            "runtime-unknown-process-executable-link-privileged-userspace-owner-parse",
            "runtime-unknown-process-executable-link-privileged-owner-classification-permission",
            "runtime-unknown-process-executable-link-privileged-owner-classification-io",
            "runtime-unknown-process-executable-link-privileged-owner-classification-parse",
            "runtime-unknown-process-executable-link-unprivileged-other-owner-permission",
            "runtime-unknown-process-executable-link-unprivileged-other-owner-io",
            "runtime-unknown-process-executable-link-unprivileged-other-owner-parse",
            "runtime-unknown-process-executable-metadata-permission",
            "runtime-unknown-process-executable-metadata-io",
            "runtime-unknown-process-executable-metadata-parse",
            "runtime-unknown-cmdline-permission",
            "runtime-unknown-cmdline-io",
            "runtime-unknown-cmdline-parse",
            "runtime-unknown-fd-permission",
            "runtime-unknown-fd-io",
            "runtime-unknown-fd-parse",
            "runtime-unknown-socket-permission",
            "runtime-unknown-socket-io",
            "runtime-unknown-socket-parse",
        ],
    )
}

fn runtime_unknown(cause: &str) -> bool {
    cause.starts_with("runtime-unknown-") && canonical_cause(cause).is_some()
}

fn generic(stage: &str, cause: &str) -> bool {
    match stage {
        "preflight" => {
            matches!(
                cause,
                "root-path"
                    | "root-contract"
                    | "arguments"
                    | "config-path"
                    | "database-path"
                    | "binary-path"
                    | "path-identity"
                    | "admission-lock"
                    | "unit-state"
                    | "runtime-process"
                    | "runtime-listener"
            ) || runtime_unknown(cause)
        }
        "publication" => matches!(
            cause,
            "manifest-target"
                | "evidence-target"
                | "manifest-serialization"
                | "marker"
                | "manifest"
        ),
        "sqlite-open" => matches!(
            cause,
            "sidecar-auth" | "connect" | "sidecar-revalidation" | "begin"
        ),
        "eligibility" => cause == "durable-facts",
        "sqlite-read" => matches!(cause, "pre-snapshot" | "post-snapshot" | "sibling-receipts"),
        "sqlite-close" => matches!(
            cause,
            "rollback"
                | "connection"
                | "sidecar-replacement-write"
                | "sidecar-replacement-mode"
                | "sidecar-revalidation"
        ),
        "postflight" => matches!(
            cause,
            "admission-lock"
                | "unit-state"
                | "runtime-absence"
                | "config-identity"
                | "database-identity"
                | "binary-identity"
        ),
        _ => false,
    }
}

fn canonical_count(value: &str) -> Option<u64> {
    let parsed = value.parse::<u64>().ok()?;
    (parsed.to_string() == value).then_some(parsed)
}

fn aggregate(cause: &str, counts: &str) -> bool {
    const FIELDS: &[&str] = &[
        "owners",
        "continuations",
        "sessions",
        "children",
        "leases",
        "admissions",
        "work",
    ];
    let parts = counts.split(',').collect::<Vec<_>>();
    if parts.len() != FIELDS.len() {
        return false;
    }
    let mut values = [0u64; 7];
    for (index, (part, expected)) in parts.iter().zip(FIELDS).enumerate() {
        let Some((name, value)) = part.split_once('=') else {
            return false;
        };
        if name != *expected || value.contains('=') {
            return false;
        }
        let Some(value) = canonical_count(value) else {
            return false;
        };
        values[index] = value;
    }
    if values == [0; 7] {
        return cause == "database-facts";
    }
    let cause_matches_count = (values[0] != 2 && cause == "owner-topology")
        || (values[1] != 1 && cause == "continuation-topology")
        || (values[2] != 0 && cause == "open-sessions")
        || (values[3] != 0 && cause == "child-reap")
        || (values[4] != 1 && cause == "lease-topology")
        || (values[5] != 1 && cause == "admission-topology")
        || (values[6] != 0 && cause == "work-queue");
    let detailed = matches!(
        cause,
        "owner-identity"
            | "owner-ordering"
            | "continuation-state"
            | "child-reap"
            | "lease-binding"
            | "admission-binding"
            | "delivery-binding"
            | "delivery-topology"
            | "terminal-event"
            | "response-receipt"
            | "stopping-references"
            | "workspace-claim"
            | "projection-outbox"
    );
    cause_matches_count || (values == [2, 1, 0, 0, 1, 1, 0] && detailed)
}

pub(super) fn parse(line: &str) -> Option<Diagnostic> {
    let body = line.strip_prefix(PREFIX)?;
    let (stage, tail) = body.split_once(" cause=")?;
    let (cause, counts) = tail.split_once(" counts=")?;
    if stage.is_empty() || cause.is_empty() || counts.is_empty() {
        return None;
    }
    let valid = if counts == "failures=1" {
        generic(stage, cause)
    } else {
        stage == "eligibility" && aggregate(cause, counts)
    };
    if !valid {
        return None;
    }
    Some(Diagnostic {
        stage: canonical_stage(stage)?,
        cause: canonical_cause(cause)?,
    })
}

pub(super) fn valid(line: &str) -> bool {
    parse(line).is_some()
}

pub(super) fn last(lines: &[&str]) -> Option<Diagnostic> {
    let mut last = None;
    for line in lines {
        last = Some(parse(line)?);
    }
    last
}
