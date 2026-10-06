const PREFIX: &str = "prepare-isolated-legacy-fixture-migration-manifest stage=";

fn runtime_unknown(cause: &str) -> bool {
    const AREAS: &[&str] = &[
        "procfs",
        "identity",
        "deployment-metadata",
        "process-executable-link-deployment-owner",
        "process-executable-link-privileged-kernel-owner",
        "process-executable-link-privileged-userspace-owner",
        "process-executable-link-privileged-owner-classification",
        "process-executable-link-unprivileged-other-owner",
        "process-executable-metadata",
        "cmdline",
        "fd",
        "socket",
    ];
    const REASONS: &[&str] = &["permission", "io", "parse"];
    AREAS.iter().any(|area| {
        REASONS
            .iter()
            .any(|reason| cause == format!("runtime-unknown-{area}-{reason}"))
    })
}

fn generic(stage: &str, cause: &str) -> bool {
    match stage {
        "preflight" => {
            matches!(
                cause,
                "root-path"
                    | "root-contract"
                    | "config-path"
                    | "database-path"
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
        "sqlite-close" => matches!(cause, "rollback" | "connection" | "sidecar-revalidation"),
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
    let expected = if values == [0; 7] && cause == "database-facts" {
        "database-facts"
    } else if values[0] != 2 {
        "owner-topology"
    } else if values[1] != 1 {
        "continuation-topology"
    } else if values[2] != 0 {
        "open-sessions"
    } else if values[3] != 0 {
        "child-reap"
    } else if values[4] != 1 || values[5] != 1 {
        "lease-admission"
    } else if values[6] != 0 {
        "work-queue"
    } else {
        "witness-or-binding"
    };
    cause == expected
}

pub(super) fn valid(line: &str) -> bool {
    let Some(body) = line.strip_prefix(PREFIX) else {
        return false;
    };
    let Some((stage, tail)) = body.split_once(" cause=") else {
        return false;
    };
    let Some((cause, counts)) = tail.split_once(" counts=") else {
        return false;
    };
    if stage.is_empty() || cause.is_empty() || counts.is_empty() {
        return false;
    }
    if counts == "failures=1" {
        generic(stage, cause)
    } else {
        stage == "eligibility" && aggregate(cause, counts)
    }
}
