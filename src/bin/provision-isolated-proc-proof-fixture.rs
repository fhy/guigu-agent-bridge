use std::{fmt::Write as _, io::Write as _, path::PathBuf};

#[path = "provision-isolated-proc-proof-fixture/orchestration.rs"]
mod orchestration;
#[path = "provision-isolated-proc-proof-fixture/prepare_boundary.rs"]
mod prepare_boundary;
#[path = "provision-isolated-proc-proof-fixture/unit_bootstrap.rs"]
mod unit_bootstrap;
#[path = "provision-isolated-proc-proof-fixture/unit_residue.rs"]
mod unit_residue;
#[path = "provision-isolated-proc-proof-fixture/unit_residue_command.rs"]
mod unit_residue_command;

use guigu_agent_bridge::privileged_proc_proof::{
    FixturePaths, StageCause, provision_temp_fixture, stage_policy, teardown_temp_fixture,
};
use sha2::{Digest, Sha256};

const PRODUCTION_PROVISIONER: &str =
    "/home/fhy/.cache/cargo-target/guigu-agent-bridge/debug/provision-isolated-proc-proof-fixture";

#[derive(Debug, PartialEq, Eq)]
enum RecordError {
    Missing,
    Malformed,
    Duplicate,
}

#[derive(Debug, PartialEq, Eq)]
enum ChildOutcome {
    Success,
    ProvisionRefusal,
    ChildFailure,
    InvalidResult,
}

fn provision_binding(parent: &str, helper: &str, policy: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"GGT050PB1");
    for value in [parent, helper, policy] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    let mut encoded = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn root_start(binding: &str) -> String {
    format!("production-provision phase=start status=attempting binding_sha256={binding}")
}

fn root_result(binding: &str) -> String {
    format!("production-provision phase=result status=0 binding_sha256={binding}")
}

fn root_refusal(binding: &str) -> String {
    format!("production-provision phase=result status=3 binding_sha256={binding}")
}

fn validate_child_records(bytes: &[u8], binding: &str) -> Result<(), RecordError> {
    if bytes.is_empty() {
        return Err(RecordError::Missing);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| RecordError::Malformed)?;
    let lines = text.lines().collect::<Vec<_>>();
    if lines.len() > 2 {
        return Err(RecordError::Duplicate);
    }
    if lines.len() < 2 {
        return Err(RecordError::Missing);
    }
    if lines[0] != root_start(binding) || lines[1] != root_result(binding) {
        return Err(RecordError::Malformed);
    }
    Ok(())
}

fn validate_root_refusal(bytes: &[u8], binding: &str) -> Result<(), RecordError> {
    if bytes.is_empty() {
        return Err(RecordError::Missing);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| RecordError::Malformed)?;
    let lines = text.lines().collect::<Vec<_>>();
    if lines.len() > 2 {
        return Err(RecordError::Duplicate);
    }
    if lines.len() < 2 {
        return Err(RecordError::Missing);
    }
    if lines[0] != root_start(binding) || lines[1] != root_refusal(binding) {
        return Err(RecordError::Malformed);
    }
    Ok(())
}

fn classify_child(output: &std::process::Output, binding: &str) -> ChildOutcome {
    if !output.stderr.is_empty() {
        return ChildOutcome::ChildFailure;
    }
    if output.status.success() {
        return if validate_child_records(&output.stdout, binding).is_ok() {
            ChildOutcome::Success
        } else {
            ChildOutcome::InvalidResult
        };
    }
    if output.status.code() == Some(3) && validate_root_refusal(&output.stdout, binding).is_ok() {
        ChildOutcome::ProvisionRefusal
    } else if output.stdout.is_empty() {
        ChildOutcome::ChildFailure
    } else {
        ChildOutcome::InvalidResult
    }
}

fn boundary_result(binding: &str, status: u8, cause: &str) -> String {
    format!(
        "production-provision-boundary phase=result status={status} cause={cause} binding_sha256={binding}"
    )
}

fn emit_boundary_result(binding: &str, status: u8, cause: &str) -> Result<(), ()> {
    println!("{}", boundary_result(binding, status, cause));
    std::io::stdout().flush().map_err(|_| ())
}

fn capability_evidence() -> guigu_agent_bridge::privileged_proc_proof::CapabilityEvidence {
    let (Ok(current), Ok(expected)) = (
        std::env::current_exe().and_then(std::fs::canonicalize),
        std::fs::canonicalize(PRODUCTION_PROVISIONER),
    ) else {
        return guigu_agent_bridge::privileged_proc_proof::CapabilityEvidence::Refused;
    };
    if current != expected {
        return guigu_agent_bridge::privileged_proc_proof::CapabilityEvidence::Refused;
    }
    guigu_agent_bridge::privileged_proc_proof::production_capability_evidence()
}

fn usage() -> ! {
    eprintln!(
        "usage: provision-isolated-proc-proof-fixture provision --root ROOT --parent P --helper H --config C --database-root D [--generation N] | teardown --root ROOT"
    );
    std::process::exit(2)
}

fn value(args: &mut impl Iterator<Item = String>, flag: &str) -> PathBuf {
    match args.next() {
        Some(value) if !value.starts_with('-') => PathBuf::from(value),
        _ => {
            eprintln!("missing {flag}");
            usage()
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("stage-policy") => {
            let mut config = None;
            let mut database_root = None;
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--config" => config = Some(value(&mut args, "--config")),
                    "--database-root" => database_root = Some(value(&mut args, "--database-root")),
                    _ => usage(),
                }
            }
            let (Some(config), Some(database_root)) = (config, database_root) else {
                usage()
            };
            match stage_policy(&config, &database_root, 3) {
                Ok(_) => eprintln!("stage-policy status=0 cause=none"),
                Err(cause) => {
                    eprintln!("stage-policy status=3 cause={}", cause.code());
                    std::process::exit(3);
                }
            }
        }
        Some("production-provision") => {
            let parent = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--parent-sha" => value,
                _ => None,
            };
            let helper = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--helper-sha" => value,
                _ => None,
            };
            let policy = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--policy-sha" => value,
                _ => None,
            };
            if args.next().is_some() {
                std::process::exit(3);
            }
            let (Some(parent), Some(helper), Some(policy)) = (parent, helper, policy) else {
                std::process::exit(3)
            };
            let binding = provision_binding(&parent, &helper, &policy);
            println!("{}", root_start(&binding));
            if std::io::stdout().flush().is_err() {
                std::process::exit(3);
            }
            if guigu_agent_bridge::privileged_proc_proof::provision_production(
                &parent, &helper, &policy, 3,
            )
            .is_err()
            {
                println!("{}", root_refusal(&binding));
                let _ = std::io::stdout().flush();
                std::process::exit(3);
            }
            println!("{}", root_result(&binding));
            if std::io::stdout().flush().is_err() {
                std::process::exit(3);
            }
        }
        Some("production-provision-boundary") => {
            let parent = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--parent-sha" => value,
                _ => None,
            };
            let helper = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--helper-sha" => value,
                _ => None,
            };
            let policy = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--policy-sha" => value,
                _ => None,
            };
            if args.next().is_some() {
                std::process::exit(3);
            }
            let (Some(parent), Some(helper), Some(policy)) = (parent, helper, policy) else {
                std::process::exit(3)
            };
            if orchestration::run_boundary_fd3(&parent, &helper, &policy).is_err() {
                std::process::exit(3);
            }
        }
        Some("production-stage-provision-boundary") => {
            if orchestration::run_cli(args).is_err() {
                std::process::exit(3);
            }
        }
        Some("production-capability-evidence") => {
            if args.next().is_some() {
                std::process::exit(3);
            }
            let evidence = capability_evidence();
            println!("{}", evidence.line());
            if std::io::stdout().flush().is_err() || !evidence.is_success() {
                std::process::exit(3);
            }
        }
        Some("production-bootstrap-isolated-unit") => {
            if args.next().is_some() {
                std::process::exit(3);
            }
            match unit_bootstrap::run_production() {
                Ok(outcome) => println!(
                    "production-bootstrap-isolated-unit result={} status=0",
                    outcome.code()
                ),
                Err(cause) => {
                    println!(
                        "production-bootstrap-isolated-unit result=rejected status=3 cause={} substage={}",
                        cause.code(),
                        cause.substage()
                    );
                    std::process::exit(3);
                }
            }
        }
        Some("production-remediate-isolated-unit-residue") => {
            match unit_residue_command::run_cli(args) {
                Ok(()) => println!(
                    "production-remediate-isolated-unit-residue result=remediated status=0"
                ),
                Err(cause) => {
                    println!(
                        "production-remediate-isolated-unit-residue result=rejected status=3 cause={} substage={}",
                        cause.code(),
                        cause.substage()
                    );
                    std::process::exit(3);
                }
            }
        }
        Some("production-prepare-boundary") => {
            if prepare_boundary::run_cli(args).is_err() {
                std::process::exit(3);
            }
        }
        Some("production-teardown") => {
            let parent = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--parent-sha" => value,
                _ => None,
            };
            let helper = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--helper-sha" => value,
                _ => None,
            };
            let policy = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--policy-sha" => value,
                _ => None,
            };
            let generation = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--generation" => value.and_then(|v| v.parse().ok()),
                _ => None,
            };
            if args.next().is_some()
                || parent.is_none()
                || helper.is_none()
                || policy.is_none()
                || generation.is_none()
                || guigu_agent_bridge::privileged_proc_proof::teardown_production(
                    &parent.unwrap(),
                    &helper.unwrap(),
                    &policy.unwrap(),
                    generation.unwrap(),
                )
                .is_err()
            {
                std::process::exit(3);
            }
            println!("production teardown complete");
        }
        Some("provision") => {
            let mut root = None;
            let mut parent = None;
            let mut helper = None;
            let mut config = None;
            let mut database_root = None;
            let mut generation = 1u64;
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--root" => root = Some(value(&mut args, "--root")),
                    "--parent" => parent = Some(value(&mut args, "--parent")),
                    "--helper" => helper = Some(value(&mut args, "--helper")),
                    "--config" => config = Some(value(&mut args, "--config")),
                    "--database-root" => database_root = Some(value(&mut args, "--database-root")),
                    "--generation" => {
                        generation = args.next().and_then(|v| v.parse().ok()).unwrap_or(0)
                    }
                    _ => usage(),
                }
            }
            let (Some(root), Some(parent), Some(helper), Some(config), Some(database_root)) =
                (root, parent, helper, config, database_root)
            else {
                usage()
            };
            let paths = FixturePaths {
                root,
                parent,
                helper,
                config,
                database_root,
            };
            if provision_temp_fixture(&paths, generation).is_err() {
                std::process::exit(3);
            }
            println!(
                "provisioned manifest-last fixture root={}",
                paths.root.display()
            );
        }
        Some("teardown") => {
            let root = match args.next().as_deref() {
                Some("--root") => args.next().map(PathBuf::from),
                _ => None,
            };
            let Some(root) = root else { usage() };
            if teardown_temp_fixture(&root).is_err() {
                std::process::exit(3);
            }
            println!("teardown manifest-first complete");
        }
        _ => usage(),
    }
}

#[cfg(test)]
#[path = "provision-isolated-proc-proof-fixture/tests.rs"]
mod tests;
