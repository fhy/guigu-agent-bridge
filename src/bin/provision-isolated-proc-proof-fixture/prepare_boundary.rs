use std::{
    fmt::Write as _,
    io::{Read, Write as _},
    os::unix::process::CommandExt as _,
    process::{Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};

#[path = "prepare_boundary/diagnostic.rs"]
mod diagnostic;

const INSTALLED_PARENT: &str = "/home/fhy/.local/lib/guigu-codex-test/guigu-agent-bridge";
const COMMAND: &str = "prepare-isolated-legacy-fixture-migration-manifest";
const MAX_CHANNEL_BYTES: usize = 16 * 1024;
const PRODUCTION_DEADLINE: Duration = Duration::from_secs(180);

#[derive(Debug, PartialEq, Eq)]
enum StreamShape {
    Valid,
    Missing,
    Malformed,
    Duplicate,
    Overflow,
}

impl StreamShape {
    const fn code(&self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Missing => "missing",
            Self::Malformed => "malformed",
            Self::Duplicate => "duplicate",
            Self::Overflow => "overflow",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ChildResult {
    status: Option<ExitStatus>,
    stdout: Vec<u8>,
    stdout_overflow: bool,
    stderr: Vec<u8>,
    stderr_overflow: bool,
    reap: &'static str,
}

#[derive(Debug, PartialEq, Eq)]
struct Classification {
    status: u8,
    cause: &'static str,
    child: &'static str,
    stdout: StreamShape,
    stderr: &'static str,
    reap: &'static str,
    prepare_stage: &'static str,
    prepare_cause: &'static str,
}

fn binding(args: &[String]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"GGT050PREPARE1");
    for value in args {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    let mut encoded = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn drain(mut pipe: impl Read) -> (Vec<u8>, bool) {
    let mut retained = Vec::new();
    let mut overflow = false;
    let mut chunk = [0u8; 4096];
    loop {
        match pipe.read(&mut chunk) {
            Ok(0) => return (retained, overflow),
            Ok(count) => {
                let available = MAX_CHANNEL_BYTES.saturating_sub(retained.len());
                retained.extend_from_slice(&chunk[..count.min(available)]);
                overflow |= count > available;
            }
            Err(_) => return (retained, true),
        }
    }
}

fn capture(mut command: Command, deadline: Duration) -> Result<ChildResult, ()> {
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = command.spawn().map_err(|_| ())?;
    let process_group = child.id() as i32;
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(());
    };
    let Some(stderr) = child.stderr.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(());
    };
    let stdout_thread = thread::spawn(move || drain(stdout));
    let stderr_thread = thread::spawn(move || drain(stderr));
    let end = Instant::now() + deadline;
    let (status, reap) = loop {
        match child.try_wait() {
            Ok(Some(status)) => break (Some(status), "complete"),
            Ok(None) if Instant::now() < end => thread::sleep(Duration::from_millis(10)),
            Ok(None) => {
                let _ = child.kill();
                break match child.wait() {
                    Ok(_) => (None, "timeout-reaped"),
                    Err(_) => (None, "failed"),
                };
            }
            Err(_) => {
                let _ = child.kill();
                break match child.wait() {
                    Ok(_) => (None, "error-reaped"),
                    Err(_) => (None, "failed"),
                };
            }
        }
    };
    // Close pipes inherited by any unexpected descendant before joining the drainers.
    unsafe {
        libc::kill(-process_group, libc::SIGKILL);
    }
    let (stdout, stdout_overflow) = stdout_thread.join().map_err(|_| ())?;
    let (stderr, stderr_overflow) = stderr_thread.join().map_err(|_| ())?;
    Ok(ChildResult {
        status,
        stdout,
        stdout_overflow,
        stderr,
        stderr_overflow,
        reap,
    })
}

fn stdout_shape(result: &ChildResult) -> StreamShape {
    if result.stdout_overflow {
        return StreamShape::Overflow;
    }
    if result.stdout.is_empty() {
        return StreamShape::Missing;
    }
    let Ok(text) = std::str::from_utf8(&result.stdout) else {
        return StreamShape::Malformed;
    };
    if !text.ends_with('\n') {
        return StreamShape::Malformed;
    }
    let lines = text.lines().collect::<Vec<_>>();
    let terminals = lines
        .iter()
        .filter(|line| line.contains(" result="))
        .count();
    if terminals == 0 {
        return StreamShape::Missing;
    }
    if terminals > 1 {
        return StreamShape::Duplicate;
    }
    let Some(last) = lines.last() else {
        return StreamShape::Missing;
    };
    let terminal = matches!(
        *last,
        "prepare-isolated-legacy-fixture-migration-manifest result=prepared status=0"
            | "prepare-isolated-legacy-fixture-migration-manifest result=rejected status=3"
            | "prepare-isolated-legacy-fixture-migration-manifest result=database status=3"
    );
    let diagnostics = lines[..lines.len() - 1]
        .iter()
        .all(|line| diagnostic::valid(line));
    if terminal && diagnostics {
        StreamShape::Valid
    } else {
        StreamShape::Malformed
    }
}

fn classify(result: &ChildResult) -> Classification {
    use std::os::unix::process::ExitStatusExt;

    let stdout = stdout_shape(result);
    let stderr = if result.stderr_overflow {
        "overflow"
    } else if result.stderr.is_empty() {
        "empty"
    } else {
        "nonempty"
    };
    let lines = std::str::from_utf8(&result.stdout)
        .ok()
        .map(|text| text.lines().collect::<Vec<_>>())
        .unwrap_or_default();
    let last = lines.last().copied();
    let retained_diagnostic = if stdout == StreamShape::Valid && lines.len() > 1 {
        diagnostic::last(&lines[..lines.len() - 1])
    } else {
        None
    };
    let (child, terminal_matches) = match result.status {
        Some(status) if status.success() => (
            "exit-0",
            lines.len() == 1
                && last
                    == Some(
                        "prepare-isolated-legacy-fixture-migration-manifest result=prepared status=0",
                    ),
        ),
        Some(status) if status.code() == Some(3) => (
            "exit-3",
            matches!(
                last,
                Some(
                    "prepare-isolated-legacy-fixture-migration-manifest result=rejected status=3"
                        | "prepare-isolated-legacy-fixture-migration-manifest result=database status=3"
                )
            ),
        ),
        Some(status) if status.signal().is_some() => ("signal", true),
        Some(_) => ("exit-other", true),
        None => ("unknown", false),
    };
    let (status, cause) = if result.reap != "complete" {
        (
            3,
            if result.reap == "timeout-reaped" {
                "timeout"
            } else {
                "reap"
            },
        )
    } else if stderr != "empty" {
        (3, "stderr")
    } else if stdout != StreamShape::Valid || !terminal_matches {
        (3, "result")
    } else if child == "exit-0" {
        (0, "none")
    } else if child == "exit-3" {
        (3, "prepare")
    } else if child == "signal" {
        (3, "signal")
    } else {
        (3, "child")
    };
    let (prepare_stage, prepare_cause) = if child == "exit-3" {
        retained_diagnostic
            .map(|value| (value.stage, value.cause))
            .unwrap_or(("none", "none"))
    } else if stdout == StreamShape::Valid {
        ("none", "none")
    } else {
        ("unknown", "unknown")
    };
    Classification {
        status,
        cause,
        child,
        stdout,
        stderr,
        reap: result.reap,
        prepare_stage,
        prepare_cause,
    }
}

fn result_line(binding: &str, classification: &Classification) -> String {
    format!(
        "production-prepare-boundary phase=result status={} cause={} child={} stdout={} stderr={} reap={} prepare_stage={} prepare_cause={} binding_sha256={binding}",
        classification.status,
        classification.cause,
        classification.child,
        classification.stdout.code(),
        classification.stderr,
        classification.reap,
        classification.prepare_stage,
        classification.prepare_cause,
    )
}

fn emit_result(binding: &str, classification: &Classification) -> Result<(), ()> {
    println!("{}", result_line(binding, classification));
    std::io::stdout().flush().map_err(|_| ())
}

fn run_command(mut command: Command, args: &[String], deadline: Duration) -> Result<(), ()> {
    let binding = binding(args);
    println!("production-prepare-boundary phase=start status=attempting binding_sha256={binding}");
    std::io::stdout().flush().map_err(|_| ())?;
    command.args(args);
    let result = match capture(command, deadline) {
        Ok(result) => result,
        Err(()) => {
            let classification = Classification {
                status: 3,
                cause: "spawn",
                child: "not-started",
                stdout: StreamShape::Missing,
                stderr: "missing",
                reap: "not-applicable",
                prepare_stage: "unknown",
                prepare_cause: "unknown",
            };
            emit_result(&binding, &classification)?;
            return Err(());
        }
    };
    let classification = classify(&result);
    emit_result(&binding, &classification)?;
    if classification.status == 0 {
        Ok(())
    } else {
        Err(())
    }
}

pub(super) fn run_cli(args: impl Iterator<Item = String>) -> Result<(), ()> {
    let args = args.collect::<Vec<_>>();
    if args.first().map(String::as_str) != Some(COMMAND) {
        return Err(());
    }
    if super::orchestration::production_identity().is_err() {
        let binding = binding(&args);
        println!(
            "production-prepare-boundary phase=start status=attempting binding_sha256={binding}"
        );
        std::io::stdout().flush().map_err(|_| ())?;
        let classification = Classification {
            status: 3,
            cause: "identity",
            child: "not-started",
            stdout: StreamShape::Missing,
            stderr: "missing",
            reap: "not-applicable",
            prepare_stage: "unknown",
            prepare_cause: "unknown",
        };
        emit_result(&binding, &classification)?;
        return Err(());
    }
    run_command(Command::new(INSTALLED_PARENT), &args, PRODUCTION_DEADLINE)
}

#[cfg(test)]
#[path = "prepare_boundary/tests.rs"]
mod tests;
