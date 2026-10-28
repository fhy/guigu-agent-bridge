//! `guigu-agent-bridge` application entry point and minimal module boundary.
//!
//! The binary (`src/main.rs`) is a thin wrapper; all entry logic lives here so
//! integration tests can exercise the real call path through the public API.

pub mod a2a;
pub mod acp;
pub mod agents;
pub mod app;
pub mod bus;
pub mod config;
pub mod error;
pub mod gateway;
pub(crate) mod historical_unknown_abandonment;
pub mod isolated_fixture_migration;
pub mod isolated_state_backup;
pub mod lease_owner_diagnostics;
pub mod legacy_owner_reconciliation;
pub(crate) mod maintenance_lock;
pub mod matrix;
pub mod models;
pub mod observer;
pub mod offline_delivery;
pub mod offline_preacceptance;
pub(crate) mod offline_production_migration;
pub mod offline_recovery;
pub mod orphaned_preacceptance;
pub mod owner_binding_remediation;
pub mod privileged_proc_proof;
pub mod readonly_tuple;
pub mod readonly_tuple_diagnostics;
pub mod runtime;
pub mod runtime_admission;
pub mod selected_preacceptance;
pub mod storage;
pub(crate) mod terminal_acknowledged_cleanup;
pub(crate) mod terminal_closure;

use std::future::Future;

use tracing::info;

pub use error::Error;

fn selection_failure(command: &'static str, error: readonly_tuple::SelectionError) -> Error {
    let (category, status) = match error {
        readonly_tuple::SelectionError::InvalidArguments => ("invalid-arguments", 2),
        readonly_tuple::SelectionError::Empty => ("empty", 3),
        readonly_tuple::SelectionError::Multiple => ("multiple", 4),
        readonly_tuple::SelectionError::Malformed => ("malformed", 5),
        readonly_tuple::SelectionError::Conflicting => ("conflicting", 6),
        readonly_tuple::SelectionError::Nonterminal => ("nonterminal", 7),
        readonly_tuple::SelectionError::Busy => ("busy", 8),
        readonly_tuple::SelectionError::Database => ("database", 9),
    };
    Error::Cli(error::CliFailure {
        command,
        category,
        status,
    })
}

fn recovery_failure(command: &'static str) -> Error {
    Error::Cli(error::CliFailure {
        command,
        category: "recovery",
        status: 10,
    })
}

fn diagnosed_failure(error: readonly_tuple_diagnostics::DiagnosedSelectionError) -> Error {
    use readonly_tuple_diagnostics::DiagnosedSelectionError::*;
    let (category, status) = match error {
        InvalidArguments => ("invalid-arguments", 2),
        Empty => ("empty", 3),
        Multiple => ("multiple", 4),
        Malformed => ("malformed", 5),
        Nonterminal => ("nonterminal", 7),
        Busy => ("busy", 8),
        Database => ("database", 9),
        Conflicting(causes) => return Error::Diagnostic(causes),
    };
    Error::Cli(error::CliFailure {
        command: "diagnose-preacceptance-conflict",
        category,
        status,
    })
}

/// Initialize the global tracing subscriber for structured logging.
///
/// Uses `try_init` so repeated calls (e.g. when `run` is invoked more than once,
/// or in tests) do not panic. Falls back to `info` level when `RUST_LOG` is unset
/// or invalid.
pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// Production entry point: initialize tracing, then run until an interrupt or
/// termination signal.
pub async fn run() -> Result<(), Error> {
    let all_args = std::env::args_os().collect::<Vec<_>>();
    if all_args.get(1).and_then(|value| value.to_str()) == Some("--version") {
        if all_args.len() == 2 {
            println!("guigu-agent-bridge 0.2.24");
            return Ok(());
        }
        return Err(Error::Cli(error::CliFailure {
            command: "--version",
            category: "invalid-arguments",
            status: 2,
        }));
    }
    if all_args.get(1).and_then(|v| v.to_str()) == Some("offline-migrate-production") {
        let req = offline_production_migration::parse_args(&all_args[1..]).map_err(|_| {
            Error::Cli(error::CliFailure {
                command: "offline-migrate-production",
                category: "invalid-arguments",
                status: 2,
            })
        })?;
        let result = offline_production_migration::run(req)
            .await
            .map_err(|error| {
                let (category, status) = match error {
                    offline_production_migration::Failure::InvalidArguments => {
                        ("invalid-arguments", 2)
                    }
                    offline_production_migration::Failure::Busy => ("busy", 4),
                    offline_production_migration::Failure::Preflight => ("preflight", 5),
                    offline_production_migration::Failure::ManualRequired => ("manual-required", 6),
                    offline_production_migration::Failure::Migration => ("migration", 7),
                };
                Error::Cli(error::CliFailure {
                    command: "offline-migrate-production",
                    category,
                    status,
                })
            })?;
        println!("{result}");
        return Ok(());
    }
    let _maintenance_guard = offline_production_migration::cli_exclusion::acquire(&all_args)
        .map_err(|_| {
            Error::Cli(error::CliFailure {
                command: "maintenance-exclusion",
                category: "busy-or-unresolved",
                status: 4,
            })
        })?;
    if all_args.get(1).and_then(|v| v.to_str()) == Some("historical-terminal-cleanup") {
        let req = terminal_acknowledged_cleanup::parse_args(&all_args[1..]).map_err(|_| {
            Error::Cli(error::CliFailure {
                command: "historical-terminal-cleanup",
                category: "invalid-arguments",
                status: 2,
            })
        })?;
        let result = terminal_acknowledged_cleanup::run(req)
            .await
            .map_err(|error| {
                let (category, status) = match error {
                    terminal_acknowledged_cleanup::CleanupError::InvalidArguments => {
                        ("invalid-arguments", 2)
                    }
                    terminal_acknowledged_cleanup::CleanupError::Schema => ("schema", 5),
                    terminal_acknowledged_cleanup::CleanupError::NoUniqueTarget => {
                        ("no-unique-target", 3)
                    }
                    terminal_acknowledged_cleanup::CleanupError::Conflict => ("conflicting", 6),
                    terminal_acknowledged_cleanup::CleanupError::Database => ("database", 9),
                };
                Error::Cli(error::CliFailure {
                    command: "historical-terminal-cleanup",
                    category,
                    status,
                })
            })?;
        println!("historical-terminal-cleanup result={result}");
        return Ok(());
    }
    if all_args.get(1).and_then(|v| v.to_str()) == Some("historical-unknown-abandon") {
        let req = historical_unknown_abandonment::parse_args(&all_args[1..]).map_err(|_| {
            Error::Cli(error::CliFailure {
                command: "historical-unknown-abandon",
                category: "invalid-arguments",
                status: 2,
            })
        })?;
        let result = historical_unknown_abandonment::run(req)
            .await
            .map_err(|_| {
                Error::Cli(error::CliFailure {
                    command: "historical-unknown-abandon",
                    category: "conflicting",
                    status: 6,
                })
            })?;
        println!("historical-unknown-abandon result={result}");
        return Ok(());
    }
    if all_args.get(1).and_then(|value| value.to_str()) == Some("--version") {
        if all_args.len() != 2 {
            return Err(Error::Cli(error::CliFailure {
                command: "--version",
                category: "invalid-arguments",
                status: 2,
            }));
        }
        println!("guigu-agent-bridge {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if all_args.get(1).and_then(|value| value.to_str()) == Some("remediate-isolated-owner-bindings")
    {
        let parsed =
            owner_binding_remediation::parse_args(&all_args[1..]).map_err(Error::OwnerBinding)?;
        let outcome = owner_binding_remediation::run_production(&parsed)
            .await
            .map_err(Error::OwnerBinding)?;
        println!(
            "remediate-isolated-owner-bindings result={} status=0 cause=none substage=none",
            outcome.code()
        );
        return Ok(());
    }
    if let Some(output) = isolated_state_backup::dispatch(std::env::args_os().collect()).await {
        match output {
            Ok(line) => {
                println!("{line}");
                return Ok(());
            }
            Err(failure) => {
                return Err(Error::Cli(error::CliFailure {
                    command: failure.command,
                    category: failure.cause,
                    status: failure.status,
                }));
            }
        }
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str())
        == Some("prepare-isolated-legacy-fixture-migration-manifest")
    {
        let mut cli = vec![std::ffi::OsString::from(
            "prepare-isolated-legacy-fixture-migration-manifest",
        )];
        cli.extend(args);
        let prepare = isolated_fixture_migration::parse_prepare_args(&cli)
            .map_err(isolated_fixture_migration::prepare_cli_rejected)
            .map_err(|e| {
                Error::Cli(error::CliFailure {
                    command: "prepare-isolated-legacy-fixture-migration-manifest",
                    category: e.slug(),
                    status: e.status(),
                })
            })?;
        isolated_fixture_migration::prepare_manifest(prepare)
            .await
            .map_err(|e| {
                Error::Cli(error::CliFailure {
                    command: "prepare-isolated-legacy-fixture-migration-manifest",
                    category: e.slug(),
                    status: e.status(),
                })
            })?;
        println!("prepare-isolated-legacy-fixture-migration-manifest result=prepared status=0");
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str()) == Some("migrate-isolated-legacy-fixture") {
        let mut cli = vec![std::ffi::OsString::from("migrate-isolated-legacy-fixture")];
        cli.extend(args);
        let manifest = isolated_fixture_migration::parse_args(&cli).map_err(|e| {
            Error::Cli(error::CliFailure {
                command: "migrate-isolated-legacy-fixture",
                category: e.slug(),
                status: e.status(),
            })
        })?;
        let outcome = isolated_fixture_migration::migrate(manifest)
            .await
            .map_err(|e| {
                Error::Cli(error::CliFailure {
                    command: "migrate-isolated-legacy-fixture",
                    category: e.slug(),
                    status: e.status(),
                })
            })?;
        println!(
            "migrate-isolated-legacy-fixture result={} status=0",
            match outcome {
                isolated_fixture_migration::Outcome::Migrated => "migrated",
                isolated_fixture_migration::Outcome::AlreadyMigrated => "already-migrated",
            }
        );
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str())
        == Some("prepare-isolated-legacy-owner-manifest")
    {
        let mut cli = vec![std::ffi::OsString::from(
            "prepare-isolated-legacy-owner-manifest",
        )];
        cli.extend(args);
        let prepare = legacy_owner_reconciliation::parse_prepare_args(&cli).map_err(|e| {
            Error::Cli(error::CliFailure {
                command: "prepare-isolated-legacy-owner-manifest",
                category: e.slug(),
                status: e.status(),
            })
        })?;
        legacy_owner_reconciliation::prepare_manifest(prepare)
            .await
            .map_err(|e| {
                Error::Cli(error::CliFailure {
                    command: "prepare-isolated-legacy-owner-manifest",
                    category: e.slug(),
                    status: e.status(),
                })
            })?;
        println!("prepare-isolated-legacy-owner-manifest result=prepared status=0");
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str()) == Some("reconcile-isolated-legacy-owner") {
        let mut cli = vec![std::ffi::OsString::from("reconcile-isolated-legacy-owner")];
        cli.extend(args);
        let manifest = legacy_owner_reconciliation::parse_args(&cli).map_err(|e| {
            Error::Cli(error::CliFailure {
                command: "reconcile-isolated-legacy-owner",
                category: e.slug(),
                status: e.status(),
            })
        })?;
        let outcome = legacy_owner_reconciliation::reconcile(manifest)
            .await
            .map_err(|e| {
                Error::Cli(error::CliFailure {
                    command: "reconcile-isolated-legacy-owner",
                    category: e.slug(),
                    status: e.status(),
                })
            })?;
        println!(
            "reconcile-isolated-legacy-owner result={} status=0",
            match outcome {
                legacy_owner_reconciliation::Outcome::Recovered => "recovered",
                legacy_owner_reconciliation::Outcome::AlreadyStopped => "already-stopped",
            }
        );
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str()) == Some("reconcile-orphaned-preacceptance") {
        let mut cli = vec![std::ffi::OsString::from("reconcile-orphaned-preacceptance")];
        cli.extend(args);
        let database = orphaned_preacceptance::parse_args(&cli).map_err(|e| {
            Error::Cli(error::CliFailure {
                command: "reconcile-orphaned-preacceptance",
                category: e.slug(),
                status: e.status(),
            })
        })?;
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        match orphaned_preacceptance::reconcile(database, &now).await {
            Ok(orphaned_preacceptance::Outcome::Reconciled) => {
                println!("reconcile-orphaned-preacceptance result=reconciled status=0")
            }
            Ok(orphaned_preacceptance::Outcome::AlreadyReconciled) => {
                println!("reconcile-orphaned-preacceptance result=already-reconciled status=0")
            }
            Err(e) => {
                return Err(Error::Cli(error::CliFailure {
                    command: "reconcile-orphaned-preacceptance",
                    category: e.slug(),
                    status: e.status(),
                }));
            }
        }
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str()) == Some("recover-runtime") {
        let mut cli = vec![std::ffi::OsString::from("recover-runtime")];
        cli.extend(args);
        let (database, token, fingerprint) = offline_recovery::parse_cli_args(&cli)
            .map_err(|_| app::AppError::Assembly("invalid recover-runtime arguments"))?;
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let result = offline_recovery::recover_stale_runtime(database, &token, &fingerprint, &now)
            .await
            .map_err(|_| app::AppError::Runtime)?;
        println!(
            "recover-runtime result={}",
            match result {
                offline_recovery::RecoveryOutcome::Recovered => "recovered",
                offline_recovery::RecoveryOutcome::AlreadyStopped => "already-stopped",
            }
        );
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str()) == Some("reconcile-delivery") {
        let mut cli = vec![std::ffi::OsString::from("reconcile-delivery")];
        cli.extend(args);
        let (database, tuples) = offline_delivery::parse_args(&cli)
            .map_err(|_| app::AppError::Assembly("invalid reconcile-delivery arguments"))?;
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let count = offline_delivery::reconcile(database, &tuples, &now)
            .await
            .map_err(|_| app::AppError::Runtime)?;
        println!("reconcile-delivery result=reconciled count={count}");
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str()) == Some("recover-preacceptance") {
        let mut cli = vec![std::ffi::OsString::from("recover-preacceptance")];
        cli.extend(args);
        let (database, tuples) = offline_preacceptance::parse_args(&cli)
            .map_err(|_| app::AppError::Assembly("invalid recover-preacceptance arguments"))?;
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let count = offline_preacceptance::recover(database, &tuples, &now)
            .await
            .map_err(|_| app::AppError::Runtime)?;
        match count {
            offline_preacceptance::PreAcceptanceOutcome::Recovered { count } => {
                println!("recover-preacceptance result=recovered count={count}");
            }
            offline_preacceptance::PreAcceptanceOutcome::AlreadyReconciled => {
                println!("recover-preacceptance result=already-reconciled");
            }
        }
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str()) == Some("select-preacceptance") {
        let mut cli = vec![std::ffi::OsString::from("select-preacceptance")];
        cli.extend(args);
        let database = readonly_tuple::parse_args(&cli)
            .map_err(|error| selection_failure("select-preacceptance", error))?;
        let selected = readonly_tuple::select(database)
            .await
            .map_err(|error| selection_failure("select-preacceptance", error))?;
        let _ = selected;
        println!("select-preacceptance result=selected count=1");
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str()) == Some("diagnose-preacceptance-conflict") {
        let mut cli = vec![std::ffi::OsString::from("diagnose-preacceptance-conflict")];
        cli.extend(args);
        let database = readonly_tuple_diagnostics::parse_args(&cli).map_err(|_| {
            Error::Cli(error::CliFailure {
                command: "diagnose-preacceptance-conflict",
                category: "invalid-arguments",
                status: 2,
            })
        })?;
        match readonly_tuple::diagnose(database).await {
            Ok(_) => println!("diagnose-preacceptance-conflict result=eligible count=1"),
            Err(readonly_tuple_diagnostics::DiagnosedSelectionError::Conflicting(causes)) => {
                return Err(Error::Diagnostic(causes));
            }
            Err(error) => return Err(diagnosed_failure(error)),
        }
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str()) == Some("reconcile-lease-owner") {
        let mut cli = vec![std::ffi::OsString::from("reconcile-lease-owner")];
        cli.extend(args);
        let database = lease_owner_diagnostics::parse_args(&cli).map_err(|category| {
            Error::Cli(error::CliFailure {
                command: "reconcile-lease-owner",
                category: category.slug(),
                status: category.status(),
            })
        })?;
        let category = lease_owner_diagnostics::diagnose(database).await;
        return Err(Error::Cli(error::CliFailure {
            command: "reconcile-lease-owner",
            category: category.slug(),
            status: category.status(),
        }));
    }
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref().and_then(|v| v.to_str()) == Some("recover-selected-preacceptance") {
        let mut cli = vec![std::ffi::OsString::from("recover-selected-preacceptance")];
        cli.extend(args);
        let database = selected_preacceptance::parse_args(&cli).map_err(|_| {
            selection_failure(
                "recover-selected-preacceptance",
                readonly_tuple::SelectionError::InvalidArguments,
            )
        })?;
        let count =
            selected_preacceptance::recover(database)
                .await
                .map_err(|error| match error {
                    selected_preacceptance::SelectedRecoveryError::Selection(selection) => {
                        selection_failure("recover-selected-preacceptance", selection)
                    }
                    selected_preacceptance::SelectedRecoveryError::InvalidArguments => {
                        selection_failure(
                            "recover-selected-preacceptance",
                            readonly_tuple::SelectionError::InvalidArguments,
                        )
                    }
                    selected_preacceptance::SelectedRecoveryError::Empty => selection_failure(
                        "recover-selected-preacceptance",
                        readonly_tuple::SelectionError::Empty,
                    ),
                    selected_preacceptance::SelectedRecoveryError::Recovery => {
                        recovery_failure("recover-selected-preacceptance")
                    }
                })?;
        println!("recover-selected-preacceptance result=recovered count={count}");
        return Ok(());
    }
    init_tracing();
    info!("starting");
    let path = std::env::args_os()
        .nth(1)
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::var_os("GUIGU_CONFIG").map(std::path::PathBuf::from))
        .ok_or(app::AppError::Assembly(
            "configuration path required as argv[1] or GUIGU_CONFIG",
        ))?;
    run_with_shutdown(path, shutdown_signal()).await
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = interrupt.recv() => Ok(()),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

/// Start the configured production runtime, then run until shutdown resolves.
///
/// The explicit path and injectable signal let integration tests exercise the
/// same assembly and cleanup path as the binary without process-global state.
pub async fn run_with_shutdown<F>(
    config_path: impl AsRef<std::path::Path>,
    shutdown: F,
) -> Result<(), Error>
where
    F: Future<Output = std::io::Result<()>>,
{
    app::run_config_with_shutdown(config_path, shutdown).await?;
    info!("shutdown signal received, exiting gracefully");
    Ok(())
}

#[cfg(test)]
pub(crate) fn test_binary_path(name: &str) -> std::path::PathBuf {
    let variable = format!("CARGO_BIN_EXE_{name}");
    resolve_test_binary(
        name,
        std::env::var_os(&variable),
        std::env::var_os("CARGO_TARGET_DIR"),
    )
}

#[cfg(test)]
fn resolve_test_binary(
    name: &str,
    cargo_path: Option<std::ffi::OsString>,
    target_dir: Option<std::ffi::OsString>,
) -> std::path::PathBuf {
    if let Some(path) = cargo_path {
        return path.into();
    }
    if let Some(target) = target_dir {
        return std::path::PathBuf::from(target).join("debug").join(name);
    }
    std::path::PathBuf::from("target/debug").join(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_failures_have_frozen_redacted_statuses() {
        use readonly_tuple::SelectionError::*;
        let cases = [
            (InvalidArguments, "invalid-arguments", 2),
            (Empty, "empty", 3),
            (Multiple, "multiple", 4),
            (Malformed, "malformed", 5),
            (Conflicting, "conflicting", 6),
            (Nonterminal, "nonterminal", 7),
            (Busy, "busy", 8),
            (Database, "database", 9),
        ];
        for (error, category, status) in cases {
            let Error::Cli(failure) = selection_failure("select-preacceptance", error) else {
                panic!("expected bounded CLI failure");
            };
            assert_eq!(failure.command, "select-preacceptance");
            assert_eq!(failure.category, category);
            assert_eq!(failure.status, status);
        }
        let Error::Cli(failure) = recovery_failure("recover-selected-preacceptance") else {
            panic!("expected bounded CLI failure");
        };
        assert_eq!(failure.category, "recovery");
        assert_eq!(failure.status, 10);
    }

    #[test]
    fn diagnostic_failures_have_frozen_redacted_statuses() {
        use readonly_tuple_diagnostics::DiagnosedSelectionError::*;
        let cases = [
            (InvalidArguments, "invalid-arguments", 2),
            (Empty, "empty", 3),
            (Multiple, "multiple", 4),
            (Malformed, "malformed", 5),
            (Nonterminal, "nonterminal", 7),
            (Busy, "busy", 8),
            (Database, "database", 9),
        ];
        for (error, category, status) in cases {
            let Error::Cli(failure) = diagnosed_failure(error) else {
                panic!("expected bounded diagnostic failure")
            };
            assert_eq!(failure.category, category);
            assert_eq!(failure.status, status);
        }
    }

    #[test]
    fn init_tracing_is_idempotent() {
        init_tracing();
        init_tracing();
    }

    #[test]
    fn process_fixture_binary_binding_uses_hyphenated_cargo_name() {
        let path = resolve_test_binary(
            "guigu-agent-bridge",
            Some("/shared/target/debug/guigu-agent-bridge".into()),
            Some("/ignored/target".into()),
        );
        assert_eq!(
            path,
            std::path::PathBuf::from("/shared/target/debug/guigu-agent-bridge")
        );
        assert_eq!(
            resolve_test_binary("guigu-agent-bridge", None, Some("/shared/target".into())),
            std::path::PathBuf::from("/shared/target/debug/guigu-agent-bridge")
        );
    }
}
