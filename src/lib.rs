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
pub mod lease_owner_diagnostics;
pub mod matrix;
pub mod models;
pub mod observer;
pub mod offline_delivery;
pub mod offline_preacceptance;
pub mod offline_recovery;
pub mod orphaned_preacceptance;
pub mod readonly_tuple;
pub mod readonly_tuple_diagnostics;
pub mod runtime;
pub mod selected_preacceptance;
pub mod storage;
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
}
