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
pub mod matrix;
pub mod models;
pub mod observer;
pub mod offline_delivery;
pub mod offline_preacceptance;
pub mod offline_recovery;
pub mod runtime;
pub mod storage;

use std::future::Future;

use tracing::info;

pub use error::Error;

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
    init_tracing();
    info!("starting");
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
        println!("recover-preacceptance result=recovered count={count}");
        return Ok(());
    }
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
    fn init_tracing_is_idempotent() {
        init_tracing();
        init_tracing();
    }
}
