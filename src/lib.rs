//! `guigu-agent-bridge` application entry point and minimal module boundary.
//!
//! The binary (`src/main.rs`) is a thin wrapper; all entry logic lives here so
//! integration tests can exercise the real call path through the public API.

pub mod acp;
pub mod agents;
pub mod app;
pub mod bus;
pub mod config;
pub mod error;
pub mod matrix;
pub mod models;
pub mod observer;
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

/// Production entry point: initialize tracing, then run until the Ctrl-C signal.
pub async fn run() -> Result<(), Error> {
    init_tracing();
    info!("starting");
    let path = std::env::args_os()
        .nth(1)
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::var_os("GUIGU_CONFIG").map(std::path::PathBuf::from))
        .ok_or(app::AppError::Assembly(
            "configuration path required as argv[1] or GUIGU_CONFIG",
        ))?;
    run_with_shutdown(path, tokio::signal::ctrl_c()).await
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
