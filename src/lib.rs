//! `guigu-agent-bridge` application entry point and minimal module boundary.
//!
//! The binary (`src/main.rs`) is a thin wrapper; all entry logic lives here so
//! integration tests can exercise the real call path through the public API.

pub mod error;

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
    run_with_shutdown(tokio::signal::ctrl_c()).await
}

/// Run until the given shutdown future resolves, then exit gracefully.
///
/// Separated from `run` so integration tests can inject an immediate or
/// controllable signal instead of a real OS signal.
pub async fn run_with_shutdown<F>(shutdown: F) -> Result<(), Error>
where
    F: Future<Output = std::io::Result<()>>,
{
    shutdown.await.map_err(Error::Shutdown)?;
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
