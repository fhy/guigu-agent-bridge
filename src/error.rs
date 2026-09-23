use thiserror::Error;

use crate::bus::BusError;
use crate::config::ConfigError;

/// Top-level application error boundary.
///
/// T001 keeps this minimal: only the errors the current entry point can actually
/// produce. Later tasks (config, storage, transport, adapters) add variants via
/// `#[from]` / `#[source]` without restructuring existing code.
#[derive(Debug, Error)]
pub enum Error {
    /// The shutdown signal (Ctrl-C) could not be awaited.
    #[error("failed to await shutdown signal: {0}")]
    Shutdown(#[source] std::io::Error),
    /// A priority value outside the valid range 0–10 was provided.
    #[error("priority out of range: {0} (valid range 0-10)")]
    PriorityOutOfRange(u8),
    /// Configuration could not be loaded or failed validation.
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),
    /// The Agent Bus rejected a submission or could not queue it.
    #[error("agent bus error: {0}")]
    Bus(#[from] BusError),
    /// Production assembly, adapter, health, or lifecycle failure.
    #[error("application error: {0}")]
    Application(#[from] crate::app::AppError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn shutdown_error_displays_and_sources() {
        let inner = io::Error::other("boom");
        let err = Error::Shutdown(inner);

        let message = err.to_string();
        assert!(message.contains("failed to await shutdown signal"));
        assert!(message.contains("boom"));

        let source = std::error::Error::source(&err).expect("expected a source error");
        assert_eq!(source.to_string(), "boom");
    }

    #[test]
    fn bus_error_converts_displays_and_sources() {
        // `#[from]` wiring: a bus failure is part of the top-level boundary and
        // keeps the bus error as its source, without leaking anything else.
        let err: Error = BusError::QueueFull.into();
        assert_eq!(err.to_string(), "agent bus error: task queue is full");

        let source = std::error::Error::source(&err).expect("bus error is the source");
        assert_eq!(source.to_string(), "task queue is full");
        assert!(source.downcast_ref::<BusError>().is_some());
    }
}
