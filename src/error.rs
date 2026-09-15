use thiserror::Error;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn shutdown_error_displays_and_sources() {
        let inner = io::Error::new(io::ErrorKind::Other, "boom");
        let err = Error::Shutdown(inner);

        let message = err.to_string();
        assert!(message.contains("failed to await shutdown signal"));
        assert!(message.contains("boom"));

        let source = std::error::Error::source(&err).expect("expected a source error");
        assert_eq!(source.to_string(), "boom");
    }
}
