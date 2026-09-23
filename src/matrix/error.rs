//! Redacted Matrix adapter errors.

use thiserror::Error;

/// Failures exposed by the Matrix adapter.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MatrixError {
    /// Matrix configuration cannot construct a client or session.
    #[error("invalid Matrix configuration")]
    Configuration,
    /// The configured session is not accepted.
    #[error("Matrix authentication failed")]
    Authentication,
    /// The configured device has not been approved for encrypted traffic.
    #[error("Matrix device is not trusted")]
    DeviceUntrusted,
    /// The restored device/session was rejected and requires operator recovery.
    #[error("Matrix device was rejected")]
    DeviceKicked,
    /// A request could not reach or receive a valid HTTP response.
    #[error("Matrix transport failed")]
    Transport,
    /// A known Matrix event or response has an invalid shape.
    #[error("invalid Matrix protocol data: {detail}")]
    Protocol {
        /// Bounded, non-sensitive description.
        detail: &'static str,
    },
    /// Conversation persistence failed.
    #[error("Matrix conversation persistence failed")]
    Storage,
    /// The bounded inbound queue is full.
    #[error("Matrix inbound event queue is full")]
    Backpressure,
    /// The inbound consumer has gone away.
    #[error("Matrix inbound event consumer is closed")]
    ConsumerClosed,
    /// The sync owner task terminated unexpectedly.
    #[error("Matrix sync task failed")]
    Join,
}
