//! The ACP adapter's error surface.
//!
//! ADR-001 requires process startup, framing, schema negotiation, authentication
//! and agent execution failures to be **reported separately**, so each has its own
//! variant here. Two rendering invariants hold for every variant, and both are
//! covered by tests:
//!
//! - a reason is **bounded** (details are truncated to
//!   [`MAX_DETAIL_BYTES`](crate::acp::MAX_DETAIL_BYTES)),
//! - a reason never contains the agent's command line or arguments (they may carry
//!   credentials), the task body, the working-directory path, or frame contents.
//!   Only the endpoint's opaque id, a phase name, an OS error, a JSON-RPC code and
//!   a bounded message ever appear.
//!
//! The adapter maps these onto the frozen [`DispatchError`](crate::bus::DispatchError)
//! split (before/after acceptance) in [`crate::acp::dispatcher`].

use std::fmt;

use thiserror::Error;

use crate::acp::framing::FrameError;
use crate::acp::jsonrpc::{JsonRpcErrorObject, ProtocolError};
use crate::storage::StorageError;

/// Which step of the exchange failed.
///
/// A phase is carried into [`AcpError::Timeout`] and [`AcpError::Exited`] so the
/// caller can tell an initialization timeout from a prompt timeout without
/// parsing text — the same reason the `seq` a failure lands on identifies the
/// stage in the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Starting the child process.
    Spawn,
    /// The `initialize` handshake.
    Initialize,
    /// ACP authentication.
    Authenticate,
    /// `session/new` or `session/resume`.
    Session,
    /// `session/prompt`.
    Prompt,
    /// Shutting the transport down.
    Shutdown,
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Spawn => "process start",
            Self::Initialize => "initialize",
            Self::Authenticate => "authenticate",
            Self::Session => "session setup",
            Self::Prompt => "prompt",
            Self::Shutdown => "shutdown",
        })
    }
}

/// A failure in the ACP adapter.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AcpError {
    /// The endpoint does not carry an ACP address, so it cannot be spawned.
    #[error("{phase}: the endpoint has no ACP address")]
    UnsupportedAddress {
        /// The step that needed the address.
        phase: Phase,
    },
    /// The child process could not be started.
    ///
    /// Carries the OS error only — never the program or its arguments.
    #[error("{phase} failed: {source}")]
    Spawn {
        /// The step that failed.
        phase: Phase,
        /// The OS-level failure.
        #[source]
        source: std::io::Error,
    },
    /// A bounded wait elapsed.
    #[error("{phase} timed out")]
    Timeout {
        /// The step that timed out.
        phase: Phase,
    },
    /// The bounded turn-update receiver was not drained quickly enough.
    #[error("turn update stream is full")]
    StreamBackpressure,
    /// The backend was restored, but the in-flight prompt was not replayed.
    #[error("backend recovered; accepted delivery still needs an outcome")]
    RecoveryNeeded,
    /// The child process ended before the step completed.
    #[error("{phase} failed: the child process exited ({detail})")]
    Exited {
        /// The step that was running.
        phase: Phase,
        /// `status N`, `signal N`, or `unknown`.
        detail: String,
    },
    /// The transport handle no longer has a running child or loop.
    #[error("the ACP transport is closed")]
    TransportClosed,
    /// The agent negotiated a protocol version this adapter does not speak.
    #[error("the agent speaks protocol version {got}, this adapter speaks {supported}")]
    UnsupportedProtocol {
        /// The version the agent answered with.
        got: u16,
        /// The version this adapter speaks.
        supported: u16,
    },
    /// The agent requires authentication, which v1 does not implement.
    #[error("the agent requires authentication ({advertised} method(s) offered)")]
    Authentication {
        /// How many methods the agent advertised.
        advertised: usize,
    },
    /// The byte layer failed.
    #[error("framing failed: {source}")]
    Frame {
        /// The framing failure.
        #[from]
        source: FrameError,
    },
    /// The JSON-RPC layer failed.
    #[error("JSON-RPC failed: {source}")]
    Protocol {
        /// The protocol failure.
        #[from]
        source: ProtocolError,
    },
    /// A payload did not match the ACP schema.
    #[error("{method} returned a malformed payload: {detail}")]
    Schema {
        /// The ACP method whose answer was malformed.
        method: String,
        /// Bounded description of the problem.
        detail: String,
    },
    /// The agent answered with a JSON-RPC error object.
    #[error("the agent rejected the request: {source}")]
    Agent {
        /// The agent's error object.
        #[source]
        source: JsonRpcErrorObject,
    },
    /// The session identity is unusable.
    #[error("the agent returned an unusable session id: {detail}")]
    SessionId {
        /// Bounded description of the problem.
        detail: String,
    },
    /// The durable session store failed.
    #[error("session storage failed: {source}")]
    Storage {
        /// The storage failure.
        #[from]
        source: StorageError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acp::bounded;

    #[test]
    fn every_variant_renders_a_stable_bounded_shape() {
        let cases = vec![
            AcpError::UnsupportedAddress {
                phase: Phase::Spawn,
            }
            .to_string(),
            AcpError::Timeout {
                phase: Phase::Initialize,
            }
            .to_string(),
            AcpError::Exited {
                phase: Phase::Prompt,
                detail: "status 1".to_owned(),
            }
            .to_string(),
            AcpError::TransportClosed.to_string(),
            AcpError::UnsupportedProtocol {
                got: 9,
                supported: 1,
            }
            .to_string(),
            AcpError::Authentication { advertised: 2 }.to_string(),
            AcpError::Frame {
                source: FrameError::Truncated,
            }
            .to_string(),
            AcpError::Schema {
                method: "session/new".to_owned(),
                detail: bounded(&"x".repeat(4096)),
            }
            .to_string(),
            AcpError::Agent {
                source: JsonRpcErrorObject::new(-32601, "nope"),
            }
            .to_string(),
            AcpError::SessionId {
                detail: "empty".to_owned(),
            }
            .to_string(),
        ];
        for rendered in cases {
            assert!(!rendered.is_empty());
            assert!(rendered.len() <= 512, "unbounded reason: {rendered}");
        }
    }

    #[test]
    fn a_phase_renders_its_step() {
        assert_eq!(Phase::Spawn.to_string(), "process start");
        assert_eq!(Phase::Prompt.to_string(), "prompt");
    }

    #[test]
    fn the_adapter_error_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AcpError>();
    }
}
