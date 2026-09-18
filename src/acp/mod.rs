//! ACP adapter: the bridge's side of the Agent Client Protocol over stdio.
//!
//! This module connects the in-memory Agent Bus to a real ACP backend process. It
//! is the first half of stage six: T014 delivers the process, the protocol and the
//! session baseline; T015 adds streaming, protocol-level cancellation and crash
//! recovery on top.
//!
//! # Layers (ADR-001)
//!
//! The transport is deliberately split so that a backend needing different framing
//! does not disturb session logic:
//!
//! | layer | module | owns |
//! |-------|--------|------|
//! | byte framing | [`framing`] | `\n`-delimited JSON, limits, UTF-8, truncation |
//! | JSON-RPC | [`jsonrpc`] | request/response/notification shapes, id correlation |
//! | ACP schema | [`schema`] | method names and message payloads, capability policy |
//! | process + routing | [`transport`] | child process, pipes, pending requests, exit |
//! | protocol client | [`client`] | handshake, session lifecycle, bounded deadlines |
//! | session store | [`session`] | the `sessions` table, without touching `Repository` |
//! | dispatcher | [`dispatcher`] | the two-stage [`TaskDispatcher`](crate::bus::TaskDispatcher) mapping |
//!
//! # What the adapter advertises
//!
//! `initialize` advertises **no** client capabilities: the bridge does not serve
//! the agent's `fs/*` or `terminal/*` requests, and advertising a capability that
//! is not served would invite exactly those calls. A permission request is always
//! **denied** (permission policy is T011's); an unsupported agent request is
//! answered with JSON-RPC `Method not found`. See
//! [`schema::agent_request_response`].
//!
//! # Failure classification (ADR-001)
//!
//! [`AcpError`](error::AcpError) keeps process start, framing, schema negotiation,
//! authentication, agent execution and storage failures in separate variants, and
//! every rendered reason is bounded and free of credentials, command lines, task
//! bodies, paths and frame contents. `dispatcher` maps them onto the frozen
//! before/after-acceptance split:
//!
//! | stage | failure | reported as |
//! |-------|---------|-------------|
//! | `deliver` (session readiness) | spawn, initialize, session, framing, protocol, auth, timeout | [`DispatchError::NotAccepted`](crate::bus::DispatchError::NotAccepted) — the task never became `Running` |
//! | `execute` (prompt) | prompt timeout, transport exit, framing/protocol errors, agent error objects | [`DispatchError::ExecutionFailed`](crate::bus::DispatchError::ExecutionFailed) |
//!
//! # Boundaries with T015
//!
//! - **No streaming API.** The baseline consumes `session/update` notifications
//!   only to accumulate the turn's text into the final
//!   [`Completed`](crate::bus::DispatchOutcome::Completed) output; it exposes no
//!   event stream and no callback. T015 owns streaming.
//! - **No protocol-level cancellation and no crash recovery.** `session/cancel`
//!   is never sent, and T014 does not recover work that was in flight when a child
//!   died; it only guarantees that a *later* attempt gets a clean process. T015
//!   owns both.
//! - **No permissions policy.** Requests are denied, not evaluated (T011).
//! - **No production assembly.** Nothing here is wired into `main`; the assembly
//!   (pool, `sync_agents`, `AcpDispatcher` construction) belongs to T017.

pub mod client;
pub mod error;
pub mod framing;
pub mod jsonrpc;
pub mod result;
pub mod schema;
pub mod session;
pub mod transport;

mod dispatcher;

pub use client::{AcpClient, AcpLimits};
pub use dispatcher::{AcpDispatcher, AcpDispatcherBuilder, AcpStructuredTurn};
pub use error::{AcpError, Phase};
pub use framing::{FrameError, MAX_FRAME_BYTES};
pub use jsonrpc::{JsonRpcErrorObject, JsonRpcMessage, ProtocolError, RequestId};
pub use result::{TurnResult, WireTurnResult};
pub use schema::PROTOCOL_VERSION;
pub use session::{MAX_SESSION_ID_BYTES, SessionState, SqliteSessionStore, StoredSession};
pub use transport::{AcpTransport, AcpTurnHandle, CompletedRequest, TransportLimits, TurnUpdate};

/// How much of any single self-authored detail string is retained.
///
/// Every reason that reaches a `Failed` event or a log line is truncated to this
/// many bytes at a character boundary, so a broken backend cannot inject a large
/// payload into the event store through an error message.
pub const MAX_DETAIL_BYTES: usize = 256;

/// Truncate `text` to [`MAX_DETAIL_BYTES`] at a character boundary.
pub(crate) fn bounded(text: &str) -> String {
    if text.len() <= MAX_DETAIL_BYTES {
        return text.to_owned();
    }
    let mut end = MAX_DETAIL_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_long_detail_is_truncated_at_a_character_boundary() {
        assert_eq!(bounded("short"), "short");
        assert_eq!(bounded(&"x".repeat(4096)).len(), MAX_DETAIL_BYTES);
        // Three-byte characters: the cut must not split one.
        let multibyte = bounded(&"€".repeat(1000));
        assert!(multibyte.len() <= MAX_DETAIL_BYTES);
        assert!(multibyte.chars().all(|character| character == '€'));
    }
}
