//! Errors produced by the Agent Bus.
//!
//! The variant set is deliberately small and closed for T004: it covers the three
//! submission-time target rejections, the two bounded-channel overflows, and
//! phase-specific closure.
//!
//! **No leaking of sensitive data.** Every variant renders only fixed text and an
//! opaque [`EndpointId`]. An [`crate::models::EndpointAddress`] (which may hold an
//! executable command line, a Matrix user id, or an external URL) is never
//! rendered, and credentials never reach this type at all.

use thiserror::Error;

use crate::models::EndpointId;

/// A task submission or event write failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BusError {
    /// The target endpoint is not declared in the endpoint registry.
    ///
    /// Nothing was enqueued and no event was written.
    #[error("unknown target endpoint: {endpoint_id}")]
    UnknownTarget {
        /// The rejected target.
        endpoint_id: EndpointId,
    },
    /// The target endpoint is declared but disabled (`enabled = false`).
    ///
    /// Nothing was enqueued and no event was written.
    #[error("target endpoint is disabled: {endpoint_id}")]
    TargetDisabled {
        /// The rejected target.
        endpoint_id: EndpointId,
    },
    /// The target endpoint is declared and enabled, but its transport address
    /// cannot be derived from the current configuration (`matrix`/`http`
    /// declarations carry no `user_id`/`url` yet — T010/T014).
    ///
    /// Nothing was enqueued and no event was written.
    #[error("endpoint address is not available: {endpoint_id}")]
    AddressUnavailable {
        /// The rejected target.
        endpoint_id: EndpointId,
    },
    /// The bounded task queue is full (explicit backpressure).
    ///
    /// Nothing was enqueued and no event was written.
    #[error("task queue is full")]
    QueueFull,
    /// The bounded event buffer is full.
    ///
    /// The task was already enqueued; only its `Queued` event is missing (see
    /// [`crate::bus::Bus::submit`]).
    #[error("event buffer is full")]
    EventBufferFull,
    /// The task consumer is gone, so the task was not enqueued.
    #[error("task channel is closed")]
    TaskChannelClosed,
    /// The event consumer is gone after the task was enqueued.
    #[error("event sink is closed")]
    EventSinkClosed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::EndpointId;

    /// A bus error must never render an address (command line, Matrix user id,
    /// external URL) — it carries no such field by construction. This test pins
    /// the rendered shape of every variant so a future variant cannot quietly
    /// start interpolating an address or a credential.
    #[test]
    fn display_renders_only_fixed_text_and_the_endpoint_id() {
        let id = EndpointId::from_uuid(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            b"bus-error-display-test",
        ));
        let cases: [(BusError, &str, bool); 7] = [
            (
                BusError::UnknownTarget { endpoint_id: id },
                "unknown target endpoint",
                true,
            ),
            (
                BusError::TargetDisabled { endpoint_id: id },
                "target endpoint is disabled",
                true,
            ),
            (
                BusError::AddressUnavailable { endpoint_id: id },
                "endpoint address is not available",
                true,
            ),
            (BusError::QueueFull, "task queue is full", false),
            (BusError::EventBufferFull, "event buffer is full", false),
            (BusError::TaskChannelClosed, "task channel is closed", false),
            (BusError::EventSinkClosed, "event sink is closed", false),
        ];

        for (err, prefix, names_endpoint) in cases {
            let rendered = err.to_string();
            assert!(rendered.starts_with(prefix), "got: {rendered}");
            assert!(
                !rendered.contains("command") && !rendered.contains("args"),
                "an address must never be rendered: {rendered}"
            );
            assert_eq!(
                rendered.contains(&id.to_string()),
                names_endpoint,
                "endpoint id rendering is part of the contract: {rendered}"
            );
        }
    }

    #[test]
    fn closed_and_overflow_variants_are_distinguishable() {
        // T011/T014 need to tell "queue full" apart from "bus shutting down":
        // one is retryable, the other is not.
        assert_ne!(BusError::QueueFull, BusError::TaskChannelClosed);
        assert_ne!(BusError::EventBufferFull, BusError::EventSinkClosed);
        assert_ne!(BusError::TaskChannelClosed, BusError::EventSinkClosed);
        assert_ne!(BusError::QueueFull, BusError::EventBufferFull);
    }
}
