//! Immutable task event model.
//!
//! Every task state change produces an immutable [`TaskEvent`]. Events carry two
//! complementary identities for idempotency and ordering:
//!
//! - `id` — a globally unique event identity (UUIDv7) used for cross-system and
//!   cross-restart deduplication.
//! - `seq` — a per-task monotonically increasing sequence number (starting at 1)
//!   used for in-task ordering and gap detection.
//!
//! This module only defines the event data contract. The state machine that
//! produces events, delivery-record persistence, and conditional updates are out
//! of scope here (T004/T005/T009).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::models::ids::{DeliveryId, EventId, TaskId};
use crate::models::task::TaskStatus;

/// An immutable record of a single task state change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEvent {
    /// Globally unique event identity (cross-system dedup).
    pub id: EventId,
    /// The task this event belongs to.
    pub task_id: TaskId,
    /// Per-task monotonically increasing sequence number (starts at 1).
    pub seq: u64,
    /// The status this event transitions the task to.
    pub status: TaskStatus,
    /// When the event occurred (UTC).
    pub timestamp: DateTime<Utc>,
    /// Status-specific payload.
    pub payload: TaskEventPayload,
}

/// Status-specific payload for a [`TaskEvent`].
///
/// Serialized as `snake_case` (externally tagged). Unknown values are rejected on
/// deserialization (fail-fast).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskEventPayload {
    /// The task was queued.
    Queued,
    /// The task was dispatched to an agent. Carries the reserved delivery identity
    /// and attempt number (consumed by T004/T005/T009).
    Dispatched {
        /// Reserved delivery-attempt identity.
        delivery_id: DeliveryId,
        /// Delivery attempt number (starts at 1).
        attempt: u32,
    },
    /// The task started running.
    Running {
        /// When execution started (UTC).
        started_at: DateTime<Utc>,
    },
    /// The task completed successfully.
    Completed {
        /// Final output.
        output: String,
    },
    /// The task failed.
    Failed {
        /// Error description.
        error: String,
    },
    /// The task exceeded its deadline.
    TimedOut {
        /// The deadline that was exceeded (UTC).
        deadline: DateTime<Utc>,
    },
    /// The task was cancelled.
    Cancelled {
        /// Cancellation reason.
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(seq: u64) -> TaskEvent {
        TaskEvent {
            id: EventId::generate(),
            task_id: TaskId::generate(),
            seq,
            status: TaskStatus::Queued,
            timestamp: "2026-09-15T12:00:00Z".parse().unwrap(),
            payload: TaskEventPayload::Queued,
        }
    }

    #[test]
    fn task_event_serde_round_trip() {
        let event = sample_event(1);
        let json = serde_json::to_string(&event).expect("serialize");
        let back: TaskEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, event);
    }

    #[test]
    fn event_id_is_globally_unique() {
        let a = EventId::generate();
        let b = EventId::generate();
        assert_ne!(a, b, "event IDs must be globally unique for dedup");
    }

    #[test]
    fn seq_is_per_task_monotonic() {
        // `seq` is a per-task monotonic sequence starting at 1. Constructing a
        // run of events for one task yields strictly increasing seq values.
        let task_id = TaskId::generate();
        let events: Vec<TaskEvent> = (1..=3)
            .map(|seq| TaskEvent {
                task_id,
                seq,
                ..sample_event(seq)
            })
            .collect();
        for window in events.windows(2) {
            assert!(
                window[0].seq < window[1].seq,
                "seq must be strictly increasing within a task"
            );
        }
        assert_eq!(events[0].seq, 1, "seq starts at 1");
    }

    #[test]
    fn dispatched_payload_carries_delivery_id() {
        let delivery_id = DeliveryId::generate();
        let payload = TaskEventPayload::Dispatched {
            delivery_id,
            attempt: 1,
        };
        let json = serde_json::to_string(&payload).expect("serialize");
        let back: TaskEventPayload = serde_json::from_str(&json).expect("deserialize");
        match back {
            TaskEventPayload::Dispatched {
                delivery_id: back_id,
                attempt,
            } => {
                assert_eq!(back_id, delivery_id);
                assert_eq!(attempt, 1);
            }
            other => panic!("expected Dispatched payload, got {other:?}"),
        }
    }

    #[test]
    fn payload_variants_use_snake_case() {
        let started_at: DateTime<Utc> = "2026-09-15T12:00:00Z".parse().unwrap();
        let deadline: DateTime<Utc> = "2026-09-15T12:05:00Z".parse().unwrap();

        let cases: Vec<(TaskEventPayload, &str)> = vec![
            (TaskEventPayload::Queued, "queued"),
            (TaskEventPayload::Running { started_at }, "running"),
            (
                TaskEventPayload::Completed {
                    output: "ok".into(),
                },
                "completed",
            ),
            (
                TaskEventPayload::Failed {
                    error: "boom".into(),
                },
                "failed",
            ),
            (TaskEventPayload::TimedOut { deadline }, "timed_out"),
            (
                TaskEventPayload::Cancelled {
                    reason: "user".into(),
                },
                "cancelled",
            ),
        ];
        for (payload, tag) in &cases {
            let json = serde_json::to_string(payload).expect("serialize");
            assert!(
                json.contains(&format!("\"{tag}\"")),
                "payload {json} should be tagged {tag}"
            );
            let back: TaskEventPayload = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(&back, payload);
        }
    }

    #[test]
    fn payload_rejects_unknown_value() {
        // Fail-fast: an unknown payload variant must not be silently accepted.
        assert!(serde_json::from_str::<TaskEventPayload>("\"suspended\"").is_err());
        assert!(serde_json::from_str::<TaskEventPayload>("\"pending\"").is_err());
    }

    #[test]
    fn older_payload_shapes_still_deserialize() {
        // Backward compatibility: data written without any future variant still
        // deserializes cleanly. A hand-written "old" event (only known variants)
        // must parse.
        let old_json = r#"{
            "id": "01923a4e-7c1e-7000-8000-000000000001",
            "task_id": "01923a4e-7c1e-7000-8000-000000000002",
            "seq": 1,
            "status": "queued",
            "timestamp": "2026-09-15T12:00:00Z",
            "payload": "queued"
        }"#;
        let event: TaskEvent = serde_json::from_str(old_json).expect("old event deserializes");
        assert_eq!(event.status, TaskStatus::Queued);
        assert_eq!(event.seq, 1);
        assert!(matches!(event.payload, TaskEventPayload::Queued));
    }
}
