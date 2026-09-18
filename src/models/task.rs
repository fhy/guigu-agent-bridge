//! Agent task model: the primary protocol for internal agent collaboration.
//!
//! An [`AgentTask`] routes a unit of work from one agent to another. `from_agent`
//! and `to_agent` are **resolved stable endpoint IDs**; `to_agent` is a single
//! target (no broadcast). The current task status is *not* stored on the task —
//! it is derived from the latest [`crate::models::event::TaskEvent`] (event
//! sourcing) to avoid dual-write inconsistency. `version` is reserved as an
//! optimistic-concurrency counter for conditional state updates (T005/T009).

use chrono::{DateTime, Utc};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::Error;
use crate::models::ids::{ConversationId, EndpointId, MessageId, TaskId};

/// Task lifecycle status.
///
/// Serialized as `snake_case`. Unknown values are rejected on deserialization
/// (fail-fast): `TaskStatus` is a state-machine contract, and silently accepting
/// an unknown status would corrupt state transitions. Existing variants are never
/// renamed or removed; new variants may be added (a public-contract change
/// requiring Coordinator confirmation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Waiting to be dispatched.
    Queued,
    /// Dispatched to an agent, awaiting acknowledgement.
    Dispatched,
    /// Actively running.
    Running,
    /// Finished successfully.
    Completed,
    /// Finished with an error.
    Failed,
    /// Exceeded its deadline.
    TimedOut,
    /// Cancelled before completion.
    Cancelled,
}

/// Task priority: a value in `0..=10`, higher meaning more urgent.
///
/// Out-of-range values are rejected explicitly (no panic, no silent truncation)
/// both at construction ([`Priority::new`]) and at the serde boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Priority(u8);

impl Priority {
    /// Lowest priority.
    pub const MIN: Self = Self(0);
    /// Default priority (used when a config omits the field).
    pub const DEFAULT: Self = Self(5);
    /// Highest priority.
    pub const MAX: Self = Self(10);

    const MAX_VALUE: u8 = 10;

    /// Create a priority, rejecting values above `10`.
    pub fn new(value: u8) -> Result<Self, Error> {
        if value > Self::MAX_VALUE {
            Err(Error::PriorityOutOfRange(value))
        } else {
            Ok(Self(value))
        }
    }

    /// The underlying numeric value.
    pub const fn value(self) -> u8 {
        self.0
    }
}

impl Default for Priority {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl Serialize for Priority {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(self.0)
    }
}

impl<'de> Deserialize<'de> for Priority {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u8::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

/// The primary protocol for internal agent collaboration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTask {
    /// This task's identifier.
    pub task_id: TaskId,
    /// Root of the task tree (a root task is its own root).
    pub root_task_id: TaskId,
    /// Parent task, if any (a root task has none).
    pub parent_task_id: Option<TaskId>,
    /// Resolved stable endpoint ID of the submitting agent.
    pub from_agent: EndpointId,
    /// Resolved stable endpoint ID of the single target agent.
    pub to_agent: EndpointId,
    /// Conversation this task belongs to.
    pub conversation_id: ConversationId,
    /// Optional message this task replies to.
    pub reply_to: Option<MessageId>,
    /// Task text / instruction.
    pub text: String,
    /// Scheduling priority.
    pub priority: Priority,
    /// Task-tree nesting depth (root = 0); used for cycle detection (T006).
    pub depth: u32,
    /// Number of agent-to-agent hops (user submission = 0); used for cycle
    /// detection (T006).
    pub hops: u32,
    /// Absolute UTC deadline, if any. `None` means no deadline.
    pub deadline: Option<DateTime<Utc>>,
    /// Reserved optimistic-concurrency version: `0` at creation, incremented on
    /// each state transition (consumed by T005/T009).
    pub version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_task() -> AgentTask {
        AgentTask {
            task_id: TaskId::generate(),
            root_task_id: TaskId::generate(),
            parent_task_id: None,
            from_agent: EndpointId::generate(),
            to_agent: EndpointId::generate(),
            conversation_id: ConversationId::generate(),
            reply_to: None,
            text: "do the thing".into(),
            priority: Priority::DEFAULT,
            depth: 0,
            hops: 0,
            deadline: None,
            version: 0,
        }
    }

    #[test]
    fn agent_task_serde_round_trip() {
        let task = sample_task();
        let json = serde_json::to_string(&task).expect("serialize");
        let back: AgentTask = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, task);
    }

    #[test]
    fn agent_task_with_parent_and_deadline_round_trips() {
        let parent = TaskId::generate();
        let reply_to = MessageId::generate();
        let deadline: DateTime<Utc> = "2026-09-15T12:00:00Z".parse().unwrap();
        let task = AgentTask {
            parent_task_id: Some(parent),
            reply_to: Some(reply_to),
            deadline: Some(deadline),
            depth: 2,
            hops: 1,
            version: 3,
            ..sample_task()
        };
        let json = serde_json::to_string(&task).expect("serialize");
        let back: AgentTask = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, task);
        assert_eq!(back.parent_task_id, Some(parent));
        assert_eq!(back.deadline, Some(deadline));
    }

    #[test]
    fn task_status_uses_snake_case() {
        let cases = [
            (TaskStatus::Queued, "queued"),
            (TaskStatus::Dispatched, "dispatched"),
            (TaskStatus::Running, "running"),
            (TaskStatus::Completed, "completed"),
            (TaskStatus::Failed, "failed"),
            (TaskStatus::TimedOut, "timed_out"),
            (TaskStatus::Cancelled, "cancelled"),
        ];
        for (status, expected) in cases {
            assert_eq!(
                serde_json::to_string(&status).unwrap(),
                format!("\"{expected}\"")
            );
            assert_eq!(
                serde_json::from_str::<TaskStatus>(&format!("\"{expected}\"")).unwrap(),
                status
            );
        }
    }

    #[test]
    fn task_status_rejects_unknown_value() {
        // Fail-fast: an unknown status must not be silently accepted.
        assert!(serde_json::from_str::<TaskStatus>("\"paused\"").is_err());
        assert!(serde_json::from_str::<TaskStatus>("\"done\"").is_err());
    }

    #[test]
    fn priority_accepts_valid_range() {
        assert_eq!(Priority::new(0).unwrap(), Priority::MIN);
        assert_eq!(Priority::new(5).unwrap(), Priority::DEFAULT);
        assert_eq!(Priority::new(10).unwrap(), Priority::MAX);
    }

    #[test]
    fn priority_rejects_out_of_range() {
        assert!(matches!(
            Priority::new(11),
            Err(Error::PriorityOutOfRange(11))
        ));
        assert!(matches!(
            Priority::new(255),
            Err(Error::PriorityOutOfRange(255))
        ));
    }

    #[test]
    fn priority_default_is_five() {
        assert_eq!(Priority::default(), Priority::DEFAULT);
        assert_eq!(Priority::default().value(), 5);
    }

    #[test]
    fn priority_serde_round_trip_and_rejects_out_of_range() {
        let priority = Priority::new(7).unwrap();
        let json = serde_json::to_string(&priority).expect("serialize");
        assert_eq!(json, "7");
        let back: Priority = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, priority);

        // The serde boundary reuses the same validation: out-of-range is a clear
        // error, not a silent truncation.
        assert!(serde_json::from_str::<Priority>("15").is_err());
        assert!(serde_json::from_str::<Priority>("255").is_err());
    }

    #[test]
    fn priority_orders_by_value() {
        let low = Priority::new(1).unwrap();
        let normal = Priority::new(5).unwrap();
        let high = Priority::new(9).unwrap();
        assert!(low < normal);
        assert!(normal < high);
        assert!(high > low);
    }

    #[test]
    fn deadline_serializes_with_z_suffix() {
        let deadline: DateTime<Utc> = "2026-09-15T12:00:00Z".parse().unwrap();
        let json = serde_json::to_string(&deadline).expect("serialize");
        // The JSON is a quoted string; the datetime value itself must end with 'Z'.
        let raw: String = serde_json::from_str(&json).expect("unquote");
        assert!(raw.ends_with('Z'), "expected Z suffix, got {raw}");
    }

    #[test]
    fn deadline_normalizes_offset_to_utc() {
        // A deadline expressed in a non-UTC offset is normalized to UTC.
        let deadline: DateTime<Utc> =
            serde_json::from_str(r#""2026-09-15T20:00:00+08:00""#).expect("deserialize");
        let expected: DateTime<Utc> = "2026-09-15T12:00:00Z".parse().unwrap();
        assert_eq!(deadline, expected);
    }

    #[test]
    fn deadline_none_and_some_branches_round_trip() {
        let no_deadline = sample_task();
        assert!(no_deadline.deadline.is_none());
        let json = serde_json::to_string(&no_deadline).expect("serialize");
        let back: AgentTask = serde_json::from_str(&json).expect("deserialize");
        assert!(back.deadline.is_none());

        let deadline: DateTime<Utc> = "2026-09-15T12:00:00Z".parse().unwrap();
        let with_deadline = AgentTask {
            deadline: Some(deadline),
            ..sample_task()
        };
        let json = serde_json::to_string(&with_deadline).expect("serialize");
        let back: AgentTask = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.deadline, Some(deadline));
    }

    #[test]
    fn to_agent_is_single_stable_endpoint_id() {
        // `to_agent` is a single resolved EndpointId; there is no broadcast field.
        let target = EndpointId::generate();
        let task = AgentTask {
            to_agent: target,
            ..sample_task()
        };
        assert_eq!(task.to_agent, target);
    }

    #[test]
    fn version_starts_at_zero() {
        // Reserved OCC version: a freshly created task has version 0.
        assert_eq!(sample_task().version, 0);
    }
}
