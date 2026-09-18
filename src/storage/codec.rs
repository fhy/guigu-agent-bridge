//! Storage-representation codec: the single place that decides how domain values
//! are written to and read back from columns (analysis §4.2-§4.5, gate D2-D5).
//!
//! Keeping the representation in one module is what lets T009's repository
//! implementation and this task's round-trip tests share exactly one mapping, so
//! the on-disk form cannot drift between the writer and the reader.
//!
//! | value | column form |
//! |-------|-------------|
//! | internal IDs | lowercase, hyphenated, 36-character UUID `TEXT` |
//! | timestamps | fixed-width UTC nanosecond RFC 3339, e.g. `2026-09-16T10:00:00.000000000Z` |
//! | `Option<DateTime<Utc>>` | SQL `NULL` (never a sentinel or the string `"null"`) |
//! | `TransportType` / `TaskStatus` | `TEXT` in `snake_case`, matching the serde contract |
//! | `TaskEventPayload`, `EndpointAddress`, `capabilities`, `participants`, `metadata` | JSON `TEXT` via serde |
//! | `u64` / `u32` | `INTEGER`, range-checked against signed 64-bit |
//!
//! Decoding never falls back to a default: a stored value that cannot be decoded
//! is [`StorageError::Malformed`]. Encoding never truncates: a value that does not
//! fit is [`StorageError::OutOfRange`]. Error details never echo the stored value,
//! so they cannot leak record contents into logs.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::models::{Priority, TaskStatus, TransportType};
use crate::storage::StorageError;

// ── identifiers ───────────────────────────────────────────────────────────────

/// Encode an internal ID as its canonical lowercase 36-character UUID text.
pub(crate) fn encode_id<T: fmt::Display>(id: T) -> String {
    id.to_string()
}

/// Decode a canonical UUID text column back into an internal ID.
///
/// # Errors
///
/// [`StorageError::Malformed`] when the stored text is not a UUID. The offending
/// text is not included in the error.
pub(crate) fn decode_id<T: FromStr>(text: &str, field: &'static str) -> Result<T, StorageError> {
    text.parse::<T>().map_err(|_| StorageError::Malformed {
        field,
        detail: "value is not a valid internal identifier".to_owned(),
    })
}

// ── timestamps ────────────────────────────────────────────────────────────────

/// Encode an instant as fixed-width UTC nanosecond RFC 3339.
///
/// Fixed width is what makes lexicographic order equal chronological order, so
/// `ORDER BY` on a timestamp column and string comparisons (such as the
/// `acknowledged_at >= dispatched_at` constraint) are correct. Variable precision
/// would break that (`"…00.5Z"` sorts after `"…00Z"` though it is earlier).
pub(crate) fn encode_timestamp(at: &DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

/// Decode a stored timestamp, normalising any offset to UTC.
///
/// # Errors
///
/// [`StorageError::Malformed`] when the stored text is not RFC 3339.
pub(crate) fn decode_timestamp(
    text: &str,
    field: &'static str,
) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(text)
        .map(|parsed| parsed.with_timezone(&Utc))
        .map_err(|error| StorageError::Malformed {
            field,
            detail: error.to_string(),
        })
}

/// Encode an optional instant: `None` becomes SQL `NULL`.
pub(crate) fn encode_optional_timestamp(at: Option<&DateTime<Utc>>) -> Option<String> {
    at.map(encode_timestamp)
}

/// Decode an optional instant: SQL `NULL` becomes `None`.
///
/// # Errors
///
/// [`StorageError::Malformed`] when a present value is not RFC 3339.
pub(crate) fn decode_optional_timestamp(
    text: Option<&str>,
    field: &'static str,
) -> Result<Option<DateTime<Utc>>, StorageError> {
    text.map(|value| decode_timestamp(value, field)).transpose()
}

// ── enumerations ──────────────────────────────────────────────────────────────

/// Encode a transport as its `snake_case` text value.
pub(crate) fn encode_transport(transport: TransportType) -> &'static str {
    match transport {
        TransportType::Acp => "acp",
        TransportType::Matrix => "matrix",
        TransportType::Http => "http",
        TransportType::A2a => "a2a",
    }
}

/// Decode a stored transport value.
///
/// # Errors
///
/// [`StorageError::Malformed`] for an unknown value: an unrecognised transport
/// means no adapter can handle the endpoint, so it is never mapped to a default.
pub(crate) fn decode_transport(
    text: &str,
    field: &'static str,
) -> Result<TransportType, StorageError> {
    match text {
        "acp" => Ok(TransportType::Acp),
        "matrix" => Ok(TransportType::Matrix),
        "http" => Ok(TransportType::Http),
        "a2a" => Ok(TransportType::A2a),
        _ => Err(malformed(field, "unknown transport")),
    }
}

/// Encode a task status as its `snake_case` text value.
pub(crate) fn encode_status(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Dispatched => "dispatched",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::TimedOut => "timed_out",
        TaskStatus::Cancelled => "cancelled",
    }
}

/// Decode a stored task status value.
///
/// # Errors
///
/// [`StorageError::Malformed`] for an unknown value: `TaskStatus` is a
/// state-machine contract, so an unknown status must not be silently accepted.
pub(crate) fn decode_status(text: &str, field: &'static str) -> Result<TaskStatus, StorageError> {
    match text {
        "queued" => Ok(TaskStatus::Queued),
        "dispatched" => Ok(TaskStatus::Dispatched),
        "running" => Ok(TaskStatus::Running),
        "completed" => Ok(TaskStatus::Completed),
        "failed" => Ok(TaskStatus::Failed),
        "timed_out" => Ok(TaskStatus::TimedOut),
        "cancelled" => Ok(TaskStatus::Cancelled),
        _ => Err(malformed(field, "unknown task status")),
    }
}

// ── JSON columns ──────────────────────────────────────────────────────────────

/// Encode a value as JSON text for storage.
///
/// # Errors
///
/// [`StorageError::Malformed`] when serialisation fails (a model value that
/// cannot be represented; no model in this crate can fail).
pub(crate) fn encode_json<T: Serialize + ?Sized>(
    value: &T,
    field: &'static str,
) -> Result<String, StorageError> {
    serde_json::to_string(value).map_err(|error| StorageError::Malformed {
        field,
        detail: error.to_string(),
    })
}

/// Decode JSON text into a model value.
///
/// # Errors
///
/// [`StorageError::Malformed`] when the text is not valid JSON for `T`.
pub(crate) fn decode_json<T: DeserializeOwned>(
    text: &str,
    field: &'static str,
) -> Result<T, StorageError> {
    serde_json::from_str(text).map_err(|error| StorageError::Malformed {
        field,
        detail: error.to_string(),
    })
}

// ── integers ──────────────────────────────────────────────────────────────────

/// Encode a `u64` as a SQLite `INTEGER`.
///
/// # Errors
///
/// [`StorageError::OutOfRange`] when the value exceeds `i64::MAX`. SQLite
/// integers are signed 64-bit; the alternative would be silent truncation.
pub(crate) fn encode_u64(value: u64, field: &'static str) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| StorageError::OutOfRange {
        field,
        value: value.to_string(),
    })
}

/// Decode a SQLite `INTEGER` into a `u64`.
///
/// # Errors
///
/// [`StorageError::OutOfRange`] for a negative stored value.
pub(crate) fn decode_u64(value: i64, field: &'static str) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::OutOfRange {
        field,
        value: value.to_string(),
    })
}

/// Encode a `u32` as a SQLite `INTEGER` (always representable).
pub(crate) fn encode_u32(value: u32) -> i64 {
    i64::from(value)
}

/// Decode a SQLite `INTEGER` into a `u32`.
///
/// # Errors
///
/// [`StorageError::OutOfRange`] when the value does not fit in `u32`.
pub(crate) fn decode_u32(value: i64, field: &'static str) -> Result<u32, StorageError> {
    u32::try_from(value).map_err(|_| StorageError::OutOfRange {
        field,
        value: value.to_string(),
    })
}

/// Encode a boolean as `INTEGER` `0`/`1`.
pub(crate) fn encode_bool(value: bool) -> i64 {
    i64::from(value)
}

/// Decode an `INTEGER` `0`/`1` into a boolean.
///
/// # Errors
///
/// [`StorageError::Malformed`] for any other value.
pub(crate) fn decode_bool(value: i64, field: &'static str) -> Result<bool, StorageError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(malformed(field, "boolean is neither 0 nor 1")),
    }
}

/// Encode a task priority as its numeric value.
pub(crate) fn encode_priority(priority: Priority) -> i64 {
    i64::from(priority.value())
}

/// Decode a numeric priority, re-applying the model's `0..=10` invariant.
///
/// # Errors
///
/// [`StorageError::Malformed`] when the stored value is not a valid priority.
pub(crate) fn decode_priority(value: i64, field: &'static str) -> Result<Priority, StorageError> {
    let raw =
        u8::try_from(value).map_err(|_| malformed(field, "priority is out of the u8 range"))?;
    Priority::new(raw).map_err(|_| malformed(field, "priority is outside 0..=10"))
}

fn malformed(field: &'static str, detail: &'static str) -> StorageError {
    StorageError::Malformed {
        field,
        detail: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::registry::derive_endpoint_id;
    use crate::models::{
        AgentEndpoint, AgentTask, Capability, Conversation, ConversationId, DeliveryId,
        EndpointAddress, EndpointId, EventId, ExternalRef, Message, MessageId, TaskEvent,
        TaskEventPayload, TaskId,
    };
    use crate::storage::{Delivery, connect, migrate};
    use chrono::Duration;
    use sqlx::{Row, SqlitePool};
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    fn temp_db_path(tag: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "guigu-storage-codec-{tag}-{}.db",
            uuid::Uuid::now_v7()
        ));
        path
    }

    async fn migrated_pool(tag: &str) -> (SqlitePool, PathBuf) {
        let path = temp_db_path(tag);
        let pool = connect(&path).await.expect("connect");
        migrate(&pool).await.expect("migrate");
        (pool, path)
    }

    fn remove_db_files(path: &Path) {
        for suffix in ["", "-wal", "-shm"] {
            let mut candidate = path.as_os_str().to_owned();
            candidate.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(candidate));
        }
    }

    #[test]
    fn identifier_encoding_is_canonical_and_reuses_the_frozen_derivation() {
        let task_id = TaskId::generate();
        let text = encode_id(task_id);
        assert_eq!(text.len(), 36, "canonical UUID text is 36 characters");
        assert!(
            text.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "canonical UUID text is lowercase: {text}"
        );
        assert_eq!(
            decode_id::<TaskId>(&text, "tasks.task_id").unwrap(),
            task_id
        );

        // Storage must reuse ADR-003's derivation rather than mint its own
        // endpoint identity: the value below is pinned in `bus::registry`.
        let endpoint = derive_endpoint_id("alpha");
        assert_eq!(encode_id(endpoint), "c8521746-e5fc-551a-b96b-86f1cd310117");
        assert_eq!(
            decode_id::<EndpointId>("c8521746-e5fc-551a-b96b-86f1cd310117", "agents.endpoint_id")
                .unwrap(),
            endpoint
        );

        for garbage in ["", "not-a-uuid", "@user:matrix.org", "!room:matrix.org"] {
            let error = decode_id::<TaskId>(garbage, "tasks.task_id").expect_err("must reject");
            assert!(matches!(error, StorageError::Malformed { .. }), "{error:?}");
        }
    }

    #[test]
    fn timestamps_are_fixed_width_utc_nanoseconds() {
        let instant: DateTime<Utc> = "2026-09-16T10:00:00.000000005Z".parse().unwrap();
        let text = encode_timestamp(&instant);
        assert_eq!(text, "2026-09-16T10:00:00.000000005Z");
        assert_eq!(text.len(), 30, "fixed width: no precision varies");
        assert_eq!(
            decode_timestamp(&text, "task_events.timestamp").unwrap(),
            instant
        );

        // Nanosecond precision survives a round trip, including `Utc::now()`.
        let now = Utc::now();
        assert_eq!(
            decode_timestamp(&encode_timestamp(&now), "task_events.timestamp").unwrap(),
            now,
            "write-then-read must be lossless"
        );

        // Non-UTC offsets are normalised to UTC on read.
        assert_eq!(
            decode_timestamp("2026-09-16T18:00:00+08:00", "task_events.timestamp").unwrap(),
            "2026-09-16T10:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );

        // Fixed width makes string order equal time order.
        let earlier: DateTime<Utc> = "2026-09-16T09:59:59.999999999Z".parse().unwrap();
        let later: DateTime<Utc> = "2026-09-16T10:00:00.000000000Z".parse().unwrap();
        assert!(encode_timestamp(&earlier) < encode_timestamp(&later));
        assert!(encode_timestamp(&later) < encode_timestamp(&(later + Duration::nanoseconds(1))));

        assert!(matches!(
            decode_timestamp("2026-09-16 10:00:00", "task_events.timestamp"),
            Err(StorageError::Malformed { .. })
        ));
    }

    #[test]
    fn optional_timestamps_map_none_to_null() {
        assert_eq!(encode_optional_timestamp(None), None);
        assert_eq!(
            decode_optional_timestamp(None, "tasks.deadline").unwrap(),
            None
        );

        let instant: DateTime<Utc> = "2026-09-16T10:00:00Z".parse().unwrap();
        let encoded = encode_optional_timestamp(Some(&instant)).expect("some");
        assert_eq!(
            decode_optional_timestamp(Some(encoded.as_str()), "tasks.deadline").unwrap(),
            Some(instant)
        );
        assert!(matches!(
            decode_optional_timestamp(Some("nope"), "tasks.deadline"),
            Err(StorageError::Malformed { .. })
        ));
    }

    #[test]
    fn enum_text_values_match_the_serde_contract_and_fail_fast() {
        for transport in [
            TransportType::Acp,
            TransportType::Matrix,
            TransportType::Http,
        ] {
            let text = encode_transport(transport);
            assert_eq!(
                serde_json::to_value(transport).unwrap().as_str().unwrap(),
                text,
                "codec must not drift from the model's serde contract"
            );
            assert_eq!(
                decode_transport(text, "agents.transport").unwrap(),
                transport
            );
        }

        for status in [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed,
            TaskStatus::Failed,
            TaskStatus::TimedOut,
            TaskStatus::Cancelled,
        ] {
            let text = encode_status(status);
            assert_eq!(
                serde_json::to_value(status).unwrap().as_str().unwrap(),
                text,
                "codec must not drift from the model's serde contract"
            );
            assert_eq!(decode_status(text, "task_events.status").unwrap(), status);
        }

        // Fail-fast, never a default.
        assert!(matches!(
            decode_transport("grpc", "agents.transport"),
            Err(StorageError::Malformed { .. })
        ));
        assert!(matches!(
            decode_status("paused", "task_events.status"),
            Err(StorageError::Malformed { .. })
        ));
    }

    #[test]
    fn json_round_trips_every_payload_variant_deterministically() {
        let delivery_id = DeliveryId::generate();
        let started_at: DateTime<Utc> = "2026-09-16T10:00:00Z".parse().unwrap();
        let deadline: DateTime<Utc> = "2026-09-16T11:00:00Z".parse().unwrap();
        let payloads = vec![
            TaskEventPayload::Queued,
            TaskEventPayload::Dispatched {
                delivery_id,
                attempt: 2,
            },
            TaskEventPayload::Running { started_at },
            TaskEventPayload::Completed {
                output: "done".into(),
            },
            TaskEventPayload::Failed {
                error: "boom".into(),
            },
            TaskEventPayload::TimedOut { deadline },
            TaskEventPayload::Cancelled {
                reason: "user".into(),
            },
        ];
        assert_eq!(payloads.len(), 7, "all payload variants are covered");

        for payload in &payloads {
            let text = encode_json(payload, "task_events.payload").expect("encode");
            assert_eq!(
                encode_json(payload, "task_events.payload").expect("encode"),
                text,
                "encoding is deterministic"
            );
            let back: TaskEventPayload = decode_json(&text, "task_events.payload").expect("decode");
            assert_eq!(&back, payload);
        }

        // metadata: open key set, deterministic ordering via BTreeMap.
        let mut metadata = BTreeMap::new();
        metadata.insert("source".to_owned(), "matrix".to_owned());
        metadata.insert("thread".to_owned(), "$root:matrix.org".to_owned());
        let text = encode_json(&metadata, "messages.metadata_json").expect("encode");
        let back: BTreeMap<String, String> =
            decode_json(&text, "messages.metadata_json").expect("decode");
        assert_eq!(back, metadata);
        assert_eq!(
            encode_json(&BTreeMap::<String, String>::new(), "messages.metadata_json").unwrap(),
            "{}"
        );

        // addresses: three shapes, one JSON column.
        for address in [
            EndpointAddress::Acp {
                command: "codex-acp".into(),
                args: vec!["--stdio".into()],
            },
            EndpointAddress::Matrix {
                user_id: "@agent:matrix.org".into(),
            },
            EndpointAddress::Http {
                url: "https://example.test/hook".into(),
            },
        ] {
            let text = encode_json(&address, "agents.address_json").expect("encode");
            let back: EndpointAddress = decode_json(&text, "agents.address_json").expect("decode");
            assert_eq!(back, address);
        }

        assert!(matches!(
            decode_json::<TaskEventPayload>("not json", "task_events.payload"),
            Err(StorageError::Malformed { .. })
        ));
    }

    #[test]
    fn decoding_errors_do_not_echo_the_stored_value() {
        const PLANTED: &str = "SECRET-BODY-DO-NOT-LEAK";
        let error = decode_json::<TaskEventPayload>(
            &format!("{{\"completed\":{{\"output\":\"{PLANTED}"),
            "task_events.payload",
        )
        .expect_err("truncated JSON must fail");
        let rendered = error.to_string();
        assert!(!rendered.contains(PLANTED), "{rendered}");
    }

    #[test]
    fn integers_are_range_checked_without_truncation() {
        assert_eq!(encode_u64(0, "task_events.seq").unwrap(), 0);
        assert_eq!(
            encode_u64(i64::MAX as u64, "task_events.seq").unwrap(),
            i64::MAX
        );
        assert!(matches!(
            encode_u64(i64::MAX as u64 + 1, "task_events.seq"),
            Err(StorageError::OutOfRange { .. })
        ));
        assert!(matches!(
            decode_u64(-1, "task_events.seq"),
            Err(StorageError::OutOfRange { .. })
        ));
        assert_eq!(decode_u64(7, "task_events.seq").unwrap(), 7);

        assert_eq!(encode_u32(u32::MAX), i64::from(u32::MAX));
        assert_eq!(
            decode_u32(i64::from(u32::MAX), "deliveries.attempt").unwrap(),
            u32::MAX
        );
        assert!(matches!(
            decode_u32(-1, "deliveries.attempt"),
            Err(StorageError::OutOfRange { .. })
        ));
        assert!(matches!(
            decode_u32(i64::from(u32::MAX) + 1, "deliveries.attempt"),
            Err(StorageError::OutOfRange { .. })
        ));

        assert_eq!(encode_bool(true), 1);
        assert_eq!(encode_bool(false), 0);
        assert!(decode_bool(1, "agents.enabled").unwrap());
        assert!(!decode_bool(0, "agents.enabled").unwrap());
        assert!(matches!(
            decode_bool(2, "agents.enabled"),
            Err(StorageError::Malformed { .. })
        ));

        let priority = Priority::new(10).unwrap();
        let encoded = encode_priority(priority);
        assert_eq!(encoded, 10);
        assert_eq!(
            decode_priority(encoded, "tasks.priority").unwrap(),
            priority
        );
        assert!(matches!(
            decode_priority(11, "tasks.priority"),
            Err(StorageError::Malformed { .. })
        ));
        assert!(matches!(
            decode_priority(-1, "tasks.priority"),
            Err(StorageError::Malformed { .. })
        ));
    }

    // ── model <-> row round trip against a real database ──────────────────────

    struct Fixture {
        agent_id: &'static str,
        agent: AgentEndpoint,
        second_agent_id: &'static str,
        second_agent: AgentEndpoint,
        conversation: Conversation,
        plain_conversation: Conversation,
        first_message: Message,
        reply_message: Message,
        root_task: AgentTask,
        child_task: AgentTask,
        events: Vec<TaskEvent>,
        deliveries: Vec<Delivery>,
    }

    fn fixture() -> Fixture {
        let t0: DateTime<Utc> = "2026-09-16T10:00:00Z".parse().unwrap();

        let agent_id = "alpha";
        let agent = AgentEndpoint {
            id: derive_endpoint_id(agent_id),
            transport: TransportType::Acp,
            address: EndpointAddress::Acp {
                command: "codex-acp".into(),
                args: vec!["--stdio".into(), "--profile=review".into()],
            },
            enabled: true,
            capabilities: vec![Capability::new("code"), Capability::new("review")],
        };
        let second_agent_id = "worker";
        let second_agent = AgentEndpoint {
            id: derive_endpoint_id(second_agent_id),
            transport: TransportType::Matrix,
            address: EndpointAddress::Matrix {
                user_id: "@worker:matrix.org".into(),
            },
            enabled: false,
            capabilities: Vec::new(),
        };

        let conversation = Conversation {
            id: ConversationId::generate(),
            participants: vec![derive_endpoint_id("alpha"), derive_endpoint_id("worker")],
            external_ref: Some(ExternalRef {
                transport: TransportType::Matrix,
                external_id: "!room:matrix.org".into(),
                thread_ref: Some("$root:matrix.org".into()),
            }),
        };
        let plain_conversation = Conversation {
            id: ConversationId::generate(),
            participants: Vec::new(),
            external_ref: None,
        };

        let first_message = Message {
            id: MessageId::generate(),
            conversation: conversation.id,
            sender: derive_endpoint_id("alpha"),
            recipient: derive_endpoint_id("worker"),
            body: "please review".into(),
            reply_to: None,
            metadata: BTreeMap::new(),
        };
        let reply_message = Message {
            id: MessageId::generate(),
            conversation: conversation.id,
            sender: derive_endpoint_id("worker"),
            recipient: derive_endpoint_id("alpha"),
            body: "on it".into(),
            reply_to: Some(first_message.id),
            metadata: BTreeMap::from([
                ("source".to_owned(), "matrix".to_owned()),
                ("thread".to_owned(), "$root:matrix.org".to_owned()),
            ]),
        };

        let delivery_id = DeliveryId::generate();
        let second_delivery_id = DeliveryId::generate();
        // A root task is its own root: this exercises the self-referencing FK.
        let root_task_id = TaskId::generate();
        let root_task = AgentTask {
            task_id: root_task_id,
            root_task_id,
            parent_task_id: None,
            from_agent: derive_endpoint_id("alpha"),
            to_agent: derive_endpoint_id("worker"),
            conversation_id: conversation.id,
            reply_to: Some(first_message.id),
            text: "review the change".into(),
            priority: Priority::new(7).unwrap(),
            depth: 0,
            hops: 0,
            deadline: Some(t0 + Duration::hours(1)),
            version: 42,
        };
        let child_task = AgentTask {
            task_id: TaskId::generate(),
            root_task_id: root_task.task_id,
            parent_task_id: Some(root_task.task_id),
            from_agent: derive_endpoint_id("worker"),
            to_agent: derive_endpoint_id("alpha"),
            conversation_id: plain_conversation.id,
            reply_to: None,
            text: "follow-up".into(),
            priority: Priority::new(0).unwrap(),
            depth: 1,
            hops: 1,
            deadline: None,
            version: 0,
        };

        let statuses = [
            (TaskStatus::Queued, TaskEventPayload::Queued),
            (
                TaskStatus::Dispatched,
                TaskEventPayload::Dispatched {
                    delivery_id,
                    attempt: 1,
                },
            ),
            (
                TaskStatus::Running,
                TaskEventPayload::Running {
                    started_at: t0 + Duration::seconds(1),
                },
            ),
            (
                TaskStatus::Completed,
                TaskEventPayload::Completed {
                    output: "looks good".into(),
                },
            ),
            (
                TaskStatus::Failed,
                TaskEventPayload::Failed {
                    error: "tests failed".into(),
                },
            ),
            (
                TaskStatus::TimedOut,
                TaskEventPayload::TimedOut {
                    deadline: t0 + Duration::hours(1),
                },
            ),
            (
                TaskStatus::Cancelled,
                TaskEventPayload::Cancelled {
                    reason: "superseded".into(),
                },
            ),
        ];
        let events = statuses
            .into_iter()
            .enumerate()
            .map(|(index, (status, payload))| TaskEvent {
                id: EventId::generate(),
                task_id: root_task.task_id,
                seq: index as u64 + 1,
                status,
                timestamp: t0 + Duration::seconds(index as i64),
                payload,
            })
            .collect();

        let deliveries = vec![
            Delivery::new(
                delivery_id,
                root_task.task_id,
                1,
                derive_endpoint_id("worker"),
                t0,
            ),
            Delivery::new(
                second_delivery_id,
                root_task.task_id,
                2,
                derive_endpoint_id("worker"),
                t0 + Duration::seconds(2),
            )
            .with_acknowledged_at(Some(t0 + Duration::seconds(3))),
        ];

        Fixture {
            agent_id,
            agent,
            second_agent_id,
            second_agent,
            conversation,
            plain_conversation,
            first_message,
            reply_message,
            root_task,
            child_task,
            events,
            deliveries,
        }
    }

    async fn insert_agent(pool: &SqlitePool, agent_id: &str, agent: &AgentEndpoint) {
        sqlx::query(
            "INSERT INTO agents (endpoint_id, agent_id, transport, enabled, address_json, \
             capabilities_json) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(encode_id(agent.id))
        .bind(agent_id)
        .bind(encode_transport(agent.transport))
        .bind(encode_bool(agent.enabled))
        .bind(encode_json(&agent.address, "agents.address_json").expect("encode address"))
        .bind(encode_json(&agent.capabilities, "agents.capabilities_json").expect("encode caps"))
        .execute(pool)
        .await
        .expect("insert agent");
    }

    async fn select_agent(pool: &SqlitePool, agent_id: &str) -> AgentEndpoint {
        let row = sqlx::query(
            "SELECT endpoint_id, transport, enabled, address_json, capabilities_json \
             FROM agents WHERE agent_id = ?",
        )
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .expect("select agent");

        let endpoint_id: String = row.try_get("endpoint_id").expect("endpoint_id");
        let transport: String = row.try_get("transport").expect("transport");
        let enabled: i64 = row.try_get("enabled").expect("enabled");
        let address_json: Option<String> = row.try_get("address_json").expect("address_json");
        let capabilities_json: String =
            row.try_get("capabilities_json").expect("capabilities_json");

        AgentEndpoint {
            id: decode_id(&endpoint_id, "agents.endpoint_id").expect("decode endpoint id"),
            transport: decode_transport(&transport, "agents.transport").expect("decode transport"),
            address: decode_json(
                address_json.as_deref().expect("address is present"),
                "agents.address_json",
            )
            .expect("decode address"),
            enabled: decode_bool(enabled, "agents.enabled").expect("decode enabled"),
            capabilities: decode_json(&capabilities_json, "agents.capabilities_json")
                .expect("decode capabilities"),
        }
    }

    async fn insert_conversation(pool: &SqlitePool, conversation: &Conversation) {
        let (transport, external_id, thread_ref) = match &conversation.external_ref {
            Some(reference) => (
                Some(encode_transport(reference.transport)),
                Some(reference.external_id.clone()),
                reference.thread_ref.clone(),
            ),
            None => (None, None, None),
        };
        sqlx::query(
            "INSERT INTO conversations (conversation_id, transport, external_id, thread_ref, \
             participants_json) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(encode_id(conversation.id))
        .bind(transport)
        .bind(external_id)
        .bind(thread_ref)
        .bind(
            encode_json(
                &conversation.participants,
                "conversations.participants_json",
            )
            .unwrap(),
        )
        .execute(pool)
        .await
        .expect("insert conversation");
    }

    async fn select_conversation(pool: &SqlitePool, id: ConversationId) -> Conversation {
        let row = sqlx::query(
            "SELECT conversation_id, transport, external_id, thread_ref, participants_json \
             FROM conversations WHERE conversation_id = ?",
        )
        .bind(encode_id(id))
        .fetch_one(pool)
        .await
        .expect("select conversation");

        let transport: Option<String> = row.try_get("transport").expect("transport");
        let external_id: Option<String> = row.try_get("external_id").expect("external_id");
        let thread_ref: Option<String> = row.try_get("thread_ref").expect("thread_ref");
        let participants_json: String =
            row.try_get("participants_json").expect("participants_json");

        let external_ref = transport
            .map(|transport| {
                Ok::<_, StorageError>(ExternalRef {
                    transport: decode_transport(&transport, "conversations.transport")?,
                    external_id: external_id.expect("external id accompanies the transport"),
                    thread_ref,
                })
            })
            .transpose()
            .expect("decode external ref");

        Conversation {
            id: decode_id(
                &row.try_get::<String, _>("conversation_id").expect("id"),
                "conversations.conversation_id",
            )
            .expect("decode conversation id"),
            participants: decode_json(&participants_json, "conversations.participants_json")
                .expect("decode participants"),
            external_ref,
        }
    }

    async fn insert_message(pool: &SqlitePool, message: &Message) {
        sqlx::query(
            "INSERT INTO messages (message_id, conversation_id, sender, recipient, body, \
             reply_to, metadata_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(encode_id(message.id))
        .bind(encode_id(message.conversation))
        .bind(encode_id(message.sender))
        .bind(encode_id(message.recipient))
        .bind(&message.body)
        .bind(message.reply_to.map(encode_id))
        .bind(encode_json(&message.metadata, "messages.metadata_json").unwrap())
        .execute(pool)
        .await
        .expect("insert message");
    }

    async fn select_message(pool: &SqlitePool, id: MessageId) -> Message {
        let row = sqlx::query(
            "SELECT message_id, conversation_id, sender, recipient, body, reply_to, \
             metadata_json FROM messages WHERE message_id = ?",
        )
        .bind(encode_id(id))
        .fetch_one(pool)
        .await
        .expect("select message");

        let reply_to: Option<String> = row.try_get("reply_to").expect("reply_to");
        Message {
            id: decode_id(
                &row.try_get::<String, _>("message_id").expect("id"),
                "messages.message_id",
            )
            .expect("decode message id"),
            conversation: decode_id(
                &row.try_get::<String, _>("conversation_id")
                    .expect("conversation_id"),
                "messages.conversation_id",
            )
            .expect("decode conversation id"),
            sender: decode_id(
                &row.try_get::<String, _>("sender").expect("sender"),
                "messages.sender",
            )
            .expect("decode sender"),
            recipient: decode_id(
                &row.try_get::<String, _>("recipient").expect("recipient"),
                "messages.recipient",
            )
            .expect("decode recipient"),
            body: row.try_get("body").expect("body"),
            reply_to: reply_to
                .map(|text| decode_id(&text, "messages.reply_to"))
                .transpose()
                .expect("decode reply_to"),
            metadata: decode_json(
                &row.try_get::<String, _>("metadata_json")
                    .expect("metadata_json"),
                "messages.metadata_json",
            )
            .expect("decode metadata"),
        }
    }

    async fn insert_task(pool: &SqlitePool, task: &AgentTask) {
        sqlx::query(
            "INSERT INTO tasks (task_id, root_task_id, parent_task_id, from_agent, to_agent, \
             conversation_id, reply_to, text, priority, depth, hops, deadline, version) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(encode_id(task.task_id))
        .bind(encode_id(task.root_task_id))
        .bind(task.parent_task_id.map(encode_id))
        .bind(encode_id(task.from_agent))
        .bind(encode_id(task.to_agent))
        .bind(encode_id(task.conversation_id))
        .bind(task.reply_to.map(encode_id))
        .bind(&task.text)
        .bind(encode_priority(task.priority))
        .bind(i64::from(task.depth))
        .bind(i64::from(task.hops))
        .bind(encode_optional_timestamp(task.deadline.as_ref()))
        .bind(encode_u64(task.version, "tasks.version").expect("encode version"))
        .execute(pool)
        .await
        .expect("insert task");
    }

    async fn select_task(pool: &SqlitePool, id: TaskId) -> AgentTask {
        let row = sqlx::query(
            "SELECT task_id, root_task_id, parent_task_id, from_agent, to_agent, conversation_id, \
             reply_to, text, priority, depth, hops, deadline, version FROM tasks WHERE task_id = ?",
        )
        .bind(encode_id(id))
        .fetch_one(pool)
        .await
        .expect("select task");

        let parent: Option<String> = row.try_get("parent_task_id").expect("parent_task_id");
        let reply_to: Option<String> = row.try_get("reply_to").expect("reply_to");
        let deadline: Option<String> = row.try_get("deadline").expect("deadline");

        AgentTask {
            task_id: decode_id(
                &row.try_get::<String, _>("task_id").expect("task_id"),
                "tasks.task_id",
            )
            .expect("decode task id"),
            root_task_id: decode_id(
                &row.try_get::<String, _>("root_task_id")
                    .expect("root_task_id"),
                "tasks.root_task_id",
            )
            .expect("decode root task id"),
            parent_task_id: parent
                .map(|text| decode_id(&text, "tasks.parent_task_id"))
                .transpose()
                .expect("decode parent"),
            from_agent: decode_id(
                &row.try_get::<String, _>("from_agent").expect("from_agent"),
                "tasks.from_agent",
            )
            .expect("decode from_agent"),
            to_agent: decode_id(
                &row.try_get::<String, _>("to_agent").expect("to_agent"),
                "tasks.to_agent",
            )
            .expect("decode to_agent"),
            conversation_id: decode_id(
                &row.try_get::<String, _>("conversation_id")
                    .expect("conversation_id"),
                "tasks.conversation_id",
            )
            .expect("decode conversation id"),
            reply_to: reply_to
                .map(|text| decode_id(&text, "tasks.reply_to"))
                .transpose()
                .expect("decode reply_to"),
            text: row.try_get("text").expect("text"),
            priority: decode_priority(row.try_get("priority").expect("priority"), "tasks.priority")
                .expect("decode priority"),
            depth: decode_u32(row.try_get("depth").expect("depth"), "tasks.depth")
                .expect("decode depth"),
            hops: decode_u32(row.try_get("hops").expect("hops"), "tasks.hops")
                .expect("decode hops"),
            deadline: decode_optional_timestamp(deadline.as_deref(), "tasks.deadline")
                .expect("decode deadline"),
            version: decode_u64(row.try_get("version").expect("version"), "tasks.version")
                .expect("decode version"),
        }
    }

    async fn insert_event(pool: &SqlitePool, event: &TaskEvent) {
        sqlx::query(
            "INSERT INTO task_events (event_id, task_id, seq, status, timestamp, payload) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(encode_id(event.id))
        .bind(encode_id(event.task_id))
        .bind(encode_u64(event.seq, "task_events.seq").expect("encode seq"))
        .bind(encode_status(event.status))
        .bind(encode_timestamp(&event.timestamp))
        .bind(encode_json(&event.payload, "task_events.payload").expect("encode payload"))
        .execute(pool)
        .await
        .expect("insert event");
    }

    async fn select_event(pool: &SqlitePool, id: EventId) -> TaskEvent {
        let row = sqlx::query(
            "SELECT event_id, task_id, seq, status, timestamp, payload FROM task_events \
             WHERE event_id = ?",
        )
        .bind(encode_id(id))
        .fetch_one(pool)
        .await
        .expect("select event");

        TaskEvent {
            id: decode_id(
                &row.try_get::<String, _>("event_id").expect("event_id"),
                "task_events.event_id",
            )
            .expect("decode event id"),
            task_id: decode_id(
                &row.try_get::<String, _>("task_id").expect("task_id"),
                "task_events.task_id",
            )
            .expect("decode task id"),
            seq: decode_u64(row.try_get("seq").expect("seq"), "task_events.seq")
                .expect("decode seq"),
            status: decode_status(
                &row.try_get::<String, _>("status").expect("status"),
                "task_events.status",
            )
            .expect("decode status"),
            timestamp: decode_timestamp(
                &row.try_get::<String, _>("timestamp").expect("timestamp"),
                "task_events.timestamp",
            )
            .expect("decode timestamp"),
            payload: decode_json(
                &row.try_get::<String, _>("payload").expect("payload"),
                "task_events.payload",
            )
            .expect("decode payload"),
        }
    }

    async fn insert_delivery(pool: &SqlitePool, delivery: &Delivery) {
        sqlx::query(
            "INSERT INTO deliveries (delivery_id, task_id, attempt, target_endpoint_id, \
             dispatched_at, acknowledged_at) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(encode_id(delivery.delivery_id()))
        .bind(encode_id(delivery.task_id()))
        .bind(encode_u32(delivery.attempt()))
        .bind(encode_id(delivery.target()))
        .bind(encode_timestamp(&delivery.dispatched_at()))
        .bind(encode_optional_timestamp(
            delivery.acknowledged_at().as_ref(),
        ))
        .execute(pool)
        .await
        .expect("insert delivery");
    }

    async fn select_delivery(pool: &SqlitePool, id: DeliveryId) -> Delivery {
        let row = sqlx::query(
            "SELECT delivery_id, task_id, attempt, target_endpoint_id, dispatched_at, \
             acknowledged_at FROM deliveries WHERE delivery_id = ?",
        )
        .bind(encode_id(id))
        .fetch_one(pool)
        .await
        .expect("select delivery");

        let acknowledged_at: Option<String> =
            row.try_get("acknowledged_at").expect("acknowledged_at");

        Delivery::new(
            decode_id(
                &row.try_get::<String, _>("delivery_id")
                    .expect("delivery_id"),
                "deliveries.delivery_id",
            )
            .expect("decode delivery id"),
            decode_id(
                &row.try_get::<String, _>("task_id").expect("task_id"),
                "deliveries.task_id",
            )
            .expect("decode task id"),
            decode_u32(
                row.try_get("attempt").expect("attempt"),
                "deliveries.attempt",
            )
            .expect("decode attempt"),
            decode_id(
                &row.try_get::<String, _>("target_endpoint_id")
                    .expect("target_endpoint_id"),
                "deliveries.target_endpoint_id",
            )
            .expect("decode target"),
            decode_timestamp(
                &row.try_get::<String, _>("dispatched_at")
                    .expect("dispatched_at"),
                "deliveries.dispatched_at",
            )
            .expect("decode dispatched_at"),
        )
        .with_acknowledged_at(
            decode_optional_timestamp(acknowledged_at.as_deref(), "deliveries.acknowledged_at")
                .expect("decode acknowledged_at"),
        )
    }

    #[tokio::test]
    async fn every_model_round_trips_through_the_real_schema() {
        let (pool, path) = migrated_pool("roundtrip").await;
        let fixture = fixture();

        // ── agents (including the empty-capability and Matrix-address shapes)
        insert_agent(&pool, fixture.agent_id, &fixture.agent).await;
        insert_agent(&pool, fixture.second_agent_id, &fixture.second_agent).await;
        assert_eq!(select_agent(&pool, fixture.agent_id).await, fixture.agent);
        assert_eq!(
            select_agent(&pool, fixture.second_agent_id).await,
            fixture.second_agent
        );

        // ── conversations (with and without an external reference)
        insert_conversation(&pool, &fixture.conversation).await;
        insert_conversation(&pool, &fixture.plain_conversation).await;
        assert_eq!(
            select_conversation(&pool, fixture.conversation.id).await,
            fixture.conversation
        );
        assert_eq!(
            select_conversation(&pool, fixture.plain_conversation.id).await,
            fixture.plain_conversation
        );

        // ── messages (reply chain + metadata)
        insert_message(&pool, &fixture.first_message).await;
        insert_message(&pool, &fixture.reply_message).await;
        assert_eq!(
            select_message(&pool, fixture.first_message.id).await,
            fixture.first_message
        );
        assert_eq!(
            select_message(&pool, fixture.reply_message.id).await,
            fixture.reply_message
        );

        // ── tasks (self-referencing root, then a child)
        insert_task(&pool, &fixture.root_task).await;
        insert_task(&pool, &fixture.child_task).await;
        assert_eq!(
            select_task(&pool, fixture.root_task.task_id).await,
            fixture.root_task
        );
        assert_eq!(
            select_task(&pool, fixture.child_task.task_id).await,
            fixture.child_task
        );

        // ── task_events (every payload variant)
        for event in &fixture.events {
            insert_event(&pool, event).await;
        }
        for event in &fixture.events {
            assert_eq!(select_event(&pool, event.id).await, event.clone());
        }

        // ── deliveries (unacknowledged + acknowledged)
        for delivery in &fixture.deliveries {
            insert_delivery(&pool, delivery).await;
        }
        for delivery in &fixture.deliveries {
            assert_eq!(
                select_delivery(&pool, delivery.delivery_id()).await,
                delivery.clone()
            );
        }

        pool.close().await;
        remove_db_files(&path);
    }

    #[tokio::test]
    async fn a_declared_but_unaddressable_agent_row_is_representable() {
        // `address_json IS NULL` is legal (matrix/http are declared but not
        // addressable in v1). The model has no way to express such a row, so
        // decoding it into `AgentEndpoint` is deliberately left to T009; this
        // test pins only that the schema accepts the row.
        let (pool, path) = migrated_pool("null-address").await;
        let endpoint_id = derive_endpoint_id("matrix-bot");
        sqlx::query(
            "INSERT INTO agents (endpoint_id, agent_id, transport, enabled, address_json, \
             capabilities_json) VALUES (?, 'matrix-bot', 'matrix', 1, NULL, '[]')",
        )
        .bind(encode_id(endpoint_id))
        .execute(&pool)
        .await
        .expect("NULL address rows are allowed");

        let address_json: Option<String> =
            sqlx::query_scalar("SELECT address_json FROM agents WHERE agent_id = 'matrix-bot'")
                .fetch_one(&pool)
                .await
                .expect("select");
        assert!(address_json.is_none());

        pool.close().await;
        remove_db_files(&path);
    }
}
