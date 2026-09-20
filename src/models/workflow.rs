use serde::de::{DeserializeSeed, Deserializer as _, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::ids::{EndpointId, TaskId};

pub const WORKFLOW_SCHEMA: &str = "workflow.v1";
pub const MAX_WORKFLOW_BYTES: usize = 64 * 1024;
pub const MAX_METADATA_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowKind {
    Dispatch,
    Ack,
    Handoff,
    Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Reserved,
    Dispatched,
    Acknowledged,
    HandoffPending,
    HandedOff,
    Busy,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowRole {
    Coordinator,
    Developer,
    Reviewer,
    Observer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticatedWorkflowIngress {
    pub endpoint: EndpointId,
    pub role: WorkflowRole,
}

pub fn authorize_workflow(role: WorkflowRole, kind: WorkflowKind) -> bool {
    match role {
        WorkflowRole::Coordinator => matches!(
            kind,
            WorkflowKind::Dispatch | WorkflowKind::Handoff | WorkflowKind::Ack
        ),
        WorkflowRole::Developer | WorkflowRole::Reviewer => {
            matches!(kind, WorkflowKind::Handoff | WorkflowKind::Ack)
        }
        WorkflowRole::Observer => false,
    }
}

pub const fn kind_name(kind: WorkflowKind) -> &'static str {
    match kind {
        WorkflowKind::Dispatch => "dispatch",
        WorkflowKind::Ack => "ack",
        WorkflowKind::Handoff => "handoff",
        WorkflowKind::Command => "command",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowEnvelope {
    pub schema: String,
    pub kind: WorkflowKind,
    pub message_id: String,
    pub from: EndpointId,
    pub to: EndpointId,
    pub task_id: TaskId,
    pub correlation_id: String,
    pub idempotency_key: String,
    pub state: Option<WorkflowState>,
    pub body: String,
    pub mention: Option<String>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WorkflowError {
    #[error("workflow payload too large")]
    TooLarge,
    #[error("invalid workflow JSON")]
    InvalidJson,
    #[error("unknown workflow field")]
    UnknownField,
    #[error("duplicate workflow field")]
    DuplicateField,
    #[error("invalid workflow envelope")]
    InvalidEnvelope,
}

pub fn parse_workflow(raw: &str) -> Result<WorkflowEnvelope, WorkflowError> {
    if raw.len() > MAX_WORKFLOW_BYTES {
        return Err(WorkflowError::TooLarge);
    }
    reject_duplicate_keys(raw)?;
    let value: Value = serde_json::from_str(raw).map_err(|_| WorkflowError::InvalidJson)?;
    let object = value.as_object().ok_or(WorkflowError::InvalidEnvelope)?;
    let allowed = [
        "schema",
        "kind",
        "message_id",
        "from",
        "to",
        "task_id",
        "correlation_id",
        "idempotency_key",
        "state",
        "body",
        "mention",
    ];
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(WorkflowError::UnknownField);
    }
    let envelope: WorkflowEnvelope =
        serde_json::from_value(value).map_err(|_| WorkflowError::InvalidEnvelope)?;
    if envelope.schema != WORKFLOW_SCHEMA
        || envelope.message_id.is_empty()
        || envelope.correlation_id.is_empty()
        || envelope.idempotency_key.is_empty()
        || envelope.body.len() > MAX_WORKFLOW_BYTES
    {
        return Err(WorkflowError::InvalidEnvelope);
    }
    Ok(envelope)
}

fn reject_duplicate_keys(raw: &str) -> Result<(), WorkflowError> {
    struct Root;
    impl<'de> Visitor<'de> for Root {
        type Value = ();
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a JSON object")
        }
        fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut keys = std::collections::BTreeSet::new();
            while let Some(key) = map.next_key::<String>()? {
                if !keys.insert(key) {
                    return Err(serde::de::Error::custom("duplicate key"));
                }
                map.next_value_seed(RecursiveValue)?;
            }
            Ok(())
        }
    }
    let mut deserializer = serde_json::Deserializer::from_str(raw);
    deserializer.deserialize_any(Root).map_err(|error| {
        if error.to_string().contains("duplicate key") {
            WorkflowError::DuplicateField
        } else {
            WorkflowError::InvalidJson
        }
    })
}

struct RecursiveValue;
impl<'de> DeserializeSeed<'de> for RecursiveValue {
    type Value = Value;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        struct Nested;
        impl<'de> Visitor<'de> for Nested {
            type Value = Value;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("JSON value")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
                let mut out = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if out.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate key"));
                    }
                    let value = map.next_value_seed(RecursiveValue)?;
                    out.insert(key, value);
                }
                Ok(Value::Object(out))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Value, A::Error> {
                let mut out = Vec::new();
                while let Some(value) = seq.next_element_seed(RecursiveValue)? {
                    out.push(value);
                }
                Ok(Value::Array(out))
            }
            fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Value, E> {
                Ok(Value::Bool(value))
            }
            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Value, E> {
                Ok(Value::Number(value.into()))
            }
            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Value, E> {
                Ok(Value::Number(value.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Value, E> {
                serde_json::Number::from_f64(value)
                    .map(Value::Number)
                    .ok_or_else(|| serde::de::Error::custom("invalid number"))
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Value, E> {
                Ok(Value::String(value.into()))
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }
        }
        deserializer.deserialize_any(Nested)
    }
}

pub fn canonical_metadata(envelope: &WorkflowEnvelope) -> Result<Vec<u8>, WorkflowError> {
    let value = serde_json::json!({
        "schema": WORKFLOW_SCHEMA,
        "task_id": envelope.task_id,
        "correlation_id": envelope.correlation_id,
        "idempotency_key": envelope.idempotency_key,
        "to": envelope.to,
    });
    let bytes = serde_json::to_vec(&value).map_err(|_| WorkflowError::InvalidEnvelope)?;
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(WorkflowError::TooLarge);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ids::{EndpointId, TaskId};

    fn sample() -> WorkflowEnvelope {
        WorkflowEnvelope {
            schema: WORKFLOW_SCHEMA.into(),
            kind: WorkflowKind::Dispatch,
            message_id: "m1".into(),
            from: EndpointId::generate(),
            to: EndpointId::generate(),
            task_id: TaskId::generate(),
            correlation_id: "c1".into(),
            idempotency_key: "i1".into(),
            state: None,
            body: "do".into(),
            mention: None,
        }
    }

    #[test]
    fn parses_and_rejects_unknown_fields() {
        let e = sample();
        let raw = serde_json::to_string(&e).unwrap();
        assert_eq!(parse_workflow(&raw).unwrap(), e);
        assert_eq!(
            parse_workflow(&raw.replace("{", "{\"extra\":1,")),
            Err(WorkflowError::UnknownField)
        );
    }
    #[test]
    fn metadata_is_bounded_and_canonical() {
        let bytes = canonical_metadata(&sample()).unwrap();
        assert!(bytes.starts_with(b"{"));
        assert!(bytes.len() <= MAX_METADATA_BYTES);
    }

    #[test]
    fn rejects_duplicate_top_level_keys() {
        let e = sample();
        let raw = serde_json::to_string(&e).unwrap();
        let duplicate = raw.replacen("{", "{\"kind\":\"dispatch\",", 1);
        assert_eq!(
            parse_workflow(&duplicate),
            Err(WorkflowError::DuplicateField)
        );
    }

    #[test]
    fn role_matrix_rejects_observer_and_allows_handoff_owners() {
        assert!(!authorize_workflow(
            WorkflowRole::Observer,
            WorkflowKind::Dispatch
        ));
        assert!(authorize_workflow(
            WorkflowRole::Coordinator,
            WorkflowKind::Dispatch
        ));
        assert!(authorize_workflow(
            WorkflowRole::Developer,
            WorkflowKind::Handoff
        ));
        assert!(authorize_workflow(
            WorkflowRole::Reviewer,
            WorkflowKind::Ack
        ));
    }
}
