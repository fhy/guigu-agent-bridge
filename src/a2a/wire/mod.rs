//! Owned, fixture-locked subset of the A2A v0.3 wire protocol.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: &str = "0.3.0";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    pub url: String,
    pub protocol_version: String,
    pub capabilities: Capabilities,
    pub skills: Vec<Skill>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    pub streaming: bool,
    pub push_notifications: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Skill {
    pub id: String,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SendParams {
    pub protocol_version: String,
    pub request_id: String,
    pub context_id: String,
    pub message: Message,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub message_id: String,
    pub role: Role,
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Agent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Part {
    Text { text: String },
    Data { data: Value },
    FileInline { mime_type: String, data: String },
    FileUri { uri: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskSnapshot {
    pub id: String,
    pub context_id: String,
    pub status: TaskState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TaskState {
    Submitted,
    Working,
    Completed,
    Failed,
    Canceled,
    Rejected,
    InputRequired,
    AuthRequired,
}

impl TaskState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Canceled | Self::Rejected
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub artifact_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskQuery {
    pub protocol_version: String,
    pub task_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireProfile {
    LegacyOwnedV030,
    A2aV030,
}

#[derive(Debug, Clone)]
pub struct NormalizedSend {
    pub profile: WireProfile,
    pub request_id: String,
    pub context_id: String,
    pub message: Message,
    pub canonical: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct NormalizedTaskQuery {
    pub profile: WireProfile,
    pub task_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalizeError;

pub fn normalize_send(params: Value, rpc_id: &Value) -> Result<NormalizedSend, NormalizeError> {
    let object = params.as_object().ok_or(NormalizeError)?;
    let legacy = ["protocolVersion", "requestId", "contextId"]
        .iter()
        .any(|key| object.contains_key(*key));
    let (profile, request_id, context_id, message) = if legacy {
        if object.contains_key("configuration") || object.contains_key("metadata") {
            return Err(NormalizeError);
        }
        let parsed: SendParams = serde_json::from_value(params).map_err(|_| NormalizeError)?;
        if parsed.protocol_version != PROTOCOL_VERSION {
            return Err(NormalizeError);
        }
        (
            WireProfile::LegacyOwnedV030,
            bounded(parsed.request_id)?,
            bounded(parsed.context_id)?,
            parsed.message,
        )
    } else {
        if object
            .keys()
            .any(|key| !matches!(key.as_str(), "message" | "configuration" | "metadata"))
        {
            return Err(NormalizeError);
        }
        bounded_extension(object.get("configuration"))?;
        bounded_extension(object.get("metadata"))?;
        let message_value = object.get("message").cloned().ok_or(NormalizeError)?;
        let message_object = message_value.as_object().ok_or(NormalizeError)?;
        let context = message_object
            .get("contextId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let message: Message = serde_json::from_value(message_value).map_err(|_| NormalizeError)?;
        let request = rpc_id_key(rpc_id)?;
        let context = context.unwrap_or_else(|| {
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("guigu-agent-bridge:a2a:context:{request}").as_bytes(),
            )
            .to_string()
        });
        (WireProfile::A2aV030, request, bounded(context)?, message)
    };
    if message.parts.len() > 64 || message.message_id.is_empty() || message.message_id.len() > 256 {
        return Err(NormalizeError);
    }
    let canonical = serde_json::to_vec(&(profile_tag(profile), &request_id, &context_id, &message))
        .map_err(|_| NormalizeError)?;
    if canonical.len() > 1_048_576 {
        return Err(NormalizeError);
    }
    Ok(NormalizedSend {
        profile,
        request_id,
        context_id,
        message,
        canonical,
    })
}

pub fn normalize_task_query(params: Value) -> Result<NormalizedTaskQuery, NormalizeError> {
    let object = params.as_object().ok_or(NormalizeError)?;
    let legacy = object.contains_key("protocolVersion") || object.contains_key("taskId");
    if legacy {
        if object.contains_key("id") {
            return Err(NormalizeError);
        }
        let query: TaskQuery = serde_json::from_value(params).map_err(|_| NormalizeError)?;
        if query.protocol_version != PROTOCOL_VERSION {
            return Err(NormalizeError);
        }
        Ok(NormalizedTaskQuery {
            profile: WireProfile::LegacyOwnedV030,
            task_id: bounded(query.task_id)?,
        })
    } else {
        if object.keys().any(|key| key != "id") {
            return Err(NormalizeError);
        }
        let id = object
            .get("id")
            .and_then(Value::as_str)
            .ok_or(NormalizeError)?;
        Ok(NormalizedTaskQuery {
            profile: WireProfile::A2aV030,
            task_id: bounded(id.to_owned())?,
        })
    }
}

fn rpc_id_key(value: &Value) -> Result<String, NormalizeError> {
    match value {
        Value::String(value) => bounded(value.clone()),
        Value::Number(value) if value.as_u64().is_some() => Ok(value.to_string()),
        _ => Err(NormalizeError),
    }
}

fn bounded(value: String) -> Result<String, NormalizeError> {
    (!value.is_empty() && value.len() <= 256)
        .then_some(value)
        .ok_or(NormalizeError)
}

fn bounded_extension(value: Option<&Value>) -> Result<(), NormalizeError> {
    match value {
        None => Ok(()),
        Some(value) => (serde_json::to_vec(value).map_err(|_| NormalizeError)?.len() <= 65_536)
            .then_some(())
            .ok_or(NormalizeError),
    }
}

fn profile_tag(profile: WireProfile) -> &'static str {
    match profile {
        WireProfile::LegacyOwnedV030 => "legacy_owned_v030",
        WireProfile::A2aV030 => "a2a_v030",
    }
}
