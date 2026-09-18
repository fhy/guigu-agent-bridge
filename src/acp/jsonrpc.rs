//! JSON-RPC 2.0 messages: envelopes, id correlation and error objects.
//!
//! The second layer of the ACP adapter. It reads and writes the three JSON-RPC
//! shapes and knows nothing about ACP methods or about bytes — framing is below
//! it ([`crate::acp::framing`]) and the ACP schema is above it
//! ([`crate::acp::schema`]).
//!
//! | shape | how it is recognised |
//! |-------|----------------------|
//! | request | has `method` **and** `id` |
//! | notification | has `method`, no `id` |
//! | response | has `id`, no `method`, and exactly one of `result`/`error` |
//!
//! `id` is `int64` or `string` (JSON-RPC allows `null`, but a `null` response id
//! cannot be correlated, so it is rejected explicitly rather than guessed). The
//! bridge allocates its own ids monotonically and never reuses them, which is what
//! makes a late response to an abandoned request harmless: it simply matches no
//! pending entry.

use std::fmt;

use serde_json::{Value, json};
use thiserror::Error;

use crate::acp::bounded;

/// JSON-RPC `Parse error`.
pub const PARSE_ERROR: i64 = -32700;
/// JSON-RPC `Invalid Request`.
pub const INVALID_REQUEST: i64 = -32600;
/// JSON-RPC `Method not found`.
pub const METHOD_NOT_FOUND: i64 = -32601;
/// JSON-RPC `Invalid params`.
pub const INVALID_PARAMS: i64 = -32602;
/// JSON-RPC `Internal error`.
pub const INTERNAL_ERROR: i64 = -32603;

/// A JSON-RPC request identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestId {
    /// A numeric id (`int64`).
    Number(i64),
    /// A string id.
    String(String),
}

impl RequestId {
    /// The id as a number, when it is one.
    pub fn as_number(&self) -> Option<i64> {
        match self {
            Self::Number(value) => Some(*value),
            Self::String(_) => None,
        }
    }

    fn from_value(value: &Value) -> Result<Self, ProtocolError> {
        match value {
            Value::Number(number) => number
                .as_i64()
                .map(Self::Number)
                .ok_or(ProtocolError::UncorrelatableId),
            Value::String(text) => Ok(Self::String(text.clone())),
            // `null` is legal JSON-RPC but cannot identify a response.
            _ => Err(ProtocolError::UncorrelatableId),
        }
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(value) => write!(f, "{value}"),
            Self::String(text) => write!(f, "{text}"),
        }
    }
}

/// A JSON-RPC request (expects a response).
#[derive(Debug, Clone, PartialEq)]
pub struct JsonRpcRequest {
    /// Correlation id.
    pub id: RequestId,
    /// Method name.
    pub method: String,
    /// Method parameters (`null` when absent).
    pub params: Value,
}

/// A JSON-RPC notification (expects no response).
#[derive(Debug, Clone, PartialEq)]
pub struct JsonRpcNotification {
    /// Method name.
    pub method: String,
    /// Method parameters (`null` when absent).
    pub params: Value,
}

/// A JSON-RPC error object.
#[derive(Debug, Clone, PartialEq, Error)]
#[error("code {code}: {message}")]
pub struct JsonRpcErrorObject {
    /// Numeric error code.
    pub code: i64,
    /// Human-readable message, bounded before it is stored or rendered.
    pub message: String,
    /// Optional structured detail (not rendered).
    pub data: Option<Value>,
}

impl JsonRpcErrorObject {
    /// Build an error object, bounding the message.
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: bounded(&message.into()),
            data: None,
        }
    }
}

/// One parsed JSON-RPC message.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum JsonRpcMessage {
    /// A request received from the peer (the ACP agent asking the client).
    Request(JsonRpcRequest),
    /// A notification received from the peer.
    Notification(JsonRpcNotification),
    /// A response to one of our requests.
    Response {
        /// The id our request used.
        id: RequestId,
        /// `Ok(result)` or the error object the peer returned.
        outcome: Result<Value, JsonRpcErrorObject>,
    },
}

/// Why a JSON value is not a usable JSON-RPC message.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// The value is not a JSON object.
    #[error("message is not a JSON object")]
    NotAnObject,
    /// The object matches none of the three JSON-RPC shapes.
    #[error("message matches no JSON-RPC shape")]
    UnrecognisedShape,
    /// A response id could not be correlated (`null` or not an id type).
    #[error("response id cannot be correlated")]
    UncorrelatableId,
    /// A response carried neither `result` nor `error`.
    #[error("response carries neither result nor error")]
    MissingOutcome,
    /// An error object was malformed.
    #[error("error object is malformed: {detail}")]
    MalformedError {
        /// Reason the object could not be read, bounded.
        detail: String,
    },
}

/// Interpret a JSON value as a JSON-RPC message.
///
/// # Errors
///
/// [`ProtocolError`] when the value is not one of the three shapes.
pub fn parse(value: &Value) -> Result<JsonRpcMessage, ProtocolError> {
    let object = value.as_object().ok_or(ProtocolError::NotAnObject)?;
    let id = object.get("id").map(RequestId::from_value).transpose()?;
    let method = object.get("method").and_then(Value::as_str);

    match (method, id) {
        (Some(method), None) => Ok(JsonRpcMessage::Notification(JsonRpcNotification {
            method: method.to_owned(),
            params: object.get("params").cloned().unwrap_or(Value::Null),
        })),
        (Some(method), Some(id)) => Ok(JsonRpcMessage::Request(JsonRpcRequest {
            id,
            method: method.to_owned(),
            params: object.get("params").cloned().unwrap_or(Value::Null),
        })),
        (None, Some(id)) => {
            let result = object.get("result");
            let error = object.get("error");
            let outcome = match (result, error) {
                (Some(result), None) => Ok(result.clone()),
                (None, Some(error)) => Err(read_error(error)?),
                _ => return Err(ProtocolError::MissingOutcome),
            };
            Ok(JsonRpcMessage::Response { id, outcome })
        }
        (None, None) => Err(ProtocolError::UnrecognisedShape),
    }
}

fn read_error(value: &Value) -> Result<JsonRpcErrorObject, ProtocolError> {
    let object = value.as_object().ok_or(ProtocolError::MalformedError {
        detail: "error is not an object".to_owned(),
    })?;
    let code = object
        .get("code")
        .and_then(Value::as_i64)
        .ok_or(ProtocolError::MalformedError {
            detail: "error has no numeric code".to_owned(),
        })?;
    let message =
        object
            .get("message")
            .and_then(Value::as_str)
            .ok_or(ProtocolError::MalformedError {
                detail: "error has no string message".to_owned(),
            })?;
    Ok(JsonRpcErrorObject {
        code,
        message: bounded(message),
        data: object.get("data").cloned(),
    })
}

/// Build a request envelope.
pub fn request(id: i64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

/// Build a notification envelope.
pub fn notification(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

/// Build a success response envelope.
pub fn success(id: RequestId, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id_value(&id), "result": result})
}

/// Build an error response envelope.
pub fn failure(id: RequestId, error: &JsonRpcErrorObject) -> Value {
    let mut envelope = json!({
        "jsonrpc": "2.0",
        "id": id_value(&id),
        "error": {"code": error.code, "message": error.message},
    });
    if let Some(data) = &error.data {
        envelope["error"]["data"] = data.clone();
    }
    envelope
}

fn id_value(id: &RequestId) -> Value {
    match id {
        RequestId::Number(value) => json!(value),
        RequestId::String(text) => json!(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_request_is_recognised_by_method_and_id() {
        let message = parse(&json!({"jsonrpc": "2.0", "id": 7, "method": "m", "params": {"a": 1}}))
            .expect("parse");
        match message {
            JsonRpcMessage::Request(request) => {
                assert_eq!(request.id, RequestId::Number(7));
                assert_eq!(request.method, "m");
                assert_eq!(request.params["a"], 1);
            }
            other => panic!("expected a request, got {other:?}"),
        }
    }

    #[test]
    fn a_notification_has_no_id_and_defaults_params_to_null() {
        let message = parse(&json!({"jsonrpc": "2.0", "method": "n"})).expect("parse");
        match message {
            JsonRpcMessage::Notification(notification) => {
                assert_eq!(notification.method, "n");
                assert_eq!(notification.params, Value::Null);
            }
            other => panic!("expected a notification, got {other:?}"),
        }
    }

    #[test]
    fn responses_carry_either_a_result_or_an_error() {
        let success =
            parse(&json!({"jsonrpc": "2.0", "id": "abc", "result": {"ok": true}})).expect("parse");
        match success {
            JsonRpcMessage::Response { id, outcome } => {
                assert_eq!(id, RequestId::String("abc".into()));
                assert_eq!(outcome.expect("result")["ok"], true);
            }
            other => panic!("expected a response, got {other:?}"),
        }

        let failure = parse(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32601, "message": "no such method"}
        }))
        .expect("parse");
        match failure {
            JsonRpcMessage::Response { outcome, .. } => {
                let error = outcome.expect_err("error");
                assert_eq!(error.code, METHOD_NOT_FOUND);
                assert_eq!(error.message, "no such method");
            }
            other => panic!("expected a response, got {other:?}"),
        }
    }

    #[test]
    fn unusable_messages_are_rejected_explicitly() {
        assert!(matches!(
            parse(&json!([1, 2])),
            Err(ProtocolError::NotAnObject)
        ));
        assert!(matches!(
            parse(&json!({"jsonrpc": "2.0"})),
            Err(ProtocolError::UnrecognisedShape)
        ));
        assert!(matches!(
            parse(&json!({"jsonrpc": "2.0", "id": null, "result": 1})),
            Err(ProtocolError::UncorrelatableId)
        ));
        assert!(matches!(
            parse(&json!({"jsonrpc": "2.0", "id": 1})),
            Err(ProtocolError::MissingOutcome)
        ));
        assert!(matches!(
            parse(&json!({"jsonrpc": "2.0", "id": 1, "error": {"message": "x"}})),
            Err(ProtocolError::MalformedError { .. })
        ));
    }

    #[test]
    fn an_error_message_is_bounded() {
        let long = "x".repeat(4096);
        let message = parse(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32603, "message": long}
        }))
        .expect("parse");
        match message {
            JsonRpcMessage::Response { outcome, .. } => {
                let error = outcome.expect_err("error");
                assert!(error.message.len() <= crate::acp::MAX_DETAIL_BYTES);
            }
            other => panic!("expected a response, got {other:?}"),
        }
    }

    #[test]
    fn envelopes_round_trip_through_parse() {
        let built = request(3, "session/prompt", json!({"sessionId": "s"}));
        assert!(matches!(parse(&built), Ok(JsonRpcMessage::Request(_))));

        let built = notification("session/cancel", json!({"sessionId": "s"}));
        assert!(matches!(parse(&built), Ok(JsonRpcMessage::Notification(_))));

        let built = success(RequestId::Number(1), json!({"ok": true}));
        assert!(matches!(
            parse(&built),
            Ok(JsonRpcMessage::Response { outcome: Ok(_), .. })
        ));

        let error = JsonRpcErrorObject::new(METHOD_NOT_FOUND, "unsupported");
        let built = failure(RequestId::String("agent-1".into()), &error);
        assert!(matches!(
            parse(&built),
            Ok(JsonRpcMessage::Response {
                outcome: Err(_),
                ..
            })
        ));
    }
}
