//! ACP message types: the schema layer.
//!
//! The third layer of the ACP adapter. It owns the protocol's method names and
//! message shapes and nothing else: no bytes, no JSON-RPC envelopes, no process
//! state. Only the subset T014 needs is modelled (the baseline two-stage
//! dispatch); `session/cancel`, session loading/forking and the streaming update
//! vocabulary belong to T015.
//!
//! # Identity of the negotiated peer
//!
//! [`InitializeResult::agent_info`] is an [`Implementation`] — the ACP way for a
//! backend to name itself and its version. ADR-001 requires the compatibility
//! claim to be versioned, so the adapter records it (see
//! [`backend_id`](crate::acp::client::AcpClient::backend_id)) rather than guessing
//! from the command line.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::acp::jsonrpc::{self, JsonRpcErrorObject, RequestId};

/// The ACP protocol version this adapter speaks.
///
/// The schema defines the version as "bumped only for breaking changes" and
/// states that a client which does not support the negotiated version should
/// disconnect. This adapter therefore speaks exactly one version and refuses
/// anything else, rather than switching behaviour heuristically.
pub const PROTOCOL_VERSION: u16 = 1;

/// `initialize`.
pub const METHOD_INITIALIZE: &str = "initialize";
pub const METHOD_AUTHENTICATE: &str = "authenticate";
/// `session/new`.
pub const METHOD_SESSION_NEW: &str = "session/new";
/// `session/resume`.
pub const METHOD_SESSION_RESUME: &str = "session/resume";
/// `session/prompt`.
pub const METHOD_SESSION_PROMPT: &str = "session/prompt";
pub const METHOD_SESSION_CANCEL: &str = "session/cancel";
/// The `session/update` notification.
pub const METHOD_SESSION_UPDATE: &str = "session/update";
/// `session/request_permission` (agent → client).
pub const METHOD_REQUEST_PERMISSION: &str = "session/request_permission";

/// The client name advertised in `initialize`.
pub const CLIENT_NAME: &str = "guigu-agent-bridge";
/// The client version advertised in `initialize`.
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Metadata about an implementation (client or agent).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Implementation {
    /// Programmatic name.
    pub name: String,
    /// Version string.
    pub version: String,
    /// Optional display title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

/// Capabilities this adapter advertises.
///
/// Deliberately empty: the bridge does not implement the agent's `fs/*` or
/// `terminal/*` requests, and advertising a capability it does not serve would
/// invite exactly those requests. A conforming agent must not send them to a
/// client that did not advertise them; one that does gets a JSON-RPC
/// `Method not found` (see [`agent_request_response`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ClientCapabilities {}

/// Parameters of `initialize`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    /// The protocol version this client speaks.
    pub protocol_version: u16,
    /// Advertised capabilities.
    pub client_capabilities: ClientCapabilities,
    /// Advertised client identity.
    pub client_info: Implementation,
}

impl InitializeParams {
    /// Parameters advertising exactly what this adapter serves.
    pub fn new() -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            client_capabilities: ClientCapabilities::default(),
            client_info: Implementation {
                name: CLIENT_NAME.to_owned(),
                version: CLIENT_VERSION.to_owned(),
                title: None,
            },
        }
    }
}

impl Default for InitializeParams {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of `initialize`.
///
/// Unknown fields (`agentCapabilities`, `_meta`, future additions) are ignored on
/// purpose: this adapter must tolerate a newer backend's extra fields, and it
/// never depends on them.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    /// The version the agent negotiated.
    pub protocol_version: u16,
    /// The agent's identity, when it reports one.
    #[serde(default)]
    pub agent_info: Option<Implementation>,
    /// Authentication methods the agent advertises.
    #[serde(default)]
    pub auth_methods: Vec<AuthMethod>,
}

/// An ACP authentication method advertised by an agent.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthMethod {
    #[serde(default)]
    pub r#type: Option<String>,
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub _meta: Option<Value>,
}

impl AuthMethod {
    pub fn is_terminal(&self) -> bool {
        self.r#type.as_deref() == Some("terminal")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticateParams {
    pub method_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AuthenticateResult {}

/// Parameters of `session/new`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NewSessionParams {
    /// Working directory for the session.
    pub cwd: String,
    /// MCP servers to attach; empty because v1 serves no MCP endpoints.
    pub mcp_servers: Vec<Value>,
    pub additional_directories: Vec<String>,
}

impl NewSessionParams {
    /// Parameters for `cwd` with no MCP servers.
    pub fn new(cwd: &str) -> Self {
        Self::new_with_workspaces(cwd, &[])
    }

    pub fn new_with_workspaces(cwd: &str, additional_directories: &[String]) -> Self {
        Self {
            cwd: cwd.to_owned(),
            mcp_servers: Vec::new(),
            additional_directories: additional_directories.to_vec(),
        }
    }
}

/// Parameters of `session/resume`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeSessionParams {
    /// The session to resume.
    pub session_id: String,
    /// Working directory for the session.
    pub cwd: String,
    /// MCP servers to attach; empty.
    pub mcp_servers: Vec<Value>,
    pub additional_directories: Vec<String>,
}

/// Result of `session/new` and `session/resume`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionResult {
    /// The agent's session identity (opaque).
    pub session_id: String,
}

/// One block of prompt content.
///
/// Only text is produced; unknown inbound blocks are tolerated so that a newer
/// backend's chunk shapes do not fail the turn (they contribute no text).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// A text block.
    Text {
        /// The text.
        text: String,
    },
    /// Any block kind this adapter does not model.
    #[serde(other)]
    Other,
}

impl ContentBlock {
    /// The text of a text block.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text { text } => Some(text),
            Self::Other => None,
        }
    }
}

/// Parameters of `session/prompt`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptParams {
    /// The session to prompt.
    pub session_id: String,
    /// The prompt content.
    pub prompt: Vec<ContentBlock>,
}

impl PromptParams {
    /// A single-text-block prompt for `session_id`.
    pub fn text(session_id: &str, text: &str) -> Self {
        Self {
            session_id: session_id.to_owned(),
            prompt: vec![ContentBlock::Text {
                text: text.to_owned(),
            }],
        }
    }
}

/// Why the agent stopped processing a turn.
///
/// Unknown values are preserved rather than rejected: a newer backend's reason is
/// still a terminal answer, and the adapter records it as a failure instead of
/// treating the turn as successful or failing to parse it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    /// The turn ended successfully.
    EndTurn,
    /// The agent hit its token limit.
    MaxTokens,
    /// The agent hit its inter-turn request limit.
    MaxTurnRequests,
    /// The agent refused to continue.
    Refusal,
    /// A reason this adapter does not model.
    Other(String),
}

impl StopReason {
    /// The wire value, for diagnostics.
    pub fn as_str(&self) -> &str {
        match self {
            Self::EndTurn => "end_turn",
            Self::MaxTokens => "max_tokens",
            Self::MaxTurnRequests => "max_turn_requests",
            Self::Refusal => "refusal",
            Self::Other(other) => other,
        }
    }

    /// Whether the turn completed the work.
    pub fn is_end_turn(&self) -> bool {
        matches!(self, Self::EndTurn)
    }
}

impl<'de> Deserialize<'de> for StopReason {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(match raw.as_str() {
            "end_turn" => Self::EndTurn,
            "max_tokens" => Self::MaxTokens,
            "max_turn_requests" => Self::MaxTurnRequests,
            "refusal" => Self::Refusal,
            other => Self::Other(crate::acp::bounded(other)),
        })
    }
}

/// Result of `session/prompt`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptResult {
    /// Why the turn stopped.
    pub stop_reason: StopReason,
    /// Explicit task lifecycle result; absent means the peer only ended a turn.
    #[serde(default)]
    pub task_result: Option<crate::acp::result::WireTurnResult>,
}

/// A `session/update` notification.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionNotification {
    /// The session this update belongs to.
    pub session_id: String,
    /// The update itself.
    pub update: SessionUpdate,
}

/// The payload of a `session/update`, discriminated by `sessionUpdate`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "sessionUpdate", rename_all = "snake_case")]
pub enum SessionUpdate {
    /// A chunk of the agent's answer.
    AgentMessageChunk {
        /// The chunk content.
        content: ContentBlock,
    },
    /// Any update kind this adapter does not model (tool calls, plans, …).
    #[serde(other)]
    Other,
}

/// Parameters of `session/request_permission`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRequestParams {
    /// The session asking.
    #[serde(default)]
    pub session_id: Option<String>,
    /// The options the agent offers.
    #[serde(default)]
    pub options: Vec<PermissionOption>,
}

/// One option offered in a permission request.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionOption {
    /// The option identity to echo back.
    pub option_id: String,
    /// The option kind (`allow_once`, `reject_always`, …).
    pub kind: String,
    /// Optional human-readable name (never rendered by this adapter).
    #[serde(default)]
    pub name: Option<String>,
}

impl PermissionOption {
    /// Whether choosing this option denies the operation.
    pub fn is_reject(&self) -> bool {
        self.kind.starts_with("reject")
    }
}

/// The answer to a permission request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum PermissionResponse {
    /// One of the offered options was chosen.
    #[serde(rename_all = "camelCase")]
    Selected {
        /// The chosen option.
        option_id: String,
    },
    /// The request was refused without choosing an option.
    Cancelled,
}

/// Answer an agent → client request.
///
/// Two policies, both deliberately conservative:
///
/// - `session/request_permission` is **denied**: the first `reject_*` option is
///   chosen when the agent offers one, otherwise the answer is `cancelled`. The
///   adapter never decides to allow anything — permission policy belongs to T011,
///   and a prompt that cannot proceed is a visible failure rather than a silent
///   grant.
/// - everything else (`fs/read_text_file`, `fs/write_text_file`, `terminal/*`,
///   unknown methods) is answered with JSON-RPC `Method not found`, because the
///   adapter never advertises those capabilities. A conforming agent will not ask.
///
/// The caller writes the returned envelope verbatim.
pub fn agent_request_response(id: RequestId, method: &str, params: &Value) -> Value {
    match method {
        METHOD_REQUEST_PERMISSION => {
            let response = deny_permission(params);
            match serde_json::to_value(response) {
                Ok(value) => jsonrpc::success(id, value),
                Err(_) => jsonrpc::failure(
                    id,
                    &JsonRpcErrorObject::new(
                        jsonrpc::INTERNAL_ERROR,
                        "could not encode the permission response",
                    ),
                ),
            }
        }
        other => jsonrpc::failure(
            id,
            &JsonRpcErrorObject::new(
                jsonrpc::METHOD_NOT_FOUND,
                format!("unsupported agent request: {other}"),
            ),
        ),
    }
}

/// Deny a permission request, preferring an option the agent offers.
fn deny_permission(params: &Value) -> PermissionResponse {
    // Malformed options still deny: `cancelled` is the refusing answer, so a
    // backend that sends an unexpected shape cannot accidentally be allowed.
    let request: PermissionRequestParams =
        serde_json::from_value(params.clone()).unwrap_or(PermissionRequestParams {
            session_id: None,
            options: Vec::new(),
        });
    match request
        .options
        .into_iter()
        .find(PermissionOption::is_reject)
    {
        Some(option) => PermissionResponse::Selected {
            option_id: option.option_id,
        },
        None => PermissionResponse::Cancelled,
    }
}

/// The prompt params for a text task.
pub fn text_prompt(session_id: &str, text: &str) -> Value {
    serde_json::to_value(PromptParams::text(session_id, text)).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn initialize_params_advertise_no_capabilities() {
        let params = InitializeParams::new();
        let value = serde_json::to_value(&params).expect("encode");
        assert_eq!(value["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(value["clientInfo"]["name"], CLIENT_NAME);
        assert_eq!(
            value["clientCapabilities"],
            json!({}),
            "advertising a capability we do not serve would invite those requests"
        );
    }

    #[test]
    fn initialize_result_tolerates_unknown_fields_and_missing_info() {
        let result: InitializeResult = serde_json::from_value(json!({
            "protocolVersion": 1,
            "agentCapabilities": {"loadSession": true},
            "_meta": {"anything": true}
        }))
        .expect("decode");
        assert_eq!(result.protocol_version, 1);
        assert_eq!(result.agent_info, None);
        assert!(result.auth_methods.is_empty());
    }

    #[test]
    fn api_key_auth_method_has_the_frozen_wire_shape() {
        let result: InitializeResult = serde_json::from_value(json!({
            "protocolVersion": 1,
            "authMethods": [{
                "id": "api-key",
                "name": "API Key",
                "description": "Use an API key to authenticate",
                "_meta": {"api-key": {"provider": "openai"}}
            }]
        }))
        .expect("API-key method");
        assert_eq!(result.auth_methods.len(), 1);
        assert_eq!(result.auth_methods[0].id, "api-key");
        assert_eq!(result.auth_methods[0].r#type, None);
        assert!(!result.auth_methods[0].is_terminal());
        let params = serde_json::to_value(AuthenticateParams {
            method_id: "api-key".to_owned(),
        })
        .expect("authenticate params");
        assert_eq!(params, json!({"methodId": "api-key"}));
        let _: AuthenticateResult = serde_json::from_value(json!({})).expect("empty result");
    }

    #[test]
    fn a_stop_reason_is_never_rejected() {
        let known: PromptResult =
            serde_json::from_value(json!({"stopReason": "end_turn"})).expect("decode");
        assert!(known.stop_reason.is_end_turn());

        let unknown: PromptResult =
            serde_json::from_value(json!({"stopReason": "some_future_reason"})).expect("decode");
        assert_eq!(
            unknown.stop_reason,
            StopReason::Other("some_future_reason".into())
        );
        assert_eq!(unknown.stop_reason.as_str(), "some_future_reason");
    }

    #[test]
    fn an_unknown_stop_reason_is_bounded() {
        let long = "y".repeat(4096);
        let result: PromptResult =
            serde_json::from_value(json!({"stopReason": long})).expect("decode");
        match result.stop_reason {
            StopReason::Other(other) => assert!(other.len() <= crate::acp::MAX_DETAIL_BYTES),
            other => panic!("expected an unknown reason, got {other:?}"),
        }
    }

    #[test]
    fn content_blocks_carry_text_and_tolerate_other_kinds() {
        let block: ContentBlock =
            serde_json::from_value(json!({"type": "text", "text": "hi"})).expect("decode");
        assert_eq!(block.as_text(), Some("hi"));

        let other: ContentBlock =
            serde_json::from_value(json!({"type": "image", "data": "..."})).expect("decode");
        assert_eq!(other.as_text(), None);

        let encoded = serde_json::to_value(ContentBlock::Text {
            text: "hi".to_owned(),
        })
        .expect("encode");
        assert_eq!(encoded, json!({"type": "text", "text": "hi"}));
    }

    #[test]
    fn session_updates_are_discriminated_and_unknown_ones_are_tolerated() {
        let notification: SessionNotification = serde_json::from_value(json!({
            "sessionId": "s1",
            "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "a"}}
        }))
        .expect("decode");
        assert_eq!(notification.session_id, "s1");
        match notification.update {
            SessionUpdate::AgentMessageChunk { content } => {
                assert_eq!(content.as_text(), Some("a"))
            }
            other => panic!("expected a message chunk, got {other:?}"),
        }

        let unknown: SessionNotification = serde_json::from_value(json!({
            "sessionId": "s1",
            "update": {"sessionUpdate": "plan", "entries": []}
        }))
        .expect("decode");
        assert_eq!(unknown.update, SessionUpdate::Other);
    }

    #[test]
    fn a_permission_request_is_denied_with_an_offered_reject_option() {
        let params = json!({
            "sessionId": "s1",
            "options": [
                {"optionId": "a", "kind": "allow_once", "name": "Allow"},
                {"optionId": "b", "kind": "reject_once", "name": "Reject"}
            ]
        });
        let response =
            agent_request_response(RequestId::Number(1), METHOD_REQUEST_PERMISSION, &params);
        assert_eq!(response["result"]["outcome"], "selected");
        assert_eq!(response["result"]["optionId"], "b");
    }

    #[test]
    fn a_permission_request_without_a_reject_option_is_cancelled() {
        let params = json!({"options": [{"optionId": "a", "kind": "allow_always"}]});
        let response =
            agent_request_response(RequestId::Number(2), METHOD_REQUEST_PERMISSION, &params);
        assert_eq!(response["result"]["outcome"], "cancelled");
    }

    #[test]
    fn a_malformed_permission_request_is_still_denied() {
        let response = agent_request_response(
            RequestId::Number(3),
            METHOD_REQUEST_PERMISSION,
            &json!("nonsense"),
        );
        assert_eq!(
            response["result"]["outcome"], "cancelled",
            "an unreadable request must not become an implicit allow"
        );
    }

    #[test]
    fn an_unsupported_agent_request_is_method_not_found() {
        for method in [
            "fs/read_text_file",
            "fs/write_text_file",
            "terminal/create",
            "something/new",
        ] {
            let response = agent_request_response(RequestId::Number(9), method, &json!({}));
            assert_eq!(response["error"]["code"], jsonrpc::METHOD_NOT_FOUND);
            assert!(
                response["error"]["message"]
                    .as_str()
                    .expect("message")
                    .contains(method)
            );
        }
    }

    #[test]
    fn a_prompt_is_a_single_text_block() {
        let params = text_prompt("s1", "do the thing");
        assert_eq!(params["sessionId"], "s1");
        assert_eq!(params["prompt"][0]["type"], "text");
        assert_eq!(params["prompt"][0]["text"], "do the thing");
    }

    #[test]
    fn session_params_carry_the_working_directory_and_no_mcp_servers() {
        let value = serde_json::to_value(NewSessionParams::new("/tmp/work")).expect("encode");
        assert_eq!(value["cwd"], "/tmp/work");
        assert_eq!(value["mcpServers"], json!([]));

        let value = serde_json::to_value(ResumeSessionParams {
            session_id: "s1".to_owned(),
            cwd: "/tmp/work".to_owned(),
            mcp_servers: Vec::new(),
            additional_directories: Vec::new(),
        })
        .expect("encode");
        assert_eq!(value["sessionId"], "s1");
    }
}
