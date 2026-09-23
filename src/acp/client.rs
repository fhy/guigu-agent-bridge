//! The ACP protocol client: handshake, session lifecycle, bounded deadlines.
//!
//! One step above the transport. It turns "a process that speaks JSON-RPC" into
//! "a backend this bridge has negotiated with", and it owns the two protocol
//! decisions ADR-001 requires to be explicit:
//!
//! - **one protocol version, no heuristics.** [`connect`](AcpClient::connect)
//!   offers [`PROTOCOL_VERSION`](crate::acp::schema::PROTOCOL_VERSION) and accepts
//!   nothing else: an agent that negotiates a different version is disconnected
//!   instead of being driven with guessed semantics.
//! - **authentication is a first-class failure.** If the agent advertises
//!   authentication methods, v1 cannot satisfy them, so the connection fails with
//!   [`AcpError::Authentication`](crate::acp::error::AcpError::Authentication)
//!   rather than prompting a turn that would be refused.
//!
//! Every wait is bounded by a deadline from [`AcpLimits`]. Those values are
//! constants with builder overrides rather than configuration: `Config` has no ACP
//! section and is frozen, so the upgrade point is the assembly layer (T016/T017).
//!
//! The negotiated [`backend_id`](AcpClient::backend_id) carries the backend's own
//! name and version, which is what makes the compatibility claim in ADR-001
//! versioned rather than inferred from a command line.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;

use crate::acp::AcpTurnHandle;
use crate::acp::error::{AcpError, Phase};
use crate::acp::result::TurnResult;
use crate::acp::schema::{
    self, AuthenticateParams, AuthenticateResult, InitializeParams, InitializeResult,
    NewSessionParams, PromptResult, ResumeSessionParams, SessionResult, StopReason,
};
use crate::acp::transport::{AcpTransport, TransportLimits};
use crate::acp::{MAX_SESSION_ID_BYTES, bounded};
use crate::models::EndpointAddress;

/// The default bound on the `initialize` handshake.
pub const DEFAULT_INITIALIZE_DEADLINE: Duration = Duration::from_secs(10);

/// The default bound on a session or prompt request.
pub const DEFAULT_REQUEST_DEADLINE: Duration = Duration::from_secs(30);

/// The adapter's bounds, as the assembly layer supplies them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcpLimits {
    /// Bound on the `initialize` handshake.
    pub initialize_deadline: Duration,
    /// Bound on `session/new`, `session/resume` and `session/prompt`.
    pub request_deadline: Duration,
    /// Bounds the transport applies (frames, output, shutdown).
    pub transport: TransportLimits,
}

impl Default for AcpLimits {
    fn default() -> Self {
        Self {
            initialize_deadline: DEFAULT_INITIALIZE_DEADLINE,
            request_deadline: DEFAULT_REQUEST_DEADLINE,
            transport: TransportLimits::default(),
        }
    }
}

/// One finished prompt turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptTurn {
    /// Why the agent stopped.
    pub stop_reason: StopReason,
    /// The text the agent streamed for this turn, bounded and possibly truncated.
    pub text: String,
    /// Explicit task lifecycle result, when supplied by the backend.
    pub task_result: Option<TurnResult>,
}

/// A negotiated ACP backend.
pub struct AcpClient {
    transport: Arc<AcpTransport>,
    limits: AcpLimits,
    backend_id: String,
}

impl AcpClient {
    /// Spawn `address` and complete the `initialize` handshake.
    ///
    /// `agent_id` is the configuration identity, used only to build a readable
    /// [`backend_id`](Self::backend_id); `cwd` is the child's working directory.
    ///
    /// # Errors
    ///
    /// [`AcpError::Spawn`] when the process cannot start, [`AcpError::Timeout`]
    /// when the handshake exceeds its deadline, [`AcpError::UnsupportedProtocol`]
    /// when the agent speaks another version, [`AcpError::Authentication`] when it
    /// requires authentication, or a framing/protocol/schema failure.
    pub async fn connect(
        address: &EndpointAddress,
        agent_id: &str,
        cwd: Option<&Path>,
        limits: AcpLimits,
    ) -> Result<Self, AcpError> {
        let transport = Arc::new(AcpTransport::spawn(address, cwd, limits.transport)?);
        let params = encode(schema::METHOD_INITIALIZE, &InitializeParams::new())?;
        let completed = transport
            .request(
                Phase::Initialize,
                schema::METHOD_INITIALIZE,
                params,
                limits.initialize_deadline,
            )
            .await?;
        let result: InitializeResult = decode(schema::METHOD_INITIALIZE, completed.result)?;

        if result.protocol_version != schema::PROTOCOL_VERSION {
            // The ACP schema tells a client that does not support the negotiated
            // version to disconnect. We do exactly that: no heuristic fallback.
            let _ = transport.shutdown().await;
            return Err(AcpError::UnsupportedProtocol {
                got: result.protocol_version,
                supported: schema::PROTOCOL_VERSION,
            });
        }
        let api_methods: Vec<_> = result
            .auth_methods
            .iter()
            .filter(|method| method.id == "api-key" && !method.is_terminal())
            .collect();
        if api_methods.len() > 1 {
            let _ = transport.shutdown().await;
            return Err(AcpError::Authentication {
                advertised: result.auth_methods.len(),
            });
        }
        if !result.auth_methods.is_empty() {
            let Some(method) = api_methods.first() else {
                let _ = transport.shutdown().await;
                return Err(AcpError::Authentication {
                    advertised: result.auth_methods.len(),
                });
            };
            let params = encode(
                schema::METHOD_AUTHENTICATE,
                &AuthenticateParams {
                    method_id: method.id.clone(),
                },
            )?;
            let completed = transport
                .request(
                    Phase::Authenticate,
                    schema::METHOD_AUTHENTICATE,
                    params,
                    limits.initialize_deadline,
                )
                .await?;
            let _: AuthenticateResult = decode(schema::METHOD_AUTHENTICATE, completed.result)?;
        }

        let backend_id = backend_id(agent_id, result.agent_info.as_ref());
        tracing::debug!(%backend_id, "acp backend negotiated");
        Ok(Self {
            transport,
            limits,
            backend_id,
        })
    }

    /// The negotiated backend identity, `agent/name@version` (ADR-001).
    pub fn backend_id(&self) -> &str {
        &self.backend_id
    }

    /// Whether the backend process and its loop are still running.
    pub fn is_alive(&self) -> bool {
        self.transport.is_alive()
    }

    /// Create a session for `cwd`.
    ///
    /// # Errors
    ///
    /// A transport/agent/schema failure, or [`AcpError::SessionId`] when the
    /// returned session id is unusable.
    pub async fn new_session(&self, cwd: &str) -> Result<String, AcpError> {
        self.new_session_with_workspaces(cwd, &[]).await
    }

    pub async fn new_session_with_workspaces(
        &self,
        cwd: &str,
        additional_directories: &[String],
    ) -> Result<String, AcpError> {
        let params = encode(
            schema::METHOD_SESSION_NEW,
            &NewSessionParams::new_with_workspaces(cwd, additional_directories),
        )?;
        let completed = self
            .transport
            .request(
                Phase::Session,
                schema::METHOD_SESSION_NEW,
                params,
                self.limits.request_deadline,
            )
            .await?;
        let result: SessionResult = decode(schema::METHOD_SESSION_NEW, completed.result)?;
        validate_session_id(result.session_id)
    }

    /// Re-bind an existing session.
    ///
    /// The agent must answer with the session id that was asked for: a resume that
    /// silently moves to another session would make the stored row and the live
    /// conversation disagree, so it is reported as
    /// [`AcpError::SessionId`] instead.
    ///
    /// # Errors
    ///
    /// As [`AcpClient::new_session`], plus a mismatch between the requested and
    /// returned session ids.
    pub async fn resume_session(&self, session_id: &str, cwd: &str) -> Result<String, AcpError> {
        self.resume_session_with_workspaces(session_id, cwd, &[])
            .await
    }

    pub async fn resume_session_with_workspaces(
        &self,
        session_id: &str,
        cwd: &str,
        additional_directories: &[String],
    ) -> Result<String, AcpError> {
        let requested = validate_session_id(session_id.to_owned())?;
        let params = encode(
            schema::METHOD_SESSION_RESUME,
            &ResumeSessionParams {
                session_id: requested.clone(),
                cwd: cwd.to_owned(),
                mcp_servers: Vec::new(),
                additional_directories: additional_directories.to_vec(),
            },
        )?;
        let completed = self
            .transport
            .request(
                Phase::Session,
                schema::METHOD_SESSION_RESUME,
                params,
                self.limits.request_deadline,
            )
            .await?;
        let result: SessionResult = decode(schema::METHOD_SESSION_RESUME, completed.result)?;
        let returned = validate_session_id(result.session_id)?;
        if returned != requested {
            return Err(AcpError::SessionId {
                detail: "resume answered with a different session id".to_owned(),
            });
        }
        Ok(returned)
    }

    /// Run one prompt turn to its terminal answer.
    ///
    /// The returned text is what the agent streamed during the turn; the baseline
    /// accumulates it without exposing a stream (T015 owns streaming).
    ///
    /// # Errors
    ///
    /// A transport/agent/schema failure or a prompt timeout.
    pub async fn prompt(&self, session_id: &str, text: &str) -> Result<PromptTurn, AcpError> {
        let session_id = validate_session_id(session_id.to_owned())?;
        let params = schema::text_prompt(&session_id, text);
        let completed = self
            .transport
            .request(
                Phase::Prompt,
                schema::METHOD_SESSION_PROMPT,
                params,
                self.limits.request_deadline,
            )
            .await?;
        let result: PromptResult = decode(schema::METHOD_SESSION_PROMPT, completed.result)?;
        let task_result = match result.task_result {
            Some(result) => Some(
                result
                    .into_result(completed.streamed_text.clone())
                    .map_err(|detail| AcpError::Schema {
                        method: schema::METHOD_SESSION_PROMPT.to_owned(),
                        detail,
                    })?,
            ),
            None => None,
        };
        Ok(PromptTurn {
            stop_reason: result.stop_reason,
            text: completed.streamed_text,
            task_result,
        })
    }

    pub async fn cancel(&self, session_id: &str) -> Result<(), AcpError> {
        let session_id = validate_session_id(session_id.to_owned())?;
        self.transport
            .notify(
                schema::METHOD_SESSION_CANCEL,
                serde_json::json!({"sessionId": session_id}),
            )
            .await
    }

    /// Notify the backend, then deterministically reap the process.
    pub async fn cancel_and_shutdown(&self, session_id: &str) -> Result<Option<i32>, AcpError> {
        self.cancel(session_id).await?;
        Ok(self.shutdown().await)
    }

    pub async fn cancel_and_reap(&self, session_id: &str) -> Result<(), AcpError> {
        self.cancel(session_id).await?;
        if self.transport.shutdown_reaped().await {
            Ok(())
        } else {
            Err(AcpError::TransportClosed)
        }
    }

    pub async fn prompt_stream(
        &self,
        session_id: &str,
        text: &str,
        capacity: usize,
    ) -> Result<AcpTurnHandle, AcpError> {
        let session_id = validate_session_id(session_id.to_owned())?;
        self.transport
            .request_stream(
                Phase::Prompt,
                schema::METHOD_SESSION_PROMPT,
                schema::text_prompt(&session_id, text),
                self.limits.request_deadline,
                capacity,
            )
            .await
    }

    pub(crate) fn decode_prompt_completion(
        completed: crate::acp::CompletedRequest,
    ) -> Result<PromptTurn, AcpError> {
        let result: PromptResult = decode(schema::METHOD_SESSION_PROMPT, completed.result)?;
        let task_result = match result.task_result {
            Some(result) => Some(
                result
                    .into_result(completed.streamed_text.clone())
                    .map_err(|detail| AcpError::Schema {
                        method: schema::METHOD_SESSION_PROMPT.to_owned(),
                        detail,
                    })?,
            ),
            None => None,
        };
        Ok(PromptTurn {
            stop_reason: result.stop_reason,
            text: completed.streamed_text,
            task_result,
        })
    }

    /// Close the backend, reaping the child process.
    pub async fn shutdown(&self) -> Option<i32> {
        self.transport.shutdown().await
    }

    /// Close the backend and report positive loop join/child reap evidence.
    pub async fn shutdown_reaped(&self) -> bool {
        self.transport.shutdown_reaped().await
    }
}

impl std::fmt::Debug for AcpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcpClient")
            .field("backend_id", &self.backend_id)
            .field("alive", &self.is_alive())
            .finish()
    }
}

/// The compatibility identity recorded for a negotiated backend.
fn backend_id(agent_id: &str, info: Option<&schema::Implementation>) -> String {
    match info {
        Some(info) => bounded(&format!("{agent_id}/{}@{}", info.name, info.version)),
        None => bounded(&format!("{agent_id}/unknown@unknown")),
    }
}

/// Encode request parameters, mapping a failure onto a schema error.
fn encode<T: Serialize>(method: &str, value: &T) -> Result<Value, AcpError> {
    serde_json::to_value(value).map_err(|error| AcpError::Schema {
        method: method.to_owned(),
        detail: bounded(&error.to_string()),
    })
}

/// Decode a result, mapping a failure onto a schema error.
fn decode<T: serde::de::DeserializeOwned>(method: &str, value: Value) -> Result<T, AcpError> {
    serde_json::from_value(value).map_err(|error| AcpError::Schema {
        method: method.to_owned(),
        detail: bounded(&error.to_string()),
    })
}

/// Accept a session id only when it is present and bounded.
fn validate_session_id(session_id: String) -> Result<String, AcpError> {
    if session_id.is_empty() {
        return Err(AcpError::SessionId {
            detail: "the session id is empty".to_owned(),
        });
    }
    if session_id.len() > MAX_SESSION_ID_BYTES {
        return Err(AcpError::SessionId {
            detail: format!("the session id exceeds {MAX_SESSION_ID_BYTES} bytes"),
        });
    }
    Ok(session_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_backend_id_carries_the_agents_own_name_and_version() {
        let info = schema::Implementation {
            name: "codex-acp".to_owned(),
            version: "1.2.3".to_owned(),
            title: None,
        };
        assert_eq!(
            backend_id("worker", Some(&info)),
            "worker/codex-acp@1.2.3",
            "ADR-001 wants a versioned compatibility identity"
        );
    }

    #[test]
    fn an_agent_without_identity_still_yields_a_stable_backend_id() {
        assert_eq!(backend_id("worker", None), "worker/unknown@unknown");
    }

    #[test]
    fn a_long_backend_id_is_bounded() {
        let info = schema::Implementation {
            name: "n".repeat(4096),
            version: "v".repeat(4096),
            title: None,
        };
        assert!(backend_id("worker", Some(&info)).len() <= crate::acp::MAX_DETAIL_BYTES);
    }

    #[test]
    fn session_ids_are_validated() {
        assert_eq!(
            validate_session_id("s1".to_owned()).expect("valid"),
            "s1".to_owned()
        );
        assert!(matches!(
            validate_session_id(String::new()),
            Err(AcpError::SessionId { .. })
        ));
        assert!(matches!(
            validate_session_id("x".repeat(MAX_SESSION_ID_BYTES + 1)),
            Err(AcpError::SessionId { .. })
        ));
    }
}
