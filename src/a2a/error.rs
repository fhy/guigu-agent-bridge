use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IngressStage {
    HttpBody,
    Authentication,
    Authorization,
    RpcEnvelope,
    Method,
    ParamsDecode,
    VersionValidate,
    IdempotencyReserve,
    ContentPersist,
    EndpointResolve,
    PeerPersist,
    Conversation,
    Admission,
    Binding,
    Snapshot,
    ResponseEncode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IngressCause {
    BodyTooLarge,
    MalformedJson,
    InvalidParams,
    AuthenticationFailed,
    TargetUnavailable,
    Unsupported,
    IdempotencyConflict,
    Storage,
    AdmissionBusy,
    AdmissionClosed,
    BindingIncomplete,
    RecoveryRequired,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Retryability {
    Never,
    CredentialsChange,
    PolicyChange,
    SameRequest,
    Backoff,
    ServiceRecovery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressFailure {
    pub stage: IngressStage,
    pub cause: IngressCause,
    pub retryability: Retryability,
}

impl IngressFailure {
    pub const fn new(stage: IngressStage, cause: IngressCause, retryability: Retryability) -> Self {
        Self {
            stage,
            cause,
            retryability,
        }
    }
}

/// Bounded A2A adapter failures. Values never contain credentials or bodies.
#[derive(Debug, Error)]
pub enum A2aError {
    #[error("A2A ingress failed")]
    Ingress(IngressFailure),
    #[error("invalid A2A configuration: {0}")]
    Config(&'static str),
    #[error("A2A authentication failed")]
    Unauthorized,
    #[error("unsupported A2A protocol or method")]
    Unsupported,
    #[error("invalid A2A request: {0}")]
    Protocol(&'static str),
    #[error("A2A request exceeds configured bounds")]
    TooLarge,
    #[error("A2A request conflicts with an existing idempotency key")]
    Conflict,
    #[error("A2A remote acceptance is unknown")]
    AcceptanceUnknown,
    #[error("A2A recovery requires operator action")]
    RecoveryNeeded,
    #[error("A2A storage operation failed")]
    Storage(#[source] sqlx::Error),
    #[error("A2A network operation failed")]
    Network(#[source] reqwest::Error),
}

impl A2aError {
    pub const fn ingress(
        stage: IngressStage,
        cause: IngressCause,
        retryability: Retryability,
    ) -> Self {
        Self::Ingress(IngressFailure::new(stage, cause, retryability))
    }
}

impl From<sqlx::Error> for A2aError {
    fn from(value: sqlx::Error) -> Self {
        Self::Storage(value)
    }
}

impl From<reqwest::Error> for A2aError {
    fn from(value: reqwest::Error) -> Self {
        Self::Network(value)
    }
}
