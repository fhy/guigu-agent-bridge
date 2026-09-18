use thiserror::Error;

/// Bounded A2A adapter failures. Values never contain credentials or bodies.
#[derive(Debug, Error)]
pub enum A2aError {
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
