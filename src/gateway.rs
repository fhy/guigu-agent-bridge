//! Transport-neutral A2A-over-Matrix gateway contracts.
//!
//! Matrix SDK and optional transports remain outside these types. SQLite is the
//! authority; this module only validates the bounded envelope and its phases.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PROFILE: &str = "a2a-matrix/1";
pub const MAX_ENVELOPE_BYTES: usize = 512 * 1024;
pub const MAX_INLINE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeKind {
    Request,
    Ack,
    Acceptance,
    Status,
    Cancel,
    Artifact,
    Error,
    Retry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointRef {
    pub endpoint_id: Uuid,
    pub peer_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMeta {
    pub artifact_id: Uuid,
    pub media_type: String,
    pub byte_len: usize,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Integrity {
    pub algorithm: String,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayEnvelope {
    pub version: String,
    pub envelope_id: Uuid,
    pub idempotency_key: String,
    pub direction: Direction,
    pub peer_id: String,
    pub sender: EndpointRef,
    pub recipient: EndpointRef,
    pub conversation_id: Uuid,
    pub correlation_id: Uuid,
    pub causal_seq: u64,
    pub created_at: String,
    pub deadline: Option<String>,
    pub kind: EnvelopeKind,
    pub content_type: String,
    pub payload: Option<Vec<u8>>,
    pub payload_sha256: String,
    pub artifact: Option<ArtifactMeta>,
    pub integrity: Integrity,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EnvelopeError {
    #[error("wrong profile version")]
    Version,
    #[error("bounded field exceeded")]
    Bounds,
    #[error("kind payload contract violated")]
    Shape,
    #[error("endpoint peer mismatch")]
    Peer,
}

impl GatewayEnvelope {
    pub fn validate(&self) -> Result<(), EnvelopeError> {
        if self.version != PROFILE {
            return Err(EnvelopeError::Version);
        }
        if self.idempotency_key.is_empty()
            || self.idempotency_key.len() > 128
            || self.peer_id.is_empty()
            || self.peer_id.len() > 128
            || self.sender.peer_id != self.peer_id
            || self.recipient.peer_id != self.peer_id
            || self.content_type.len() > 128
            || self
                .payload
                .as_ref()
                .is_some_and(|p| p.len() > MAX_INLINE_BYTES)
        {
            return Err(EnvelopeError::Bounds);
        }
        if self.sender.peer_id != self.peer_id || self.recipient.peer_id != self.peer_id {
            return Err(EnvelopeError::Peer);
        }
        let has_payload = self.payload.is_some();
        let has_artifact = self.artifact.is_some();
        match self.kind {
            EnvelopeKind::Request => {
                if has_payload == has_artifact {
                    return Err(EnvelopeError::Shape);
                }
            }
            EnvelopeKind::Ack | EnvelopeKind::Acceptance => {
                if has_payload || has_artifact {
                    return Err(EnvelopeError::Shape);
                }
            }
            EnvelopeKind::Artifact => {
                if !has_payload || self.artifact.is_none() {
                    return Err(EnvelopeError::Shape);
                }
            }
            EnvelopeKind::Retry => {
                if has_payload || has_artifact {
                    return Err(EnvelopeError::Shape);
                }
            }
            EnvelopeKind::Status | EnvelopeKind::Cancel | EnvelopeKind::Error => {}
        }
        if self
            .artifact
            .as_ref()
            .is_some_and(|a| a.byte_len > MAX_INLINE_BYTES)
        {
            return Err(EnvelopeError::Bounds);
        }
        Ok(())
    }

    pub fn encoded_len(&self) -> Result<usize, serde_json::Error> {
        serde_json::to_vec(self).map(|v| v.len())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPhase {
    Pending,
    SendUnknown,
    TransportAcked,
    TaskAccepted,
    Terminal,
    Stale,
    RecoveryNeeded,
}

impl DeliveryPhase {
    pub fn can_transition(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Pending,
                Self::SendUnknown | Self::TransportAcked | Self::Stale | Self::RecoveryNeeded
            ) | (
                Self::SendUnknown,
                Self::TransportAcked | Self::RecoveryNeeded
            ) | (
                Self::TransportAcked,
                Self::TaskAccepted | Self::Stale | Self::RecoveryNeeded
            ) | (Self::TaskAccepted, Self::Terminal | Self::Stale)
        )
    }
}
