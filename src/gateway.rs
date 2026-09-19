//! Transport-neutral A2A-over-Matrix gateway contracts.
//!
//! Matrix SDK and optional transports remain outside these types. SQLite is the
//! authority; this module only validates the bounded envelope and its phases.

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::mpsc;
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

/// Private transport seam used by Matrix and the deferred Redis adapter.
pub trait GatewayTransport: Send + Sync {
    fn send<'a>(
        &'a self,
        envelope: &'a GatewayEnvelope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), EnvelopeError>> + Send + 'a>>;
}

#[derive(Clone)]
pub struct GatewayStore {
    pool: SqlitePool,
}

impl GatewayStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn insert_envelope(
        &self,
        envelope: &GatewayEnvelope,
        canonical_json: &[u8],
        retained_bytes: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO gateway_envelopes (envelope_id,version,direction,peer_id,sender_user_id,idempotency_key,sender_endpoint,recipient_endpoint,conversation_id,correlation_id,kind,canonical_json,payload_sha256,created_at,route_generation,state,retained_bytes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(envelope.envelope_id.to_string()).bind(&envelope.version).bind(match envelope.direction { Direction::Inbound => "inbound", Direction::Outbound => "outbound" })
            .bind(&envelope.peer_id).bind(&envelope.sender.peer_id).bind(&envelope.idempotency_key)
            .bind(envelope.sender.endpoint_id.to_string()).bind(envelope.recipient.endpoint_id.to_string())
            .bind(envelope.conversation_id.to_string()).bind(envelope.correlation_id.to_string())
            .bind(format!("{:?}", envelope.kind).to_lowercase()).bind(canonical_json).bind(&envelope.payload_sha256)
            .bind(&envelope.created_at).bind(0_i64).bind("received").bind(retained_bytes)
            .execute(&self.pool).await.map(|_| ())
    }

    pub async fn transition_delivery(
        &self,
        envelope_id: &str,
        phase: DeliveryPhase,
        owner_runtime: &str,
        owner_revision: i64,
    ) -> Result<bool, sqlx::Error> {
        let phase = format!("{phase:?}").to_lowercase();
        let result = sqlx::query("UPDATE gateway_deliveries SET phase=?, owner_runtime=?, owner_revision=owner_revision+1 WHERE envelope_id=? AND owner_runtime=? AND owner_revision=?")
            .bind(phase).bind(owner_runtime).bind(envelope_id).bind(owner_runtime).bind(owner_revision).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }
}

/// Deterministic bounded transport fake for contract and recovery tests.
pub struct MemoryTransport {
    tx: mpsc::Sender<GatewayEnvelope>,
}

impl MemoryTransport {
    pub fn new(capacity: usize) -> (Arc<Self>, mpsc::Receiver<GatewayEnvelope>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (Arc::new(Self { tx }), rx)
    }
}

impl GatewayTransport for MemoryTransport {
    fn send<'a>(
        &'a self,
        envelope: &'a GatewayEnvelope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), EnvelopeError>> + Send + 'a>>
    {
        Box::pin(async move {
            envelope.validate()?;
            self.tx
                .try_send(envelope.clone())
                .map_err(|_| EnvelopeError::Bounds)
        })
    }
}
