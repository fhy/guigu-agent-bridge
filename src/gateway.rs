//! Transport-neutral A2A-over-Matrix gateway contracts.
//!
//! Matrix SDK and optional transports remain outside these types. SQLite is the
//! authority; this module only validates the bounded envelope and its phases.

use crate::matrix::{InboundMatrixEvent, MatrixSender, ReplyContext};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundAdmission {
    Inserted { envelope_id: String },
    Replay { envelope_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskWinner {
    Inserted(String),
    Replay(String),
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

    #[allow(clippy::too_many_arguments)]
    pub async fn admit_inbound(
        &self,
        envelope: &GatewayEnvelope,
        canonical_json: &[u8],
        sender_user: &str,
        event_id: &str,
        room_id: &str,
        thread_root: Option<&str>,
        route_generation: u64,
    ) -> Result<InboundAdmission, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query("INSERT OR IGNORE INTO gateway_envelopes (envelope_id,version,direction,peer_id,sender_user_id,idempotency_key,sender_endpoint,recipient_endpoint,conversation_id,correlation_id,kind,canonical_json,payload_sha256,created_at,route_generation,state,retained_bytes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(envelope.envelope_id.to_string()).bind(&envelope.version).bind("inbound")
            .bind(&envelope.peer_id).bind(sender_user).bind(&envelope.idempotency_key)
            .bind(envelope.sender.endpoint_id.to_string()).bind(envelope.recipient.endpoint_id.to_string())
            .bind(envelope.conversation_id.to_string()).bind(envelope.correlation_id.to_string())
            .bind(format!("{:?}", envelope.kind).to_lowercase()).bind(canonical_json).bind(&envelope.payload_sha256)
            .bind(&envelope.created_at).bind(route_generation as i64).bind("received").bind(canonical_json.len() as i64)
            .execute(&mut *tx).await?.rows_affected() == 1;
        let winner: String = sqlx::query_scalar("SELECT envelope_id FROM gateway_envelopes WHERE peer_id=? AND sender_user_id=? AND direction='inbound' AND idempotency_key=?")
            .bind(&envelope.peer_id).bind(sender_user).bind(&envelope.idempotency_key).fetch_one(&mut *tx).await?;
        if inserted {
            sqlx::query("INSERT INTO gateway_deliveries (envelope_id,direction,transport,phase,attempt,event_id,room_id,thread_root) VALUES (?,'inbound','matrix','transport_acked',0,?,?,?)")
                .bind(&winner).bind(event_id).bind(room_id).bind(thread_root).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(if inserted {
            InboundAdmission::Inserted {
                envelope_id: winner,
            }
        } else {
            InboundAdmission::Replay {
                envelope_id: winner,
            }
        })
    }

    /// Gateway-private admission: task, queued event, receipt and ready lease
    /// are committed before the caller attempts an in-memory enqueue.
    #[allow(clippy::too_many_arguments)]
    pub async fn admit_task_ready(
        &self,
        envelope: &GatewayEnvelope,
        conversation_id: &str,
        now: &str,
        runtime_instance: &str,
        sender_user: &str,
        event_id: &str,
        room_id: &str,
        thread_root: Option<&str>,
        route_generation: u64,
    ) -> Result<TaskWinner, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let task_id = envelope.correlation_id.to_string();
        let canonical_len = serde_json::to_vec(envelope)
            .map_err(|_| sqlx::Error::Protocol("envelope encode".into()))?
            .len() as i64;
        let inserted = sqlx::query("INSERT OR IGNORE INTO gateway_envelopes (envelope_id,version,direction,peer_id,sender_user_id,idempotency_key,sender_endpoint,recipient_endpoint,conversation_id,correlation_id,internal_task_id,kind,canonical_json,payload_sha256,created_at,route_generation,state,retained_bytes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(envelope.envelope_id.to_string()).bind(&envelope.version).bind("inbound")
            .bind(&envelope.peer_id).bind(sender_user).bind(&envelope.idempotency_key)
            .bind(envelope.sender.endpoint_id.to_string()).bind(envelope.recipient.endpoint_id.to_string())
            .bind(envelope.conversation_id.to_string()).bind(envelope.correlation_id.to_string()).bind(&task_id)
            .bind(format!("{:?}", envelope.kind).to_lowercase()).bind(serde_json::to_vec(envelope).map_err(|_| sqlx::Error::Protocol("envelope encode".into()))?)
            .bind(&envelope.payload_sha256).bind(now).bind(route_generation as i64).bind("received").bind(canonical_len)
            .execute(&mut *tx).await?.rows_affected() == 1;
        if !inserted {
            let existing: (String, String) = sqlx::query_as("SELECT internal_task_id,payload_sha256 FROM gateway_envelopes WHERE peer_id=? AND sender_user_id=? AND direction='inbound' AND idempotency_key=?")
                .bind(&envelope.peer_id).bind(sender_user).bind(&envelope.idempotency_key).fetch_one(&mut *tx).await?;
            if existing.0 != task_id || existing.1 != envelope.payload_sha256 {
                tx.rollback().await?;
                return Err(sqlx::Error::Protocol(
                    "gateway idempotency collision".into(),
                ));
            }
            tx.commit().await?;
            return Ok(TaskWinner::Replay(task_id));
        }
        sqlx::query("INSERT INTO gateway_deliveries (envelope_id,direction,transport,phase,attempt,event_id,room_id,thread_root) VALUES (?,'inbound','matrix','transport_acked',0,?,?,?)")
            .bind(envelope.envelope_id.to_string()).bind(event_id).bind(room_id).bind(thread_root).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO tasks (task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,deadline,version) VALUES (?,?,?,?,?,?,?,?,?,?,0)")
            .bind(&task_id).bind(&task_id).bind(envelope.sender.endpoint_id.to_string())
            .bind(envelope.recipient.endpoint_id.to_string()).bind(conversation_id)
            .bind(envelope.payload.as_ref().map(|p| String::from_utf8_lossy(p).to_string()).unwrap_or_default())
            .bind(5_i64).bind(0_i64).bind(0_i64).bind(envelope.deadline.clone())
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO task_events (event_id,task_id,seq,status,timestamp,payload) VALUES (?,?,?,?,?,json(?))")
            .bind(envelope.envelope_id.to_string()).bind(&task_id).bind(1_i64).bind("queued").bind(now).bind("{\"gateway\":true}")
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO task_admissions (task_id,state,revision,runtime_instance,created_at,updated_at) VALUES (?,'ready',0,?,?,?)")
            .bind(&task_id).bind(runtime_instance).bind(now).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(TaskWinner::Inserted(task_id))
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

    pub async fn retained_bytes(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT COALESCE(SUM(retained_bytes),0) FROM gateway_envelopes")
            .fetch_one(&self.pool)
            .await
    }

    pub async fn pressure_ok(&self, high: i64) -> Result<bool, sqlx::Error> {
        Ok(self.retained_bytes().await? < high)
    }

    pub async fn claim_cleanup(
        &self,
        owner: &str,
        now: &str,
        limit: i64,
    ) -> Result<u64, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query("UPDATE gateway_envelopes SET cleanup_owner=?, cleanup_revision=cleanup_revision+1, cleanup_claimed_at=? WHERE envelope_id IN (SELECT envelope_id FROM gateway_envelopes WHERE state IN ('terminal','stale') AND terminal_at IS NOT NULL AND terminal_at <= datetime(?, '-7 days') AND (cleanup_claimed_at IS NULL OR cleanup_claimed_at <= datetime(?, '-900 seconds')) LIMIT ?) AND (cleanup_owner IS NULL OR cleanup_owner=? OR cleanup_claimed_at <= datetime(?, '-900 seconds'))")
            .bind(owner).bind(now).bind(now).bind(now).bind(limit).bind(owner).bind(now)
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(result.rows_affected())
    }

    pub async fn delete_cleanup(
        &self,
        envelope_id: &str,
        owner: &str,
        revision: i64,
        now: &str,
    ) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let eligible: Option<i64> = sqlx::query_scalar("SELECT 1 FROM gateway_envelopes e WHERE e.envelope_id=? AND e.cleanup_owner=? AND e.cleanup_revision=? AND e.state IN ('terminal','stale') AND e.terminal_at IS NOT NULL AND e.terminal_at <= datetime(?, '-7 days') AND NOT EXISTS (SELECT 1 FROM gateway_deliveries d WHERE d.envelope_id=e.envelope_id AND d.phase NOT IN ('terminal','stale','recovery_needed')) AND (e.internal_task_id IS NULL OR EXISTS (SELECT 1 FROM task_events te WHERE te.task_id=e.internal_task_id AND te.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=e.internal_task_id) AND te.payload_json LIKE '%completed%'))")
            .bind(envelope_id).bind(owner).bind(revision).bind(now).fetch_optional(&mut *tx).await?;
        if eligible.is_none() {
            tx.rollback().await?;
            return Ok(false);
        }
        let result = sqlx::query("DELETE FROM gateway_envelopes WHERE envelope_id=? AND cleanup_owner=? AND cleanup_revision=?")
            .bind(envelope_id).bind(owner).bind(revision).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn validate_retained_bytes(&self) -> Result<bool, sqlx::Error> {
        let mismatch: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM gateway_envelopes e WHERE e.retained_bytes != length(e.canonical_json) + COALESCE((SELECT SUM(a.byte_len) FROM gateway_artifacts a WHERE a.envelope_id=e.envelope_id),0)")
                .fetch_one(&self.pool)
                .await?;
        Ok(mismatch == 0)
    }
}

/// Deterministic bounded transport fake for contract and recovery tests.
pub struct MemoryTransport {
    tx: mpsc::Sender<GatewayEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatrixRoute {
    pub room_id: String,
    pub peer_id: String,
    pub local_endpoint_id: String,
    pub remote_endpoint_id: String,
    pub generation: u64,
    pub max_payload_bytes: usize,
    pub deadline_seconds: u64,
    pub allowed_senders: Vec<String>,
    pub own_user: String,
    pub runtime_instance: String,
}

pub struct MatrixGateway {
    sender: std::sync::Arc<dyn MatrixSender>,
    route: MatrixRoute,
}

impl MatrixGateway {
    pub fn new(sender: std::sync::Arc<dyn MatrixSender>, route: MatrixRoute) -> Self {
        Self { sender, route }
    }

    pub fn accept_event(&self, event: &InboundMatrixEvent) -> bool {
        !self.route.room_id.is_empty()
            && !self.route.local_endpoint_id.is_empty()
            && !self.route.remote_endpoint_id.is_empty()
            && uuid::Uuid::parse_str(&self.route.local_endpoint_id).is_ok()
            && uuid::Uuid::parse_str(&self.route.remote_endpoint_id).is_ok()
            && self.route.max_payload_bytes <= MAX_INLINE_BYTES
            && self.route.deadline_seconds > 0
            && event.body.len() <= self.route.max_payload_bytes
            && event.room_id == self.route.room_id
            && event.sender != self.route.own_user
            && self
                .route
                .allowed_senders
                .iter()
                .any(|s| s == &event.sender)
    }

    pub fn decode_event(&self, raw: &str) -> Result<Option<GatewayEnvelope>, EnvelopeError> {
        let value: serde_json::Value =
            serde_json::from_str(raw).map_err(|_| EnvelopeError::Shape)?;
        if value.get("type").and_then(|v| v.as_str()) != Some("m.room.message") {
            return Ok(None);
        }
        let content = value
            .get("content")
            .and_then(|v| v.as_object())
            .ok_or(EnvelopeError::Shape)?;
        if content.get("msgtype").and_then(|v| v.as_str()) != Some("com.guigu.bridge.a2a.v1") {
            return Ok(None);
        }
        let envelope: GatewayEnvelope = content
            .get("com.guigu.bridge.a2a.v1")
            .and_then(|v| v.get("envelope"))
            .cloned()
            .ok_or(EnvelopeError::Shape)
            .and_then(|v| serde_json::from_value(v).map_err(|_| EnvelopeError::Shape))?;
        envelope.validate()?;
        if envelope.peer_id != self.route.peer_id
            || envelope.sender.peer_id != self.route.peer_id
            || envelope.recipient.peer_id != self.route.peer_id
            || envelope.sender.endpoint_id.to_string() != self.route.remote_endpoint_id
            || envelope.recipient.endpoint_id.to_string() != self.route.local_endpoint_id
            || envelope.encoded_len().map_err(|_| EnvelopeError::Bounds)?
                > self.route.max_payload_bytes
        {
            return Err(EnvelopeError::Peer);
        }
        Ok(Some(envelope))
    }

    pub fn encode_event(
        envelope: &GatewayEnvelope,
        thread_root: Option<&str>,
    ) -> Result<serde_json::Value, serde_json::Error> {
        let mut content = serde_json::json!({
            "msgtype": "com.guigu.bridge.a2a.v1",
            "body": PROFILE,
            "com.guigu.bridge.a2a.v1": {"envelope": envelope},
        });
        if let Some(root) = thread_root {
            content["m.relates_to"] = serde_json::json!({"rel_type":"m.thread","event_id":root});
        }
        Ok(serde_json::json!({"type":"m.room.message","content":content}))
    }

    pub async fn send(
        &self,
        envelope: &GatewayEnvelope,
        event_id: &str,
        thread_root: Option<String>,
    ) -> Result<(), crate::matrix::ReplyError> {
        let body = serde_json::to_string(
            &Self::encode_event(envelope, thread_root.as_deref())
                .map_err(|_| crate::matrix::ReplyError)?,
        )
        .map_err(|_| crate::matrix::ReplyError)?;
        self.sender
            .send_reply(
                &ReplyContext {
                    room_id: self.route.room_id.clone(),
                    thread_root,
                    event_id: event_id.to_owned(),
                },
                &body,
            )
            .await
    }

    pub async fn admit_raw(
        &self,
        store: &GatewayStore,
        raw: &str,
        event_id: &str,
        sender_user: &str,
        thread_root: Option<&str>,
    ) -> Result<bool, EnvelopeError> {
        let Some(envelope) = self.decode_event(raw)? else {
            return Ok(false);
        };
        if !self
            .route
            .allowed_senders
            .iter()
            .any(|user| user == sender_user)
        {
            return Err(EnvelopeError::Peer);
        }
        let canonical = serde_json::to_vec(&envelope).map_err(|_| EnvelopeError::Shape)?;
        let _ = canonical;
        store
            .admit_task_ready(
                &envelope,
                &envelope.conversation_id.to_string(),
                &envelope.created_at,
                &self.route.runtime_instance,
                sender_user,
                event_id,
                &self.route.room_id,
                thread_root,
                self.route.generation,
            )
            .await
            .map_err(|_| EnvelopeError::Shape)?;
        Ok(true)
    }
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
