//! Transport-neutral A2A-over-Matrix gateway contracts.
//!
//! Matrix SDK and optional transports remain outside these types. SQLite is the
//! authority; this module only validates the bounded envelope and its phases.

use crate::matrix::InboundMatrixEvent;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("gateway storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    #[error("gateway sqlite error: {0}")]
    Query(String),
}
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use uuid::Uuid;

pub const PROFILE: &str = "a2a-matrix/1";
pub const MAX_ENVELOPE_BYTES: usize = 512 * 1024;
pub const MAX_INLINE_BYTES: usize = 256 * 1024;
pub const RETAINED_BYTES_HIGH: i64 = 64 * 1024 * 1024;
pub const RETAINED_BYTES_LOW: i64 = 48 * 1024 * 1024;

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

pub type GatewaySendFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<String, crate::matrix::ReplyError>> + Send + 'a>,
>;

/// Gateway-private Matrix sender; ordinary replies cannot use this namespaced path.
pub trait GatewayMatrixSender: Send + Sync {
    fn send_gateway<'a>(
        &'a self,
        room_id: &'a str,
        content: &'a serde_json::Value,
        txn_id: &'a str,
    ) -> GatewaySendFuture<'a>;
}

#[derive(Clone)]
pub struct GatewayStore {
    owner: crate::storage::BusinessStore,
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
    pub fn new_owner(owner: crate::storage::BusinessStore) -> Self {
        Self { owner }
    }

    pub async fn insert_envelope(
        &self,
        envelope: &GatewayEnvelope,
        canonical_json: &[u8],
        retained_bytes: i64,
    ) -> Result<(), GatewayError> {
        let envelope_id = envelope.envelope_id.to_string();
        let version = envelope.version.clone();
        let direction = match envelope.direction {
            Direction::Inbound => "inbound",
            Direction::Outbound => "outbound",
        }
        .to_owned();
        let peer_id = envelope.peer_id.clone();
        let sender_user_id = envelope.sender.peer_id.clone();
        let idempotency_key = envelope.idempotency_key.clone();
        let sender_endpoint = envelope.sender.endpoint_id.to_string();
        let recipient_endpoint = envelope.recipient.endpoint_id.to_string();
        let conversation_id = envelope.conversation_id.to_string();
        let correlation_id = envelope.correlation_id.to_string();
        let kind = format!("{:?}", envelope.kind).to_lowercase();
        let canonical = canonical_json.to_vec();
        let payload_hash = envelope.payload_sha256.clone();
        let created_at = envelope.created_at.clone();
        self.owner.transaction(move |tx| {
            tx.execute("INSERT INTO gateway_envelopes (envelope_id,version,direction,peer_id,sender_user_id,idempotency_key,sender_endpoint,recipient_endpoint,conversation_id,correlation_id,kind,canonical_json,payload_sha256,created_at,route_generation,state,retained_bytes) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,0,'received',?15)", rusqlite::params![envelope_id, version, direction, peer_id, sender_user_id, idempotency_key, sender_endpoint, recipient_endpoint, conversation_id, correlation_id, kind, canonical, payload_hash, created_at, retained_bytes]).map_err(|error| GatewayError::Query(error.to_string()))?;
            Ok(())
        }).map_err(GatewayError::from)
    }

    #[allow(dead_code)]
    async fn insert_envelope_sqlx_legacy(
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
    #[allow(dead_code)]
    async fn admit_inbound(
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
        let fenced = sqlx::query("UPDATE runtime_instances SET heartbeat_at=heartbeat_at WHERE instance_token=? AND state='active'")
            .bind(runtime_instance).execute(&mut *tx).await?.rows_affected() == 1;
        if !fenced {
            tx.rollback().await?;
            return Err(sqlx::Error::Protocol(
                "gateway runtime is not active".into(),
            ));
        }
        let task_id = envelope.correlation_id.to_string();
        let existing: Option<(String, String)> = sqlx::query_as("SELECT internal_task_id,payload_sha256 FROM gateway_envelopes WHERE peer_id=? AND sender_user_id=? AND direction='inbound' AND idempotency_key=?")
            .bind(&envelope.peer_id).bind(sender_user).bind(&envelope.idempotency_key).fetch_optional(&mut *tx).await?;
        if let Some(existing) = existing {
            if existing.0 != task_id || existing.1 != envelope.payload_sha256 {
                tx.rollback().await?;
                return Err(sqlx::Error::Protocol(
                    "gateway idempotency collision".into(),
                ));
            }
            let siblings: (i64, i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM gateway_deliveries WHERE envelope_id=? AND direction='inbound' AND event_id IS NOT NULL), (SELECT COUNT(*) FROM tasks WHERE task_id=? AND root_task_id=?), (SELECT COUNT(*) FROM task_events WHERE task_id=? AND seq=1 AND status='queued' AND event_id=?), (SELECT COUNT(*) FROM task_admissions WHERE task_id=? AND state IN ('ready','enqueued') AND runtime_instance=?)")
                .bind(envelope.envelope_id.to_string()).bind(&task_id).bind(&task_id).bind(&task_id).bind(envelope.envelope_id.to_string()).bind(&task_id).bind(runtime_instance).fetch_one(&mut *tx).await?;
            if siblings != (1, 1, 1, 1) {
                tx.rollback().await?;
                return Err(sqlx::Error::Protocol("gateway sibling corruption".into()));
            }
            tx.commit().await?;
            return Ok(TaskWinner::Replay(task_id));
        }
        sqlx::query("INSERT INTO tasks (task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,deadline,version) VALUES (?,?,?,?,?,?,?,?,?,?,0)")
            .bind(&task_id).bind(&task_id).bind(envelope.sender.endpoint_id.to_string())
            .bind(envelope.recipient.endpoint_id.to_string()).bind(conversation_id)
            .bind(envelope.payload.as_ref().map(|p| String::from_utf8_lossy(p).to_string()).unwrap_or_default())
            .bind(5_i64).bind(0_i64).bind(0_i64).bind(envelope.deadline.clone())
            .execute(&mut *tx).await?;
        let queued = serde_json::to_string(&crate::models::TaskEventPayload::Queued)
            .map_err(|_| sqlx::Error::Protocol("queued event encode".into()))?;
        sqlx::query("INSERT INTO task_events (event_id,task_id,seq,status,timestamp,payload) VALUES (?,?,?,?,?,json(?))")
            .bind(envelope.envelope_id.to_string()).bind(&task_id).bind(1_i64).bind("queued").bind(now).bind(queued)
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO task_admissions (task_id,state,revision,runtime_instance,created_at,updated_at) VALUES (?,'ready',0,?,?,?)")
            .bind(&task_id).bind(runtime_instance).bind(now).bind(now).execute(&mut *tx).await?;
        let canonical = serde_json::to_vec(envelope)
            .map_err(|_| sqlx::Error::Protocol("envelope encode".into()))?;
        let retained: i64 =
            sqlx::query_scalar("SELECT COALESCE(SUM(retained_bytes),0) FROM gateway_envelopes")
                .fetch_one(&mut *tx)
                .await?;
        if retained.saturating_add(canonical.len() as i64) >= RETAINED_BYTES_HIGH {
            tx.rollback().await?;
            return Err(sqlx::Error::Protocol("gateway storage pressure".into()));
        }
        sqlx::query("INSERT INTO gateway_envelopes (envelope_id,version,direction,peer_id,sender_user_id,idempotency_key,sender_endpoint,recipient_endpoint,conversation_id,correlation_id,internal_task_id,kind,canonical_json,payload_sha256,created_at,route_generation,state,retained_bytes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(envelope.envelope_id.to_string()).bind(&envelope.version).bind("inbound").bind(&envelope.peer_id).bind(sender_user).bind(&envelope.idempotency_key)
            .bind(envelope.sender.endpoint_id.to_string()).bind(envelope.recipient.endpoint_id.to_string()).bind(envelope.conversation_id.to_string()).bind(envelope.correlation_id.to_string()).bind(&task_id)
            .bind(format!("{:?}", envelope.kind).to_lowercase()).bind(&canonical).bind(&envelope.payload_sha256).bind(now).bind(route_generation as i64).bind("received").bind(canonical.len() as i64)
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO gateway_deliveries (envelope_id,direction,transport,phase,attempt,event_id,room_id,thread_root) VALUES (?,'inbound','matrix','transport_acked',0,?,?,?)")
            .bind(envelope.envelope_id.to_string()).bind(event_id).bind(room_id).bind(thread_root).execute(&mut *tx).await?;
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

    pub async fn reserve_outbound(
        &self,
        envelope: &GatewayEnvelope,
        room_id: &str,
        thread_root: Option<&str>,
        txn_id: &str,
        generation: u64,
        owner: &str,
    ) -> Result<bool, sqlx::Error> {
        let canonical = serde_json::to_vec(envelope)
            .map_err(|_| sqlx::Error::Protocol("envelope encode".into()))?;
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query("INSERT OR IGNORE INTO gateway_envelopes(envelope_id,version,direction,peer_id,sender_user_id,idempotency_key,sender_endpoint,recipient_endpoint,conversation_id,correlation_id,kind,canonical_json,payload_sha256,created_at,deadline,route_generation,state,owner_runtime,retained_bytes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(envelope.envelope_id.to_string()).bind(&envelope.version).bind("outbound")
            .bind(&envelope.peer_id).bind(&envelope.sender.peer_id).bind(&envelope.idempotency_key)
            .bind(envelope.sender.endpoint_id.to_string()).bind(envelope.recipient.endpoint_id.to_string())
            .bind(envelope.conversation_id.to_string()).bind(envelope.correlation_id.to_string())
            .bind(format!("{:?}", envelope.kind).to_lowercase()).bind(&canonical).bind(&envelope.payload_sha256)
            .bind(&envelope.created_at).bind(&envelope.deadline).bind(generation as i64).bind("received").bind(owner).bind(canonical.len() as i64)
            .execute(&mut *tx).await?.rows_affected() == 1;
        if inserted {
            sqlx::query("INSERT INTO gateway_deliveries(envelope_id,direction,transport,phase,attempt,txn_id,room_id,thread_root,owner_runtime,owner_revision) VALUES (?,'outbound','matrix','pending',0,?,?,?, ?,0)")
                .bind(envelope.envelope_id.to_string()).bind(txn_id).bind(room_id).bind(thread_root).bind(owner)
                .execute(&mut *tx).await?;
        } else {
            let valid: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM gateway_envelopes e JOIN gateway_deliveries d ON d.envelope_id=e.envelope_id WHERE e.envelope_id=? AND e.canonical_json=? AND d.direction='outbound' AND d.transport='matrix' AND d.txn_id=? AND d.room_id=?")
                .bind(envelope.envelope_id.to_string()).bind(&canonical).bind(txn_id).bind(room_id)
                .fetch_one(&mut *tx).await?;
            if valid != 1 {
                tx.rollback().await?;
                return Err(sqlx::Error::Protocol("gateway outbound collision".into()));
            }
        }
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn outbound_state(
        &self,
        envelope_id: &str,
        owner: &str,
    ) -> Result<(String, i64, i64), sqlx::Error> {
        sqlx::query_as("SELECT phase,attempt,owner_revision FROM gateway_deliveries WHERE envelope_id=? AND direction='outbound' AND owner_runtime=?")
            .bind(envelope_id).bind(owner).fetch_one(&self.pool).await
    }

    pub async fn begin_outbound_attempt(
        &self,
        envelope_id: &str,
        owner: &str,
        revision: i64,
        expected_phase: &str,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("UPDATE gateway_deliveries SET attempt=attempt+1,owner_revision=owner_revision+1 WHERE envelope_id=? AND direction='outbound' AND owner_runtime=? AND owner_revision=? AND phase=? AND attempt<2")
            .bind(envelope_id).bind(owner).bind(revision).bind(expected_phase).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn finish_outbound(
        &self,
        envelope_id: &str,
        owner: &str,
        revision: i64,
        phase: &str,
        event_id: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("UPDATE gateway_deliveries SET phase=?,event_id=COALESCE(?,event_id),owner_revision=owner_revision+1 WHERE envelope_id=? AND direction='outbound' AND owner_runtime=? AND owner_revision=?")
            .bind(phase).bind(event_id).bind(envelope_id).bind(owner).bind(revision).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn enqueue_ready(
        &self,
        task_id: &str,
        runtime: &str,
        revision: i64,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("UPDATE task_admissions SET state='enqueued', revision=revision+1, updated_at=datetime('now') WHERE task_id=? AND state='ready' AND runtime_instance=? AND revision=?")
            .bind(task_id).bind(runtime).bind(revision).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn restore_ready(
        &self,
        task_id: &str,
        runtime: &str,
        revision: i64,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("UPDATE task_admissions SET state='ready', revision=revision+1, updated_at=datetime('now') WHERE task_id=? AND state='enqueued' AND runtime_instance=? AND revision=?")
            .bind(task_id).bind(runtime).bind(revision).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn ready_revision(
        &self,
        task_id: &str,
        runtime: &str,
    ) -> Result<Option<i64>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT revision FROM task_admissions WHERE task_id=? AND state='ready' AND runtime_instance=?",
        )
        .bind(task_id)
        .bind(runtime)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn retained_bytes(&self) -> Result<i64, GatewayError> {
        self.owner
            .execute(|connection| {
                connection
                    .query_row(
                        "SELECT COALESCE(SUM(retained_bytes),0) FROM gateway_envelopes",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| GatewayError::Query(error.to_string()))
            })
            .map_err(GatewayError::from)
    }

    pub async fn pressure_ok(&self, high: i64) -> Result<bool, GatewayError> {
        Ok(self.retained_bytes().await? < high)
    }

    pub async fn claim_cleanup(
        &self,
        owner: &str,
        now: &str,
        limit: i64,
    ) -> Result<u64, GatewayError> {
        let owner_name = owner.to_owned();
        let now_value = now.to_owned();
        self.owner.transaction(move |tx| {
            let result = tx.execute("UPDATE gateway_envelopes SET cleanup_owner=?1, cleanup_revision=cleanup_revision+1, cleanup_claimed_at=?2 WHERE envelope_id IN (SELECT envelope_id FROM gateway_envelopes WHERE state IN ('terminal','stale') AND terminal_at IS NOT NULL AND terminal_at <= datetime(?2, '-7 days') AND (cleanup_owner IS NULL OR cleanup_owner=?1 OR (cleanup_claimed_at <= datetime(?2, '-900 seconds') AND EXISTS (SELECT 1 FROM runtime_instances r WHERE r.instance_token=cleanup_owner AND r.state='stopped'))) LIMIT ?3) AND (cleanup_owner IS NULL OR cleanup_owner=?1 OR (cleanup_claimed_at <= datetime(?2, '-900 seconds') AND EXISTS (SELECT 1 FROM runtime_instances r WHERE r.instance_token=cleanup_owner AND r.state='stopped')))", rusqlite::params![owner_name, now_value, limit]).map_err(|error| GatewayError::Query(error.to_string()))?;
            Ok(result as u64)
        }).map_err(GatewayError::from)
    }

    pub async fn delete_cleanup(
        &self,
        envelope_id: &str,
        owner: &str,
        revision: i64,
        now: &str,
    ) -> Result<bool, GatewayError> {
        let envelope_id = envelope_id.to_owned();
        let owner = owner.to_owned();
        let now = now.to_owned();
        self.owner.transaction(move |tx| {
            let eligible: Option<i64> = tx.query_row("SELECT 1 FROM gateway_envelopes e WHERE e.envelope_id=?1 AND e.cleanup_owner=?2 AND e.cleanup_revision=?3 AND e.state IN ('terminal','stale') AND e.terminal_at IS NOT NULL AND e.terminal_at <= datetime(?4, '-7 days') AND NOT EXISTS (SELECT 1 FROM gateway_deliveries d WHERE d.envelope_id=e.envelope_id AND d.phase NOT IN ('terminal','stale','recovery_needed')) AND (e.internal_task_id IS NULL OR EXISTS (SELECT 1 FROM task_events te WHERE te.task_id=e.internal_task_id AND te.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=e.internal_task_id) AND te.status IN ('completed','failed','timed_out','cancelled')))", rusqlite::params![envelope_id, owner, revision, now], |row| row.get(0)).optional().map_err(|error| GatewayError::Query(error.to_string()))?;
            if eligible.is_none() { return Ok(false); }
            let changed = tx.execute("DELETE FROM gateway_envelopes WHERE envelope_id=?1 AND cleanup_owner=?2 AND cleanup_revision=?3", rusqlite::params![envelope_id, owner, revision]).map_err(|error| GatewayError::Query(error.to_string()))?;
            Ok(changed == 1)
        }).map_err(GatewayError::from)
    }

    pub async fn validate_retained_bytes(&self) -> Result<bool, sqlx::Error> {
        let mismatch: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM gateway_envelopes e WHERE e.retained_bytes != length(e.canonical_json) + COALESCE((SELECT SUM(a.byte_len) FROM gateway_artifacts a WHERE a.envelope_id=e.envelope_id),0)")
                .fetch_one(&self.pool)
                .await?;
        Ok(mismatch == 0 && self.retained_bytes().await? < RETAINED_BYTES_HIGH)
    }

    pub async fn drain_cleanup(
        &self,
        owner: &str,
        now: &str,
        limit: i64,
    ) -> Result<usize, sqlx::Error> {
        self.claim_cleanup(owner, now, limit).await?;
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT envelope_id,cleanup_revision FROM gateway_envelopes WHERE cleanup_owner=? ORDER BY terminal_at LIMIT ?",
        )
        .bind(owner)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut deleted = 0;
        for (envelope_id, revision) in rows {
            if self
                .delete_cleanup(&envelope_id, owner, revision, now)
                .await?
            {
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

pub struct GatewayCleanupHandle {
    shutdown: watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}

impl GatewayCleanupHandle {
    pub fn start(store: GatewayStore, owner: String) -> Self {
        let (shutdown, mut receiver) = watch::channel(false);
        let join = tokio::spawn(async move {
            loop {
                let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
                let _ = store.drain_cleanup(&owner, &now, 100).await;
                tokio::select! {
                    changed = receiver.changed() => { let _ = changed; break; }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
                }
            }
        });
        Self { shutdown, join }
    }

    pub async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        let _ = self.shutdown.send(true);
        self.join.await
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
    sender: std::sync::Arc<dyn GatewayMatrixSender>,
    route: MatrixRoute,
}

pub struct GatewayRawConsumer {
    gateway: Arc<MatrixGateway>,
    store: GatewayStore,
    repository: crate::storage::SqliteRepository,
    handoff: crate::app::gateway_handoff::GatewayHandoff,
}

impl GatewayRawConsumer {
    pub fn new(
        gateway: Arc<MatrixGateway>,
        store: GatewayStore,
        repository: crate::storage::SqliteRepository,
        handoff: crate::app::gateway_handoff::GatewayHandoff,
    ) -> Self {
        Self {
            gateway,
            store,
            repository,
            handoff,
        }
    }
}

impl crate::matrix::RawMatrixEventConsumer for GatewayRawConsumer {
    fn consume<'a>(
        &'a self,
        raw: &'a str,
        room_id: &'a str,
    ) -> crate::matrix::SyncTokenFuture<'a, Result<bool, crate::matrix::MatrixError>> {
        Box::pin(async move {
            let value: serde_json::Value =
                serde_json::from_str(raw).map_err(|_| crate::matrix::MatrixError::Protocol {
                    detail: "malformed gateway event",
                })?;
            let content = value.get("content").and_then(|v| v.as_object());
            if content
                .and_then(|v| v.get("msgtype"))
                .and_then(|v| v.as_str())
                != Some("com.guigu.bridge.a2a.v1")
            {
                return Ok(false);
            }
            let sender = value.get("sender").and_then(|v| v.as_str()).ok_or(
                crate::matrix::MatrixError::Protocol {
                    detail: "gateway sender missing",
                },
            )?;
            if sender == self.gateway.route.own_user || room_id != self.gateway.route.room_id {
                return Ok(true);
            }
            let event_id = value.get("event_id").and_then(|v| v.as_str()).ok_or(
                crate::matrix::MatrixError::Protocol {
                    detail: "gateway event id missing",
                },
            )?;
            let thread = content
                .and_then(|v| v.get("m.relates_to"))
                .and_then(|v| v.get("event_id"))
                .and_then(|v| v.as_str());
            let winner = self
                .gateway
                .admit_raw(&self.store, raw, event_id, sender, thread)
                .await
                .map_err(|_| crate::matrix::MatrixError::Storage)?;
            let Some((winner, envelope)) = winner else {
                return Ok(false);
            };
            let task_id = match winner {
                TaskWinner::Inserted(task_id) | TaskWinner::Replay(task_id) => task_id,
            };
            let mut handed_off = false;
            if let Some(revision) = self
                .store
                .ready_revision(&task_id, &self.gateway.route.runtime_instance)
                .await
                .map_err(|_| crate::matrix::MatrixError::Storage)?
            {
                use crate::storage::Repository;
                let parsed = task_id
                    .parse()
                    .map_err(|_| crate::matrix::MatrixError::Storage)?;
                let task = self
                    .repository
                    .get_task(parsed)
                    .await
                    .map_err(|_| crate::matrix::MatrixError::Storage)?
                    .ok_or(crate::matrix::MatrixError::Storage)?;
                self.handoff
                    .handoff(task, revision)
                    .await
                    .map_err(|_| crate::matrix::MatrixError::Backpressure)?;
                handed_off = true;
            }
            if handed_off {
                self.gateway
                    .send_acceptance(&self.store, &envelope, thread)
                    .await
                    .map_err(|_| crate::matrix::MatrixError::Storage)?;
            }
            Ok(true)
        })
    }
}

impl MatrixGateway {
    pub fn new(sender: std::sync::Arc<dyn GatewayMatrixSender>, route: MatrixRoute) -> Self {
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
        thread_root: Option<String>,
        txn_id: &str,
    ) -> Result<String, crate::matrix::ReplyError> {
        let event = Self::encode_event(envelope, thread_root.as_deref())
            .map_err(|_| crate::matrix::ReplyError)?;
        let content = event.get("content").ok_or(crate::matrix::ReplyError)?;
        self.sender
            .send_gateway(&self.route.room_id, content, txn_id)
            .await
    }

    async fn send_acceptance(
        &self,
        store: &GatewayStore,
        inbound: &GatewayEnvelope,
        thread_root: Option<&str>,
    ) -> Result<(), EnvelopeError> {
        let envelope_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, inbound.envelope_id.as_bytes());
        let txn_id = format!("gateway-{envelope_id}");
        let envelope = GatewayEnvelope {
            version: PROFILE.into(),
            envelope_id,
            idempotency_key: format!("acceptance:{}", inbound.envelope_id),
            direction: Direction::Outbound,
            peer_id: inbound.peer_id.clone(),
            sender: inbound.recipient.clone(),
            recipient: inbound.sender.clone(),
            conversation_id: inbound.conversation_id,
            correlation_id: inbound.correlation_id,
            causal_seq: inbound.causal_seq.saturating_add(1),
            created_at: inbound.created_at.clone(),
            deadline: None,
            kind: EnvelopeKind::Acceptance,
            content_type: "application/json".into(),
            payload: None,
            payload_sha256: "0".repeat(64),
            artifact: None,
            integrity: Integrity {
                algorithm: "sha256".into(),
                digest: "0".repeat(64),
            },
        };
        store
            .reserve_outbound(
                &envelope,
                &self.route.room_id,
                thread_root,
                &txn_id,
                self.route.generation,
                &self.route.runtime_instance,
            )
            .await
            .map_err(|_| EnvelopeError::Shape)?;
        let (phase, attempt, revision) = store
            .outbound_state(&envelope_id.to_string(), &self.route.runtime_instance)
            .await
            .map_err(|_| EnvelopeError::Shape)?;
        if matches!(
            phase.as_str(),
            "transport_acked" | "task_accepted" | "terminal" | "recovery_needed"
        ) {
            return Ok(());
        }
        let expected = if phase == "pending" && attempt == 0 {
            "pending"
        } else if phase == "send_unknown" && attempt == 1 {
            "send_unknown"
        } else {
            return Err(EnvelopeError::Shape);
        };
        if !store
            .begin_outbound_attempt(
                &envelope_id.to_string(),
                &self.route.runtime_instance,
                revision,
                expected,
            )
            .await
            .map_err(|_| EnvelopeError::Shape)?
        {
            return Err(EnvelopeError::Shape);
        }
        let attempt_revision = revision + 1;
        match self
            .send(&envelope, thread_root.map(str::to_owned), &txn_id)
            .await
        {
            Ok(event_id) => {
                store
                    .finish_outbound(
                        &envelope_id.to_string(),
                        &self.route.runtime_instance,
                        attempt_revision,
                        "transport_acked",
                        Some(&event_id),
                    )
                    .await
                    .map_err(|_| EnvelopeError::Shape)?;
                Ok(())
            }
            Err(_) => {
                let failure_phase = if attempt == 0 {
                    "send_unknown"
                } else {
                    "recovery_needed"
                };
                store
                    .finish_outbound(
                        &envelope_id.to_string(),
                        &self.route.runtime_instance,
                        attempt_revision,
                        failure_phase,
                        None,
                    )
                    .await
                    .map_err(|_| EnvelopeError::Shape)?;
                if failure_phase == "send_unknown" {
                    Box::pin(self.send_acceptance(store, inbound, thread_root)).await
                } else {
                    Ok(())
                }
            }
        }
    }

    pub async fn admit_raw(
        &self,
        store: &GatewayStore,
        raw: &str,
        event_id: &str,
        sender_user: &str,
        thread_root: Option<&str>,
    ) -> Result<Option<(TaskWinner, GatewayEnvelope)>, EnvelopeError> {
        let Some(envelope) = self.decode_event(raw)? else {
            return Ok(None);
        };
        if !self
            .route
            .allowed_senders
            .iter()
            .any(|user| user == sender_user)
        {
            return Err(EnvelopeError::Peer);
        }
        if envelope.kind != EnvelopeKind::Request {
            return Err(EnvelopeError::Shape);
        }
        let winner = store
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
        Ok(Some((winner, envelope)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct TestDir(std::path::PathBuf);
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct StableSender {
        calls: Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl GatewayMatrixSender for StableSender {
        fn send_gateway<'a>(
            &'a self,
            _room_id: &'a str,
            content: &'a serde_json::Value,
            txn_id: &'a str,
        ) -> GatewaySendFuture<'a> {
            Box::pin(async move {
                let mut calls = self.calls.lock().unwrap();
                calls.push((txn_id.to_owned(), content.clone()));
                if calls.len() == 1 {
                    Err(crate::matrix::ReplyError)
                } else {
                    Ok("$accepted:test".into())
                }
            })
        }
    }

    async fn fixture() -> (TestDir, SqlitePool, GatewayEnvelope) {
        let path = std::env::temp_dir().join(format!("guigu-gateway-{}", Uuid::now_v7()));
        std::fs::create_dir(&path).unwrap();
        let dir = TestDir(path);
        let path = dir.0.join("gateway.db");
        let pool = crate::storage::connect(&path).await.unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        let sender = Uuid::from_u128(1);
        let recipient = Uuid::from_u128(2);
        for (id, name) in [(sender, "sender"), (recipient, "recipient")] {
            sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES (?,?,'acp',1,NULL,'[]')")
                .bind(id.to_string()).bind(name).execute(&pool).await.unwrap();
        }
        let conversation = Uuid::from_u128(3);
        sqlx::query("INSERT INTO conversations(conversation_id,transport,external_id,thread_ref,participants_json) VALUES (?,NULL,NULL,NULL,'[]')")
            .bind(conversation.to_string()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES ('runtime','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','active','test')")
            .execute(&pool).await.unwrap();
        let envelope = GatewayEnvelope {
            version: PROFILE.into(),
            envelope_id: Uuid::from_u128(4),
            idempotency_key: "key".into(),
            direction: Direction::Inbound,
            peer_id: "peer".into(),
            sender: EndpointRef {
                endpoint_id: sender,
                peer_id: "peer".into(),
            },
            recipient: EndpointRef {
                endpoint_id: recipient,
                peer_id: "peer".into(),
            },
            conversation_id: conversation,
            correlation_id: Uuid::from_u128(5),
            causal_seq: 1,
            created_at: "2026-01-01T00:00:00Z".into(),
            deadline: None,
            kind: EnvelopeKind::Request,
            content_type: "text/plain".into(),
            payload: Some(b"work".to_vec()),
            payload_sha256: "a".repeat(64),
            artifact: None,
            integrity: Integrity {
                algorithm: "sha256".into(),
                digest: "b".repeat(64),
            },
        };
        (dir, pool, envelope)
    }

    async fn admit(
        store: &GatewayStore,
        envelope: &GatewayEnvelope,
        conversation: &str,
    ) -> Result<TaskWinner, sqlx::Error> {
        store
            .admit_task_ready(
                envelope,
                conversation,
                "2026-01-01T00:00:00Z",
                "runtime",
                "@peer:test",
                "$event",
                "!gateway:test",
                None,
                1,
            )
            .await
    }

    #[tokio::test]
    async fn outbound_unknown_retries_once_with_same_transaction_and_content() {
        let (_dir, pool, envelope) = fixture().await;
        let sender = Arc::new(StableSender::default());
        let gateway = MatrixGateway::new(
            sender.clone(),
            MatrixRoute {
                room_id: "!gateway:test".into(),
                peer_id: "peer".into(),
                local_endpoint_id: envelope.recipient.endpoint_id.to_string(),
                remote_endpoint_id: envelope.sender.endpoint_id.to_string(),
                generation: 1,
                max_payload_bytes: MAX_INLINE_BYTES,
                deadline_seconds: 300,
                allowed_senders: vec!["@peer:test".into()],
                own_user: "@bridge:test".into(),
                runtime_instance: "runtime".into(),
            },
        );
        let store = GatewayStore::new(pool.clone());
        gateway
            .send_acceptance(&store, &envelope, Some("$root:test"))
            .await
            .unwrap();
        gateway
            .send_acceptance(&store, &envelope, Some("$root:test"))
            .await
            .unwrap();
        let calls = sender.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], calls[1]);
        let state: (String, i64, String) = sqlx::query_as(
            "SELECT phase,attempt,event_id FROM gateway_deliveries WHERE direction='outbound'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            state,
            ("transport_acked".into(), 2, "$accepted:test".into())
        );
        pool.close().await;
    }

    #[tokio::test]
    async fn raw_matrix_non_request_kinds_never_create_executable_tasks() {
        let (_dir, pool, request) = fixture().await;
        let gateway = MatrixGateway::new(
            Arc::new(StableSender::default()),
            MatrixRoute {
                room_id: "!gateway:test".into(),
                peer_id: "peer".into(),
                local_endpoint_id: request.recipient.endpoint_id.to_string(),
                remote_endpoint_id: request.sender.endpoint_id.to_string(),
                generation: 1,
                max_payload_bytes: MAX_INLINE_BYTES,
                deadline_seconds: 300,
                allowed_senders: vec!["@peer:test".into()],
                own_user: "@bridge:test".into(),
                runtime_instance: "runtime".into(),
            },
        );
        let store = GatewayStore::new(pool.clone());
        for (index, kind) in [
            EnvelopeKind::Ack,
            EnvelopeKind::Acceptance,
            EnvelopeKind::Status,
            EnvelopeKind::Cancel,
            EnvelopeKind::Artifact,
            EnvelopeKind::Error,
            EnvelopeKind::Retry,
        ]
        .into_iter()
        .enumerate()
        {
            let mut envelope = request.clone();
            envelope.envelope_id = Uuid::from_u128(100 + index as u128);
            envelope.idempotency_key = format!("non-request-{index}");
            envelope.kind = kind;
            envelope.payload = (kind == EnvelopeKind::Artifact).then(|| vec![1]);
            envelope.artifact = (kind == EnvelopeKind::Artifact).then(|| ArtifactMeta {
                artifact_id: Uuid::from_u128(200 + index as u128),
                media_type: "application/octet-stream".into(),
                byte_len: 1,
                sha256: "c".repeat(64),
            });
            let event_id = format!("$event-{index}:test");
            let raw = serde_json::json!({
                "type": "m.room.message",
                "event_id": event_id,
                "sender": "@peer:test",
                "content": {
                    "msgtype": "com.guigu.bridge.a2a.v1",
                    "body": PROFILE,
                    "com.guigu.bridge.a2a.v1": {"envelope": envelope}
                }
            })
            .to_string();
            assert_eq!(
                gateway
                    .admit_raw(&store, &raw, &event_id, "@peer:test", None)
                    .await,
                Err(EnvelopeError::Shape),
                "{kind:?} must not enter task admission"
            );
        }
        for table in [
            "gateway_envelopes",
            "gateway_deliveries",
            "tasks",
            "task_events",
            "task_admissions",
        ] {
            let sql = format!("SELECT COUNT(*) FROM {table}");
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&sql)
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                0,
                "{table} must remain empty"
            );
        }
        pool.close().await;
    }

    #[tokio::test]
    async fn real_sqlite_admission_replays_one_complete_winner() {
        let (_dir, pool, envelope) = fixture().await;
        let store = GatewayStore::new(pool.clone());
        let conversation = envelope.conversation_id.to_string();
        assert!(matches!(
            admit(&store, &envelope, &conversation).await.unwrap(),
            TaskWinner::Inserted(_)
        ));
        assert!(matches!(
            admit(&store, &envelope, &conversation).await.unwrap(),
            TaskWinner::Replay(_)
        ));
        for table in [
            "gateway_envelopes",
            "gateway_deliveries",
            "tasks",
            "task_events",
            "task_admissions",
        ] {
            let sql = format!("SELECT COUNT(*) FROM {table}");
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&sql)
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                1
            );
        }
    }

    #[tokio::test]
    async fn collision_and_fk_failure_leave_no_partial_siblings() {
        let (_dir, pool, envelope) = fixture().await;
        let store = GatewayStore::new(pool.clone());
        let conversation = envelope.conversation_id.to_string();
        admit(&store, &envelope, &conversation).await.unwrap();
        let mut collision = envelope.clone();
        collision.payload_sha256 = "c".repeat(64);
        assert!(admit(&store, &collision, &conversation).await.is_err());
        let mut invalid = envelope.clone();
        invalid.envelope_id = Uuid::from_u128(6);
        invalid.idempotency_key = "invalid".into();
        invalid.correlation_id = Uuid::from_u128(7);
        assert!(
            admit(&store, &invalid, &Uuid::from_u128(99).to_string())
                .await
                .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM gateway_envelopes")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tasks")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn two_connections_observe_one_scoped_winner() {
        let (_dir, pool, envelope) = fixture().await;
        let a = GatewayStore::new(pool.clone());
        let b = GatewayStore::new(pool.clone());
        let conversation = envelope.conversation_id.to_string();
        let (left, right) = tokio::join!(
            admit(&a, &envelope, &conversation),
            admit(&b, &envelope, &conversation)
        );
        let results = [left.unwrap(), right.unwrap()];
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, TaskWinner::Inserted(_)))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, TaskWinner::Replay(_)))
                .count(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tasks")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
    }
}
