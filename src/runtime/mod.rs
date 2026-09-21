//! Durable runtime control for exclusive execution and bounded continuation.

use crate::{
    acp::{AcpDispatcher, TurnResult},
    bus::{
        BusFuture, DispatchError, DispatchOutcome, DispatchRequest, FinalizationCapability,
        FinalizeResult, LifecycleResult, PreparedExecution, TaskDispatcher, TaskLifecycle,
    },
    models::{DeliveryId, EndpointId, TaskId},
    storage::StorageError,
};
use chrono::{DateTime, Utc};
use sqlx::{Row, SqlitePool};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::Path,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

const RESOURCE_NAMESPACE: Uuid = Uuid::from_u128(0x8ce9c0d7_8257_5068_a369_b075e9d30e21);
const WORKSPACE_NAMESPACE: Uuid = Uuid::from_u128(0xf6af15e8_6e64_56df_851f_24589ba97713);
const TERMINAL_OUTBOX_NAMESPACE: Uuid = Uuid::from_u128(0x6c1ab2ee_4834_5f40_901b_0fbdbf95dbd0);
const OUTBOX_BODY_NAMESPACE: Uuid = Uuid::from_u128(0x7297ec04_1338_57b4_9bc4_f0ff9f84fa5a);
const CONTINUATION_RUNTIME_GENERATION: &str = "generation.v1:strict";
pub const MAX_CONTINUATION_PROMPT_BYTES: usize = 4096;
const MAX_OUTBOX_BODY_BYTES: usize = 2048;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkspaceId(Uuid);
impl WorkspaceId {
    pub fn from_canonical_path(path: impl AsRef<Path>) -> Result<Self, RuntimeError> {
        let path = std::fs::canonicalize(path).map_err(|_| RuntimeError::Workspace)?;
        if !path.is_dir() {
            return Err(RuntimeError::Workspace);
        }
        let text = path.to_str().ok_or(RuntimeError::Workspace)?;
        Ok(Self(Uuid::new_v5(&WORKSPACE_NAMESPACE, text.as_bytes())))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExecutionResourceKey(Uuid);
impl ExecutionResourceKey {
    pub fn new(endpoint: EndpointId, workspace: WorkspaceId) -> Self {
        Self(Uuid::new_v5(
            &RESOURCE_NAMESPACE,
            format!("execution-resource/v1\0{endpoint}\0{}", workspace.0).as_bytes(),
        ))
    }
}
impl std::fmt::Display for ExecutionResourceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("workspace identity is unavailable")]
    Workspace,
    #[error("runtime storage failed")]
    Storage(#[source] StorageError),
    #[error("runtime row is malformed")]
    Malformed,
    #[error("runtime owner was fenced")]
    Fenced,
    #[error("workspace claim is busy")]
    Busy,
    #[error("runtime lease expired")]
    Expired,
    #[error("continuation policy exhausted: {0}")]
    Exhausted(PolicyLimit),
    #[error("continuation result is unusable")]
    Continuation,
}
impl From<sqlx::Error> for RuntimeError {
    fn from(v: sqlx::Error) -> Self {
        Self::Storage(StorageError::from(v))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PolicyLimit {
    Turns,
    Deadline,
    Inactivity,
    NoProgress,
    OutputBytes,
}
impl std::fmt::Display for PolicyLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Turns => "turns",
            Self::Deadline => "deadline",
            Self::Inactivity => "inactivity",
            Self::NoProgress => "no_progress",
            Self::OutputBytes => "output_bytes",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub resource: ExecutionResourceKey,
    pub task_id: TaskId,
    owner: Uuid,
    pub fence: u64,
    pub expires_at: DateTime<Utc>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOutcome {
    Acquired(Lease),
    Busy,
    RecoveryNeeded,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseDisposition {
    Released,
    RecoveryNeeded,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuationState {
    Ready,
    InFlight,
    RecoveryNeeded,
    Blocked,
    Terminal,
}
impl ContinuationState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::InFlight => "in_flight",
            Self::RecoveryNeeded => "recovery_needed",
            Self::Blocked => "blocked",
            Self::Terminal => "terminal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Continuation {
    pub task_id: TaskId,
    pub resource: ExecutionResourceKey,
    pub delivery_id: DeliveryId,
    pub lease_fence: u64,
    pub revision: u64,
    pub state: ContinuationState,
    pub next_turn: u32,
    pub completed_turns: u32,
    pub consecutive_no_progress: u32,
    pub next_prompt: String,
    pub started_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
    pub last_progress_at: DateTime<Utc>,
    pub observed_output_bytes: u64,
    pub runtime_generation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeCounts {
    pub active_leases: u64,
    pub expired_leases: u64,
    pub recovery_needed_leases: u64,
    pub ready_turns: u64,
    pub in_flight_turns: u64,
    pub recovery_backlog: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoverySnapshot {
    pub ready: Vec<TaskId>,
    pub ambiguous: Vec<TaskId>,
}

#[derive(Clone)]
pub struct SqliteRuntimeStore {
    pool: SqlitePool,
}

fn path_components(path: &Path) -> Result<Vec<String>, RuntimeError> {
    let canonical = std::fs::canonicalize(path).map_err(|_| RuntimeError::Workspace)?;
    if !canonical.is_dir() {
        return Err(RuntimeError::Workspace);
    }
    canonical
        .components()
        .map(|component| {
            let text = component
                .as_os_str()
                .to_str()
                .ok_or(RuntimeError::Workspace)?;
            Ok(text.to_owned())
        })
        .collect()
}

fn overlaps(left: &[String], right: &[String]) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

impl SqliteRuntimeStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn acquire(
        &self,
        resource: ExecutionResourceKey,
        task_id: TaskId,
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<AcquireOutcome, RuntimeError> {
        let owner = Uuid::now_v7();
        let expires = add(now, ttl)?;
        let mut tx = match self.pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(value) => value,
            Err(error) if error.to_string().contains("database is locked") => {
                return Ok(AcquireOutcome::Busy);
            }
            Err(error) => return Err(error.into()),
        };
        let inserted = match sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,1,'active',?,?,?) ON CONFLICT(resource_key) DO NOTHING")
            .bind(resource.to_string()).bind(task_id.to_string()).bind(owner.to_string()).bind(ts(now)).bind(ts(now)).bind(ts(expires)).execute(&mut *tx).await {
            Ok(value) => value,
            Err(error) if error.to_string().contains("database is locked") => return Ok(AcquireOutcome::Busy),
            Err(error) => return Err(error.into()),
        };
        if inserted.rows_affected() == 1 {
            if let Err(error) = tx.commit().await {
                if error.to_string().contains("database is locked") {
                    return Ok(AcquireOutcome::Busy);
                }
                return Err(error.into());
            }
            return Ok(AcquireOutcome::Acquired(Lease {
                resource,
                task_id,
                owner,
                fence: 1,
                expires_at: expires,
            }));
        }
        let row = match sqlx::query(
            "SELECT fence,state,expires_at FROM execution_leases WHERE resource_key=?",
        )
        .bind(resource.to_string())
        .fetch_optional(&mut *tx)
        .await
        {
            Ok(value) => value,
            Err(error) if error.to_string().contains("database is locked") => {
                return Ok(AcquireOutcome::Busy);
            }
            Err(error) => return Err(error.into()),
        };
        let outcome = if let Some(row) = row {
            let state: String = row.try_get("state").map_err(|_| RuntimeError::Malformed)?;
            match state.as_str() {
                "active" => {
                    let stored: String = row
                        .try_get("expires_at")
                        .map_err(|_| RuntimeError::Malformed)?;
                    let expired: DateTime<Utc> =
                        stored.parse().map_err(|_| RuntimeError::Malformed)?;
                    if expired <= now {
                        let fence: i64 =
                            row.try_get("fence").map_err(|_| RuntimeError::Malformed)?;
                        if let Err(error) = sqlx::query("UPDATE execution_leases SET state='recovery_needed',heartbeat_at=? WHERE resource_key=? AND state='active' AND fence=?")
                            .bind(ts(now)).bind(resource.to_string()).bind(fence).execute(&mut *tx).await {
                            if error.to_string().contains("database is locked") { return Ok(AcquireOutcome::Busy); }
                            return Err(error.into());
                        }
                        AcquireOutcome::RecoveryNeeded
                    } else {
                        AcquireOutcome::Busy
                    }
                }
                "recovery_needed" => AcquireOutcome::RecoveryNeeded,
                "released" => {
                    let old: i64 = row.try_get("fence").map_err(|_| RuntimeError::Malformed)?;
                    let fence = u64::try_from(old)
                        .ok()
                        .and_then(|v| v.checked_add(1))
                        .ok_or(RuntimeError::Malformed)?;
                    let result=match sqlx::query("UPDATE execution_leases SET task_id=?,owner_token=?,fence=?,state='active',acquired_at=?,heartbeat_at=?,expires_at=? WHERE resource_key=? AND state='released' AND fence=?")
                        .bind(task_id.to_string()).bind(owner.to_string()).bind(to_i64(fence)?).bind(ts(now)).bind(ts(now)).bind(ts(expires)).bind(resource.to_string()).bind(old).execute(&mut *tx).await {
                        Ok(value) => value,
                        Err(error) if error.to_string().contains("database is locked") => return Ok(AcquireOutcome::Busy),
                        Err(error) => return Err(error.into()),
                    };
                    if result.rows_affected() != 1 {
                        return Err(RuntimeError::Fenced);
                    }
                    AcquireOutcome::Acquired(Lease {
                        resource,
                        task_id,
                        owner,
                        fence,
                        expires_at: expires,
                    })
                }
                _ => return Err(RuntimeError::Malformed),
            }
        } else {
            return Err(RuntimeError::Malformed);
        };
        if let Err(error) = tx.commit().await {
            if error.to_string().contains("database is locked") {
                return Ok(AcquireOutcome::Busy);
            }
            return Err(error.into());
        }
        Ok(outcome)
    }

    pub async fn renew(
        &self,
        lease: &Lease,
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<Lease, RuntimeError> {
        if now >= lease.expires_at {
            return Err(RuntimeError::Expired);
        }
        let expires = add(now, ttl)?;
        let result=sqlx::query("UPDATE execution_leases SET heartbeat_at=?,expires_at=? WHERE resource_key=? AND owner_token=? AND fence=? AND state='active' AND expires_at>?")
            .bind(ts(now)).bind(ts(expires)).bind(lease.resource.to_string()).bind(lease.owner.to_string()).bind(to_i64(lease.fence)?).bind(ts(now)).execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            return Err(RuntimeError::Fenced);
        }
        let mut next = lease.clone();
        next.expires_at = expires;
        Ok(next)
    }

    pub async fn claim_workspaces(
        &self,
        lease: &Lease,
        paths: &[std::path::PathBuf],
    ) -> Result<(), RuntimeError> {
        let mut candidates = Vec::with_capacity(paths.len());
        for path in paths {
            candidates.push((path.to_string_lossy().into_owned(), path_components(path)?));
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let rows = sqlx::query(
            "SELECT canonical_path,path_components_json FROM workspace_claims WHERE state='active'",
        )
        .fetch_all(&mut *tx)
        .await?;
        for (_, components) in &candidates {
            for row in &rows {
                let existing: String = row
                    .try_get("path_components_json")
                    .map_err(|_| RuntimeError::Malformed)?;
                let existing: Vec<String> =
                    serde_json::from_str(&existing).map_err(|_| RuntimeError::Malformed)?;
                if overlaps(components, &existing) {
                    return Err(RuntimeError::Busy);
                }
            }
        }
        for (path, components) in candidates {
            sqlx::query("INSERT INTO workspace_claims(claim_id,runtime_owner,task_id,canonical_path,path_components_json,owner_fence,revision,state,created_at,updated_at) VALUES(?,?,?,?,?,?,1,'active',?,?)")
                .bind(Uuid::now_v7().to_string()).bind(lease.owner.to_string()).bind(lease.task_id.to_string())
                .bind(path).bind(serde_json::to_string(&components).map_err(|_| RuntimeError::Malformed)?)
                .bind(to_i64(lease.fence)?).bind(ts(lease.expires_at)).bind(ts(lease.expires_at)).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn release(
        &self,
        lease: &Lease,
        disposition: ReleaseDisposition,
        now: DateTime<Utc>,
    ) -> Result<(), RuntimeError> {
        let state = match disposition {
            ReleaseDisposition::Released => "released",
            ReleaseDisposition::RecoveryNeeded => "recovery_needed",
        };
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let result=sqlx::query("UPDATE execution_leases SET state=?,heartbeat_at=?,expires_at=? WHERE resource_key=? AND owner_token=? AND fence=? AND state='active'")
            .bind(state).bind(ts(now)).bind(ts(now)).bind(lease.resource.to_string()).bind(lease.owner.to_string()).bind(to_i64(lease.fence)?).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(RuntimeError::Fenced);
        }
        if disposition == ReleaseDisposition::Released {
            sqlx::query("DELETE FROM workspace_claims WHERE runtime_owner=? AND task_id=? AND owner_fence=? AND state='active'")
                .bind(lease.owner.to_string()).bind(lease.task_id.to_string()).bind(to_i64(lease.fence)?).execute(&mut *tx).await?;
        } else {
            sqlx::query("UPDATE workspace_claims SET state='recovery_needed',revision=revision+1,updated_at=? WHERE runtime_owner=? AND task_id=? AND owner_fence=? AND state='active'")
                .bind(ts(now)).bind(lease.owner.to_string()).bind(lease.task_id.to_string()).bind(to_i64(lease.fence)?).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn begin_continuation(
        &self,
        lease: &Lease,
        delivery_id: DeliveryId,
        prompt: &str,
        now: DateTime<Utc>,
    ) -> Result<(), RuntimeError> {
        let prompt = bounded_prompt(prompt)?;
        let result=sqlx::query("INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes) SELECT ?,?,?,?,1,'ready',1,0,0,?,?,?,?,0 FROM execution_leases l WHERE l.resource_key=? AND l.task_id=? AND l.owner_token=? AND l.fence=? AND l.state='active' AND l.expires_at>?")
            .bind(lease.task_id.to_string()).bind(lease.resource.to_string()).bind(delivery_id.to_string()).bind(to_i64(lease.fence)?).bind(prompt).bind(ts(now)).bind(ts(now)).bind(ts(now)).bind(lease.resource.to_string()).bind(lease.task_id.to_string()).bind(lease.owner.to_string()).bind(to_i64(lease.fence)?).bind(ts(now)).execute(&self.pool).await?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(RuntimeError::Fenced)
        }
    }

    pub async fn continuation(
        &self,
        task_id: TaskId,
    ) -> Result<Option<Continuation>, RuntimeError> {
        sqlx::query("SELECT * FROM task_continuations WHERE task_id=?")
            .bind(task_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .map(|r| decode_continuation(&r))
            .transpose()
    }

    async fn ensure_runtime_generation(&self, task_id: TaskId) -> Result<(), RuntimeError> {
        let generation: Option<String> =
            sqlx::query_scalar("SELECT runtime_generation FROM task_continuations WHERE task_id=?")
                .bind(task_id.to_string())
                .fetch_optional(&self.pool)
                .await?;
        if generation.as_deref() == Some(CONTINUATION_RUNTIME_GENERATION) {
            Ok(())
        } else {
            Err(RuntimeError::Fenced)
        }
    }

    pub async fn claim_turn(
        &self,
        lease: &Lease,
        revision: u64,
        now: DateTime<Utc>,
    ) -> Result<Continuation, RuntimeError> {
        self.ensure_runtime_generation(lease.task_id).await?;
        let mut tx = self.pool.begin().await?;
        let result=sqlx::query("UPDATE task_continuations SET state='in_flight',revision=revision+1,heartbeat_at=? WHERE task_id=? AND resource_key=? AND lease_fence=? AND revision=? AND state='ready' AND EXISTS (SELECT 1 FROM execution_leases l WHERE l.resource_key=task_continuations.resource_key AND l.task_id=task_continuations.task_id AND l.owner_token=? AND l.fence=task_continuations.lease_fence AND l.state='active' AND l.expires_at>?)")
            .bind(ts(now)).bind(lease.task_id.to_string()).bind(lease.resource.to_string()).bind(to_i64(lease.fence)?).bind(to_i64(revision)?).bind(lease.owner.to_string()).bind(ts(now)).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(RuntimeError::Fenced);
        }
        let row = sqlx::query("SELECT * FROM task_continuations WHERE task_id=?")
            .bind(lease.task_id.to_string())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(RuntimeError::Malformed)?;
        let continuation = decode_continuation(&row)?;
        tx.commit().await?;
        Ok(continuation)
    }

    pub async fn claim_turn_with_generation(
        &self,
        lease: &Lease,
        revision: u64,
        generation: &str,
        now: DateTime<Utc>,
    ) -> Result<Continuation, RuntimeError> {
        let current: Option<String> =
            sqlx::query_scalar("SELECT runtime_generation FROM task_continuations WHERE task_id=?")
                .bind(lease.task_id.to_string())
                .fetch_optional(&self.pool)
                .await?;
        if current.as_deref() != Some(generation) {
            return Err(RuntimeError::Fenced);
        }
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query("UPDATE task_continuations SET state='in_flight',revision=revision+1,heartbeat_at=? WHERE task_id=? AND resource_key=? AND lease_fence=? AND revision=? AND runtime_generation=? AND state='ready' AND EXISTS (SELECT 1 FROM execution_leases l WHERE l.resource_key=task_continuations.resource_key AND l.task_id=task_continuations.task_id AND l.owner_token=? AND l.fence=task_continuations.lease_fence AND l.state='active' AND l.expires_at>?)")
            .bind(ts(now)).bind(lease.task_id.to_string()).bind(lease.resource.to_string()).bind(to_i64(lease.fence)?).bind(to_i64(revision)?).bind(generation).bind(lease.owner.to_string()).bind(ts(now)).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(RuntimeError::Fenced);
        }
        let row = sqlx::query("SELECT * FROM task_continuations WHERE task_id=?")
            .bind(lease.task_id.to_string())
            .fetch_one(&mut *tx)
            .await?;
        let continuation = decode_continuation(&row)?;
        tx.commit().await?;
        Ok(continuation)
    }

    pub async fn record_continue(
        &self,
        lease: &Lease,
        revision: u64,
        next_prompt: &str,
        observed_bytes: u64,
        now: DateTime<Utc>,
    ) -> Result<Continuation, RuntimeError> {
        self.ensure_runtime_generation(lease.task_id).await?;
        let prompt = bounded_prompt(next_prompt)?;
        let mut tx = self.pool.begin().await?;
        let result=sqlx::query("UPDATE task_continuations SET state='ready',revision=revision+1,next_turn=next_turn+1,completed_turns=completed_turns+1,consecutive_no_progress=consecutive_no_progress+1,next_prompt=?,heartbeat_at=?,observed_output_bytes=observed_output_bytes+? WHERE task_id=? AND resource_key=? AND lease_fence=? AND revision=? AND state='in_flight' AND EXISTS (SELECT 1 FROM execution_leases l WHERE l.resource_key=task_continuations.resource_key AND l.task_id=task_continuations.task_id AND l.owner_token=? AND l.fence=task_continuations.lease_fence AND l.state='active' AND l.expires_at>?)")
            .bind(prompt).bind(ts(now)).bind(to_i64(observed_bytes)?).bind(lease.task_id.to_string()).bind(lease.resource.to_string()).bind(to_i64(lease.fence)?).bind(to_i64(revision)?).bind(lease.owner.to_string()).bind(ts(now)).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(RuntimeError::Fenced);
        }
        let row = sqlx::query("SELECT * FROM task_continuations WHERE task_id=?")
            .bind(lease.task_id.to_string())
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(RuntimeError::Malformed)?;
        let continuation = decode_continuation(&row)?;
        tx.commit().await?;
        Ok(continuation)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn record_continue_with_receipt(
        &self,
        lease: &Lease,
        revision: u64,
        generation: &str,
        next_prompt: &str,
        observed_bytes: u64,
        outcome: &str,
        response: &str,
        now: DateTime<Utc>,
    ) -> Result<Continuation, RuntimeError> {
        let prompt = bounded_prompt(next_prompt)?;
        let key = format!(
            "{}:{}:{}:{}",
            lease.task_id,
            self.delivery_for(lease.task_id).await?,
            lease.fence,
            revision
        );
        let hash = Uuid::new_v5(&OUTBOX_BODY_NAMESPACE, response.as_bytes()).to_string();
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query("UPDATE task_continuations SET state='ready',revision=revision+1,next_turn=next_turn+1,completed_turns=completed_turns+1,consecutive_no_progress=consecutive_no_progress+1,next_prompt=?,heartbeat_at=?,observed_output_bytes=observed_output_bytes+? WHERE task_id=? AND resource_key=? AND lease_fence=? AND revision=? AND runtime_generation=? AND state='in_flight' AND EXISTS (SELECT 1 FROM execution_leases l WHERE l.resource_key=task_continuations.resource_key AND l.task_id=task_continuations.task_id AND l.owner_token=? AND l.fence=task_continuations.lease_fence AND l.state='active' AND l.expires_at>?)")
            .bind(prompt).bind(ts(now)).bind(to_i64(observed_bytes)?).bind(lease.task_id.to_string()).bind(lease.resource.to_string()).bind(to_i64(lease.fence)?).bind(to_i64(revision)?).bind(generation).bind(lease.owner.to_string()).bind(ts(now)).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(RuntimeError::Fenced);
        }
        let inserted = sqlx::query("INSERT INTO continuation_response_receipts(idempotency_key,task_id,delivery_id,resource_key,lease_fence,continuation_revision,response_hash,outcome,output_bytes,runtime_generation,created_at) SELECT ?,task_id,delivery_id,resource_key,lease_fence,revision-1,?,?,?,?,? FROM task_continuations WHERE task_id=? ON CONFLICT(idempotency_key) DO NOTHING")
            .bind(&key).bind(&hash).bind(outcome).bind(i64::try_from(response.len()).unwrap_or(i64::MAX)).bind(generation).bind(ts(now)).bind(lease.task_id.to_string()).execute(&mut *tx).await?;
        if inserted.rows_affected() != 1 {
            return Err(RuntimeError::Continuation);
        }
        let row = sqlx::query("SELECT * FROM task_continuations WHERE task_id=?")
            .bind(lease.task_id.to_string())
            .fetch_one(&mut *tx)
            .await?;
        let continuation = decode_continuation(&row)?;
        tx.commit().await?;
        Ok(continuation)
    }

    async fn delivery_for(&self, task_id: TaskId) -> Result<String, RuntimeError> {
        sqlx::query_scalar("SELECT delivery_id FROM task_continuations WHERE task_id=?")
            .bind(task_id.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn record_response_receipt(
        &self,
        continuation: &Continuation,
        outcome: &str,
        response: &str,
        now: DateTime<Utc>,
    ) -> Result<(), RuntimeError> {
        let key = format!(
            "{}:{}:{}:{}",
            continuation.task_id,
            continuation.delivery_id,
            continuation.lease_fence,
            continuation.revision
        );
        let hash = Uuid::new_v5(&OUTBOX_BODY_NAMESPACE, response.as_bytes()).to_string();
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query("INSERT INTO continuation_response_receipts(idempotency_key,task_id,delivery_id,resource_key,lease_fence,continuation_revision,response_hash,outcome,output_bytes,runtime_generation,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(idempotency_key) DO NOTHING")
            .bind(&key).bind(continuation.task_id.to_string()).bind(continuation.delivery_id.to_string()).bind(continuation.resource.to_string())
            .bind(to_i64(continuation.lease_fence)?).bind(to_i64(continuation.revision)?).bind(&hash).bind(outcome)
            .bind(i64::try_from(response.len()).unwrap_or(i64::MAX)).bind(&continuation.runtime_generation).bind(ts(now))
            .execute(&mut *tx).await?;
        if inserted.rows_affected() == 0 {
            let existing: String = sqlx::query_scalar(
                "SELECT response_hash FROM continuation_response_receipts WHERE idempotency_key=?",
            )
            .bind(&key)
            .fetch_one(&mut *tx)
            .await?;
            if existing != hash {
                return Err(RuntimeError::Continuation);
            }
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_activity(
        &self,
        lease: &Lease,
        revision: u64,
        observed_bytes: u64,
        now: DateTime<Utc>,
    ) -> Result<(), RuntimeError> {
        self.ensure_runtime_generation(lease.task_id).await?;
        let result=sqlx::query("UPDATE task_continuations SET heartbeat_at=?,observed_output_bytes=observed_output_bytes+? WHERE task_id=? AND resource_key=? AND lease_fence=? AND revision=? AND state='in_flight' AND EXISTS (SELECT 1 FROM execution_leases l WHERE l.resource_key=task_continuations.resource_key AND l.task_id=task_continuations.task_id AND l.owner_token=? AND l.fence=task_continuations.lease_fence AND l.state='active' AND l.expires_at>?)")
            .bind(ts(now)).bind(to_i64(observed_bytes)?).bind(lease.task_id.to_string()).bind(lease.resource.to_string()).bind(to_i64(lease.fence)?).bind(to_i64(revision)?).bind(lease.owner.to_string()).bind(ts(now)).execute(&self.pool).await?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(RuntimeError::Fenced)
        }
    }

    pub async fn finish(
        &self,
        lease: &Lease,
        revision: u64,
        state: ContinuationState,
        observed_bytes: u64,
        now: DateTime<Utc>,
    ) -> Result<(), RuntimeError> {
        self.ensure_runtime_generation(lease.task_id).await?;
        let mut tx = self.pool.begin().await?;
        let result=sqlx::query("UPDATE task_continuations SET state=?,revision=revision+1,completed_turns=completed_turns+1,heartbeat_at=?,observed_output_bytes=observed_output_bytes+? WHERE task_id=? AND resource_key=? AND lease_fence=? AND revision=? AND state='in_flight' AND EXISTS (SELECT 1 FROM execution_leases l WHERE l.resource_key=task_continuations.resource_key AND l.task_id=task_continuations.task_id AND l.owner_token=? AND l.fence=task_continuations.lease_fence AND l.state='active' AND l.expires_at>?)")
            .bind(state.as_str()).bind(ts(now)).bind(to_i64(observed_bytes)?).bind(lease.task_id.to_string()).bind(lease.resource.to_string()).bind(to_i64(lease.fence)?).bind(to_i64(revision)?).bind(lease.owner.to_string()).bind(ts(now)).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(RuntimeError::Fenced);
        }
        let lease_state = if state == ContinuationState::RecoveryNeeded {
            "recovery_needed"
        } else {
            "released"
        };
        let result=sqlx::query("UPDATE execution_leases SET state=?,heartbeat_at=?,expires_at=? WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND state='active' AND expires_at>?")
            .bind(lease_state).bind(ts(now)).bind(ts(now)).bind(lease.resource.to_string()).bind(lease.task_id.to_string()).bind(lease.owner.to_string()).bind(to_i64(lease.fence)?).bind(ts(now)).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(RuntimeError::Fenced);
        }
        if state == ContinuationState::RecoveryNeeded {
            sqlx::query(
                "UPDATE delivery_dispositions SET state='outcome_unknown',reason_code='recovery_needed' \
                 WHERE delivery_id=(SELECT delivery_id FROM task_continuations WHERE task_id=?) \
                 AND task_id=? AND state IN ('prepared','acknowledged')",
            )
            .bind(lease.task_id.to_string())
            .bind(lease.task_id.to_string())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn finalize_terminal(
        &self,
        lease: &Lease,
        revision: u64,
        state: ContinuationState,
        observed_bytes: u64,
        event: &crate::models::TaskEvent,
        now: DateTime<Utc>,
    ) -> Result<FinalizeResult, RuntimeError> {
        if self.ensure_runtime_generation(lease.task_id).await.is_err() {
            return Ok(FinalizeResult::Fenced);
        }
        let mut tx = self.pool.begin().await?;
        let envelope = sqlx::query(
            "SELECT a.reply_room,a.reply_thread_root,a.reply_event_id,a.monitor_room,
                    a.monitor_generation,COALESCE(a.render_version,'v1') AS render_version,
                    t.root_task_id,t.parent_task_id,t.from_agent,t.to_agent
             FROM task_admissions a JOIN tasks t ON t.task_id=a.task_id
             WHERE a.task_id=?",
        )
        .bind(event.task_id.to_string())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(RuntimeError::Malformed)?;
        let continuation=sqlx::query("UPDATE task_continuations SET state=?,revision=revision+1,completed_turns=completed_turns+1,heartbeat_at=?,observed_output_bytes=observed_output_bytes+? WHERE task_id=? AND resource_key=? AND lease_fence=? AND revision=? AND state='in_flight' AND EXISTS (SELECT 1 FROM execution_leases l WHERE l.resource_key=task_continuations.resource_key AND l.task_id=task_continuations.task_id AND l.owner_token=? AND l.fence=task_continuations.lease_fence AND l.state='active' AND l.expires_at>?)")
            .bind(state.as_str()).bind(ts(now)).bind(to_i64(observed_bytes)?).bind(lease.task_id.to_string()).bind(lease.resource.to_string()).bind(to_i64(lease.fence)?).bind(to_i64(revision)?).bind(lease.owner.to_string()).bind(ts(now)).execute(&mut *tx).await?;
        if continuation.rows_affected() != 1 {
            return Ok(FinalizeResult::Fenced);
        }
        let terminal_response =
            serde_json::to_string(&event.payload).map_err(|_| RuntimeError::Malformed)?;
        let terminal_hash =
            Uuid::new_v5(&OUTBOX_BODY_NAMESPACE, terminal_response.as_bytes()).to_string();
        let receipt_key = format!(
            "{}:{}:{}:{}",
            lease.task_id, event.id, lease.fence, revision
        );
        let _ = sqlx::query("INSERT INTO continuation_response_receipts(idempotency_key,task_id,delivery_id,resource_key,lease_fence,continuation_revision,response_hash,outcome,output_bytes,runtime_generation,created_at) SELECT ?,task_id,delivery_id,resource_key,lease_fence,revision-1,?,'structured',?,?,runtime_generation,? FROM task_continuations WHERE task_id=? ON CONFLICT(idempotency_key) DO NOTHING")
            .bind(&receipt_key).bind(&terminal_hash).bind(i64::try_from(terminal_response.len()).unwrap_or(i64::MAX)).bind(ts(now)).bind(event.task_id.to_string()).execute(&mut *tx).await;
        let latest: Option<i64> =
            sqlx::query_scalar("SELECT MAX(seq) FROM task_events WHERE task_id=?")
                .bind(event.task_id.to_string())
                .fetch_one(&mut *tx)
                .await?;
        if latest
            != Some(
                i64::try_from(event.seq.saturating_sub(1)).map_err(|_| RuntimeError::Malformed)?,
            )
        {
            return Ok(FinalizeResult::Stale);
        }
        let expected_version = event.seq.checked_sub(2).ok_or(RuntimeError::Malformed)?;
        let version =
            sqlx::query("UPDATE tasks SET version=version+1 WHERE task_id=? AND version=?")
                .bind(event.task_id.to_string())
                .bind(to_i64(expected_version)?)
                .execute(&mut *tx)
                .await?;
        if version.rows_affected() != 1 {
            return Ok(FinalizeResult::Stale);
        }
        let status = match event.status {
            crate::models::TaskStatus::Completed => "completed",
            crate::models::TaskStatus::Failed => "failed",
            crate::models::TaskStatus::TimedOut => "timed_out",
            crate::models::TaskStatus::Cancelled => "cancelled",
            _ => return Err(RuntimeError::Malformed),
        };
        let payload = serde_json::to_string(&event.payload).map_err(|_| RuntimeError::Malformed)?;
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES (?,?,?,?,?,?)").bind(event.id.to_string()).bind(event.task_id.to_string()).bind(to_i64(event.seq)?).bind(status).bind(ts(event.timestamp)).bind(payload).execute(&mut *tx).await?;
        let lease_update = sqlx::query("UPDATE execution_leases SET state='released',heartbeat_at=?,expires_at=? WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND state='active' AND expires_at>?")
            .bind(ts(now)).bind(ts(now)).bind(lease.resource.to_string()).bind(lease.task_id.to_string()).bind(lease.owner.to_string()).bind(to_i64(lease.fence)?).bind(ts(now)).execute(&mut *tx).await?;
        if lease_update.rows_affected() != 1 {
            return Ok(FinalizeResult::Fenced);
        }
        let claim_state = if state == ContinuationState::RecoveryNeeded {
            "recovery_needed"
        } else {
            "released"
        };
        if claim_state == "released" {
            sqlx::query("DELETE FROM workspace_claims WHERE runtime_owner=? AND task_id=? AND owner_fence=? AND state='active'")
                .bind(lease.owner.to_string()).bind(lease.task_id.to_string()).bind(to_i64(lease.fence)?).execute(&mut *tx).await?;
        } else {
            sqlx::query("UPDATE workspace_claims SET state='recovery_needed',revision=revision+1,updated_at=? WHERE runtime_owner=? AND task_id=? AND owner_fence=? AND state='active'")
                .bind(ts(now)).bind(lease.owner.to_string()).bind(lease.task_id.to_string()).bind(to_i64(lease.fence)?).execute(&mut *tx).await?;
        }
        let admission = sqlx::query("UPDATE task_admissions SET state='terminal',revision=revision+1,updated_at=? WHERE task_id=? AND state IN ('enqueued','dispatching','running')").bind(ts(now)).bind(event.task_id.to_string()).execute(&mut *tx).await?;
        if admission.rows_affected() != 1 {
            return Ok(FinalizeResult::Stale);
        }
        sqlx::query(
            "UPDATE delivery_dispositions SET state='terminal',reason_code=NULL \
             WHERE delivery_id=(SELECT delivery_id FROM task_continuations WHERE task_id=?) \
             AND task_id=? AND state IN ('prepared','acknowledged')",
        )
        .bind(event.task_id.to_string())
        .bind(event.task_id.to_string())
        .execute(&mut *tx)
        .await?;
        let render_version: String = envelope.try_get("render_version")?;
        if let Some(room) = envelope.try_get::<Option<String>, _>("reply_room")? {
            let body = terminal_reply_body(&event.payload).ok_or(RuntimeError::Malformed)?;
            insert_terminal_projection(
                &mut tx,
                event,
                "terminal_reply",
                &room,
                envelope
                    .try_get::<Option<String>, _>("reply_thread_root")?
                    .as_deref(),
                envelope
                    .try_get::<Option<String>, _>("reply_event_id")?
                    .as_deref(),
                envelope.try_get::<Option<i64>, _>("monitor_generation")?,
                &render_version,
                &body,
                now,
            )
            .await?;
        }
        if let Some(room) = envelope.try_get::<Option<String>, _>("monitor_room")? {
            let body = bounded_outbox(format!(
                "category={} severity={} status={} task_id={} root_task_id={} parent_task_id={} from_agent={} to_agent={} seq={} timestamp={}",
                if matches!(event.status, crate::models::TaskStatus::Completed) {
                    "summary"
                } else {
                    "alert"
                },
                match event.status {
                    crate::models::TaskStatus::Completed => "info",
                    crate::models::TaskStatus::Cancelled => "warning",
                    _ => "error",
                },
                status,
                event.task_id,
                envelope.try_get::<String, _>("root_task_id")?,
                envelope
                    .try_get::<Option<String>, _>("parent_task_id")?
                    .unwrap_or_else(|| "none".into()),
                envelope.try_get::<String, _>("from_agent")?,
                envelope.try_get::<String, _>("to_agent")?,
                event.seq,
                ts(event.timestamp),
            ));
            insert_terminal_projection(
                &mut tx,
                event,
                "observer",
                &room,
                None,
                None,
                envelope.try_get::<Option<i64>, _>("monitor_generation")?,
                &render_version,
                &body,
                now,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(FinalizeResult::Committed)
    }

    pub async fn counts(&self) -> Result<RuntimeCounts, RuntimeError> {
        self.counts_at(Utc::now()).await
    }

    pub async fn counts_at(&self, now: DateTime<Utc>) -> Result<RuntimeCounts, RuntimeError> {
        let row=sqlx::query("SELECT (SELECT count(*) FROM execution_leases WHERE state='active') active_leases,(SELECT count(*) FROM execution_leases WHERE state='active' AND expires_at<=?) expired_leases,(SELECT count(*) FROM execution_leases WHERE state='recovery_needed') recovery_leases,(SELECT count(*) FROM task_continuations WHERE state='ready') ready_turns,(SELECT count(*) FROM task_continuations WHERE state='in_flight') in_flight_turns,(SELECT count(*) FROM task_continuations WHERE state='recovery_needed') recovery_turns").bind(ts(now)).fetch_one(&self.pool).await?;
        Ok(RuntimeCounts {
            active_leases: count(&row, "active_leases")?,
            expired_leases: count(&row, "expired_leases")?,
            recovery_needed_leases: count(&row, "recovery_leases")?,
            ready_turns: count(&row, "ready_turns")?,
            in_flight_turns: count(&row, "in_flight_turns")?,
            recovery_backlog: count(&row, "recovery_turns")?,
        })
    }

    pub async fn recovery_snapshot(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<RecoverySnapshot, RuntimeError> {
        if limit == 0 {
            return Ok(RecoverySnapshot::default());
        }
        let ready = sqlx::query(
            "SELECT task_id FROM task_continuations WHERE state='ready' ORDER BY task_id LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let ambiguous=sqlx::query("SELECT DISTINCT c.task_id FROM task_continuations c JOIN execution_leases l ON l.resource_key=c.resource_key WHERE c.state IN ('in_flight','recovery_needed') OR l.state='recovery_needed' OR (l.state='active' AND l.expires_at<=?) ORDER BY c.task_id LIMIT ?").bind(ts(now)).bind(i64::from(limit)).fetch_all(&self.pool).await?;
        Ok(RecoverySnapshot {
            ready: ready
                .iter()
                .map(|row| parse_id(row, "task_id"))
                .collect::<Result<_, _>>()?,
            ambiguous: ambiguous
                .iter()
                .map(|row| parse_id(row, "task_id"))
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Clone)]
pub struct SqliteTaskLifecycle {
    pool: SqlitePool,
}

impl SqliteTaskLifecycle {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

impl TaskLifecycle for SqliteTaskLifecycle {
    fn transition<'a>(
        &'a self,
        event: &'a crate::models::TaskEvent,
    ) -> BusFuture<'a, Result<LifecycleResult, DispatchError>> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await.map_err(runtime_dispatch_error)?;
            let replay: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM task_events WHERE event_id=? AND task_id=? AND seq=?",
            )
            .bind(event.id.to_string())
            .bind(event.task_id.to_string())
            .bind(to_i64(event.seq).map_err(runtime_dispatch_error)?)
            .fetch_optional(&mut *tx)
            .await
            .map_err(runtime_dispatch_error)?;
            if replay.is_some() {
                return Ok(LifecycleResult::Replay);
            }
            let expected = event.seq.checked_sub(2).ok_or_else(|| {
                runtime_dispatch_error("durable lifecycle does not own queued events")
            })?;
            let version =
                sqlx::query("UPDATE tasks SET version=version+1 WHERE task_id=? AND version=?")
                    .bind(event.task_id.to_string())
                    .bind(to_i64(expected).map_err(runtime_dispatch_error)?)
                    .execute(&mut *tx)
                    .await
                    .map_err(runtime_dispatch_error)?;
            if version.rows_affected() != 1 {
                return Ok(LifecycleResult::Stale);
            }
            let (status, from_state, to_state) = lifecycle_transition(event.status)
                .ok_or_else(|| runtime_dispatch_error("invalid lifecycle transition"))?;
            let admission = sqlx::query(&format!(
                "UPDATE task_admissions SET state=?,revision=revision+1,updated_at=? \
                 WHERE task_id=? AND state IN ({from_state})"
            ))
            .bind(to_state)
            .bind(ts(event.timestamp))
            .bind(event.task_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(runtime_dispatch_error)?;
            if admission.rows_affected() != 1 {
                return Ok(LifecycleResult::Stale);
            }
            let payload = serde_json::to_string(&event.payload).map_err(runtime_dispatch_error)?;
            sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES (?,?,?,?,?,?)")
                .bind(event.id.to_string())
                .bind(event.task_id.to_string())
                .bind(to_i64(event.seq).map_err(runtime_dispatch_error)?)
                .bind(status)
                .bind(ts(event.timestamp))
                .bind(payload)
                .execute(&mut *tx)
                .await
                .map_err(runtime_dispatch_error)?;
            tx.commit().await.map_err(runtime_dispatch_error)?;
            Ok(LifecycleResult::Committed)
        })
    }
}

fn lifecycle_transition(
    status: crate::models::TaskStatus,
) -> Option<(&'static str, &'static str, &'static str)> {
    use crate::models::TaskStatus;
    match status {
        TaskStatus::Dispatched => Some(("dispatched", "'ready','enqueued'", "dispatching")),
        TaskStatus::Running => Some(("running", "'dispatching'", "running")),
        TaskStatus::Completed => Some(("completed", "'running'", "terminal")),
        TaskStatus::Failed => Some((
            "failed",
            "'ready','enqueued','dispatching','running'",
            "terminal",
        )),
        TaskStatus::TimedOut => Some((
            "timed_out",
            "'ready','enqueued','dispatching','running'",
            "terminal",
        )),
        TaskStatus::Cancelled => Some((
            "cancelled",
            "'ready','enqueued','dispatching','running'",
            "terminal",
        )),
        TaskStatus::Queued => None,
    }
}

fn runtime_dispatch_error(error: impl std::fmt::Display) -> DispatchError {
    DispatchError::ExecutionFailed {
        reason: crate::acp::bounded(&error.to_string()),
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ContinuationPolicy {
    pub max_turns: u32,
    pub max_wall_time: Duration,
    pub max_inactivity: Duration,
    pub max_consecutive_no_progress: u32,
    pub max_observed_output_bytes: u64,
    pub lease_ttl: Duration,
}
impl ContinuationPolicy {
    pub fn check(
        &self,
        state: &Continuation,
        now: DateTime<Utc>,
        deadline: Option<DateTime<Utc>>,
    ) -> Result<(), RuntimeError> {
        if state.next_turn > self.max_turns {
            return Err(RuntimeError::Exhausted(PolicyLimit::Turns));
        }
        if now >= add(state.started_at, self.max_wall_time)? || deadline.is_some_and(|v| now >= v) {
            return Err(RuntimeError::Exhausted(PolicyLimit::Deadline));
        }
        if now >= add(state.heartbeat_at, self.max_inactivity)? {
            return Err(RuntimeError::Exhausted(PolicyLimit::Inactivity));
        }
        if state.consecutive_no_progress >= self.max_consecutive_no_progress {
            return Err(RuntimeError::Exhausted(PolicyLimit::NoProgress));
        }
        if state.observed_output_bytes > self.max_observed_output_bytes {
            return Err(RuntimeError::Exhausted(PolicyLimit::OutputBytes));
        }
        Ok(())
    }
}

pub trait RuntimeClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}
#[derive(Debug, Default)]
pub struct SystemRuntimeClock;
impl RuntimeClock for SystemRuntimeClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Degradation {
    RecoveryBacklog,
    LeaseRenewal,
    Backpressure,
    ConsumerError,
    Adapter,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Readiness {
    Ready,
    StorageUnavailable,
    OwnerStopped,
    RecoveryBlocked,
    AdapterUnavailable,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    pub live: bool,
    pub readiness: Readiness,
    pub degraded: BTreeSet<Degradation>,
    pub observed_at: DateTime<Utc>,
}
pub async fn health_snapshot(
    store: &SqliteRuntimeStore,
    now: DateTime<Utc>,
    owner_running: bool,
    adapter_ready: bool,
) -> HealthSnapshot {
    match store.counts_at(now).await {
        Err(_) => HealthSnapshot {
            live: true,
            readiness: Readiness::StorageUnavailable,
            degraded: BTreeSet::new(),
            observed_at: now,
        },
        Ok(counts) => {
            let mut degraded = BTreeSet::new();
            if counts.recovery_backlog > 0 || counts.recovery_needed_leases > 0 {
                degraded.insert(Degradation::RecoveryBacklog);
            }
            let readiness = if !owner_running {
                Readiness::OwnerStopped
            } else if !adapter_ready {
                Readiness::AdapterUnavailable
            } else if !degraded.is_empty() {
                Readiness::RecoveryBlocked
            } else {
                Readiness::Ready
            };
            HealthSnapshot {
                live: true,
                readiness,
                degraded,
                observed_at: now,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Metric {
    LeaseAcquired,
    LeaseBusy,
    LeaseRecoveryNeeded,
    TurnStarted,
    TurnContinued,
    TurnCompleted,
    TurnBlocked,
    TurnFailed,
    PolicyExhausted(PolicyLimit),
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub values: BTreeMap<Metric, u64>,
    pub gauges: RuntimeCounts,
}
#[derive(Debug, Default)]
pub struct RuntimeMetrics(Mutex<MetricsSnapshot>);
impl RuntimeMetrics {
    pub fn increment(&self, metric: Metric) {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let value = state.values.entry(metric).or_default();
        *value = value.saturating_add(1);
    }
    pub fn set_gauges(&self, gauges: RuntimeCounts) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).gauges = gauges;
    }
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

pub trait RuntimeTimer: Send + Sync {
    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

#[derive(Debug, Default)]
pub struct TokioRuntimeTimer;
impl RuntimeTimer for TokioRuntimeTimer {
    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

struct PendingLease {
    lease: Lease,
    claimed: tokio::sync::oneshot::Sender<()>,
}

struct ExecutionGuard {
    dispatcher: AcpDispatcher,
    store: SqliteRuntimeStore,
    clock: Arc<dyn RuntimeClock>,
    lease: Option<Lease>,
    cleanup: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}
impl ExecutionGuard {
    fn lease(&self) -> &Lease {
        self.lease.as_ref().expect("active lease")
    }
    fn replace(&mut self, lease: Lease) {
        self.lease = Some(lease);
    }
    fn disarm(&mut self) {
        self.lease = None;
    }
}
impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        let dispatcher = self.dispatcher.clone();
        let store = self.store.clone();
        let clock = Arc::clone(&self.clock);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let task = runtime.spawn(async move {
                let _ = dispatcher.shutdown().await;
                let _ = store
                    .release(&lease, ReleaseDisposition::RecoveryNeeded, clock.now())
                    .await;
            });
            self.cleanup
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(task);
        }
    }
}

#[derive(Clone)]
pub struct LeasedAcpDispatcher {
    dispatcher: AcpDispatcher,
    store: SqliteRuntimeStore,
    workspace: WorkspaceId,
    workspace_paths: Vec<std::path::PathBuf>,
    policy: ContinuationPolicy,
    clock: Arc<dyn RuntimeClock>,
    timer: Arc<dyn RuntimeTimer>,
    metrics: Arc<RuntimeMetrics>,
    pending: Arc<Mutex<std::collections::HashMap<TaskId, PendingLease>>>,
    cleanup: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    active:
        Arc<Mutex<std::collections::HashMap<TaskId, tokio::sync::mpsc::Sender<SupervisorCommand>>>>,
}

struct SupervisorCommand {
    reason: String,
    response: tokio::sync::oneshot::Sender<Result<FinalizationCapability, DispatchError>>,
}

struct DeferredTerminal {
    outcome: DispatchOutcome,
    lease: Lease,
    revision: u64,
    continuation_state: ContinuationState,
    observed_bytes: u64,
}

enum LoopOutcome {
    Finished(DispatchOutcome),
    Deferred(DeferredTerminal),
    Cancelled(
        DeferredTerminal,
        tokio::sync::oneshot::Sender<Result<FinalizationCapability, DispatchError>>,
    ),
}

enum TurnRun {
    Result(TurnResult),
    Cancelled(SupervisorCommand),
}

impl LeasedAcpDispatcher {
    pub fn new(
        dispatcher: AcpDispatcher,
        store: SqliteRuntimeStore,
        workspace: WorkspaceId,
        policy: ContinuationPolicy,
        clock: Arc<dyn RuntimeClock>,
        timer: Arc<dyn RuntimeTimer>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self::new_with_paths(
            dispatcher,
            store,
            workspace,
            Vec::new(),
            policy,
            clock,
            timer,
            metrics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_paths(
        dispatcher: AcpDispatcher,
        store: SqliteRuntimeStore,
        workspace: WorkspaceId,
        workspace_paths: Vec<std::path::PathBuf>,
        policy: ContinuationPolicy,
        clock: Arc<dyn RuntimeClock>,
        timer: Arc<dyn RuntimeTimer>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            dispatcher,
            store,
            workspace,
            workspace_paths,
            policy,
            clock,
            timer,
            metrics,
            pending: Arc::new(Mutex::new(std::collections::HashMap::new())),
            cleanup: Arc::new(Mutex::new(Vec::new())),
            active: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    pub async fn shutdown(&self) {
        let leases: Vec<_> = self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain()
            .map(|(_, pending)| pending.lease)
            .collect();
        if !leases.is_empty() {
            let _ = self.dispatcher.shutdown().await;
        }
        for lease in leases {
            let _ = self
                .store
                .release(&lease, ReleaseDisposition::RecoveryNeeded, self.clock.now())
                .await;
        }
        loop {
            let tasks =
                std::mem::take(&mut *self.cleanup.lock().unwrap_or_else(|p| p.into_inner()));
            if tasks.is_empty() {
                break;
            }
            for task in tasks {
                let _ = task.await;
            }
        }
        let _ = self.dispatcher.shutdown().await;
    }

    fn execution_error(error: impl std::fmt::Display) -> DispatchError {
        DispatchError::ExecutionFailed {
            reason: crate::acp::bounded(&error.to_string()),
        }
    }

    async fn terminal(
        &self,
        guard: &mut ExecutionGuard,
        revision: u64,
        state: ContinuationState,
        bytes: u64,
        outcome: DispatchOutcome,
        defer: bool,
    ) -> Result<LoopOutcome, DispatchError> {
        if defer {
            let lease = guard.lease().clone();
            guard.disarm();
            return Ok(LoopOutcome::Deferred(DeferredTerminal {
                outcome,
                lease,
                revision,
                continuation_state: state,
                observed_bytes: bytes,
            }));
        }
        self.store
            .finish(guard.lease(), revision, state, bytes, self.clock.now())
            .await
            .map_err(Self::execution_error)?;
        guard.disarm();
        Ok(LoopOutcome::Finished(outcome))
    }

    async fn exhaust(
        &self,
        guard: &mut ExecutionGuard,
        state: &Continuation,
        error: RuntimeError,
        defer: bool,
    ) -> Result<LoopOutcome, DispatchError> {
        if let RuntimeError::Exhausted(limit) = error {
            self.metrics.increment(Metric::PolicyExhausted(limit));
        }
        let now = self.clock.now();
        let claimed = self
            .store
            .claim_turn(guard.lease(), state.revision, now)
            .await
            .map_err(Self::execution_error)?;
        self.terminal(
            guard,
            claimed.revision,
            ContinuationState::Blocked,
            0,
            DispatchOutcome::Failed {
                error: crate::acp::bounded(&error.to_string()),
            },
            defer,
        )
        .await
    }

    async fn run_turn(
        &self,
        request: &DispatchRequest<'_>,
        guard: &mut ExecutionGuard,
        state: &Continuation,
        cancel: &mut tokio::sync::mpsc::Receiver<SupervisorCommand>,
    ) -> Result<TurnRun, DispatchError> {
        let mut turn = self
            .dispatcher
            .start_turn(request, &state.next_prompt, 8)
            .await
            .map_err(Self::execution_error)?;
        let mut last_activity = self.clock.now();
        let wall_deadline =
            add(state.started_at, self.policy.max_wall_time).map_err(Self::execution_error)?;
        let hard_deadline = request
            .task
            .deadline
            .map_or(wall_deadline, |deadline| deadline.min(wall_deadline));
        loop {
            let now = self.clock.now();
            if now >= hard_deadline {
                let _ = self.dispatcher.shutdown().await;
                return Err(Self::execution_error(RuntimeError::Exhausted(
                    PolicyLimit::Deadline,
                )));
            }
            let elapsed = now
                .signed_duration_since(last_activity)
                .to_std()
                .unwrap_or(Duration::ZERO);
            if elapsed >= self.policy.max_inactivity {
                return Err(Self::execution_error(RuntimeError::Exhausted(
                    PolicyLimit::Inactivity,
                )));
            }
            let remaining = self.policy.max_inactivity.saturating_sub(elapsed);
            let hard_remaining = hard_deadline
                .signed_duration_since(now)
                .to_std()
                .unwrap_or(Duration::ZERO);
            let heartbeat = (self.policy.lease_ttl / 2).max(Duration::from_millis(1));
            tokio::select! {
                biased;
                command=cancel.recv()=>{
                    let Some(command)=command else {
                        return Err(DispatchError::RecoveryNeeded);
                    };
                    return match turn.cancel_and_shutdown().await {
                        Ok(()) => Ok(TurnRun::Cancelled(command)),
                        Err(_) => {
                            let _=command.response.send(Err(DispatchError::RecoveryNeeded));
                            Err(DispatchError::RecoveryNeeded)
                        }
                    };
                }
                update=turn.recv()=>match update {
                    Some(crate::acp::TurnUpdate::AgentMessageChunk(chunk))=>{
                        let observed=u64::try_from(chunk.len()).unwrap_or(u64::MAX);
                        let at=self.clock.now();
                        self.store.record_activity(guard.lease(),state.revision,observed,at).await.map_err(Self::execution_error)?;
                        let renewed=self.store.renew(guard.lease(),at,self.policy.lease_ttl).await.map_err(Self::execution_error)?;
                        guard.replace(renewed);last_activity=at;
                    }
                    None=>break,
                },
                _=self.timer.sleep(heartbeat)=>{
                    let at=self.clock.now();
                    let renewed=self.store.renew(guard.lease(),at,self.policy.lease_ttl).await.map_err(Self::execution_error)?;
                    self.store.record_activity(guard.lease(),state.revision,0,at).await.map_err(Self::execution_error)?;
                    guard.replace(renewed);
                }
                _=self.timer.sleep(remaining)=>return Err(Self::execution_error(RuntimeError::Exhausted(PolicyLimit::Inactivity))),
                _=self.timer.sleep(hard_remaining)=>{
                    let _ = self.dispatcher.shutdown().await;
                    return Err(Self::execution_error(RuntimeError::Exhausted(PolicyLimit::Deadline)));
                }
            }
        }
        turn.finish()
            .await
            .map(TurnRun::Result)
            .map_err(Self::execution_error)
    }

    async fn execute_loop(
        &self,
        request: &DispatchRequest<'_>,
        lease: Lease,
        defer: bool,
        cancel: &mut tokio::sync::mpsc::Receiver<SupervisorCommand>,
    ) -> Result<LoopOutcome, DispatchError> {
        let mut guard = ExecutionGuard {
            dispatcher: self.dispatcher.clone(),
            store: self.store.clone(),
            clock: Arc::clone(&self.clock),
            lease: Some(lease),
            cleanup: Arc::clone(&self.cleanup),
        };
        let mut state = self
            .store
            .continuation(request.task.task_id)
            .await
            .map_err(Self::execution_error)?
            .ok_or_else(|| Self::execution_error(RuntimeError::Malformed))?;
        loop {
            let now = self.clock.now();
            if let Err(error) = self.policy.check(&state, now, request.task.deadline) {
                return self.exhaust(&mut guard, &state, error, defer).await;
            }
            let renewed = self
                .store
                .renew(guard.lease(), now, self.policy.lease_ttl)
                .await
                .map_err(Self::execution_error)?;
            guard.replace(renewed);
            state = self
                .store
                .claim_turn(guard.lease(), state.revision, now)
                .await
                .map_err(Self::execution_error)?;
            self.metrics.increment(Metric::TurnStarted);
            let result = self.run_turn(request, &mut guard, &state, cancel).await;
            if let Ok(TurnRun::Cancelled(command)) = result {
                let lease = guard.lease().clone();
                guard.disarm();
                return Ok(LoopOutcome::Cancelled(
                    DeferredTerminal {
                        outcome: DispatchOutcome::Failed {
                            error: command.reason.clone(),
                        },
                        lease,
                        revision: state.revision,
                        continuation_state: ContinuationState::Terminal,
                        observed_bytes: 0,
                    },
                    command.response,
                ));
            }
            let result = result.map(|result| match result {
                TurnRun::Result(result) => result,
                TurnRun::Cancelled(_) => unreachable!(),
            });
            state = self
                .store
                .continuation(request.task.task_id)
                .await
                .map_err(Self::execution_error)?
                .ok_or_else(|| Self::execution_error(RuntimeError::Malformed))?;
            let result_bytes = match &result {
                Ok(TurnResult::Continue {
                    reason,
                    next_prompt,
                }) => reason
                    .len()
                    .saturating_add(next_prompt.as_ref().map_or(0, String::len)),
                Ok(TurnResult::Completed { output }) => output.len(),
                Ok(TurnResult::Blocked { reason } | TurnResult::Failed { reason }) => reason.len(),
                Err(_) => 0,
            } as u64;
            if state.observed_output_bytes.saturating_add(result_bytes)
                > self.policy.max_observed_output_bytes
            {
                self.metrics
                    .increment(Metric::PolicyExhausted(PolicyLimit::OutputBytes));
                return self
                    .terminal(
                        &mut guard,
                        state.revision,
                        ContinuationState::Blocked,
                        result_bytes,
                        DispatchOutcome::Failed {
                            error: "continuation policy exhausted: output_bytes".into(),
                        },
                        defer,
                    )
                    .await;
            }
            let receipt_text = match &result {
                Ok(TurnResult::Continue {
                    reason,
                    next_prompt,
                }) => format!("continue:{reason}:{}", next_prompt.as_deref().unwrap_or("")),
                Ok(TurnResult::Completed { output }) => format!("completed:{output}"),
                Ok(TurnResult::Blocked { reason }) => format!("blocked:{reason}"),
                Ok(TurnResult::Failed { reason }) => format!("failed:{reason}"),
                Err(error) => format!("protocol:{error}"),
            };
            let receipt_outcome = match &result {
                Ok(TurnResult::Continue { .. }) => "structured",
                Ok(_) => "structured",
                Err(_) => "protocol_failure",
            };
            match result {
                Ok(TurnResult::Continue {
                    reason,
                    next_prompt,
                }) => {
                    let Some(prompt) = next_prompt.filter(|value| !value.trim().is_empty()) else {
                        return self
                            .terminal(
                                &mut guard,
                                state.revision,
                                ContinuationState::Blocked,
                                reason.len() as u64,
                                DispatchOutcome::Failed {
                                    error: "continue result has no next prompt".into(),
                                },
                                defer,
                            )
                            .await;
                    };
                    self.metrics.increment(Metric::TurnContinued);
                    let bytes = reason.len().saturating_add(prompt.len()) as u64;
                    state = self
                        .store
                        .record_continue_with_receipt(
                            guard.lease(),
                            state.revision,
                            &state.runtime_generation,
                            &prompt,
                            bytes,
                            receipt_outcome,
                            &receipt_text,
                            self.clock.now(),
                        )
                        .await
                        .map_err(Self::execution_error)?;
                }
                Ok(TurnResult::Completed { output }) => {
                    self.metrics.increment(Metric::TurnCompleted);
                    return self
                        .terminal(
                            &mut guard,
                            state.revision,
                            ContinuationState::Terminal,
                            output.len() as u64,
                            DispatchOutcome::Completed { output },
                            defer,
                        )
                        .await;
                }
                Ok(TurnResult::Blocked { reason }) => {
                    self.metrics.increment(Metric::TurnBlocked);
                    return self
                        .terminal(
                            &mut guard,
                            state.revision,
                            ContinuationState::Blocked,
                            reason.len() as u64,
                            DispatchOutcome::Failed {
                                error: crate::acp::bounded(&format!("blocked: {reason}")),
                            },
                            defer,
                        )
                        .await;
                }
                Ok(TurnResult::Failed { reason }) => {
                    self.metrics.increment(Metric::TurnFailed);
                    return self
                        .terminal(
                            &mut guard,
                            state.revision,
                            ContinuationState::Terminal,
                            reason.len() as u64,
                            DispatchOutcome::Failed {
                                error: crate::acp::bounded(&reason),
                            },
                            defer,
                        )
                        .await;
                }
                Err(error) => {
                    self.store
                        .finish(
                            guard.lease(),
                            state.revision,
                            ContinuationState::RecoveryNeeded,
                            0,
                            self.clock.now(),
                        )
                        .await
                        .map_err(Self::execution_error)?;
                    guard.disarm();
                    self.metrics.increment(Metric::LeaseRecoveryNeeded);
                    return Err(error);
                }
            }
        }
    }
}

impl TaskDispatcher for LeasedAcpDispatcher {
    fn deliver<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move {
            let resource = ExecutionResourceKey::new(request.task.to_agent, self.workspace);
            let lease = match self
                .store
                .acquire(
                    resource,
                    request.task.task_id,
                    self.clock.now(),
                    self.policy.lease_ttl,
                )
                .await
                .map_err(Self::execution_error)?
            {
                AcquireOutcome::Acquired(value) => {
                    self.metrics.increment(Metric::LeaseAcquired);
                    value
                }
                AcquireOutcome::Busy => {
                    self.metrics.increment(Metric::LeaseBusy);
                    return Err(DispatchError::NotAccepted {
                        reason: "execution resource busy".into(),
                    });
                }
                AcquireOutcome::RecoveryNeeded => {
                    self.metrics.increment(Metric::LeaseRecoveryNeeded);
                    return Err(DispatchError::NotAccepted {
                        reason: "execution resource needs recovery".into(),
                    });
                }
            };
            if let Err(error) = self
                .store
                .claim_workspaces(&lease, &self.workspace_paths)
                .await
            {
                let _ = self
                    .store
                    .release(&lease, ReleaseDisposition::Released, self.clock.now())
                    .await;
                return Err(Self::execution_error(error));
            }
            if let Err(error) = self.dispatcher.deliver(request.clone()).await {
                let _ = self
                    .store
                    .release(&lease, ReleaseDisposition::Released, self.clock.now())
                    .await;
                return Err(error);
            }
            if let Err(error) = self
                .store
                .begin_continuation(
                    &lease,
                    request.delivery_id,
                    &request.task.text,
                    self.clock.now(),
                )
                .await
            {
                let _ = self.dispatcher.shutdown().await;
                let _ = self
                    .store
                    .release(&lease, ReleaseDisposition::RecoveryNeeded, self.clock.now())
                    .await;
                return Err(Self::execution_error(error));
            }
            let (claimed, receiver) = tokio::sync::oneshot::channel();
            let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel(1);
            self.pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(
                    request.task.task_id,
                    PendingLease {
                        lease: lease.clone(),
                        claimed,
                    },
                );
            self.active
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(request.task.task_id, cancel_tx);
            let pending = Arc::clone(&self.pending);
            let active = Arc::clone(&self.active);
            let dispatcher = self.dispatcher.clone();
            let store = self.store.clone();
            let clock = Arc::clone(&self.clock);
            let timer = Arc::clone(&self.timer);
            let task_id = request.task.task_id;
            let ttl = self.policy.lease_ttl;
            let supervisor = tokio::spawn(async move {
                tokio::select! {
                    biased;
                    _ = receiver => {}
                    command = cancel_rx.recv() => {
                        active.lock().unwrap_or_else(|p|p.into_inner()).remove(&task_id);
                        let Some(command) = command else { return; };
                        let Some(pending) = pending.lock().unwrap_or_else(|p|p.into_inner()).remove(&task_id) else {
                            let _ = command.response.send(Err(DispatchError::RecoveryNeeded));
                            return;
                        };
                        let lease = pending.lease;
                        if !dispatcher.shutdown_reaped().await {
                            let _ = store.release(&lease,ReleaseDisposition::RecoveryNeeded,clock.now()).await;
                            let _ = command.response.send(Err(DispatchError::RecoveryNeeded));
                            return;
                        }
                        let Some(state) = store.continuation(task_id).await.ok().flatten() else {
                            let _ = store.release(&lease,ReleaseDisposition::RecoveryNeeded,clock.now()).await;
                            let _ = command.response.send(Err(DispatchError::RecoveryNeeded));
                            return;
                        };
                        let state = match store.claim_turn(&lease,state.revision,clock.now()).await {
                            Ok(state) => state,
                            Err(_) => {
                                let _ = store.release(&lease,ReleaseDisposition::RecoveryNeeded,clock.now()).await;
                                let _ = command.response.send(Err(DispatchError::RecoveryNeeded));
                                return;
                            }
                        };
                        let (finalize_tx, finalize_rx) = tokio::sync::oneshot::channel();
                        let capability=FinalizationCapability{task_id,command:Some(finalize_tx)};
                        if command.response.send(Ok(capability)).is_err() {
                            let _=store.finish(&lease,state.revision,ContinuationState::RecoveryNeeded,0,clock.now()).await;
                            return;
                        }
                        match finalize_rx.await {
                            Ok(finalize) if finalize.task_id == task_id => {
                                let result=store.finalize_terminal(&lease,state.revision,ContinuationState::Terminal,0,&finalize.event,clock.now()).await.unwrap_or(FinalizeResult::RecoveryNeeded);
                                let _=finalize.response.send(result);
                            }
                            _ => { let _=store.finish(&lease,state.revision,ContinuationState::RecoveryNeeded,0,clock.now()).await; }
                        }
                    }
                    _ = timer.sleep(ttl) => {
                        active.lock().unwrap_or_else(|p|p.into_inner()).remove(&task_id);
                        let stale=pending.lock().unwrap_or_else(|p|p.into_inner()).remove(&task_id).map(|v|v.lease);
                        if let Some(stale)=stale { let _=dispatcher.shutdown().await; let _=store.release(&stale,ReleaseDisposition::RecoveryNeeded,clock.now()).await; }
                    }
                }
            });
            self.cleanup
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(supervisor);
            Ok(())
        })
    }

    fn execute<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
        Box::pin(async move {
            let pending = self
                .pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&request.task.task_id)
                .ok_or_else(|| Self::execution_error("execution lease is missing"))?;
            let _ = pending.claimed.send(());
            let (_cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel(1);
            match self
                .execute_loop(&request, pending.lease, false, &mut cancel_rx)
                .await?
            {
                LoopOutcome::Finished(outcome) => Ok(outcome),
                LoopOutcome::Deferred(_) | LoopOutcome::Cancelled(_, _) => {
                    Err(Self::execution_error("unexpected deferred outcome"))
                }
            }
        })
    }

    fn supports_prepared(&self) -> bool {
        true
    }

    fn execute_prepared<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<PreparedExecution, DispatchError>> {
        Box::pin(async move {
            let pending = self
                .pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&request.task.task_id)
                .ok_or(DispatchError::RecoveryNeeded)?;
            self.active
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&request.task.task_id);
            let _ = pending.claimed.send(());
            let task = request.task.clone();
            let target = request.target.clone();
            let delivery_id = request.delivery_id;
            let attempt = request.attempt;
            let dispatcher = self.clone();
            let (result_tx, result_rx) = tokio::sync::oneshot::channel();
            let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel(1);
            self.active
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(task.task_id, cancel_tx);
            let supervisor = tokio::spawn(async move {
                let owned = DispatchRequest {
                    task: &task,
                    target: &target,
                    delivery_id,
                    attempt,
                };
                let outcome = dispatcher
                    .execute_loop(&owned, pending.lease, true, &mut cancel_rx)
                    .await;
                dispatcher
                    .active
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&task.task_id);
                match outcome {
                    Ok(LoopOutcome::Deferred(deferred)) => {
                        let (command_tx, command_rx) = tokio::sync::oneshot::channel();
                        let capability = FinalizationCapability {
                            task_id: task.task_id,
                            command: Some(command_tx),
                        };
                        if result_tx
                            .send(Ok(PreparedExecution {
                                outcome: deferred.outcome.clone(),
                                capability,
                            }))
                            .is_err()
                        {
                            let _ = dispatcher.dispatcher.shutdown().await;
                            let _ = dispatcher
                                .store
                                .finish(
                                    &deferred.lease,
                                    deferred.revision,
                                    ContinuationState::RecoveryNeeded,
                                    0,
                                    dispatcher.clock.now(),
                                )
                                .await;
                            return;
                        }
                        match command_rx.await {
                            Ok(command) if command.task_id == task.task_id => {
                                let result = dispatcher
                                    .store
                                    .finalize_terminal(
                                        &deferred.lease,
                                        deferred.revision,
                                        deferred.continuation_state,
                                        deferred.observed_bytes,
                                        &command.event,
                                        dispatcher.clock.now(),
                                    )
                                    .await
                                    .unwrap_or(FinalizeResult::RecoveryNeeded);
                                let _ = command.response.send(result);
                            }
                            _ => {
                                let _ = dispatcher.dispatcher.shutdown().await;
                                let _ = dispatcher
                                    .store
                                    .finish(
                                        &deferred.lease,
                                        deferred.revision,
                                        ContinuationState::RecoveryNeeded,
                                        0,
                                        dispatcher.clock.now(),
                                    )
                                    .await;
                            }
                        }
                    }
                    Ok(LoopOutcome::Finished(_)) => {
                        let _ = result_tx.send(Err(DispatchError::RecoveryNeeded));
                    }
                    Ok(LoopOutcome::Cancelled(deferred, cancel_response)) => {
                        let (command_tx, command_rx) = tokio::sync::oneshot::channel();
                        let capability = FinalizationCapability {
                            task_id: task.task_id,
                            command: Some(command_tx),
                        };
                        let _ = result_tx.send(Err(DispatchError::RecoveryNeeded));
                        if cancel_response.send(Ok(capability)).is_err() {
                            let _ = dispatcher
                                .store
                                .finish(
                                    &deferred.lease,
                                    deferred.revision,
                                    ContinuationState::RecoveryNeeded,
                                    0,
                                    dispatcher.clock.now(),
                                )
                                .await;
                            return;
                        }
                        match command_rx.await {
                            Ok(command) if command.task_id == task.task_id => {
                                let result = dispatcher
                                    .store
                                    .finalize_terminal(
                                        &deferred.lease,
                                        deferred.revision,
                                        deferred.continuation_state,
                                        0,
                                        &command.event,
                                        dispatcher.clock.now(),
                                    )
                                    .await
                                    .unwrap_or(FinalizeResult::RecoveryNeeded);
                                let _ = command.response.send(result);
                            }
                            _ => {
                                let _ = dispatcher
                                    .store
                                    .finish(
                                        &deferred.lease,
                                        deferred.revision,
                                        ContinuationState::RecoveryNeeded,
                                        0,
                                        dispatcher.clock.now(),
                                    )
                                    .await;
                            }
                        }
                    }
                    Err(error) => {
                        let _ = result_tx.send(Err(error));
                    }
                }
            });
            self.cleanup
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(supervisor);
            result_rx
                .await
                .unwrap_or(Err(DispatchError::RecoveryNeeded))
        })
    }

    fn cancel_prepared<'a>(
        &'a self,
        request: DispatchRequest<'a>,
        reason: &'a str,
    ) -> BusFuture<'a, Result<FinalizationCapability, DispatchError>> {
        Box::pin(async move {
            let sender = self
                .active
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&request.task.task_id)
                .cloned()
                .ok_or(DispatchError::RecoveryNeeded)?;
            let (response, receiver) = tokio::sync::oneshot::channel();
            sender
                .send(SupervisorCommand {
                    reason: crate::acp::bounded(reason),
                    response,
                })
                .await
                .map_err(|_| DispatchError::RecoveryNeeded)?;
            receiver.await.unwrap_or(Err(DispatchError::RecoveryNeeded))
        })
    }
}

fn bounded_prompt(v: &str) -> Result<&str, RuntimeError> {
    if v.is_empty() || v.len() > MAX_CONTINUATION_PROMPT_BYTES {
        Err(RuntimeError::Continuation)
    } else {
        Ok(v)
    }
}

fn terminal_reply_body(payload: &crate::models::TaskEventPayload) -> Option<String> {
    use crate::models::TaskEventPayload;
    let body = match payload {
        TaskEventPayload::Completed { output } => output.clone(),
        TaskEventPayload::Failed { error } => error.clone(),
        TaskEventPayload::TimedOut { .. } => "The task timed out.".into(),
        TaskEventPayload::Cancelled { reason } => reason.clone(),
        TaskEventPayload::Queued
        | TaskEventPayload::Dispatched { .. }
        | TaskEventPayload::Running { .. } => return None,
    };
    Some(bounded_outbox(body))
}

fn bounded_outbox(mut body: String) -> String {
    if body.len() <= MAX_OUTBOX_BODY_BYTES {
        return body;
    }
    let mut boundary = MAX_OUTBOX_BODY_BYTES.saturating_sub(3);
    while !body.is_char_boundary(boundary) {
        boundary = boundary.saturating_sub(1);
    }
    body.truncate(boundary);
    body.push_str("...");
    body
}

#[allow(clippy::too_many_arguments)]
async fn insert_terminal_projection(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event: &crate::models::TaskEvent,
    projection: &str,
    room: &str,
    thread: Option<&str>,
    reply: Option<&str>,
    monitor_generation: Option<i64>,
    render_version: &str,
    body: &str,
    now: DateTime<Utc>,
) -> Result<(), RuntimeError> {
    let stable_txn_id = Uuid::new_v5(
        &TERMINAL_OUTBOX_NAMESPACE,
        format!("task_event\0{}\0{projection}", event.id).as_bytes(),
    )
    .to_string();
    let body_hash = Uuid::new_v5(&OUTBOX_BODY_NAMESPACE, body.as_bytes()).to_string();
    sqlx::query("INSERT INTO projection_outbox(source_kind,source_id,projection,transport,stable_txn_id,room_id,thread_root,reply_event_id,monitor_generation,render_version,body,body_hash,state,attempt_count,created_at,updated_at) VALUES('task_event',?,?, 'matrix',?,?,?,?,?,?,?,?,'pending',0,?,?)")
        .bind(event.id.to_string())
        .bind(projection)
        .bind(stable_txn_id)
        .bind(room)
        .bind(thread)
        .bind(reply)
        .bind(monitor_generation)
        .bind(render_version)
        .bind(body)
        .bind(body_hash)
        .bind(ts(now))
        .bind(ts(now))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn ts(v: DateTime<Utc>) -> String {
    v.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}
fn add(now: DateTime<Utc>, v: Duration) -> Result<DateTime<Utc>, RuntimeError> {
    now.checked_add_signed(chrono::Duration::from_std(v).map_err(|_| RuntimeError::Malformed)?)
        .ok_or(RuntimeError::Malformed)
}
fn to_i64(v: u64) -> Result<i64, RuntimeError> {
    i64::try_from(v).map_err(|_| RuntimeError::Malformed)
}
fn count(row: &sqlx::sqlite::SqliteRow, name: &str) -> Result<u64, RuntimeError> {
    u64::try_from(
        row.try_get::<i64, _>(name)
            .map_err(|_| RuntimeError::Malformed)?,
    )
    .map_err(|_| RuntimeError::Malformed)
}
fn parse_id<T: FromStr>(row: &sqlx::sqlite::SqliteRow, name: &str) -> Result<T, RuntimeError> {
    row.try_get::<String, _>(name)
        .map_err(|_| RuntimeError::Malformed)?
        .parse()
        .map_err(|_| RuntimeError::Malformed)
}
fn parse_time(row: &sqlx::sqlite::SqliteRow, name: &str) -> Result<DateTime<Utc>, RuntimeError> {
    row.try_get::<String, _>(name)
        .map_err(|_| RuntimeError::Malformed)?
        .parse()
        .map_err(|_| RuntimeError::Malformed)
}
fn decode_continuation(row: &sqlx::sqlite::SqliteRow) -> Result<Continuation, RuntimeError> {
    let resource = ExecutionResourceKey(
        Uuid::parse_str(
            &row.try_get::<String, _>("resource_key")
                .map_err(|_| RuntimeError::Malformed)?,
        )
        .map_err(|_| RuntimeError::Malformed)?,
    );
    let state = match row
        .try_get::<String, _>("state")
        .map_err(|_| RuntimeError::Malformed)?
        .as_str()
    {
        "ready" => ContinuationState::Ready,
        "in_flight" => ContinuationState::InFlight,
        "recovery_needed" => ContinuationState::RecoveryNeeded,
        "blocked" => ContinuationState::Blocked,
        "terminal" => ContinuationState::Terminal,
        _ => return Err(RuntimeError::Malformed),
    };
    Ok(Continuation {
        task_id: parse_id(row, "task_id")?,
        resource,
        delivery_id: parse_id(row, "delivery_id")?,
        lease_fence: count(row, "lease_fence")?,
        revision: count(row, "revision")?,
        state,
        next_turn: u32::try_from(count(row, "next_turn")?).map_err(|_| RuntimeError::Malformed)?,
        completed_turns: u32::try_from(count(row, "completed_turns")?)
            .map_err(|_| RuntimeError::Malformed)?,
        consecutive_no_progress: u32::try_from(count(row, "consecutive_no_progress")?)
            .map_err(|_| RuntimeError::Malformed)?,
        next_prompt: row
            .try_get("next_prompt")
            .map_err(|_| RuntimeError::Malformed)?,
        started_at: parse_time(row, "started_at")?,
        heartbeat_at: parse_time(row, "heartbeat_at")?,
        last_progress_at: parse_time(row, "last_progress_at")?,
        observed_output_bytes: count(row, "observed_output_bytes")?,
        runtime_generation: row
            .try_get("runtime_generation")
            .map_err(|_| RuntimeError::Malformed)?,
    })
}
