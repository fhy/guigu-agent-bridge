//! The `Repository` boundary: the public storage contract.
//!
//! This trait is **frozen by T008** (design gate D7/Q2=A) and implemented by
//! T009. The signatures below are the contract; every method body belongs to
//! T009, which is why this module defines no `Storage` type either.
//!
//! # Shape
//!
//! - Every method takes `&self`. `Repository` implementations hold a
//!   [`sqlx::SqlitePool`], which is internally synchronised, so
//!   `Arc<dyn Repository>` can be shared by the T009 event consumer and the
//!   T011/T012/T014 call paths with **no lock of our own** — and therefore no
//!   lock-ordering or lock-while-awaiting hazard.
//! - Every method returns [`StorageFuture`], a boxed `Send` future, mirroring
//!   `BusFuture`. This keeps the trait object-safe (`Arc<dyn Repository>`) and
//!   `Send + Sync`, consistent with `Bus`/`EventSink`/`TaskDispatcher`/
//!   `EventConsumer`.
//! - Transaction boundaries stay **inside** the implementation. Leaking a
//!   transaction into the signature would put `sqlx` types into the public API
//!   and make every caller responsible for commit/rollback. The cost is that
//!   atomicity across two logical operations is T009's business: if "insert the
//!   task and append its first event" must be atomic, T009 exposes that as one
//!   method rather than expecting callers to open a transaction.
//!
//! # Contracts for implementers (T009)
//!
//! - **Foreign-key write order** (`agents` → `conversations`/`messages` →
//!   `tasks` → `task_events`/`deliveries`). `tasks` rows cannot be rebuilt from
//!   events, so the submission path must insert the task before its first event.
//! - **`append_event` is append-only.** A repeated `event_id` or
//!   `(task_id, seq)` is [`StorageError::Duplicate`]. Whether that is tolerated
//!   or propagated is T009's policy; the constraint and the classification are
//!   what T008 freezes.
//! - **No external I/O while a write transaction is open.** SQLite has one
//!   writer; awaiting Matrix/ACP/network work inside a transaction starves every
//!   other writer. Commit first, then await.
//! - **Calls are awaited to completion.** T008 defines no cancellation or
//!   timeout around storage; `sqlx` compensates for a dropped request by retiring
//!   the affected connection, but retry/cancel policy is not part of this
//!   contract.
//! - **`Vec`-returning methods collect the full result.** Each SQLite connection
//!   has a bounded row channel; partial/streaming results must never leak to
//!   callers.
//! - **Arrival order is not `seq` order** (T005 Q1=A). `events_for_task` orders by
//!   `seq`; callers must group by `task_id` and must not assume contiguity.
//!
//! # Frozen semantics of the recovery/query helpers
//!
//! - `unfinished_tasks`: task ids whose latest event status is not one of
//!   `completed`/`failed`/`timed_out`/`cancelled`, including task rows with no
//!   events yet.
//! - `unacknowledged_deliveries`: rows with `acknowledged_at IS NULL` — the
//!   retry input.
//! - `deliveries_awaiting_outcome`: rows with `acknowledged_at IS NOT NULL` whose
//!   task has no terminal latest event — the restart-recovery input.
//! - `compare_and_increment_version`: succeeds only when `expected_version`
//!   matches the stored one, returning the new version; a mismatch is
//!   [`StorageError::Duplicate`] (conditional-update conflict, not corruption).
//! - `acknowledge_delivery`: idempotent. The first call for a delivery returns
//!   [`AckOutcome::Recorded`]; a replay returns
//!   [`AckOutcome::AlreadyAcknowledged`]. An unknown delivery id is
//!   [`StorageError::NotFound`].

use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use super::BusinessStoreOwner;
use chrono::{DateTime, Utc};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};

use crate::models::{
    AgentEndpoint, AgentTask, Conversation, ConversationId, DeliveryId, EndpointId, ExternalRef,
    Message, MessageId, TaskEvent, TaskId,
};
use crate::storage::StorageError;
use crate::storage::codec::{
    decode_bool, decode_id, decode_json, decode_optional_timestamp, decode_priority, decode_status,
    decode_timestamp, decode_transport, decode_u32, decode_u64, encode_bool, encode_id,
    encode_json, encode_optional_timestamp, encode_priority, encode_status, encode_timestamp,
    encode_transport, encode_u32, encode_u64,
};

/// A boxed, `Send` future returned by [`Repository`] methods.
///
/// Same shape as `BusFuture`: used instead of native `async fn` in traits so the
/// trait stays object-safe (`Arc<dyn Repository>`).
pub type StorageFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The storage boundary: durable reads and writes over the event store.
///
/// Implemented by T009 (`Storage`), consumed by T009/T011/T012/T014/T018. See
/// the module documentation for the contracts every implementation must honour.
pub trait Repository: Send + Sync {
    // ── agents: the configuration snapshot ───────────────────────────────────

    /// Insert or update the endpoint declared under `agent_id`.
    ///
    /// `agent_id` is the ADR-003 derivation input; the endpoint identity is
    /// `endpoint.id`, which the caller obtained from
    /// `crate::bus::registry::derive_endpoint_id`. Upserting keeps the stored
    /// snapshot in step with configuration without deleting history.
    fn upsert_agent<'a>(
        &'a self,
        agent_id: &'a str,
        endpoint: &'a AgentEndpoint,
    ) -> StorageFuture<'a, Result<(), StorageError>>;

    /// Fetch one endpoint by identity.
    fn get_agent<'a>(
        &'a self,
        id: EndpointId,
    ) -> StorageFuture<'a, Result<Option<AgentEndpoint>, StorageError>>;

    /// Fetch every stored endpoint.
    fn agents<'a>(&'a self) -> StorageFuture<'a, Result<Vec<AgentEndpoint>, StorageError>>;

    // ── conversations ───────────────────────────────────────────────────────

    /// Insert a conversation.
    fn insert_conversation<'a>(
        &'a self,
        conversation: &'a Conversation,
    ) -> StorageFuture<'a, Result<(), StorageError>>;

    /// Fetch a conversation by identity.
    fn get_conversation<'a>(
        &'a self,
        id: ConversationId,
    ) -> StorageFuture<'a, Result<Option<Conversation>, StorageError>>;

    /// Resolve the conversation linked to an external room / thread / session.
    fn conversation_by_external_ref<'a>(
        &'a self,
        reference: &'a ExternalRef,
    ) -> StorageFuture<'a, Result<Option<Conversation>, StorageError>>;

    // ── messages ────────────────────────────────────────────────────────────

    /// Insert a message.
    fn insert_message<'a>(
        &'a self,
        message: &'a Message,
    ) -> StorageFuture<'a, Result<(), StorageError>>;

    /// Fetch a message by identity.
    fn get_message<'a>(
        &'a self,
        id: MessageId,
    ) -> StorageFuture<'a, Result<Option<Message>, StorageError>>;

    /// Fetch a conversation's messages in a stable order.
    fn messages_in_conversation<'a>(
        &'a self,
        id: ConversationId,
    ) -> StorageFuture<'a, Result<Vec<Message>, StorageError>>;

    // ── tasks: immutable rows, status derived from events ───────────────────

    /// Insert a task row. Must precede the task's first appended event.
    fn insert_task<'a>(
        &'a self,
        task: &'a AgentTask,
    ) -> StorageFuture<'a, Result<(), StorageError>>;

    /// Fetch a task by identity.
    fn get_task<'a>(
        &'a self,
        id: TaskId,
    ) -> StorageFuture<'a, Result<Option<AgentTask>, StorageError>>;

    /// The direct children of a task (task-tree / call-chain projection).
    fn child_tasks<'a>(
        &'a self,
        parent: TaskId,
    ) -> StorageFuture<'a, Result<Vec<TaskId>, StorageError>>;

    /// Every task without a terminal latest event (restart-recovery input).
    fn unfinished_tasks<'a>(&'a self) -> StorageFuture<'a, Result<Vec<TaskId>, StorageError>>;

    // ── task_events: the append-only log ────────────────────────────────────

    /// Append one immutable event.
    fn append_event<'a>(
        &'a self,
        event: &'a TaskEvent,
    ) -> StorageFuture<'a, Result<(), StorageError>>;

    /// All events of one task, ordered by `seq` ascending.
    fn events_for_task<'a>(
        &'a self,
        task_id: TaskId,
    ) -> StorageFuture<'a, Result<Vec<TaskEvent>, StorageError>>;

    /// The event with the highest `seq` for a task, if any.
    fn latest_event<'a>(
        &'a self,
        task_id: TaskId,
    ) -> StorageFuture<'a, Result<Option<TaskEvent>, StorageError>>;

    // ── optimistic concurrency ──────────────────────────────────────────────

    /// Increment a task's version if it still equals `expected_version`.
    ///
    /// Returns the new version on success; a mismatch is
    /// [`StorageError::Duplicate`].
    fn compare_and_increment_version<'a>(
        &'a self,
        task_id: TaskId,
        expected_version: u64,
    ) -> StorageFuture<'a, Result<u64, StorageError>>;

    // ── deliveries: dispatch is not acknowledgement ─────────────────────────

    /// Record a delivery attempt.
    fn record_delivery<'a>(
        &'a self,
        delivery: &'a Delivery,
    ) -> StorageFuture<'a, Result<(), StorageError>>;

    /// Acknowledge a delivery. Idempotent; see the module documentation.
    fn acknowledge_delivery<'a>(
        &'a self,
        delivery_id: DeliveryId,
        at: DateTime<Utc>,
    ) -> StorageFuture<'a, Result<AckOutcome, StorageError>>;

    /// Fetch a delivery by identity.
    fn get_delivery<'a>(
        &'a self,
        id: DeliveryId,
    ) -> StorageFuture<'a, Result<Option<Delivery>, StorageError>>;

    /// Deliveries that were never acknowledged (retry input).
    fn unacknowledged_deliveries<'a>(
        &'a self,
    ) -> StorageFuture<'a, Result<Vec<Delivery>, StorageError>>;

    /// Acknowledged deliveries whose task has not reached a terminal state
    /// (restart-recovery input).
    fn deliveries_awaiting_outcome<'a>(
        &'a self,
    ) -> StorageFuture<'a, Result<Vec<Delivery>, StorageError>>;
}

/// One delivery attempt: when it was dispatched and whether it was acknowledged.
///
/// The model layer has no delivery structure (only `DeliveryId` inside a
/// payload), so storage owns this type. Fields are private and read through
/// accessors, matching the `RegisteredEndpoint` precedent: a row is materialised
/// once and cannot be half-updated afterwards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    delivery_id: DeliveryId,
    task_id: TaskId,
    attempt: u32,
    target: EndpointId,
    dispatched_at: DateTime<Utc>,
    acknowledged_at: Option<DateTime<Utc>>,
}

impl Delivery {
    /// Create a freshly dispatched, not-yet-acknowledged delivery.
    ///
    /// `attempt` starts at 1 (matching `TaskEventPayload::Dispatched`); retries
    /// with `attempt > 1` are T006's concern.
    pub fn new(
        delivery_id: DeliveryId,
        task_id: TaskId,
        attempt: u32,
        target: EndpointId,
        dispatched_at: DateTime<Utc>,
    ) -> Self {
        Self {
            delivery_id,
            task_id,
            attempt,
            target,
            dispatched_at,
            acknowledged_at: None,
        }
    }

    /// The delivery attempt identity.
    pub fn delivery_id(&self) -> DeliveryId {
        self.delivery_id
    }

    /// The task that was delivered.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// The attempt number (1-based).
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The endpoint the task was delivered to.
    pub fn target(&self) -> EndpointId {
        self.target
    }

    /// When the attempt was dispatched.
    pub fn dispatched_at(&self) -> DateTime<Utc> {
        self.dispatched_at
    }

    /// When the attempt was acknowledged, if it was.
    pub fn acknowledged_at(&self) -> Option<DateTime<Utc>> {
        self.acknowledged_at
    }

    /// Whether this attempt has been acknowledged.
    pub fn is_acknowledged(&self) -> bool {
        self.acknowledged_at.is_some()
    }

    /// Return this delivery with its acknowledgement set.
    ///
    /// Used by T009 when materialising a stored row; not part of the public
    /// contract.
    pub(crate) fn with_acknowledged_at(self, at: Option<DateTime<Utc>>) -> Self {
        Self {
            acknowledged_at: at,
            ..self
        }
    }
}

/// The result of [`Repository::acknowledge_delivery`].
///
/// `#[non_exhaustive]` so finer-grained outcomes can be added without breaking
/// downstream `match` arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AckOutcome {
    /// This call wrote the acknowledgement.
    Recorded,
    /// The delivery was already acknowledged; the call was a no-op replay.
    AlreadyAcknowledged,
}

// ── the real implementation (T009) ──────────────────────────────────────────

/// SQLite-backed [`Repository`].
///
/// Construct it from a pool that [`crate::storage::connect`] opened and
/// [`crate::storage::migrate`] migrated; that ordering is the assembly layer's
/// (T017's) responsibility, and `new` deliberately does neither so a caller can
/// distinguish "cannot open" from "cannot migrate".
///
/// # Duplicate policy (T009, gate D3/Q2 = A)
///
/// Every write classifies a `Duplicate` by **reading the conflicting row back
/// and comparing it field by field**:
///
/// - **identical** ⇒ the call was a replay ⇒ `Ok(())`, and the stored row is
///   **not** touched (the first writer wins; no write method ever upserts over
///   an existing row);
/// - **different** ⇒ [`StorageError::Duplicate`] — a defined signal for the
///   caller to read the winner back and reconcile. It is never a crash, never a
///   silent overwrite, and never a second row.
///
/// `record_delivery` compares the *dispatch facts* only, because the
/// acknowledgement is later state written by
/// [`Repository::acknowledge_delivery`]: re-recording an already-acknowledged
/// attempt is a replay of the dispatch, and it never clears the acknowledgement.
///
/// # Transaction boundaries
///
/// Every trait method is a single SQL statement, or two statements where the
/// second only *classifies* the first (`Duplicate` disambiguation, `0 rows
/// affected` disambiguation). Rows in this schema are immutable — there is no
/// `UPDATE` outside `compare_and_increment_version` and no `DELETE` at all — so
/// those classifying reads cannot observe a torn state and need no transaction.
///
/// [`SqliteRepository::insert_task_and_event`] is the one exception: the
/// submission path's "insert the task, then append its first event" pair is
/// wrapped in one transaction, so a crash cannot leave a task row without its
/// event. The trait documentation asks for exactly this
/// ([`Repository`] module docs).
#[derive(Debug, Clone)]
pub struct SqliteRepository {
    pool: SqlitePool,
    owner: Option<Arc<BusinessStoreOwner>>,
}

impl SqliteRepository {
    pub fn new_owner(owner: BusinessStoreOwner) -> Self {
        Self {
            pool: SqlitePool::connect_lazy("sqlite::memory:").expect("owner facade placeholder"),
            owner: Some(Arc::new(owner)),
        }
    }
}

/// Owner-backed repository slice used while the business adapter migrates away
/// from SQLx. The query and row decoding never expose a rusqlite handle.
pub struct OwnerRepository {
    owner: BusinessStoreOwner,
}

impl OwnerRepository {
    pub fn new(owner: BusinessStoreOwner) -> Self {
        Self { owner }
    }

    pub fn agent_exists(&self, agent_id: &str) -> Result<bool, StorageError> {
        let agent_id = agent_id.to_owned();
        self.owner.execute(move |connection| {
            let mut statement = connection
                .prepare("SELECT 1 FROM agents WHERE agent_id = ?1 LIMIT 1")
                .map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
            statement
                .exists([agent_id])
                .map_err(|error| StorageError::OwnerQuery(error.to_string()))
        })
    }

    pub fn insert_agent_raw(
        &self,
        endpoint_id: &str,
        agent_id: &str,
        transport: &str,
        enabled: bool,
    ) -> Result<(), StorageError> {
        let endpoint_id = endpoint_id.to_owned();
        let agent_id = agent_id.to_owned();
        let transport = transport.to_owned();
        self.owner.transaction(move |transaction| {
            transaction
                .execute(
                    "INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES (?1,?2,?3,?4,NULL,'[]')",
                    rusqlite::params![endpoint_id, agent_id, transport, enabled as i64],
                )
                .map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
            Ok(())
        })
    }
}

impl SqliteRepository {
    /// Wrap an opened, migrated pool.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool, owner: None }
    }

    /// Insert a task row and its first event **atomically**.
    ///
    /// The foreign key requires the task row to exist before any of its events,
    /// and `tasks` rows cannot be rebuilt from events (a `TaskEvent` carries no
    /// task body), so the submission path must write both. One transaction makes
    /// that ordering atomic: either both rows exist or neither does, which closes
    /// the crash window that would otherwise leave an orphan task row. See
    /// the `recovery` module for how orphans that predate this method are
    /// surfaced.
    ///
    /// Both writes follow the [`SqliteRepository`] duplicate policy:
    /// re-inserting an identical task and an identical event is a replay and
    /// returns `Ok(())`; a conflicting row (a different task under the same
    /// `task_id`, or a different event at the same `task_id`/`seq`) returns
    /// [`StorageError::Duplicate`] **and rolls both writes back**.
    ///
    /// # Errors
    ///
    /// [`StorageError::Malformed`] when `event` belongs to a different task than
    /// `task` — checked before the transaction opens, so the caller gets the real
    /// reason instead of a foreign-key failure.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use guigu_agent_bridge::models::{AgentTask, TaskEvent};
    /// # use guigu_agent_bridge::storage::SqliteRepository;
    /// # async fn run(repository: &SqliteRepository, task: &AgentTask, queued: &TaskEvent)
    /// #     -> Result<(), guigu_agent_bridge::storage::StorageError> {
    /// repository.insert_task_and_event(task, queued).await
    /// # }
    /// ```
    pub async fn insert_task_and_event(
        &self,
        task: &AgentTask,
        event: &TaskEvent,
    ) -> Result<(), StorageError> {
        if event.task_id != task.task_id {
            return Err(StorageError::Malformed {
                field: "task_events.task_id",
                detail: "does not match the task being inserted".to_owned(),
            });
        }

        let mut transaction = self.pool.begin().await.map_err(StorageError::from)?;

        if let Err(error) = insert_task_row(&mut *transaction, task).await {
            match classify_task_duplicate(&mut *transaction, task, error).await? {
                DuplicateVerdict::Replay => {}
                DuplicateVerdict::Propagate(error) => return Err(error),
            }
        }
        if let Err(error) = insert_event_row(&mut *transaction, event).await {
            match classify_event_duplicate(&mut *transaction, event, error).await? {
                DuplicateVerdict::Replay => {}
                DuplicateVerdict::Propagate(error) => return Err(error),
            }
        }

        transaction.commit().await.map_err(StorageError::from)
    }
}

/// The outcome of reading back a conflicting row.
enum DuplicateVerdict {
    /// The stored row equals the requested one: the call was a replay.
    Replay,
    /// The stored row differs (or the failure was not a duplicate): propagate.
    Propagate(StorageError),
}

/// Classify a failed `tasks` insert against the stored row.
async fn classify_task_duplicate<'e, E>(
    executor: E,
    task: &AgentTask,
    error: StorageError,
) -> Result<DuplicateVerdict, StorageError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    if !matches!(error, StorageError::Duplicate { .. }) {
        return Ok(DuplicateVerdict::Propagate(error));
    }
    match select_task_row(executor, task.task_id).await? {
        Some(stored) if stored == *task => Ok(DuplicateVerdict::Replay),
        _ => Ok(DuplicateVerdict::Propagate(StorageError::Duplicate {
            detail: "tasks.task_id".to_owned(),
        })),
    }
}

/// Classify a failed `task_events` insert against the stored row.
///
/// One lookup by `(task_id, seq)` covers both unique constraints: a stored event
/// equal to `event` means the same event with the same identity and position, so
/// the replay is recognised; anything else (a different event at that position, or
/// the same `event_id` at another position) is a genuine conflict.
async fn classify_event_duplicate<'e, E>(
    executor: E,
    event: &TaskEvent,
    error: StorageError,
) -> Result<DuplicateVerdict, StorageError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    if !matches!(error, StorageError::Duplicate { .. }) {
        return Ok(DuplicateVerdict::Propagate(error));
    }
    match select_event_row(executor, event.task_id, event.seq).await? {
        Some(stored) if stored == *event => Ok(DuplicateVerdict::Replay),
        _ => Ok(DuplicateVerdict::Propagate(StorageError::Duplicate {
            detail: "task_events.task_id, task_events.seq".to_owned(),
        })),
    }
}

/// Whether two rows record the same dispatch, ignoring the acknowledgement.
///
/// The acknowledgement is written later by
/// [`Repository::acknowledge_delivery`], so it is not part of the dispatch fact
/// that [`Repository::record_delivery`] replays.
fn same_dispatch(stored: &Delivery, requested: &Delivery) -> bool {
    stored.delivery_id() == requested.delivery_id()
        && stored.task_id() == requested.task_id()
        && stored.attempt() == requested.attempt()
        && stored.target() == requested.target()
        && stored.dispatched_at() == requested.dispatched_at()
}

impl Repository for SqliteRepository {
    fn upsert_agent<'a>(
        &'a self,
        agent_id: &'a str,
        endpoint: &'a AgentEndpoint,
    ) -> StorageFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            if let Some(owner) = &self.owner {
                let endpoint_id = encode_id(endpoint.id);
                let transport = encode_transport(endpoint.transport).to_owned();
                let address = encode_json(&endpoint.address, "agents.address_json")?;
                let capabilities = encode_json(&endpoint.capabilities, "agents.capabilities_json")?;
                let agent_id = agent_id.to_owned();
                let enabled = encode_bool(endpoint.enabled);
                let owner = Arc::clone(owner);
                return owner.transaction(move |tx| {
                    tx.execute(
                        "INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES (?1,?2,?3,?4,?5,?6)",
                        rusqlite::params![endpoint_id, agent_id, transport, enabled, address, capabilities],
                    )
                    .map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
                    Ok(())
                });
            }
            let address = encode_json(&endpoint.address, "agents.address_json")?;
            let capabilities = encode_json(&endpoint.capabilities, "agents.capabilities_json")?;
            let result = sqlx::query(UPSERT_AGENT)
                .bind(encode_id(endpoint.id))
                .bind(agent_id)
                .bind(encode_transport(endpoint.transport))
                .bind(encode_bool(endpoint.enabled))
                .bind(address)
                .bind(capabilities)
                .execute(&self.pool)
                .await
                .map_err(StorageError::from)?;
            if result.rows_affected() == 1 {
                return Ok(());
            }
            // The `agent_id` is already mapped to a different endpoint identity.
            // ADR-003 derives the identity from the id, so this is a caller bug
            // (or a renamed agent), never a legitimate update.
            Err(StorageError::IntegrityViolation {
                detail: "agents.endpoint_id does not match the stored identity".to_owned(),
            })
        })
    }

    fn get_agent<'a>(
        &'a self,
        id: EndpointId,
    ) -> StorageFuture<'a, Result<Option<AgentEndpoint>, StorageError>> {
        Box::pin(async move {
            if let Some(owner) = &self.owner {
                let key = encode_id(id);
                let owner = Arc::clone(owner);
                return owner.execute(move |connection| {
                    let mut statement = connection
                        .prepare("SELECT endpoint_id,transport,address_json,enabled,capabilities_json FROM agents WHERE endpoint_id=?1 AND address_json IS NOT NULL")
                        .map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
                    let mut rows = statement
                        .query([key])
                        .map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
                    let Some(row) = rows.next().map_err(|error| StorageError::OwnerQuery(error.to_string()))? else {
                        return Ok(None);
                    };
                    let endpoint_id: String = row.get(0).map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
                    let transport: String = row.get(1).map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
                    let address: String = row.get(2).map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
                    let enabled: i64 = row.get(3).map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
                    let capabilities: String = row.get(4).map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
                    Ok(Some(AgentEndpoint {
                        id: decode_id(&endpoint_id, "agents.endpoint_id")?,
                        transport: decode_transport(&transport, "agents.transport")?,
                        address: decode_json(&address, "agents.address_json")?,
                        enabled: decode_bool(enabled, "agents.enabled")?,
                        capabilities: decode_json(&capabilities, "agents.capabilities_json")?,
                    }))
                });
            }
            let row = sqlx::query(SELECT_AGENT)
                .bind(encode_id(id))
                .fetch_optional(&self.pool)
                .await
                .map_err(StorageError::from)?;
            row.as_ref().map(agent_from_row).transpose()
        })
    }

    fn agents<'a>(&'a self) -> StorageFuture<'a, Result<Vec<AgentEndpoint>, StorageError>> {
        Box::pin(async move {
            let rows = sqlx::query(SELECT_AGENTS)
                .fetch_all(&self.pool)
                .await
                .map_err(StorageError::from)?;
            rows.iter().map(agent_from_row).collect()
        })
    }

    fn insert_conversation<'a>(
        &'a self,
        conversation: &'a Conversation,
    ) -> StorageFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let (transport, external_id, thread_ref) = match &conversation.external_ref {
                Some(reference) => (
                    Some(encode_transport(reference.transport)),
                    Some(reference.external_id.as_str()),
                    reference.thread_ref.as_deref(),
                ),
                None => (None, None, None),
            };
            let participants = encode_json(
                &conversation.participants,
                "conversations.participants_json",
            )?;
            let error = match sqlx::query(INSERT_CONVERSATION)
                .bind(encode_id(conversation.id))
                .bind(transport)
                .bind(external_id)
                .bind(thread_ref)
                .bind(participants)
                .execute(&self.pool)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) => StorageError::from(error),
            };
            if !matches!(error, StorageError::Duplicate { .. }) {
                return Err(error);
            }
            // Same identity and same content ⇒ a replay. A conversation that
            // merely *shares* the external reference with the requested one is
            // deliberately not a replay: the caller's id was not persisted, so it
            // must read the winner back and reconcile (`conversation_by_external_ref`).
            match self.select_conversation_row(conversation.id).await? {
                Some(stored) if stored == *conversation => Ok(()),
                _ => Err(StorageError::Duplicate {
                    detail: "conversations external reference".to_owned(),
                }),
            }
        })
    }

    fn get_conversation<'a>(
        &'a self,
        id: ConversationId,
    ) -> StorageFuture<'a, Result<Option<Conversation>, StorageError>> {
        Box::pin(async move { self.select_conversation_row(id).await })
    }

    fn conversation_by_external_ref<'a>(
        &'a self,
        reference: &'a ExternalRef,
    ) -> StorageFuture<'a, Result<Option<Conversation>, StorageError>> {
        Box::pin(async move {
            let row = match &reference.thread_ref {
                Some(thread_ref) => {
                    sqlx::query(SELECT_CONVERSATION_BY_THREADED_REF)
                        .bind(encode_transport(reference.transport))
                        .bind(reference.external_id.as_str())
                        .bind(thread_ref.as_str())
                        .fetch_optional(&self.pool)
                        .await
                }
                None => {
                    sqlx::query(SELECT_CONVERSATION_BY_UNTHREADED_REF)
                        .bind(encode_transport(reference.transport))
                        .bind(reference.external_id.as_str())
                        .fetch_optional(&self.pool)
                        .await
                }
            }
            .map_err(StorageError::from)?;
            row.as_ref().map(conversation_from_row).transpose()
        })
    }

    fn insert_message<'a>(
        &'a self,
        message: &'a Message,
    ) -> StorageFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let metadata = encode_json(&message.metadata, "messages.metadata_json")?;
            let error = match sqlx::query(INSERT_MESSAGE)
                .bind(encode_id(message.id))
                .bind(encode_id(message.conversation))
                .bind(encode_id(message.sender))
                .bind(encode_id(message.recipient))
                .bind(message.body.as_str())
                .bind(message.reply_to.map(encode_id))
                .bind(metadata)
                .execute(&self.pool)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) => StorageError::from(error),
            };
            if !matches!(error, StorageError::Duplicate { .. }) {
                return Err(error);
            }
            match select_message_row(&self.pool, message.id).await? {
                Some(stored) if stored == *message => Ok(()),
                _ => Err(StorageError::Duplicate {
                    detail: "messages.message_id".to_owned(),
                }),
            }
        })
    }

    fn get_message<'a>(
        &'a self,
        id: MessageId,
    ) -> StorageFuture<'a, Result<Option<Message>, StorageError>> {
        Box::pin(async move { select_message_row(&self.pool, id).await })
    }

    fn messages_in_conversation<'a>(
        &'a self,
        id: ConversationId,
    ) -> StorageFuture<'a, Result<Vec<Message>, StorageError>> {
        Box::pin(async move {
            let rows = sqlx::query(SELECT_MESSAGES_IN_CONVERSATION)
                .bind(encode_id(id))
                .fetch_all(&self.pool)
                .await
                .map_err(StorageError::from)?;
            rows.iter().map(message_from_row).collect()
        })
    }

    fn insert_task<'a>(
        &'a self,
        task: &'a AgentTask,
    ) -> StorageFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            match insert_task_row(&self.pool, task).await {
                Ok(()) => Ok(()),
                Err(error) => match classify_task_duplicate(&self.pool, task, error).await? {
                    DuplicateVerdict::Replay => Ok(()),
                    DuplicateVerdict::Propagate(error) => Err(error),
                },
            }
        })
    }

    fn get_task<'a>(
        &'a self,
        id: TaskId,
    ) -> StorageFuture<'a, Result<Option<AgentTask>, StorageError>> {
        Box::pin(async move { select_task_row(&self.pool, id).await })
    }

    fn child_tasks<'a>(
        &'a self,
        parent: TaskId,
    ) -> StorageFuture<'a, Result<Vec<TaskId>, StorageError>> {
        Box::pin(async move {
            let rows = sqlx::query(SELECT_CHILD_TASKS)
                .bind(encode_id(parent))
                .fetch_all(&self.pool)
                .await
                .map_err(StorageError::from)?;
            collect_ids(&rows, "task_id", "tasks.task_id")
        })
    }

    fn unfinished_tasks<'a>(&'a self) -> StorageFuture<'a, Result<Vec<TaskId>, StorageError>> {
        Box::pin(async move {
            let rows = sqlx::query(SELECT_UNFINISHED_TASKS)
                .fetch_all(&self.pool)
                .await
                .map_err(StorageError::from)?;
            collect_ids(&rows, "task_id", "tasks.task_id")
        })
    }

    fn append_event<'a>(
        &'a self,
        event: &'a TaskEvent,
    ) -> StorageFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            match insert_event_row(&self.pool, event).await {
                Ok(()) => Ok(()),
                Err(error) => match classify_event_duplicate(&self.pool, event, error).await? {
                    DuplicateVerdict::Replay => Ok(()),
                    DuplicateVerdict::Propagate(error) => Err(error),
                },
            }
        })
    }

    fn events_for_task<'a>(
        &'a self,
        task_id: TaskId,
    ) -> StorageFuture<'a, Result<Vec<TaskEvent>, StorageError>> {
        Box::pin(async move {
            let rows = sqlx::query(SELECT_EVENTS_FOR_TASK)
                .bind(encode_id(task_id))
                .fetch_all(&self.pool)
                .await
                .map_err(StorageError::from)?;
            rows.iter().map(event_from_row).collect()
        })
    }

    fn latest_event<'a>(
        &'a self,
        task_id: TaskId,
    ) -> StorageFuture<'a, Result<Option<TaskEvent>, StorageError>> {
        Box::pin(async move {
            let row = sqlx::query(SELECT_LATEST_EVENT)
                .bind(encode_id(task_id))
                .fetch_optional(&self.pool)
                .await
                .map_err(StorageError::from)?;
            row.as_ref().map(event_from_row).transpose()
        })
    }

    fn compare_and_increment_version<'a>(
        &'a self,
        task_id: TaskId,
        expected_version: u64,
    ) -> StorageFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            // The increment itself must stay representable: SQLite `INTEGER` is
            // signed 64-bit, so the ceiling is reported instead of letting the
            // driver widen the column to REAL and fail to decode it.
            let next = expected_version
                .checked_add(1)
                .filter(|next| *next <= i64::MAX as u64)
                .ok_or_else(|| StorageError::OutOfRange {
                    field: "tasks.version",
                    value: expected_version.to_string(),
                })?;
            debug_assert!(next > expected_version);

            let row = sqlx::query(UPDATE_VERSION_IF_CURRENT)
                .bind(encode_id(task_id))
                .bind(encode_u64(expected_version, "tasks.version")?)
                .fetch_optional(&self.pool)
                .await
                .map_err(StorageError::from)?;
            if let Some(row) = row {
                return decode_u64(integer(&row, "version")?, "tasks.version");
            }
            // No row updated: either the task is unknown or the version moved. The
            // conditional `WHERE` is what makes this a lost-update-free compare.
            let stored = sqlx::query_scalar::<_, i64>(SELECT_TASK_VERSION)
                .bind(encode_id(task_id))
                .fetch_optional(&self.pool)
                .await
                .map_err(StorageError::from)?;
            match stored {
                Some(_) => Err(StorageError::Duplicate {
                    detail: "tasks.version".to_owned(),
                }),
                None => Err(StorageError::NotFound {
                    entity: "task",
                    id: encode_id(task_id),
                }),
            }
        })
    }

    fn record_delivery<'a>(
        &'a self,
        delivery: &'a Delivery,
    ) -> StorageFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let acknowledged = encode_optional_timestamp(delivery.acknowledged_at().as_ref());
            let error = match sqlx::query(INSERT_DELIVERY)
                .bind(encode_id(delivery.delivery_id()))
                .bind(encode_id(delivery.task_id()))
                .bind(encode_u32(delivery.attempt()))
                .bind(encode_id(delivery.target()))
                .bind(encode_timestamp(&delivery.dispatched_at()))
                .bind(acknowledged)
                .execute(&self.pool)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) => StorageError::from(error),
            };
            if !matches!(error, StorageError::Duplicate { .. }) {
                return Err(error);
            }
            let by_delivery = select_delivery_row(&self.pool, delivery.delivery_id()).await?;
            if let Some(stored) = &by_delivery
                && same_dispatch(stored, delivery)
            {
                return Ok(());
            }
            let by_attempt =
                select_delivery_attempt_row(&self.pool, delivery.task_id(), delivery.attempt())
                    .await?;
            if let Some(stored) = &by_attempt
                && same_dispatch(stored, delivery)
            {
                return Ok(());
            }
            Err(StorageError::Duplicate {
                detail: "deliveries.task_id, deliveries.attempt".to_owned(),
            })
        })
    }

    fn acknowledge_delivery<'a>(
        &'a self,
        delivery_id: DeliveryId,
        at: DateTime<Utc>,
    ) -> StorageFuture<'a, Result<AckOutcome, StorageError>> {
        Box::pin(async move {
            let id = encode_id(delivery_id);
            let result = sqlx::query(ACKNOWLEDGE_DELIVERY)
                .bind(encode_timestamp(&at))
                .bind(&id)
                .execute(&self.pool)
                .await
                .map_err(StorageError::from)?;
            if result.rows_affected() == 1 {
                return Ok(AckOutcome::Recorded);
            }
            // Zero rows: either already acknowledged (the first timestamp stands)
            // or the delivery does not exist. Nothing deletes rows, so a single
            // read settles it.
            let stored = sqlx::query_scalar::<_, Option<String>>(SELECT_DELIVERY_ACKNOWLEDGEMENT)
                .bind(&id)
                .fetch_optional(&self.pool)
                .await
                .map_err(StorageError::from)?;
            match stored {
                Some(_) => Ok(AckOutcome::AlreadyAcknowledged),
                None => Err(StorageError::NotFound {
                    entity: "delivery",
                    id,
                }),
            }
        })
    }

    fn get_delivery<'a>(
        &'a self,
        id: DeliveryId,
    ) -> StorageFuture<'a, Result<Option<Delivery>, StorageError>> {
        Box::pin(async move { select_delivery_row(&self.pool, id).await })
    }

    fn unacknowledged_deliveries<'a>(
        &'a self,
    ) -> StorageFuture<'a, Result<Vec<Delivery>, StorageError>> {
        Box::pin(async move {
            let rows = sqlx::query(SELECT_UNACKNOWLEDGED_DELIVERIES)
                .fetch_all(&self.pool)
                .await
                .map_err(StorageError::from)?;
            rows.iter().map(delivery_from_row).collect()
        })
    }

    fn deliveries_awaiting_outcome<'a>(
        &'a self,
    ) -> StorageFuture<'a, Result<Vec<Delivery>, StorageError>> {
        Box::pin(async move {
            let rows = sqlx::query(SELECT_DELIVERIES_AWAITING_OUTCOME)
                .fetch_all(&self.pool)
                .await
                .map_err(StorageError::from)?;
            rows.iter().map(delivery_from_row).collect()
        })
    }
}

impl SqliteRepository {
    /// Fetch one conversation row by identity (shared by two trait methods).
    async fn select_conversation_row(
        &self,
        id: ConversationId,
    ) -> Result<Option<Conversation>, StorageError> {
        let row = sqlx::query(SELECT_CONVERSATION)
            .bind(encode_id(id))
            .fetch_optional(&self.pool)
            .await
            .map_err(StorageError::from)?;
        row.as_ref().map(conversation_from_row).transpose()
    }
}

// ── statements ──────────────────────────────────────────────────────────────
//
// Every column list is explicit: the schema is frozen, and naming the columns
// keeps a future appended migration from silently reordering a `SELECT *`.

const UPSERT_AGENT: &str = "\
    INSERT INTO agents (endpoint_id, agent_id, transport, enabled, address_json, capabilities_json) \
    VALUES (?, ?, ?, ?, ?, ?) \
    ON CONFLICT (agent_id) DO UPDATE SET \
        transport = excluded.transport, \
        enabled = excluded.enabled, \
        address_json = excluded.address_json, \
        capabilities_json = excluded.capabilities_json \
    WHERE agents.endpoint_id = excluded.endpoint_id";

/// `address_json IS NOT NULL` is the model-visibility rule: a declared but
/// unaddressable endpoint (v1 `matrix`/`http`) has no `AgentEndpoint` form,
/// because the model's `address` field is mandatory. Those rows stay in the table
/// and are simply not surfaced.
const SELECT_AGENT: &str = "\
    SELECT endpoint_id, transport, enabled, address_json, capabilities_json \
    FROM agents WHERE endpoint_id = ? AND address_json IS NOT NULL";

const SELECT_AGENTS: &str = "\
    SELECT endpoint_id, transport, enabled, address_json, capabilities_json \
    FROM agents WHERE address_json IS NOT NULL ORDER BY agent_id ASC";

const INSERT_CONVERSATION: &str = "\
    INSERT INTO conversations (conversation_id, transport, external_id, thread_ref, participants_json) \
    VALUES (?, ?, ?, ?, ?)";

const SELECT_CONVERSATION: &str = "\
    SELECT conversation_id, transport, external_id, thread_ref, participants_json \
    FROM conversations WHERE conversation_id = ?";

const SELECT_CONVERSATION_BY_UNTHREADED_REF: &str = "\
    SELECT conversation_id, transport, external_id, thread_ref, participants_json \
    FROM conversations WHERE transport = ? AND external_id = ? AND thread_ref IS NULL";

const SELECT_CONVERSATION_BY_THREADED_REF: &str = "\
    SELECT conversation_id, transport, external_id, thread_ref, participants_json \
    FROM conversations WHERE transport = ? AND external_id = ? AND thread_ref = ?";

const INSERT_MESSAGE: &str = "\
    INSERT INTO messages (message_id, conversation_id, sender, recipient, body, reply_to, metadata_json) \
    VALUES (?, ?, ?, ?, ?, ?, ?)";

const SELECT_MESSAGE: &str = "\
    SELECT message_id, conversation_id, sender, recipient, body, reply_to, metadata_json \
    FROM messages WHERE message_id = ?";

/// Ordered by `message_id`: identifiers are UUIDv7, so this is also arrival
/// order, and it is stable across runs (no `rowid` dependence).
const SELECT_MESSAGES_IN_CONVERSATION: &str = "\
    SELECT message_id, conversation_id, sender, recipient, body, reply_to, metadata_json \
    FROM messages WHERE conversation_id = ? ORDER BY message_id ASC";

const INSERT_TASK: &str = "\
    INSERT INTO tasks (task_id, root_task_id, parent_task_id, from_agent, to_agent, \
                       conversation_id, reply_to, text, priority, depth, hops, deadline, version) \
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

const SELECT_TASK: &str = "\
    SELECT task_id, root_task_id, parent_task_id, from_agent, to_agent, conversation_id, \
           reply_to, text, priority, depth, hops, deadline, version \
    FROM tasks WHERE task_id = ?";

const SELECT_CHILD_TASKS: &str = "\
    SELECT task_id FROM tasks WHERE parent_task_id = ? ORDER BY task_id ASC";

/// `unfinished_tasks` (frozen semantics): the latest event is not terminal, and a
/// task with **no** events at all counts as unfinished. The `LEFT JOIN` keeps
/// orphan task rows in the result — the correlated `MAX(seq)` is `NULL` for them,
/// so `e.status IS NULL` matches.
const SELECT_UNFINISHED_TASKS: &str = "\
    SELECT t.task_id FROM tasks t \
    LEFT JOIN task_events e \
        ON e.task_id = t.task_id \
       AND e.seq = (SELECT MAX(seq) FROM task_events WHERE task_id = t.task_id) \
    WHERE e.status IS NULL \
       OR e.status NOT IN ('completed', 'failed', 'timed_out', 'cancelled') \
    ORDER BY t.task_id ASC";

const INSERT_EVENT: &str = "\
    INSERT INTO task_events (event_id, task_id, seq, status, timestamp, payload) \
    VALUES (?, ?, ?, ?, ?, ?)";

const SELECT_EVENT: &str = "\
    SELECT event_id, task_id, seq, status, timestamp, payload \
    FROM task_events WHERE task_id = ? AND seq = ?";

const SELECT_EVENTS_FOR_TASK: &str = "\
    SELECT event_id, task_id, seq, status, timestamp, payload \
    FROM task_events WHERE task_id = ? ORDER BY seq ASC";

const SELECT_LATEST_EVENT: &str = "\
    SELECT event_id, task_id, seq, status, timestamp, payload \
    FROM task_events WHERE task_id = ? ORDER BY seq DESC LIMIT 1";

const UPDATE_VERSION_IF_CURRENT: &str = "\
    UPDATE tasks SET version = version + 1 WHERE task_id = ? AND version = ? RETURNING version";

const SELECT_TASK_VERSION: &str = "SELECT version FROM tasks WHERE task_id = ?";

const INSERT_DELIVERY: &str = "\
    INSERT INTO deliveries (delivery_id, task_id, attempt, target_endpoint_id, dispatched_at, acknowledged_at) \
    VALUES (?, ?, ?, ?, ?, ?)";

const SELECT_DELIVERY: &str = "\
    SELECT delivery_id, task_id, attempt, target_endpoint_id, dispatched_at, acknowledged_at \
    FROM deliveries WHERE delivery_id = ?";

const SELECT_DELIVERY_BY_ATTEMPT: &str = "\
    SELECT delivery_id, task_id, attempt, target_endpoint_id, dispatched_at, acknowledged_at \
    FROM deliveries WHERE task_id = ? AND attempt = ?";

const ACKNOWLEDGE_DELIVERY: &str = "\
    UPDATE deliveries SET acknowledged_at = ? WHERE delivery_id = ? AND acknowledged_at IS NULL";

/// `Option<Option<String>>`: the outer `None` is "no such delivery", the inner is
/// the nullable column.
const SELECT_DELIVERY_ACKNOWLEDGEMENT: &str =
    "SELECT acknowledged_at FROM deliveries WHERE delivery_id = ?";

const SELECT_UNACKNOWLEDGED_DELIVERIES: &str = "\
    SELECT delivery_id, task_id, attempt, target_endpoint_id, dispatched_at, acknowledged_at \
    FROM deliveries WHERE acknowledged_at IS NULL \
    ORDER BY dispatched_at ASC, delivery_id ASC";

/// `deliveries_awaiting_outcome` (frozen semantics): acknowledged, and the task's
/// latest event is not terminal. A task with no events counts, matching
/// `SELECT_UNFINISHED_TASKS`.
const SELECT_DELIVERIES_AWAITING_OUTCOME: &str = "\
    SELECT d.delivery_id, d.task_id, d.attempt, d.target_endpoint_id, d.dispatched_at, d.acknowledged_at \
    FROM deliveries d \
    LEFT JOIN task_events e \
        ON e.task_id = d.task_id \
       AND e.seq = (SELECT MAX(seq) FROM task_events WHERE task_id = d.task_id) \
    WHERE d.acknowledged_at IS NOT NULL \
      AND (e.status IS NULL \
           OR e.status NOT IN ('completed', 'failed', 'timed_out', 'cancelled')) \
    ORDER BY d.dispatched_at ASC, d.delivery_id ASC";

// ── writes ──────────────────────────────────────────────────────────────────

/// Insert one `tasks` row.
async fn insert_task_row<'e, E>(executor: E, task: &AgentTask) -> Result<(), StorageError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    sqlx::query(INSERT_TASK)
        .bind(encode_id(task.task_id))
        .bind(encode_id(task.root_task_id))
        .bind(task.parent_task_id.map(encode_id))
        .bind(encode_id(task.from_agent))
        .bind(encode_id(task.to_agent))
        .bind(encode_id(task.conversation_id))
        .bind(task.reply_to.map(encode_id))
        .bind(task.text.as_str())
        .bind(encode_priority(task.priority))
        .bind(encode_u32(task.depth))
        .bind(encode_u32(task.hops))
        .bind(encode_optional_timestamp(task.deadline.as_ref()))
        .bind(encode_u64(task.version, "tasks.version")?)
        .execute(executor)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

/// Insert one `task_events` row.
async fn insert_event_row<'e, E>(executor: E, event: &TaskEvent) -> Result<(), StorageError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    sqlx::query(INSERT_EVENT)
        .bind(encode_id(event.id))
        .bind(encode_id(event.task_id))
        .bind(encode_u64(event.seq, "task_events.seq")?)
        .bind(encode_status(event.status))
        .bind(encode_timestamp(&event.timestamp))
        .bind(encode_json(&event.payload, "task_events.payload")?)
        .execute(executor)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

// ── reads ───────────────────────────────────────────────────────────────────

/// Fetch one `tasks` row.
async fn select_task_row<'e, E>(executor: E, id: TaskId) -> Result<Option<AgentTask>, StorageError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(SELECT_TASK)
        .bind(encode_id(id))
        .fetch_optional(executor)
        .await
        .map_err(StorageError::from)?;
    row.as_ref().map(task_from_row).transpose()
}

/// Fetch one `task_events` row by its per-task position.
async fn select_event_row<'e, E>(
    executor: E,
    task_id: TaskId,
    seq: u64,
) -> Result<Option<TaskEvent>, StorageError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(SELECT_EVENT)
        .bind(encode_id(task_id))
        .bind(encode_u64(seq, "task_events.seq")?)
        .fetch_optional(executor)
        .await
        .map_err(StorageError::from)?;
    row.as_ref().map(event_from_row).transpose()
}

async fn select_message_row(
    pool: &SqlitePool,
    id: MessageId,
) -> Result<Option<Message>, StorageError> {
    let row = sqlx::query(SELECT_MESSAGE)
        .bind(encode_id(id))
        .fetch_optional(pool)
        .await
        .map_err(StorageError::from)?;
    row.as_ref().map(message_from_row).transpose()
}

async fn select_delivery_row(
    pool: &SqlitePool,
    id: DeliveryId,
) -> Result<Option<Delivery>, StorageError> {
    let row = sqlx::query(SELECT_DELIVERY)
        .bind(encode_id(id))
        .fetch_optional(pool)
        .await
        .map_err(StorageError::from)?;
    row.as_ref().map(delivery_from_row).transpose()
}

async fn select_delivery_attempt_row(
    pool: &SqlitePool,
    task_id: TaskId,
    attempt: u32,
) -> Result<Option<Delivery>, StorageError> {
    let row = sqlx::query(SELECT_DELIVERY_BY_ATTEMPT)
        .bind(encode_id(task_id))
        .bind(encode_u32(attempt))
        .fetch_optional(pool)
        .await
        .map_err(StorageError::from)?;
    row.as_ref().map(delivery_from_row).transpose()
}

// ── row materialisation ─────────────────────────────────────────────────────

fn text(row: &SqliteRow, column: &'static str) -> Result<String, StorageError> {
    row.try_get::<String, _>(column).map_err(StorageError::from)
}

fn optional_text(row: &SqliteRow, column: &'static str) -> Result<Option<String>, StorageError> {
    row.try_get::<Option<String>, _>(column)
        .map_err(StorageError::from)
}

fn integer(row: &SqliteRow, column: &'static str) -> Result<i64, StorageError> {
    row.try_get::<i64, _>(column).map_err(StorageError::from)
}

/// Decode an ID column, naming the column in the error.
fn id<T: FromStr>(
    row: &SqliteRow,
    column: &'static str,
    field: &'static str,
) -> Result<T, StorageError> {
    decode_id(&text(row, column)?, field)
}

fn collect_ids<T: FromStr>(
    rows: &[SqliteRow],
    column: &'static str,
    field: &'static str,
) -> Result<Vec<T>, StorageError> {
    rows.iter().map(|row| id(row, column, field)).collect()
}

fn agent_from_row(row: &SqliteRow) -> Result<AgentEndpoint, StorageError> {
    Ok(AgentEndpoint {
        id: id(row, "endpoint_id", "agents.endpoint_id")?,
        transport: decode_transport(&text(row, "transport")?, "agents.transport")?,
        address: decode_json(&text(row, "address_json")?, "agents.address_json")?,
        enabled: decode_bool(integer(row, "enabled")?, "agents.enabled")?,
        capabilities: decode_json(&text(row, "capabilities_json")?, "agents.capabilities_json")?,
    })
}

fn conversation_from_row(row: &SqliteRow) -> Result<Conversation, StorageError> {
    let transport = optional_text(row, "transport")?;
    let external_id = optional_text(row, "external_id")?;
    let thread_ref = optional_text(row, "thread_ref")?;
    let external_ref = match (transport, external_id) {
        (Some(transport), Some(external_id)) => Some(ExternalRef {
            transport: decode_transport(&transport, "conversations.transport")?,
            external_id,
            thread_ref,
        }),
        (None, None) => None,
        // The schema's CHECK makes a partially-NULL reference unrepresentable;
        // this arm exists so that a row written by an external tool fails loudly
        // instead of silently losing the reference.
        _ => {
            return Err(StorageError::Malformed {
                field: "conversations.transport",
                detail: "external reference is partially NULL".to_owned(),
            });
        }
    };
    Ok(Conversation {
        id: id(row, "conversation_id", "conversations.conversation_id")?,
        participants: decode_json(
            &text(row, "participants_json")?,
            "conversations.participants_json",
        )?,
        external_ref,
    })
}

fn message_from_row(row: &SqliteRow) -> Result<Message, StorageError> {
    Ok(Message {
        id: id(row, "message_id", "messages.message_id")?,
        conversation: id(row, "conversation_id", "messages.conversation_id")?,
        sender: id(row, "sender", "messages.sender")?,
        recipient: id(row, "recipient", "messages.recipient")?,
        body: text(row, "body")?,
        reply_to: optional_text(row, "reply_to")?
            .map(|value| decode_id(&value, "messages.reply_to"))
            .transpose()?,
        metadata: decode_json(&text(row, "metadata_json")?, "messages.metadata_json")?,
    })
}

fn task_from_row(row: &SqliteRow) -> Result<AgentTask, StorageError> {
    Ok(AgentTask {
        task_id: id(row, "task_id", "tasks.task_id")?,
        root_task_id: id(row, "root_task_id", "tasks.root_task_id")?,
        parent_task_id: optional_text(row, "parent_task_id")?
            .map(|value| decode_id(&value, "tasks.parent_task_id"))
            .transpose()?,
        from_agent: id(row, "from_agent", "tasks.from_agent")?,
        to_agent: id(row, "to_agent", "tasks.to_agent")?,
        conversation_id: id(row, "conversation_id", "tasks.conversation_id")?,
        reply_to: optional_text(row, "reply_to")?
            .map(|value| decode_id(&value, "tasks.reply_to"))
            .transpose()?,
        text: text(row, "text")?,
        priority: decode_priority(integer(row, "priority")?, "tasks.priority")?,
        depth: decode_u32(integer(row, "depth")?, "tasks.depth")?,
        hops: decode_u32(integer(row, "hops")?, "tasks.hops")?,
        deadline: decode_optional_timestamp(
            optional_text(row, "deadline")?.as_deref(),
            "tasks.deadline",
        )?,
        version: decode_u64(integer(row, "version")?, "tasks.version")?,
    })
}

fn event_from_row(row: &SqliteRow) -> Result<TaskEvent, StorageError> {
    Ok(TaskEvent {
        id: id(row, "event_id", "task_events.event_id")?,
        task_id: id(row, "task_id", "task_events.task_id")?,
        seq: decode_u64(integer(row, "seq")?, "task_events.seq")?,
        status: decode_status(&text(row, "status")?, "task_events.status")?,
        timestamp: decode_timestamp(&text(row, "timestamp")?, "task_events.timestamp")?,
        payload: decode_json(&text(row, "payload")?, "task_events.payload")?,
    })
}

fn delivery_from_row(row: &SqliteRow) -> Result<Delivery, StorageError> {
    let dispatched = Delivery::new(
        id(row, "delivery_id", "deliveries.delivery_id")?,
        id(row, "task_id", "deliveries.task_id")?,
        decode_u32(integer(row, "attempt")?, "deliveries.attempt")?,
        id(row, "target_endpoint_id", "deliveries.target_endpoint_id")?,
        decode_timestamp(&text(row, "dispatched_at")?, "deliveries.dispatched_at")?,
    );
    let acknowledged = decode_optional_timestamp(
        optional_text(row, "acknowledged_at")?.as_deref(),
        "deliveries.acknowledged_at",
    )?;
    Ok(dispatched.with_acknowledged_at(acknowledged))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A test double that exercises the frozen signatures.
    ///
    /// T008 ships no production `Repository` implementation (that is T009); this
    /// double proves the trait is implementable and object-safe.
    struct StubRepository;

    impl Repository for StubRepository {
        fn upsert_agent<'a>(
            &'a self,
            _agent_id: &'a str,
            _endpoint: &'a AgentEndpoint,
        ) -> StorageFuture<'a, Result<(), StorageError>> {
            Box::pin(async { Ok(()) })
        }

        fn get_agent<'a>(
            &'a self,
            _id: EndpointId,
        ) -> StorageFuture<'a, Result<Option<AgentEndpoint>, StorageError>> {
            Box::pin(async { Ok(None) })
        }

        fn agents<'a>(&'a self) -> StorageFuture<'a, Result<Vec<AgentEndpoint>, StorageError>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn insert_conversation<'a>(
            &'a self,
            _conversation: &'a Conversation,
        ) -> StorageFuture<'a, Result<(), StorageError>> {
            Box::pin(async { Ok(()) })
        }

        fn get_conversation<'a>(
            &'a self,
            _id: ConversationId,
        ) -> StorageFuture<'a, Result<Option<Conversation>, StorageError>> {
            Box::pin(async { Ok(None) })
        }

        fn conversation_by_external_ref<'a>(
            &'a self,
            _reference: &'a ExternalRef,
        ) -> StorageFuture<'a, Result<Option<Conversation>, StorageError>> {
            Box::pin(async { Ok(None) })
        }

        fn insert_message<'a>(
            &'a self,
            _message: &'a Message,
        ) -> StorageFuture<'a, Result<(), StorageError>> {
            Box::pin(async { Ok(()) })
        }

        fn get_message<'a>(
            &'a self,
            _id: MessageId,
        ) -> StorageFuture<'a, Result<Option<Message>, StorageError>> {
            Box::pin(async { Ok(None) })
        }

        fn messages_in_conversation<'a>(
            &'a self,
            _id: ConversationId,
        ) -> StorageFuture<'a, Result<Vec<Message>, StorageError>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn insert_task<'a>(
            &'a self,
            _task: &'a AgentTask,
        ) -> StorageFuture<'a, Result<(), StorageError>> {
            Box::pin(async { Ok(()) })
        }

        fn get_task<'a>(
            &'a self,
            _id: TaskId,
        ) -> StorageFuture<'a, Result<Option<AgentTask>, StorageError>> {
            Box::pin(async { Ok(None) })
        }

        fn child_tasks<'a>(
            &'a self,
            _parent: TaskId,
        ) -> StorageFuture<'a, Result<Vec<TaskId>, StorageError>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn unfinished_tasks<'a>(&'a self) -> StorageFuture<'a, Result<Vec<TaskId>, StorageError>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn append_event<'a>(
            &'a self,
            _event: &'a TaskEvent,
        ) -> StorageFuture<'a, Result<(), StorageError>> {
            Box::pin(async { Ok(()) })
        }

        fn events_for_task<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> StorageFuture<'a, Result<Vec<TaskEvent>, StorageError>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn latest_event<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> StorageFuture<'a, Result<Option<TaskEvent>, StorageError>> {
            Box::pin(async { Ok(None) })
        }

        fn compare_and_increment_version<'a>(
            &'a self,
            _task_id: TaskId,
            _expected_version: u64,
        ) -> StorageFuture<'a, Result<u64, StorageError>> {
            Box::pin(async {
                Err(StorageError::NotFound {
                    entity: "task",
                    id: String::new(),
                })
            })
        }

        fn record_delivery<'a>(
            &'a self,
            _delivery: &'a Delivery,
        ) -> StorageFuture<'a, Result<(), StorageError>> {
            Box::pin(async { Ok(()) })
        }

        fn acknowledge_delivery<'a>(
            &'a self,
            _delivery_id: DeliveryId,
            _at: DateTime<Utc>,
        ) -> StorageFuture<'a, Result<AckOutcome, StorageError>> {
            Box::pin(async { Ok(AckOutcome::Recorded) })
        }

        fn get_delivery<'a>(
            &'a self,
            _id: DeliveryId,
        ) -> StorageFuture<'a, Result<Option<Delivery>, StorageError>> {
            Box::pin(async { Ok(None) })
        }

        fn unacknowledged_deliveries<'a>(
            &'a self,
        ) -> StorageFuture<'a, Result<Vec<Delivery>, StorageError>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn deliveries_awaiting_outcome<'a>(
            &'a self,
        ) -> StorageFuture<'a, Result<Vec<Delivery>, StorageError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[test]
    fn public_storage_types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StorageError>();
        assert_send_sync::<Delivery>();
        assert_send_sync::<AckOutcome>();
        assert_send_sync::<SqliteRepository>();
        assert_send_sync::<Arc<dyn Repository>>();
    }

    #[test]
    fn the_repository_trait_is_object_safe() {
        // Compiles only if every method is `dyn`-compatible.
        fn assert_object_safe(_: &dyn Repository) {}
        let repository: Arc<dyn Repository> = Arc::new(StubRepository);
        assert_object_safe(repository.as_ref());
        assert_eq!(Arc::strong_count(&repository), 1);
    }

    #[tokio::test]
    async fn the_frozen_signatures_are_callable_through_a_trait_object() {
        fn assert_send<T: Send>(_: T) {}

        let repository: Arc<dyn Repository> = Arc::new(StubRepository);
        let worker_handle = Arc::clone(&repository);
        assert_eq!(Arc::strong_count(&repository), 2);
        drop(worker_handle);

        assert_send(repository.agents());
        assert!(repository.agents().await.expect("agents").is_empty());
        assert!(
            repository
                .get_task(TaskId::generate())
                .await
                .expect("task")
                .is_none()
        );
        assert!(
            repository
                .unacknowledged_deliveries()
                .await
                .expect("deliveries")
                .is_empty()
        );
        assert_eq!(
            repository
                .acknowledge_delivery(DeliveryId::generate(), Utc::now())
                .await
                .expect("ack"),
            AckOutcome::Recorded
        );
    }

    #[test]
    fn delivery_accessors_expose_the_row_and_the_acknowledgement_flag() {
        let dispatched_at: DateTime<Utc> = "2026-09-16T10:00:00Z".parse().unwrap();
        let acknowledged_at: DateTime<Utc> = "2026-09-16T10:00:05Z".parse().unwrap();
        let delivery_id = DeliveryId::generate();
        let task_id = TaskId::generate();
        let target = EndpointId::generate();

        let delivery = Delivery::new(delivery_id, task_id, 1, target, dispatched_at);
        assert_eq!(delivery.delivery_id(), delivery_id);
        assert_eq!(delivery.task_id(), task_id);
        assert_eq!(delivery.attempt(), 1);
        assert_eq!(delivery.target(), target);
        assert_eq!(delivery.dispatched_at(), dispatched_at);
        assert_eq!(delivery.acknowledged_at(), None);
        assert!(!delivery.is_acknowledged());

        let acknowledged = delivery.clone().with_acknowledged_at(Some(acknowledged_at));
        assert_eq!(acknowledged.acknowledged_at(), Some(acknowledged_at));
        assert!(acknowledged.is_acknowledged());
        // The original is untouched: rows are materialised, then read-only.
        assert!(!delivery.is_acknowledged());
        // The crate-internal materialiser can also clear the acknowledgement.
        assert!(
            !acknowledged
                .clone()
                .with_acknowledged_at(None)
                .is_acknowledged()
        );
    }

    #[test]
    fn same_dispatch_ignores_the_acknowledgement() {
        let task_id = TaskId::generate();
        let target = EndpointId::generate();
        let dispatched_at: DateTime<Utc> = "2026-09-16T10:00:00Z".parse().unwrap();
        let requested = Delivery::new(DeliveryId::generate(), task_id, 1, target, dispatched_at);

        let stored = requested
            .clone()
            .with_acknowledged_at(Some(dispatched_at + chrono::Duration::seconds(2)));
        assert!(
            same_dispatch(&stored, &requested),
            "the acknowledgement is later state, not part of the dispatch fact"
        );

        let different_attempt =
            Delivery::new(DeliveryId::generate(), task_id, 2, target, dispatched_at);
        assert!(!same_dispatch(&stored, &different_attempt));
    }

    #[tokio::test]
    async fn re_recording_an_acknowledged_dispatch_is_a_replay() {
        use crate::models::{AgentEndpoint, EndpointAddress, Priority, TransportType};
        use crate::storage::{connect, migrate};

        let mut path = std::env::temp_dir();
        path.push(format!(
            "guigu-storage-repository-unit-{}.db",
            uuid::Uuid::now_v7()
        ));
        let pool = connect(&path).await.expect("connect");
        migrate(&pool).await.expect("migrate");
        let repository = SqliteRepository::new(pool.clone());

        let conversation = Conversation {
            id: ConversationId::generate(),
            participants: Vec::new(),
            external_ref: None,
        };
        repository
            .insert_conversation(&conversation)
            .await
            .expect("conversation");

        let task_id = TaskId::generate();
        let endpoint = AgentEndpoint {
            id: EndpointId::generate(),
            transport: TransportType::Acp,
            address: EndpointAddress::Acp {
                command: "unit-acp".into(),
                args: Vec::new(),
            },
            enabled: true,
            capabilities: Vec::new(),
        };
        repository
            .upsert_agent("unit", &endpoint)
            .await
            .expect("agent");
        repository
            .insert_task(&AgentTask {
                task_id,
                root_task_id: task_id,
                parent_task_id: None,
                from_agent: EndpointId::generate(),
                to_agent: EndpointId::generate(),
                conversation_id: conversation.id,
                reply_to: None,
                text: "unit".into(),
                priority: Priority::DEFAULT,
                depth: 0,
                hops: 0,
                deadline: None,
                version: 0,
            })
            .await
            .expect("task");

        let dispatched_at: DateTime<Utc> = "2026-09-16T10:00:00Z".parse().unwrap();
        let delivery = Delivery::new(
            DeliveryId::generate(),
            task_id,
            1,
            endpoint.id,
            dispatched_at,
        );
        repository.record_delivery(&delivery).await.expect("record");
        repository
            .acknowledge_delivery(
                delivery.delivery_id(),
                dispatched_at + chrono::Duration::seconds(1),
            )
            .await
            .expect("acknowledge");

        // The stored row now differs from the requested one in `acknowledged_at`
        // alone; re-recording the dispatch must still be a replay, and must not
        // clear the acknowledgement.
        repository
            .record_delivery(&delivery)
            .await
            .expect("a replayed dispatch is Ok");
        assert!(
            repository
                .get_delivery(delivery.delivery_id())
                .await
                .expect("get")
                .expect("present")
                .is_acknowledged(),
            "re-recording a dispatch never clears the acknowledgement"
        );

        pool.close().await;
        for suffix in ["", "-wal", "-shm"] {
            let mut candidate = path.as_os_str().to_owned();
            candidate.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(candidate));
        }
    }
}
