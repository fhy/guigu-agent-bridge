//! Durable storage: the SQLite event store that is the bridge's source of truth.
//!
//! Two public surfaces, deliberately separated (analysis §4.7.5, design gate D10):
//!
//! - [`connect`] / [`migrate`] — pool construction and embedded-migration
//!   application, for the assembly layer (T017) and for tests that need real SQL
//!   without a `Repository` implementation. [`sqlx::SqlitePool`] appearing here is
//!   an intentional, bounded driver leak: domain consumers depend only on
//!   [`Repository`].
//! - [`Repository`] — the frozen public boundary consumed by T009 (persistence,
//!   restart recovery, idempotency), T011 (user entry), T012 (projection) and
//!   T014 (ACP adapter). T008 freezes its signatures; **all method bodies belong
//!   to T009**.
//!
//! # Status
//!
//! T008 delivered the schema, the migration, the pool options, the representation
//! codec and the `Repository` trait. T009 implements the behaviour: the
//! [`SqliteRepository`] method bodies, the idempotency policy, the
//! [`plan_recovery`]/[`sync_agents`] restart primitives and the persisted-tree
//! [`detect`] cycle gate.
//!
//! Storage is still **not** wired into the production entry point ([`crate::Error`]
//! gains no `Storage` variant, and `main`/`run` do not connect; wiring is T017).
//! There is no `sessions` table: session semantics belong to T014, which
//! introduces them through an appended migration (`0002_*.sql`).
//!
//! Event persistence itself is not dispatched from here either: [`Repository`] is
//! the write path, and assembling an [`EventConsumer`](crate::bus::EventConsumer)
//! over it is T012/T017/T018 work. What T009 guarantees is that the write path is
//! idempotent and that the task-then-event order is atomic (see
//! [`SqliteRepository::insert_task_and_event`]).
//!
//! # Schema overview
//!
//! | table | holds | notes |
//! |-------|-------|-------|
//! | `agents` | endpoint snapshot from config | `agent_id` = ADR-003 derivation input |
//! | `conversations` | context + optional external room/thread/session | at most one per external reference |
//! | `messages` | user/agent/system messages | one sender, one recipient |
//! | `tasks` | immutable task rows | **no `status`**; status is event-sourced |
//! | `task_events` | append-only event log | `UNIQUE (task_id, seq)`, `PK (event_id)` |
//! | `deliveries` | dispatch vs. acknowledgement | no terminal-outcome column |
//!
//! # Contracts every consumer must respect
//!
//! 1. **Foreign-key write order.** `PRAGMA foreign_keys` is ON for every
//!    connection, so rows must be written in dependency order:
//!    `agents` → `conversations` → `messages` → `tasks` → `task_events` /
//!    `deliveries`. A violation is never silently persisted, but it surfaces as
//!    `StorageError::IntegrityViolation` and, in the T005 event consumer, as a
//!    logged-and-dropped event — so T009 must synchronise `agents` at startup
//!    *before* it consumes any `Dispatched` event.
//! 2. **`tasks` rows are only written by the submission path.** A `TaskEvent`
//!    carries no task body (`text`, `priority`, `depth`, `hops`, `deadline`,
//!    `from_agent`, `to_agent`, `conversation_id`), so the event stream cannot
//!    rebuild a task row. T009 must insert the task before appending its events.
//! 3. **`task_events` is append-only.** There is no update path; `Duplicate` is
//!    the idempotency signal for a replayed `event_id` or `(task_id, seq)`.
//! 4. **No external I/O while a write transaction is open.** SQLite has exactly
//!    one writer; awaiting Matrix/ACP/network I/O inside a transaction would
//!    stall every other writer past `busy_timeout` (and block WAL checkpoints).
//!    T009/T014 must commit first and await afterwards.
//! 5. **Repository calls are awaited to completion.** T008 introduces no
//!    cancellation or timeout around storage; `sqlx` compensates for dropped
//!    requests by discarding the affected connection, but retry/cancel policy is
//!    not part of this contract (T006/T016 own any such decision).
//! 6. **Arrival order is not `seq` order** (T005 Q1=A). `events_for_task` returns
//!    events `ORDER BY seq ASC`; consumers must group by `task_id` and never
//!    assume the arrival order of the broadcast stream.
//! 7. **An external conversation reference is unique.**
//!    [`Repository::conversation_by_external_ref`] returns a single
//!    `Option<Conversation>`, and the schema enforces it with two `UNIQUE`
//!    partial indexes: one conversation per `(transport, external_id)` when
//!    `thread_ref IS NULL`, and one per `(transport, external_id, thread_ref)`
//!    when it is not. A replayed or concurrent first message for the same
//!    reference is therefore [`StorageError::Duplicate`], **never** a second
//!    conversation: implementations must not insert a fresh row on that signal.
//!    Conversations without an external reference (all three columns NULL) are
//!    excluded from both indexes and may be numerous.
//!
//! # Duplicate handling (the idempotency policy)
//!
//! A unique-violation is normal input, not a crash. Every write in
//! [`SqliteRepository`] classifies it by **reading the conflicting row back and
//! comparing it field by field**:
//!
//! - identical content ⇒ the call repeated a write that already happened ⇒
//!   `Ok(())`, and the stored row is left untouched (the first writer wins; no
//!   method upserts over an existing row except [`Repository::upsert_agent`],
//!   whose documented meaning *is* replacement);
//! - different content ⇒ [`StorageError::Duplicate`], a defined signal for the
//!   caller to read the winner back and reconcile.
//!
//! [`Repository::append_event`] applies the same rule, which is why a replayed
//! `event_id` is tolerated while two *different* events claiming one
//! `(task_id, seq)` position are reported: silently accepting the latter would
//! rewrite per-task history.
//!
//! # Visibility of declared-but-unaddressable endpoints
//!
//! `agents` permits a NULL `address_json` (a declared endpoint with no derivable
//! address — `matrix`/`http` in v1), but [`crate::models::AgentEndpoint`] has a
//! mandatory address, so the model layer cannot represent such a row. Reads
//! therefore filter on `address_json IS NOT NULL`: the row stays in the table and
//! is simply not surfaced, and [`sync_agents`] never creates one.
//!
//! # Task-tree cycle gate
//!
//! [`detect`] adds the durable half of cycle detection (visited agents, observed
//! chain length, subtask count) on top of the worker's in-memory `depth`/`hops`
//! checks; [`CycleHit::failed_event`] produces the same `Failed` terminal shape
//! T006 uses. The `cycle` module carries the layering and the wiring recipe.
//!
//! # Concurrency
//!
//! SQLite is single-writer; WAL gives one writer plus concurrent readers, and
//! writers queue behind `busy_timeout` ([`DEFAULT_BUSY_TIMEOUT`]). The pool is
//! bounded by [`MAX_CONNECTIONS`]. Storage holds **no locks of its own**: the
//! pool serialises internally and every `Repository` method takes `&self`, so
//! there is no lock-ordering or lock-while-awaiting hazard to reason about.
//!
//! v1 assumes a single process owns the database file. sqlx's SQLite migrator
//! lock is a no-op, so two processes migrating the same file concurrently are
//! not serialised (they fail loudly instead of corrupting); multi-instance
//! deployment is a T016/T017 concern.
//!
//! # Migration immutability
//!
//! [`migrate`] runs the migrations embedded by `sqlx::migrate!("./migrations")`.
//! Applied versions are recorded with a checksum in `_sqlx_migrations`, so a
//! re-run is a no-op and editing an already-applied file fails with
//! `VersionMismatch` (enforced by tests, not just documented). Schema changes are
//! appended as new files.

// The representation layer is `pub(crate)`: it is the single place that decides
// how domain values map to columns (analysis §4.2-§4.5, design gate D11), and it
// is the mapping both `SqliteRepository` and the codec round-trip tests use.
pub(crate) mod codec;

mod cycle;
mod error;
mod event_consumer;
mod pool;
mod recovery;
mod reliability;
mod repository;

pub use cycle::{CycleHit, CycleKind, CycleLimits, DEFAULT_MAX_SUBTASKS, detect};
pub use error::StorageError;
pub use event_consumer::RepositoryEventConsumer;
pub use pool::{DEFAULT_BUSY_TIMEOUT, MAX_CONNECTIONS, connect, migrate};
pub use recovery::{RecoveryPlan, plan_recovery, sync_agents};
pub use reliability::{
    AdmissionRecovery, PendingProjection, ReceiptOutcome, ReliabilityError, ReliabilityStore,
    RetryTaskInput,
};
pub use repository::{AckOutcome, Delivery, Repository, SqliteRepository, StorageFuture};
