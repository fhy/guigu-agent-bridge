//! Agent Bus: the internal task execution channel.
//!
//! The bus accepts an [`AgentTask`], validates its target against the runtime
//! [`EndpointRegistry`], enqueues it, and emits exactly one immutable `Queued`
//! event (`seq = 1`). The concrete queue (`tokio::sync::mpsc`, see [`memory`]) is
//! hidden behind the object-safe [`Bus`] trait so the queue can later be replaced
//! by a SQLite queue, Redis, or NATS without changing Bridge Core
//! (ARCHITECTURE.md §6); callers depend only on `Arc<dyn Bus>`.
//!
//! # Scope
//!
//! This module owns task *submission*, endpoint identity/registry, the event
//! write boundary and queue lifecycle (T004); task *consumption* — the worker
//! state machine, `Dispatched`/`Running`/… events and `seq > 1` — plus event
//! fan-out (T005); and the worker's runtime controls — timeout, cancellation,
//! retry, and the stateless `depth`/`hops` cycle limits (T006) with their
//! injectable boundaries in [`control`] ([`Cancellation`], [`Timer`]). It
//! deliberately does **not** own:
//!
//! - **visited-agent cycle detection and the maximum-subtask-count limit.**
//!   `AgentTask` carries no visited-agent field and the worker holds no task
//!   store, so only the stateless `depth`/`hops` ceilings can be enforced here.
//!   The stateful half needs the durable task tree (`parent_task_id`
//!   backtracking) and belongs to T009 — T006 deliberately keeps **no** per-root
//!   state table (it would grow without bound and vanish on restart). Do not read
//!   “cycle detection” as complete.
//! - protocol-level cancellation (an ACP `cancel` notification, subprocess
//!   reaping) — T014/T015;
//! - the mock agent — T007;
//! - durable idempotency (`seq` uniqueness, duplicate `task_id` dedup),
//!   persistence and restart recovery — T009/T018;
//! - transports (Matrix / ACP / HTTP adapters) — T010+/T014+;
//! - concurrency limiting — T016.
//!
//! # Consumption and fan-out (T005)
//!
//! [`Worker`] takes the task receiver handed out at construction, dequeues by
//! priority, re-validates the target, dispatches through [`TaskDispatcher`]
//! (`deliver` = explicit acceptance, then `execute` = terminal outcome) and
//! records `seq > 1` through the same [`EventSink`] the bus writes to.
//! [`EventBroadcaster`] owns the event channel's receiving end and forwards each
//! event to every registered [`EventConsumer`] (T009 persistence, T012 Matrix
//! projection). [`Worker`] and [`EventBroadcaster`] are wired by the owner of the
//! runtime (T017); this crate assembles them only in tests.
//!
//! Two contracts from T005 must be respected by every consumer of this module:
//!
//! - **`AgentTask::version` is passed through untouched** — [`Worker`] neither
//!   reads nor rewrites the reserved OCC counter; conditional updates are T009.
//! - **Arrival order is not `seq` order.** [`EventBroadcaster`] preserves arrival
//!   order; `seq` is the authoritative per-task ordering key, so event consumers
//!   must sort/merge per `task_id` by `seq`.
//!
//! Both are documented in full on [`worker`] and [`fanout`].
//!
//! # Runtime control (T006)
//!
//! [`Worker`] can be assembled through [`Worker::builder`] with a
//! [`WorkerConfig`] to add a deadline that preempts an in-flight stage, a
//! [`Cancellation`] signal source, a [`RetryPolicy`] with backoff, and explicit
//! [`LoopLimits`]. [`Worker::new`]/[`Worker::with_dispatcher`] keep their frozen
//! signatures and their T005 semantics (one attempt, never cancelled, never
//! preempted), while the `depth`/`hops` limits and the checkpoint deadline checks
//! are always active.
//!
//! The extended per-task `seq` grammar is documented on [`worker`]: `seq` stays
//! contiguous and monotonic, retries add events instead of rewriting them, and
//! `Dispatched` may therefore occur more than once for one task. Consumers must
//! group by `task_id` and sort by `seq`, and must not assume arrival order equals
//! `seq` order or that a task's events are contiguous.
//!
//! Cancellation state is lifecycle-bounded: the worker takes ownership of a
//! task's record and retires it at the terminal transition, and the retained
//! tables have hard ceilings ([`MAX_PENDING_CANCELLATIONS`],
//! [`MAX_REASON_BYTES`]) so distinct cancellation ids cannot grow memory without
//! bound. [`Cancellation::retire`] and [`Cancellation::tracked`] expose the
//! lifecycle and the current size.
//!
//! # Submission contract (frozen for T005/T007/T011/T014)
//!
//! `submit` performs, in this fixed order:
//!
//! 1. **target validation** against the registry —
//!    unknown → [`BusError::UnknownTarget`],
//!    known but `enabled == false` → [`BusError::TargetDisabled`],
//!    known and enabled but not addressable (`matrix`/`http` today) →
//!    [`BusError::AddressUnavailable`]. No check is a silent drop or a broadcast.
//! 2. **enqueue** on the bounded task channel — full → [`BusError::QueueFull`],
//!    closed → [`BusError::TaskChannelClosed`].
//! 3. **emit** the `Queued` event (`seq = 1`) — full → [`BusError::EventBufferFull`],
//!    closed → [`BusError::EventSinkClosed`].
//!
//! Steps 2 and 3 use `try_send`: submission never blocks and never waits for a
//! consumer. The two channels are independent, so a failure in step 3 happens
//! after the task has been enqueued; that single partial-commit window is
//! documented on [`Bus::submit`] and is the only one.
//!
//! # Priority
//!
//! v1 enqueues in arrival order (channel FIFO); the full [`AgentTask`] — including
//! its `Priority` — is enqueued unchanged. Priority *ordering* is the consumer's
//! job (T005), not T004's; the field is never dropped or rewritten.
//!
//! # Shutdown
//!
//! There is no shutdown sentinel. Dropping every [`Bus`] handle closes the
//! channels, and `tokio::sync::mpsc` drains already-queued items before
//! signalling completion, so a consumer observes the remaining tasks/events and
//! then `None`. See [`MemoryBus`].

use std::future::Future;
use std::pin::Pin;

use crate::models::AgentTask;

pub mod control;
pub mod error;
pub mod event;
pub mod fanout;
pub mod memory;
pub mod registry;
pub mod worker;

pub use control::{Cancellation, MAX_PENDING_CANCELLATIONS, MAX_REASON_BYTES, Timer, TokioTimer};
pub use error::BusError;
pub use event::{Clock, EventSink, MpscEventSink};
pub use fanout::{ConsumerError, EventBroadcaster, EventConsumer};
pub use memory::{BusReceivers, DEFAULT_QUEUE_CAPACITY, MemoryBus};
pub use registry::{
    ENDPOINT_NAMESPACE_LABEL, EndpointRegistry, RegisteredEndpoint, derive_endpoint_id,
};
pub use worker::{
    Backoff, DispatchError, DispatchOutcome, DispatchRequest, DispatcherRegistry,
    FinalizationCapability, FinalizeCommand, FinalizeResult, LifecycleResult, LoopLimits,
    PreparedExecution, RetryPolicy, TaskDispatcher, TaskLifecycle, Worker, WorkerBuilder,
    WorkerConfig, WorkerError,
};

/// A boxed, `Send` future returned by the async bus traits.
///
/// Used instead of native `async fn` in traits so [`Bus`] and [`EventSink`] stay
/// object-safe (`Arc<dyn Bus>`), which is what lets the queue implementation be
/// swapped without touching callers.
pub type BusFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone)]
pub struct AdmissionContext {
    pub transport: String,
    pub external_event_id: String,
    pub room_id: String,
    pub thread_root: Option<String>,
    pub reply_event_id: String,
    pub monitor_room: Option<String>,
    pub monitor_generation: u64,
}

/// Submission-side abstraction of the task queue.
///
/// Implementations hide the concrete queue. T011 (message routing) and T014 (ACP
/// adapter) submit through this trait; T005 consumes the receiving end handed out
/// at construction ([`BusReceivers`]); T007 (mock agent) and tests can inject a
/// substitute.
pub trait Bus: Send + Sync {
    /// Validate the target, enqueue `task`, then emit its `Queued` event.
    ///
    /// Returns `Ok(())` once the task is queued and its `Queued` event
    /// (`seq = 1`, `timestamp` from the bus clock) has been written.
    ///
    /// # Errors
    ///
    /// - [`BusError::UnknownTarget`] / [`BusError::TargetDisabled`] /
    ///   [`BusError::AddressUnavailable`] — target rejected; nothing was enqueued
    ///   and no event was emitted.
    /// - [`BusError::QueueFull`] — the bounded task queue is full; nothing was
    ///   enqueued and no event was emitted.
    /// - [`BusError::TaskChannelClosed`] — the task channel is closed; nothing
    ///   was enqueued and no event was emitted.
    /// - [`BusError::EventBufferFull`] / [`BusError::EventSinkClosed`] — the
    ///   event write failed *after* the task was enqueued (the only
    ///   partial-commit window).
    ///   The task stays queued; durable atomicity is T009's transaction.
    ///
    /// Submission never blocks: an overloaded bus reports an error instead of
    /// waiting for capacity, leaving retry/backpressure policy to the caller.
    ///
    /// # Idempotency
    ///
    /// v1 does not deduplicate `task_id`: submitting the same task twice enqueues
    /// it twice and produces two independent `seq = 1` events. Durable
    /// idempotency (unique `seq` per task, duplicate suppression) is T009/T018.
    fn submit<'a>(&'a self, task: AgentTask) -> BusFuture<'a, Result<(), BusError>>;

    fn submit_with_context<'a>(
        &'a self,
        task: AgentTask,
        _context: AdmissionContext,
    ) -> BusFuture<'a, Result<(), BusError>> {
        self.submit(task)
    }
}
