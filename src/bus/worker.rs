//! Bus worker: task consumption, the delivery/dispatch contract, and the
//! `seq > 1` half of the task state machine.
//!
//! The worker is the only consumer of the task channel the T004
//! [`Bus`](crate::bus::Bus) hands out. It dequeues by priority, re-validates the
//! target, hands the task to a [`TaskDispatcher`] through a two-stage *deliver →
//! execute* protocol, and records every transition as an immutable [`TaskEvent`]:
//!
//! ```text
//! submit(AgentTask)                          [T004 bus, unchanged]
//!   -> registry.validate_target(to_agent)
//!   -> tasks.try_send(task)
//!   -> events.emit(Queued, seq = 1)          ┐
//!                                            │ one shared Arc<dyn EventSink>
//! Worker::run                                │
//!   -> tasks.try_recv / tasks.recv           │
//!   -> PriorityQueue::push(task, arrival)    │
//!   -> PriorityQueue::pop (priority, FIFO)   │
//!   -> process(task).await                   │
//!        CP-0  cancel   -> Cancelled(2)      │ dispatch-time gate: nothing is
//!              deadline -> TimedOut(2)       │ delivered when a gate hits, so
//!              limits   -> Failed(2)         │ there is no Dispatched event
//!              registry.validate_target       │ re-validation (defensive)
//!              DispatcherRegistry::get       │ dispatch selection
//!        for attempt in 1..=max_attempts:   │
//!          CP-1  cancel/deadline            │
//!          emit Dispatched{delivery_id, attempt}
//!          select!(biased) stage | cancel | deadline
//!          CP-2  cancel/deadline            │
//!          emit Running{started_at}         │
//!          select!(biased) stage | cancel | deadline
//!          emit Completed|Failed            │ terminal, never retried
//!          refusable? backoff, next attempt │ else -> Dispatched(a + 1)
//! ```
//!
//! `seq` is owned from 2 upwards: the worker never rewrites `seq = 1`, never
//! emits a second `Queued`, and never leaves a gap in a task's own sequence.
//! The `Queued` event and the task channel are independent writes performed by
//! the bus, so `seq` — not arrival order — is the authoritative per-task
//! ordering key (see “Arrival order is not `seq` order” below).
//!
//! # Delivery is not acceptance
//!
//! Sending a task to an agent does not mean the agent took it. ARCHITECTURE §6
//! and INC-001 therefore split the interaction into two stages: [`TaskDispatcher::deliver`]
//! must return `Ok(())` only after the target acknowledged *this* delivery
//! (matching `task_id` and attempt), and only then does the worker write
//! `Running`. A dispatcher that cannot obtain that acknowledgement returns
//! [`DispatchError::NotAccepted`], and the task fails without ever being marked
//! as running. This is the structural block against “delivery implies claim”.
//!
//! # Arrival order is not `seq` order (Q1 = A, accepted)
//!
//! `Bus::submit` enqueues the task and *then* writes `Queued` (`src/bus/memory.rs`).
//! On a multi-thread runtime this worker can be scheduled between those two
//! steps, so `Dispatched(seq = 2)` can reach the event stream before
//! `Queued(seq = 1)`. The window is invisible on a single-thread runtime, where
//! `submit` runs both steps without yielding.
//!
//! This worker cannot fix that: it has no view of the event stream and does not
//! write `seq = 1`. The contract is therefore explicit: **the event stream
//! preserves arrival order; `seq` is the per-task ordering key.** Consumers of
//! the stream (T009 persistence, T012 Matrix projection) must sort or merge
//! events per `task_id` by `seq` and must not assume arrival order equals `seq`
//! order. [`crate::bus::EventBroadcaster`] deliberately preserves arrival order
//! rather than pretending to reorder.
//!
//! # `AgentTask::version` is not touched (D9)
//!
//! The worker does **not** read, increment or rewrite [`AgentTask::version`]; the
//! task is handed to the dispatcher exactly as submitted. `TaskEvent` has no
//! version field and v1 has no task store, so any local bump would be
//! unobservable and untestable. Conditional (optimistic-concurrency) updates are
//! T009's responsibility — nobody may assume this module maintains the counter.
//!
//! # Failure semantics
//!
//! The `seq` a failure lands on identifies the stage that failed, so the number
//! is never inferred from the error text:
//!
//! | Path | Event sequence | Trigger |
//! |------|----------------|---------|
//! | A | `Failed(seq = 2)` | target re-validation failed, or no dispatcher for the transport |
//! | A1 | `Failed(seq = 2)` | a `depth`/`hops` limit was exceeded (delivery blocked) |
//! | A2 | `TimedOut(seq = 2)` | the deadline had already passed |
//! | A3 | `Cancelled(seq = 2)` | the task was already cancelled |
//! | B | `Dispatched(2)` → `Failed(seq = 3)` | `deliver` did not obtain acceptance |
//! | B1 | `Dispatched(2)` → `TimedOut(seq = 3)` | the deadline passed while delivering |
//! | B2 | `Dispatched(2)` → `Cancelled(seq = 3)` | the task was cancelled while delivering |
//! | C | `Dispatched(2)` → `Running(3)` → `Failed(seq = 4)` | `execute` reported a failure |
//! | C1 | `Dispatched(2)` → `Running(3)` → `TimedOut(seq = 4)` | the deadline passed while executing |
//! | C2 | `Dispatched(2)` → `Running(3)` → `Cancelled(seq = 4)` | the task was cancelled while executing |
//! | D | `Dispatched(2)` → `Running(3)` → `Completed(4)` | `execute` succeeded |
//!
//! `seq` therefore stays contiguous and monotonic for every task: `2/3/4/…` with
//! no holes, never a rewritten `seq = 1`. Retries extend the sequence instead of
//! repeating it — see “Runtime control” below for the full grammar.
//!
//! - **Failure text is supplied, never invented.** A `Failed` payload carries
//!   [`DispatchError`]'s `Display` (a refused stage), the adapter's own error
//!   string (a terminal [`DispatchOutcome::Failed`]), [`BusError`]'s `Display`
//!   (a target rejected before dispatch), or the worker's own bounded
//!   description of a cycle limit. None of these render an address or a
//!   credential, and none render `AgentTask::text`.
//! - **A failed transition is a `Failed` event, not a worker error.** The worker
//!   keeps running; the failure is part of the task's own event log.
//! - **A failed event write is fail-stop.** A transition is atomic (one
//!   `emit`, the event is fully built before the call), so an error means the
//!   transition is entirely unobservable — there is no half-written state. v1
//!   does not retry: [`BusError::EventSinkClosed`] and [`BusError::EventBufferFull`]
//!   stop the worker with [`WorkerError::EventStreamClosed`] /
//!   [`WorkerError::EventBackpressure`]. Never retrying and never swallowing
//!   keeps status records honest, and matches T004's “report explicitly instead
//!   of blocking” stance. Recovery is T009/T016.
//! - **Fail-stop consequences:** the worker drops the task receiver, so later
//!   `Bus::submit` calls report [`BusError::TaskChannelClosed`]; in-flight tasks
//!   stay at the last successfully written event. No compensating event is invented.
//!
//! # Runtime control: timeout, cancellation, retry and cycle limits (T006)
//!
//! Four runtime controls are wired into the state machine above. All of them are
//! **opt-in except the stateless checks**: a worker built with
//! [`Worker::new`]/[`Worker::with_dispatcher`] gets the default [`WorkerConfig`]
//! and therefore behaves exactly as T005 did — one attempt, no cancellation
//! source, no timer — while the deadline and `depth`/`hops` checks are always
//! active (they need no injected resource, and every T005 test uses
//! `deadline: None`, `depth: 0`, `hops: 0`).
//!
//! ## Checkpoint order (fixed)
//!
//! ```text
//! CP-0 (before dispatch, the only seq = 2 gate): cancel -> deadline -> limits -> re-validate -> select dispatcher
//! CP-1 (before every Dispatched, retries included): cancel -> deadline
//! CP-2 (after deliver returned Ok, before Running): cancel -> deadline
//! CP-3 (after execute returned): a retryable failure is not retried past the deadline
//! ```
//!
//! The order is deliberate: an explicit human cancellation outranks the clock,
//! and the stateless cycle check runs before the registry lookup (both land on
//! `seq = 2`, so the sequence contract is unaffected).
//!
//! ## Timeout
//!
//! - A deadline that has already passed is **never delivered**: `TimedOut` at
//!   `seq = 2`. Delivering first and deciding afterwards would produce a task
//!   that actually ran while being recorded as timed out.
//! - A deadline that passes *during* a stage is enforced preemptively: the stage
//!   future is raced against [`Timer::sleep_until`], so a `deliver` or `execute`
//!   that never resolves cannot block the worker forever. The losing future is
//!   dropped — the worker spawns no task and calls no `abort()`.
//! - The `select!` is `biased`: **stage result > cancellation > deadline**. A
//!   stage result that is already available is the authoritative fact and is
//!   never overwritten by a signal that arrived in the same poll; cancellation is
//!   an explicit intent and outranks the clock.
//! - Without an injected [`Timer`] the preemption race is disabled and only the
//!   checkpoints apply (CP-0/CP-1/CP-2). That is a documented degradation, not an
//!   error: the deadline is still enforced at every stage boundary.
//!
//! ## Cancellation
//!
//! - [`Cancellation`] carries a global signal and per-`task_id` signals; the
//!   worker observes it at CP-0/CP-1/CP-2 and inside both stage races.
//! - The landed `seq` follows the same stage grammar as a failure: `2` before
//!   dispatch, `3` while delivering, `4` while executing, or the next unused
//!   sequence when the signal arrives during a backoff wait.
//! - `Cancelled` is terminal: the task is never retried and never transitions
//!   again. Cancelling a task that has already finished writes nothing.
//! - Each task's cancellation record is bound to its lifetime: the worker takes
//!   ownership of the record when it starts the task and retires it at the
//!   terminal transition, so a finished task retains nothing and a late `cancel`
//!   for it is dropped rather than accumulated. [`crate::bus::control`] states the
//!   exact bounds; a cancel that arrives while the task is still queued is kept
//!   and moves with the task when it is taken over.
//! - Only the *worker's* interest in the delivery ends here. A protocol-level
//!   cancel (an ACP `cancel` notification, subprocess reaping) requires a method
//!   on the frozen [`TaskDispatcher`] and belongs to T014/T015.
//!
//! ## Retry
//!
//! - Only a refused stage is retryable: [`DispatchError::NotAccepted`] (the task
//!   never became `Running`, so a redelivery cannot duplicate execution) and
//!   [`DispatchError::ExecutionFailed`] (acceptance was lost, no terminal result
//!   was produced). [`DispatchOutcome::Failed`] is an adapter's *deterministic*
//!   verdict — retrying it would repeat side effects, which is the submitter's
//!   policy (T011), not the transport's.
//! - `TimedOut`, `Cancelled`, a cycle-limit hit, a rejected target and a missing
//!   dispatcher are never retried: they are terminal or deterministic facts.
//! - A retry emits a **new** `Dispatched` carrying a fresh [`DeliveryId`] and an
//!   incremented `attempt`; both stages of one attempt keep seeing the same
//!   identity (the T005 contract). `attempt` lives in `process`'s local state —
//!   the worker keeps no cross-task counter, no `seen` set and no dedup.
//! - A failed **non-terminal** attempt writes **no event**: the model has no
//!   “retrying” status and a `Failed` event would read as terminal and as a state
//!   regression. The retry is visible as the `Dispatched` that follows; the
//!   reason is recorded with `tracing::warn!`.
//! - Backoff is [`Backoff::None`] by default and never jitters (determinism
//!   first). A non-`None` backoff is waited out through the [`Timer`] and races
//!   cancellation and the deadline; with no timer configured the next attempt
//!   starts immediately instead of panicking.
//! - Enabled [`DispatchError::ExecutionFailed`] retries can produce
//!   `Running -> Dispatched`. Each `Dispatched` carries a new `delivery_id` and a
//!   higher `attempt`, so an event consumer (T012) must group by `task_id` +
//!   `attempt` and must not assume `Dispatched` appears once.
//!
//! ## Cycle limits
//!
//! - `depth` and `hops` are checked against [`LoopLimits`], which is injected
//!   through [`WorkerConfig`] and normally comes from `Config.bridge`
//!   ([`LoopLimits::from_bridge`]); the defaults match that configuration's
//!   defaults (8/16), pinned by a unit test so the two cannot drift.
//! - The comparison is strict (`depth > max_depth`), so a root task
//!   (`depth = 0`) is allowed at `max_depth = 0`, and a hit **blocks delivery**:
//!   no `Dispatched` event and no dispatcher call.
//! - The model has no `LoopDetected` status, so a hit reuses `Failed { error }`
//!   at `seq = 2`. The error text names the bounded identity set only: `task_id`,
//!   `root_task_id`, `parent_task_id`, `from_agent`, `to_agent`, the observed and
//!   allowed values, and which limit was hit. It never renders `text` (which may
//!   hold sensitive content), an address or a credential.
//! - **`AgentTask` has no visited-agent field and the worker has no task store,
//!   so ARCHITECTURE §7's visited-agent detection and the maximum-subtask-count
//!   limit are not implemented here.** They need the durable task tree
//!   (`parent_task_id` backtracking) that T009 provides. [`crate::bus`]'s scope
//!   note records the same gap; do not read this module as “cycle detection is
//!   complete”.
//!
//! ## The extended `seq` grammar
//!
//! ```text
//! Queued(1)
//! GATE{ Cancelled(2) | TimedOut(2) | Failed(2) }?      # CP-0, terminal on hit
//! ( Dispatched(seq, attempt = k)                       # k = 1, 2, 3, ...
//!   [ Running(seq) ]?                                  # only after deliver Ok
//! )*                                                   # repeats per attempt
//! Terminal{ Completed | Failed | TimedOut | Cancelled }(seq)
//! ```
//!
//! Invariants: `seq >= 2`, strictly increasing, contiguous and hole-free per
//! task; `seq = 1` is never rewritten and no second `Queued` is written;
//! `Running` appears at most once per `Dispatched` and always after it; every
//! `Dispatched` carries a distinct `delivery_id` and an increasing `attempt`.
//!
//! Worked examples: `Queued(1) Failed(2)` (a limit hit), `Queued(1) TimedOut(2)`
//! (an expired deadline), `Queued(1) Dispatched(2,a=1) TimedOut(3)`,
//! `Queued(1) Dispatched(2,a=1) Dispatched(3,a=2) Running(4) Completed(5)` (one
//! refused delivery, then success), and
//! `Queued(1) Dispatched(2,a=1) Running(3) Dispatched(4,a=2) Failed(5)` (an
//! execution failure that was retried and then gave up).
//!
//! ## Termination of the attempt loop
//!
//! `max_attempts` bounds the loop from below — attempt `k` retries only while
//! `k < max_attempts` — and every other exit writes a terminal event. A
//! `max_attempts` of `0` is treated as `1`: the task still runs exactly once, so
//! the loop can neither spin without doing work nor drop a task.
//!
//! # Lifecycle
//!
//! - Single task at a time (v1). That is an implementation shape, **not** a
//!   limiter: there is no semaphore, no configurable ceiling and no rejection
//!   path. Concurrency control is T016 and its extension point is the main loop
//!   body of [`Worker::run`], marked in the code.
//! - Exits with `Ok(())` when the task channel is closed *and* the local priority
//!   heap is empty (drain-then-terminate, T004 D8). Already received tasks are
//!   never dropped on shutdown, so nothing is leaked.
//! - No lock, no sub-task, no global state. The deadline and backoff waits are
//!   polled inline through the injected [`Timer`], so they are dropped with the
//!   race that created them and nothing outlives `process`. Production paths
//!   hold no lock at all: [`Cancellation`]'s two mutexes are acquired
//!   synchronously, one at a time, and are never held across an `.await` (see
//!   [`crate::bus::control`]). The local heap is bounded
//!   by the channel capacity: **one admission step reads at most
//!   `capacity − heap.len()` tasks**, where `capacity` is the task channel's own
//!   [`max_capacity`](tokio::sync::mpsc::Receiver::max_capacity), so the heap
//!   never holds more unprocessed tasks than the channel it drains. The budget is
//!   taken from the channel, never from how many tasks happen to be readable:
//!   producers can refill freed slots *while* a step runs, so a step that stopped
//!   only after observing `Empty` could run for as long as submissions keep up —
//!   growing the heap without bound and starving every dispatch behind it.
//! - [`Worker::spawn`] returns a `JoinHandle`; `abort()` remains **not** graceful
//!   cancellation. Aborting inside `deliver`/`execute` can leave a task with
//!   `Dispatched`/`Running` and no terminal event; graceful cancellation is
//!   [`Cancellation`], which terminates the task with a `Cancelled` event.
//!
//! # Boundaries
//!
//! - **No permission check (Q2 = A).** Authorisation happens in T011 at routing
//!   time, before `submit`; the registry carries no permission data, so any check
//!   here would invent policy and add a TOCTOU window. T011 can insert a
//!   `TaskDispatcher` decorator without touching this module.
//! - **No concurrency limiting (Q2 = A).** See T016 above.
//! - **No protocol-level cancellation.** Cancelling drops the worker's interest
//!   in an in-flight stage; the far side is not told (T014/T015).
//! - **Visited-agent cycle detection and the maximum-subtask count are not
//!   implemented** — see “Cycle limits” above; they belong to T009.
//! - No persistence (T009), no transport implementation (T010+/T014+).

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tracing::warn;

use crate::bus::control::{Cancellation, Timer};
use crate::bus::event::{Clock, EventSink};
use crate::bus::registry::{EndpointRegistry, RegisteredEndpoint};
use crate::bus::{BusError, BusFuture};
use crate::config::BridgeConfig;
use crate::models::{
    AgentTask, DeliveryId, EventId, Priority, TaskEvent, TaskEventPayload, TaskId, TaskStatus,
    TransportType,
};

/// One delivery attempt's context, shared by both dispatch stages.
///
/// `delivery_id` and `attempt` identify the attempt and are identical for
/// [`TaskDispatcher::deliver`] and [`TaskDispatcher::execute`], which is what
/// lets a dispatcher (and, later, T009) correlate the acceptance with the
/// terminal outcome.
#[derive(Debug, Clone)]
pub struct DispatchRequest<'a> {
    /// The task being delivered, exactly as submitted (`version` included, untouched).
    pub task: &'a AgentTask,
    /// The target that passed re-validation; its transport selected the dispatcher.
    pub target: &'a RegisteredEndpoint,
    /// Identity of this delivery attempt.
    pub delivery_id: DeliveryId,
    /// Attempt number. T005 always uses `1`; retries (`> 1`) belong to T006.
    pub attempt: u32,
}

/// The terminal outcome produced by [`TaskDispatcher::execute`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOutcome {
    /// The task finished successfully.
    Completed {
        /// Final output, recorded verbatim in the `Completed` event.
        output: String,
    },
    /// The task was executed but finished with an error.
    Failed {
        /// Error description, recorded verbatim in the `Failed` event.
        error: String,
    },
}

/// Why a dispatch stage did not produce a usable result.
///
/// The variants line up with the two stages, which is what makes the `seq`
/// path unambiguous: `NotAccepted` means the task never became `Running`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DispatchError {
    /// The delivery was not accepted: sending failed, or the target never gave
    /// an acknowledgement carrying this `task_id` and attempt.
    ///
    /// The task never entered `Running` (failure path B).
    #[error("delivery was not accepted: {reason}")]
    NotAccepted {
        /// Adapter-supplied description. Must not contain credentials.
        reason: String,
    },
    /// The delivery was accepted, but execution produced no terminal result.
    ///
    /// The task was `Running` (failure path C).
    #[error("execution failed after acceptance: {reason}")]
    ExecutionFailed {
        /// Adapter-supplied description. Must not contain credentials.
        reason: String,
    },
    #[error("prepared execution is unsupported")]
    UnsupportedPrepared,
    #[error("execution acceptance is unknown")]
    AcceptanceUnknown,
    #[error("execution requires recovery")]
    RecoveryNeeded,
}

/// Result of the crate-owned reliable finalization protocol.
///
/// This type is public so lifecycle integrations and diagnostics can name
/// outcomes without exposing a capability constructor.
#[derive(Debug)]
pub enum FinalizeResult {
    Committed,
    AlreadyTerminal,
    Stale,
    Fenced,
    RecoveryNeeded,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FinalizeCommand {
    pub(crate) task_id: TaskId,
    pub(crate) event: TaskEvent,
    pub(crate) response: tokio::sync::oneshot::Sender<FinalizeResult>,
}

/// A single-use terminal handoff owned by the crate's ACP supervisor.
///
/// Construction and command fields are deliberately crate-private. Third-party
/// [`TaskDispatcher`] implementations remain supported through `deliver` and
/// `execute`, but cannot claim `supports_prepared` or mint an unfenced terminal
/// capability. The type is `Send`, is not `Clone`, and dropping it closes the
/// sole command sender observed by the supervisor.
#[derive(Debug)]
pub struct FinalizationCapability {
    #[allow(dead_code)]
    pub(crate) task_id: TaskId,
    #[allow(dead_code)]
    pub(crate) command: Option<tokio::sync::oneshot::Sender<FinalizeCommand>>,
}

#[derive(Debug)]
/// A prepared terminal outcome from the crate-owned reliable ACP path.
///
/// The fields are readable by Worker, while construction remains limited by
/// the private fields of [`FinalizationCapability`].
pub struct PreparedExecution {
    pub outcome: DispatchOutcome,
    pub capability: FinalizationCapability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleResult {
    Committed,
    Replay,
    Stale,
}

/// Captured durable execution identity. It is issued by the runtime lease
/// authority and is never reconstructed from an AgentTask.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueLeaseContext {
    pub resource_key: String,
    pub runtime_owner: String,
    pub owner_fence: i64,
    pub expires_at: String,
}

pub trait TaskLifecycle: Send + Sync {
    fn transition<'a>(
        &'a self,
        event: &'a TaskEvent,
    ) -> BusFuture<'a, Result<LifecycleResult, DispatchError>>;

    /// Optional durable queue hooks. The default preserves legacy callers that
    /// do not provide a SQLite queue; production assembly supplies these hooks
    /// together with the real lease/workspace authority.
    fn claim_queue<'a>(
        &'a self,
        _task: &'a AgentTask,
    ) -> BusFuture<'a, Result<Option<QueueLeaseContext>, DispatchError>> {
        Box::pin(async { Ok(None) })
    }
    fn mark_queue_send_started<'a>(
        &'a self,
        _task: &'a AgentTask,
        _lease: &'a QueueLeaseContext,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async { Ok(()) })
    }
    fn renew_queue_lease<'a>(
        &'a self,
        _task: &'a AgentTask,
        _lease: &'a QueueLeaseContext,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async { Ok(()) })
    }
    fn release_queue_lease<'a>(
        &'a self,
        _task: &'a AgentTask,
        _lease: &'a QueueLeaseContext,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Sends tasks to a target agent and runs them to a terminal outcome.
///
/// Two stages, because “sent” and “accepted” are different facts
/// (ARCHITECTURE §6, INC-001). Implemented by T007 (mock agent), T014 (ACP),
/// T010/T011 (Matrix), and by test doubles.
///
/// # Implementation contract
///
/// - Object-safe: implementations are held as `Arc<dyn TaskDispatcher>`, so
///   methods take `&self` and return [`BusFuture`].
/// - `Send + Sync`, because the worker may be spawned onto another thread.
/// - **Return `Err`, never panic.** A panic aborts the worker task and is
///   surfaced only as a `JoinError`; it must not be used for adapter failures.
/// - **Never hold a lock across `.await`** and never block the executor: the
///   worker awaits these calls inline, so a blocking implementation stalls the
///   whole consumption loop.
/// - `deliver` returns `Ok(())` **only** after receiving an explicit
///   acknowledgement for this `task_id` and attempt — not merely after a write.
/// - Error `reason` strings must not contain credentials, and are written into
///   the task's `Failed` event.
pub trait TaskDispatcher: Send + Sync {
    /// Stage one: hand the task to the target and wait for explicit acceptance.
    ///
    /// # Errors
    ///
    /// [`DispatchError::NotAccepted`] when no acknowledgement is obtained. The
    /// worker then writes `Failed(seq = 3)` and never writes `Running`.
    fn deliver<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>>;

    /// Stage two: run an accepted task to its terminal outcome.
    ///
    /// # Errors
    ///
    /// [`DispatchError::ExecutionFailed`] when execution yields no terminal
    /// result; the worker writes `Failed(seq = 4)`.
    fn execute<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>>;

    /// Additive prepared path. Legacy implementations retain their frozen
    /// deliver/execute behavior and report a typed unsupported result.
    fn supports_prepared(&self) -> bool {
        false
    }
    fn execute_prepared<'a>(
        &'a self,
        _request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<PreparedExecution, DispatchError>> {
        Box::pin(async { Err(DispatchError::UnsupportedPrepared) })
    }
    fn cancel_prepared<'a>(
        &'a self,
        _request: DispatchRequest<'a>,
        _reason: &'a str,
    ) -> BusFuture<'a, Result<FinalizationCapability, DispatchError>> {
        Box::pin(async { Err(DispatchError::UnsupportedPrepared) })
    }
    fn finalize_delivery_failure<'a>(
        &'a self,
        _request: DispatchRequest<'a>,
        _event: TaskEvent,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async { Err(DispatchError::UnsupportedPrepared) })
    }
}

/// Maps a [`TransportType`] to the dispatcher that can reach it.
///
/// Selection uses the transport of the **re-validated** endpoint (the registry
/// is the single source of truth for identity), never a transport guessed from
/// the task. A transport with no dispatcher is a failure, not a silent drop (see
/// [`Worker::run`]) and not a panic.
///
/// The table is only ever probed by key, never iterated, so its ordering is not
/// part of any contract — which is why a `HashMap` is enough
/// ([`TransportType`] is `Hash + Eq` but not `Ord`). It is built at assembly
/// time and immutable afterwards, so there is no lock and nothing to hold across
/// an `await`.
#[derive(Default)]
pub struct DispatcherRegistry {
    by_transport: HashMap<TransportType, Arc<dyn TaskDispatcher>>,
}

impl DispatcherRegistry {
    /// An empty registry: every dispatch attempt fails with “no dispatcher”.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `dispatcher` for `transport`, replacing any previous entry.
    pub fn with(mut self, transport: TransportType, dispatcher: Arc<dyn TaskDispatcher>) -> Self {
        self.by_transport.insert(transport, dispatcher);
        self
    }

    /// The dispatcher registered for `transport`, if any.
    pub fn get(&self, transport: TransportType) -> Option<&Arc<dyn TaskDispatcher>> {
        self.by_transport.get(&transport)
    }

    /// Number of registered transports.
    pub fn len(&self) -> usize {
        self.by_transport.len()
    }

    /// Whether no dispatcher is registered.
    pub fn is_empty(&self) -> bool {
        self.by_transport.is_empty()
    }
}

impl fmt::Debug for DispatcherRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Dispatchers are opaque trait objects; report the shape, not internals.
        f.debug_struct("DispatcherRegistry")
            .field("transports", &self.by_transport.len())
            .finish()
    }
}

/// A task waiting for its turn, ordered by priority then arrival.
///
/// `Priority` already orders by urgency with larger values more urgent, so the
/// max-heap pops the most urgent task first. Equal priorities are ordered by an
/// arrival counter that increases in channel order, which makes same-priority
/// ordering strict FIFO and fully deterministic.
#[derive(Debug)]
struct QueuedTask {
    priority: Priority,
    arrival: u64,
    task: AgentTask,
}

impl PartialEq for QueuedTask {
    fn eq(&self, other: &Self) -> bool {
        // Deliberately ignores `task`: equality must agree with `Ord`, which
        // orders by (priority, arrival) only.
        self.priority == other.priority && self.arrival == other.arrival
    }
}

impl Eq for QueuedTask {}

impl PartialOrd for QueuedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            // Reversed: the earliest arrival must compare as the greater value so
            // the max-heap yields FIFO within one priority.
            .then_with(|| other.arrival.cmp(&self.arrival))
    }
}

/// The worker's local, bounded priority queue.
#[derive(Debug, Default)]
struct PriorityQueue {
    heap: BinaryHeap<QueuedTask>,
}

impl PriorityQueue {
    fn push(&mut self, task: AgentTask, arrival: u64) {
        self.heap.push(QueuedTask {
            priority: task.priority,
            arrival,
            task,
        });
    }

    fn pop(&mut self) -> Option<AgentTask> {
        self.heap.pop().map(|queued| queued.task)
    }

    fn len(&self) -> usize {
        self.heap.len()
    }
}

/// Move currently available tasks into `queue` without letting the queue exceed
/// the channel's own capacity. Returns `true` once the channel is closed *and*
/// drained.
///
/// The read budget is `capacity − queue.len()`, where `capacity` is read from the
/// channel ([`mpsc::Receiver::max_capacity`]). Deriving the budget from the
/// channel — instead of draining until `try_recv` reports `Empty` — is what keeps
/// the local queue bounded: the channel is bounded, but producers can refill freed
/// slots *while this step runs*, so a step that stopped only on `Empty` could run
/// for as long as submissions keep up, moving an unbounded number of tasks into
/// the queue without ever reaching the pop side.
///
/// This step therefore performs at most `capacity` `try_recv` calls and always
/// returns. The budget is a ceiling, never a target: an idle channel still ends
/// the step early on `Empty`, and buffered tasks are always read out before
/// `Disconnected` is reported, so a closed channel is drained rather than
/// signalled early.
fn admit_ready(
    tasks: &mut mpsc::Receiver<AgentTask>,
    queue: &mut PriorityQueue,
    arrival: &mut u64,
) -> bool {
    let budget = tasks.max_capacity().saturating_sub(queue.len());
    for _ in 0..budget {
        match tasks.try_recv() {
            Ok(task) => {
                queue.push(task, *arrival);
                *arrival += 1;
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => return true,
        }
    }
    false
}

/// A fail-stop reason for [`Worker::run`].
///
/// Only event-write failures live here: a failed *transition* (bad target, no
/// dispatcher, not accepted, execution failed) is recorded as a `Failed` event
/// and leaves the worker running. Every variant renders fixed text plus a
/// [`TaskId`] and a sequence number — never an address, command or credential.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WorkerError {
    /// The event stream is closed; no further transition can be recorded.
    ///
    /// The worker stops, drops the task receiver, and later submissions report
    /// [`BusError::TaskChannelClosed`].
    #[error("event stream is closed; task {task_id} could not record seq {seq}")]
    EventStreamClosed {
        /// The task whose transition was lost.
        task_id: TaskId,
        /// The sequence number that could not be written.
        seq: u64,
    },
    /// The event buffer is full: the event stream is stalled (a slow consumer is
    /// holding the broadcast loop), so state recording is no longer reliable.
    #[error("event buffer is full; task {task_id} could not record seq {seq}")]
    EventBackpressure {
        /// The task whose transition was lost.
        task_id: TaskId,
        /// The sequence number that could not be written.
        seq: u64,
    },
    /// Defensive catch-all: an event write failed for a reason that cannot
    /// originate from an event write (the remaining [`BusError`] variants belong
    /// to target validation and the task queue).
    #[error("event write failed for task {task_id} at seq {seq}: {error}")]
    EventWrite {
        /// The task whose transition was lost.
        task_id: TaskId,
        /// The sequence number that could not be written.
        seq: u64,
        /// The underlying bus error.
        error: BusError,
    },
}

// ---------------------------------------------------------------------------
// Runtime-control configuration (T006)
// ---------------------------------------------------------------------------

/// The `Config.bridge.max_task_depth` default, repeated here so
/// [`LoopLimits::default`] and `Worker::new` can exist without a `Config`.
///
/// Private and pinned by a unit test against the real configuration defaults, so
/// the two definitions of the same policy cannot drift apart.
const DEFAULT_MAX_DEPTH: u32 = 8;

/// The `Config.bridge.max_task_hops` default (see [`DEFAULT_MAX_DEPTH`]).
const DEFAULT_MAX_HOPS: u32 = 16;

/// The `depth`/`hops` ceilings above which the worker blocks delivery.
///
/// Both fields are ceilings, not budgets: a task is blocked when its value is
/// *greater* than the limit, so `depth == max_depth` is still allowed and a root
/// task (`depth = 0`) passes at `max_depth = 0`.
///
/// This is the stateless half of ARCHITECTURE §7's cycle detection. The
/// stateful half — “has this call chain already visited this agent”, and the
/// maximum number of subtasks — needs the persisted task tree (`parent_task_id`
/// backtracking) and belongs to T009; see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopLimits {
    /// Maximum task-tree nesting depth (`AgentTask::depth`, root = 0).
    pub max_depth: u32,
    /// Maximum agent-to-agent hops (`AgentTask::hops`, a user submission = 0).
    pub max_hops: u32,
}

impl LoopLimits {
    /// Take the limits from a validated bridge configuration.
    ///
    /// `Config.bridge` is the single source of truth for these values — T003
    /// validates their ranges (`1..=64` and `1..=256`) and applies the defaults
    /// (8 and 16) — so the worker never hardcodes a second policy. T017
    /// assembles this.
    pub fn from_bridge(bridge: &BridgeConfig) -> Self {
        Self {
            max_depth: bridge.max_task_depth,
            max_hops: bridge.max_task_hops,
        }
    }

    /// `Some(reason)` when `task` is over a limit, `None` when it is within them.
    ///
    /// The reason is what reaches the task's `Failed` payload, so it names only
    /// bounded identifiers — task identity, the two observed values and their
    /// limits, and which limit was hit. It never renders `AgentTask::text`, an
    /// [`crate::models::EndpointAddress`], or any credential.
    fn violation(&self, task: &AgentTask) -> Option<String> {
        let depth_over = task.depth > self.max_depth;
        let hops_over = task.hops > self.max_hops;
        if !depth_over && !hops_over {
            return None;
        }

        let mut hit = Vec::new();
        if depth_over {
            hit.push("depth");
        }
        if hops_over {
            hit.push("hops");
        }

        Some(format!(
            "cycle limit exceeded (hit: {}): task_id={} root_task_id={} parent_task_id={} \
             from_agent={} to_agent={} depth={} (max {}) hops={} (max {})",
            hit.join(", "),
            task.task_id,
            task.root_task_id,
            task.parent_task_id
                .map_or_else(|| "none".to_string(), |parent| parent.to_string()),
            task.from_agent,
            task.to_agent,
            task.depth,
            self.max_depth,
            task.hops,
            self.max_hops,
        ))
    }
}

impl Default for LoopLimits {
    /// The `Config.bridge` defaults (depth 8, hops 16).
    ///
    /// `Worker::new`/`Worker::with_dispatcher` take no configuration argument, so
    /// this is what their workers enforce; a unit test pins it against the real
    /// configuration defaults.
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_hops: DEFAULT_MAX_HOPS,
        }
    }
}

/// How many delivery attempts a task gets, and how long the worker waits between
/// them.
///
/// The default is exactly the T005 behaviour: one attempt, no retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts allowed for one task, *including* the first. `1` means “no
    /// retry”; `0` is treated as `1` so a task always runs once.
    pub max_attempts: u32,
    /// Delay before a retry. Waited out through the injected [`Timer`]; without
    /// one the next attempt starts immediately.
    pub backoff: Backoff,
}

impl RetryPolicy {
    /// Whether another attempt is allowed after `attempt` was refused.
    ///
    /// A refused attempt is retried only while it is not the last one the policy
    /// allows, which is what bounds the attempt loop.
    fn allows_another_attempt_after(&self, attempt: u32) -> bool {
        attempt < self.max_attempts.max(1)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff: Backoff::None,
        }
    }
}

/// The delay between retry attempts.
///
/// Deliberately jitter-free: the worker's behaviour must be reproducible in
/// tests, and a retry storm across many tasks is a concurrency-control concern
/// (T016), not something to hide in a random sleep.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Backoff {
    /// Retry immediately (the default).
    #[default]
    None,
    /// Wait the same delay before every retry.
    Fixed(Duration),
    /// Wait `base * 2^(k-1)` before the retry that follows attempt `k`, capped
    /// at `max`.
    Exponential {
        /// Delay before the first retry.
        base: Duration,
        /// Upper bound for the doubled delay.
        max: Duration,
    },
}

impl Backoff {
    /// The delay the worker waits before the retry that follows `failed_attempt`.
    ///
    /// [`Backoff::None`] yields the zero duration, which the worker reads as
    /// “retry immediately” — it never calls a timer for it. The exponential curve
    /// saturates instead of overflowing, so an absurd attempt count cannot panic
    /// or wrap into a short delay.
    pub fn delay_for(&self, failed_attempt: u32) -> Duration {
        match *self {
            Backoff::None => Duration::ZERO,
            Backoff::Fixed(delay) => delay,
            Backoff::Exponential { base, max } => {
                // `2^(k-1)`, clamped before shifting so the shift cannot overflow.
                let shift = failed_attempt.saturating_sub(1).min(31);
                base.saturating_mul(1u32 << shift).min(max)
            }
        }
    }
}

/// The opt-in runtime controls of a [`Worker`].
///
/// A plain value: every field is either a policy (`limits`, `retry`) or an
/// injected handle ([`Cancellation`], [`Timer`]) the worker uses but does not
/// create. `Default` reproduces T005 exactly — one attempt, no cancellation
/// source, no timer, the configuration's default `depth`/`hops` ceilings — which
/// is why [`Worker::new`]/[`Worker::with_dispatcher`] can keep their frozen
/// signatures and their semantics.
#[derive(Clone, Default)]
pub struct WorkerConfig {
    /// `depth`/`hops` ceilings. Always enforced, in every construction path.
    pub limits: LoopLimits,
    /// Attempt count and backoff. Defaults to a single attempt.
    pub retry: RetryPolicy,
    /// Cancellation signal source. `None` means the worker can never be
    /// cancelled — the T005 behaviour.
    pub cancellation: Option<Cancellation>,
    /// Asynchronous wait boundary, used to preempt a deadline and to wait out a
    /// backoff. `None` disables preemption: the deadline is then enforced at the
    /// checkpoints only.
    pub timer: Option<Arc<dyn Timer>>,
}

impl fmt::Debug for WorkerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The injected handles are trait objects and opaque; report whether they
        // are present instead of trying to print them.
        f.debug_struct("WorkerConfig")
            .field("limits", &self.limits)
            .field("retry", &self.retry)
            .field("cancellation", &self.cancellation.is_some())
            .field("timer", &self.timer.is_some())
            .finish()
    }
}

/// Assembles a [`Worker`] whose runtime controls differ from the defaults.
///
/// Returned by [`Worker::builder`]. The frozen constructors stay as they are:
/// this is the additive path for timeout, cancellation, retry and cycle limits.
pub struct WorkerBuilder {
    worker: Worker,
}

impl WorkerBuilder {
    /// Replace the whole [`WorkerConfig`].
    pub fn config(mut self, config: WorkerConfig) -> Self {
        self.worker.config = config;
        self
    }

    /// Install the crate-owned durable transition authority.
    pub fn lifecycle(mut self, lifecycle: Arc<dyn TaskLifecycle>) -> Self {
        self.worker.lifecycle = Some(lifecycle);
        self
    }

    /// Register a single dispatcher, exactly as [`Worker::with_dispatcher`] does.
    pub fn with_dispatcher(
        mut self,
        transport: TransportType,
        dispatcher: Arc<dyn TaskDispatcher>,
    ) -> Self {
        let registered = std::mem::take(&mut self.worker.dispatchers);
        self.worker.dispatchers = registered.with(transport, dispatcher);
        self
    }

    /// The configured worker.
    pub fn build(self) -> Worker {
        self.worker
    }
}

impl fmt::Debug for WorkerBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerBuilder")
            .field("config", &self.worker.config)
            .field("dispatchers", &self.worker.dispatchers.len())
            .finish()
    }
}

/// Consumes the task channel and drives tasks through the state machine.
///
/// Assembled with the *same* [`EventSink`] the [`Bus`](crate::bus::Bus) writes
/// to (`MemoryBus::with_event_sink`, T004's injection point), so the bus's
/// `Queued` and the worker's `seq > 1` events land in one stream. The event
/// stream's receiving end is owned by [`crate::bus::EventBroadcaster`]; the
/// worker never reads it and never creates a second stream.
///
/// See the module docs for lifecycle, failure and ordering contracts.
pub struct Worker {
    registry: Arc<EndpointRegistry>,
    tasks: mpsc::Receiver<AgentTask>,
    events: Arc<dyn EventSink>,
    clock: Clock,
    dispatchers: DispatcherRegistry,
    queue: PriorityQueue,
    /// Runtime controls. [`WorkerConfig::default`] is the T005 behaviour, so the
    /// frozen constructors keep working unchanged.
    config: WorkerConfig,
    lifecycle: Option<Arc<dyn TaskLifecycle>>,
}

impl Worker {
    /// Build a worker over the task receiver handed out by the bus.
    ///
    /// Uses [`WorkerConfig::default`]: a single attempt, no cancellation source
    /// and no timer, so the deadline and cycle checks are enforced at the
    /// checkpoints and nothing can preempt an in-flight stage. Use
    /// [`Worker::builder`] for timeout/cancellation/retry.
    pub fn new(
        registry: Arc<EndpointRegistry>,
        tasks: mpsc::Receiver<AgentTask>,
        events: Arc<dyn EventSink>,
        clock: Clock,
        dispatchers: DispatcherRegistry,
    ) -> Self {
        Self {
            registry,
            tasks,
            events,
            clock,
            dispatchers,
            queue: PriorityQueue::default(),
            config: WorkerConfig::default(),
            lifecycle: None,
        }
    }

    /// Start building a worker with non-default runtime controls.
    ///
    /// The arguments are exactly [`Worker::new`]'s — this is an additive entry
    /// point, not a changed signature — and the returned [`WorkerBuilder`]
    /// starts from the same defaults, so `.build()` without any call is
    /// indistinguishable from [`Worker::new`].
    pub fn builder(
        registry: Arc<EndpointRegistry>,
        tasks: mpsc::Receiver<AgentTask>,
        events: Arc<dyn EventSink>,
        clock: Clock,
        dispatchers: DispatcherRegistry,
    ) -> WorkerBuilder {
        WorkerBuilder {
            worker: Self::new(registry, tasks, events, clock, dispatchers),
        }
    }

    /// Build a worker with a single dispatcher for `transport`.
    ///
    /// Convenience over [`Worker::new`] for the common single-transport assembly
    /// (and for tests); it is exactly
    /// `DispatcherRegistry::new().with(transport, dispatcher)`.
    pub fn with_dispatcher(
        registry: Arc<EndpointRegistry>,
        tasks: mpsc::Receiver<AgentTask>,
        events: Arc<dyn EventSink>,
        clock: Clock,
        transport: TransportType,
        dispatcher: Arc<dyn TaskDispatcher>,
    ) -> Self {
        Self::new(
            registry,
            tasks,
            events,
            clock,
            DispatcherRegistry::new().with(transport, dispatcher),
        )
    }

    /// Run the consumption loop until the task channel closes.
    ///
    /// Returns `Ok(())` after a drain-then-terminate shutdown (channel closed and
    /// the local priority heap empty). Returns `Err` only for a fail-stop
    /// event-write failure; see the module docs.
    ///
    /// The channel's capacity bounds the local heap as well as the channel: each
    /// admission step reads at most `capacity − heap.len()` tasks, so sustained
    /// submissions that refill the channel during a drain cannot grow the backlog
    /// without bound or starve dispatch. See [`admit_ready`].
    pub async fn run(mut self) -> Result<(), WorkerError> {
        // Arrival counter for the FIFO tie-break. It follows channel order, which
        // is submission order.
        let mut arrival: u64 = 0;
        let mut closed = false;

        loop {
            // 1. Take what is available — but only as much as the local queue may
            //    hold. `admit_ready` derives its read budget from the channel's
            //    own capacity, so a producer refilling freed slots cannot extend a
            //    single step: the step always returns to the pop side below.
            //    Only the pop side reorders.
            if !closed {
                closed = admit_ready(&mut self.tasks, &mut self.queue, &mut arrival);
            }

            // 2. Always work on the most urgent task on hand. Shutdown is
            //    drain-then-terminate: a closed channel still drains the heap, so
            //    an accepted task is never dropped.
            //
            // T016 extension point: concurrency control replaces this single
            // in-flight `process` call; T005 is serial by design and has no
            // limiter, semaphore or rejection path.
            match self.queue.pop() {
                Some(task) => self.process(task).await?,
                None if closed => return Ok(()),
                // 3. Nothing to do: wait for either a task or a close.
                None => match self.tasks.recv().await {
                    Some(task) => {
                        self.queue.push(task, arrival);
                        arrival += 1;
                    }
                    None => return Ok(()),
                },
            }
        }
    }

    /// Run the worker on the current runtime, for owners that want a handle.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime context (mirrors
    /// [`tokio::spawn`]).
    pub fn spawn(self) -> tokio::task::JoinHandle<Result<(), WorkerError>> {
        tokio::spawn(self.run())
    }

    /// Drive one task through its transitions, from `seq = 2` to its terminal
    /// event.
    ///
    /// This is the single implementation of the checkpoint order, the `seq`
    /// grammar and the retry/limit rules documented on the module: every branch
    /// either loops for another attempt or writes exactly one terminal event.
    async fn process(&self, task: AgentTask) -> Result<(), WorkerError> {
        let task_id = task.task_id;
        let deadline = task.deadline;

        // The worker now owns this task, so its cancellation record is bound to
        // that ownership: a reason recorded while the task sat in the queue moves
        // with it (no longer displaceable by the pre-arm cap), and exactly one
        // terminal transition below retires it. See `control`'s lifecycle
        // contract.
        if let Some(cancellation) = &self.config.cancellation {
            cancellation.activate(task_id);
        }

        // CP-0 — the dispatch-time gate. Every branch below terminates the task
        // at `seq = 2` without a `Dispatched` event, so a task blocked here is
        // never handed to a dispatcher. Order: cancel, deadline, cycle limits.
        if let Some(reason) = self.cancel_reason(task_id) {
            return self.cancel_task(task_id, 2, reason).await;
        }
        if let Some(deadline) = self.expired(deadline) {
            return self.timed_out_task(task_id, 2, deadline).await;
        }
        if let Some(error) = self.config.limits.violation(&task) {
            // A cycle hit reuses `Failed` (the model has no `LoopDetected`) and
            // blocks delivery: no `Dispatched`, no dispatcher call.
            return self.fail_task(task_id, 2, error).await;
        }

        // Re-validate against this worker's registry. The registry is immutable
        // after construction, so in the v1 assembly this is defensive — but the
        // worker may hold a different registry than the submitter, and
        // T010/T014 will make `matrix`/`http` addressable. Either way the branch
        // is reachable and tested, and it is never a silent drop.
        let target = match self.registry.validate_target(task.to_agent) {
            Ok(target) => target,
            Err(err) => {
                // Path A: nothing was dispatched, so this is the first event after
                // the bus's `Queued` and it takes `seq = 2`.
                return self.fail_task(task_id, 2, err.to_string()).await;
            }
        };

        // Dispatch selection by the validated transport. No dispatcher means the
        // task cannot run: fail explicitly instead of dropping it or panicking.
        let Some(dispatcher) = self.dispatchers.get(target.transport()) else {
            let error = format!(
                "no dispatcher registered for transport {:?}",
                target.transport()
            );
            return self.fail_task(task_id, 2, error).await;
        };

        // Durable queue admission is consumed here, immediately before the
        // first delivery event. The lifecycle implementation owns the real
        // workspace-derived lease and returns its captured fence; Worker never
        // fabricates an execution identity.
        let queue_lease = if let Some(lifecycle) = &self.lifecycle {
            match lifecycle.claim_queue(&task).await {
                Ok(value) => value,
                Err(error) => return self.fail_task(task_id, 2, error.to_string()).await,
            }
        } else {
            None
        };

        // One subscription for the whole task, created before the first await
        // that a cancellation could race. The receiver is version-triggered, so a
        // signal recorded after this point cannot be missed, and one recorded
        // before it is caught by the synchronous checkpoints (CP-0/CP-1).
        let mut cancel_rx = self
            .config
            .cancellation
            .as_ref()
            .map(Cancellation::subscribe);

        // The bus owns `seq = 1`; this worker owns 2 upwards. `attempt` lives
        // only in this call: no cross-task counter, no `seen` set, no dedup
        // (T005 D6).
        let mut seq: u64 = 2;
        let mut attempt: u32 = 1;

        loop {
            // CP-1 — before every `Dispatched`, retries included.
            if let Some(reason) = self.cancel_reason(task_id) {
                return self.cancel_task(task_id, seq, reason).await;
            }
            if let Some(deadline) = self.expired(deadline) {
                return self.timed_out_task(task_id, seq, deadline).await;
            }

            // One delivery attempt: a fresh identity and an incremented attempt,
            // so a retry is a new delivery record rather than a rewrite of the
            // previous one. Both stages of this attempt share both fields.
            let delivery_id = DeliveryId::generate();
            let request = DispatchRequest {
                task: &task,
                target,
                delivery_id,
                attempt,
            };

            // Paths B/C: the delivery attempt is recorded before it is made.
            let dispatched = TaskEvent {
                id: EventId::generate(),
                task_id,
                seq,
                status: TaskStatus::Dispatched,
                timestamp: self.clock.now(),
                payload: TaskEventPayload::Dispatched {
                    delivery_id,
                    attempt,
                },
            };
            self.emit(dispatched, task_id, seq).await?;
            seq += 1;

            if let (Some(lifecycle), Some(lease)) = (&self.lifecycle, &queue_lease)
                && let Err(error) = lifecycle.mark_queue_send_started(&task, lease).await
            {
                return self.fail_task(task_id, seq, error.to_string()).await;
            }

            // Stage one, raced: a `deliver` that never resolves is abandoned by
            // the timeout/cancellation boundary instead of blocking the worker.
            if let (Some(lifecycle), Some(lease)) = (&self.lifecycle, &queue_lease)
                && let Err(error) = lifecycle.renew_queue_lease(&task, lease).await
            {
                return self.fail_task(task_id, seq, error.to_string()).await;
            }
            match self
                .race(
                    task_id,
                    deadline,
                    &mut cancel_rx,
                    dispatcher.deliver(request.clone()),
                )
                .await
            {
                Raced::Done(Ok(())) => {}
                Raced::Done(Err(err)) => {
                    if dispatcher.supports_prepared() {
                        let event = TaskEvent {
                            id: EventId::generate(),
                            task_id,
                            seq,
                            status: TaskStatus::Failed,
                            timestamp: self.clock.now(),
                            payload: TaskEventPayload::Failed {
                                error: err.to_string(),
                            },
                        };
                        dispatcher
                            .finalize_delivery_failure(request.clone(), event)
                            .await
                            .map_err(|_| WorkerError::EventWrite {
                                task_id,
                                seq,
                                error: BusError::TaskChannelClosed,
                            })?;
                        return Ok(());
                    }
                    match self
                        .refused(task_id, attempt, &err, deadline, &mut cancel_rx)
                        .await
                    {
                        Raced::Done(Some(next_attempt)) => {
                            attempt = next_attempt;
                            continue;
                        }
                        Raced::Done(None) => {
                            return self.fail_task(task_id, seq, err.to_string()).await;
                        }
                        Raced::Cancelled(reason) => {
                            return self.cancel_task(task_id, seq, reason).await;
                        }
                        Raced::TimedOut(deadline) => {
                            return self.timed_out_task(task_id, seq, deadline).await;
                        }
                    }
                }
                Raced::Cancelled(reason) => return self.cancel_task(task_id, seq, reason).await,
                Raced::TimedOut(deadline) => {
                    return self.timed_out_task(task_id, seq, deadline).await;
                }
            }

            // CP-2 — acceptance was observed, so the task may become `Running`;
            // a cancellation or an expiry that landed meanwhile still wins.
            if let Some(reason) = self.cancel_reason(task_id) {
                if dispatcher.supports_prepared() {
                    let capability = dispatcher
                        .cancel_prepared(request.clone(), &reason)
                        .await
                        .map_err(|_| WorkerError::EventWrite {
                            task_id,
                            seq,
                            error: BusError::TaskChannelClosed,
                        })?;
                    return self
                        .finalize_capability(
                            task_id,
                            seq,
                            TaskStatus::Cancelled,
                            TaskEventPayload::Cancelled { reason },
                            capability,
                        )
                        .await;
                }
                return self.cancel_task(task_id, seq, reason).await;
            }
            if let Some(deadline) = self.expired(deadline) {
                if dispatcher.supports_prepared() {
                    let capability = dispatcher
                        .cancel_prepared(request.clone(), "task deadline elapsed")
                        .await
                        .map_err(|_| WorkerError::EventWrite {
                            task_id,
                            seq,
                            error: BusError::TaskChannelClosed,
                        })?;
                    return self
                        .finalize_capability(
                            task_id,
                            seq,
                            TaskStatus::TimedOut,
                            TaskEventPayload::TimedOut { deadline },
                            capability,
                        )
                        .await;
                }
                return self.timed_out_task(task_id, seq, deadline).await;
            }

            // The task is now running. `started_at` reuses the event timestamp, so
            // a fixed clock makes both exactly assertable.
            let started_at = self.clock.now();
            let running = TaskEvent {
                id: EventId::generate(),
                task_id,
                seq,
                status: TaskStatus::Running,
                timestamp: started_at,
                payload: TaskEventPayload::Running { started_at },
            };
            self.emit(running, task_id, seq).await?;
            seq += 1;

            // Stage two, raced like stage one. Terminal outcomes come from the
            // adapter (`DispatchError`'s `Display`, or the `DispatchOutcome`
            // error string) — the worker never invents one.
            if dispatcher.supports_prepared() {
                match self
                    .race(
                        task_id,
                        deadline,
                        &mut cancel_rx,
                        dispatcher.execute_prepared(request.clone()),
                    )
                    .await
                {
                    Raced::Done(Ok(prepared)) => {
                        return self.finalize_prepared(task_id, seq, prepared).await;
                    }
                    Raced::Done(Err(error)) => {
                        return self.fail_task(task_id, seq, error.to_string()).await;
                    }
                    Raced::Cancelled(reason) => {
                        let capability = dispatcher
                            .cancel_prepared(request.clone(), &reason)
                            .await
                            .map_err(|_| WorkerError::EventWrite {
                                task_id,
                                seq,
                                error: BusError::TaskChannelClosed,
                            })?;
                        return self
                            .finalize_capability(
                                task_id,
                                seq,
                                TaskStatus::Cancelled,
                                TaskEventPayload::Cancelled { reason },
                                capability,
                            )
                            .await;
                    }
                    Raced::TimedOut(deadline) => {
                        let capability = dispatcher
                            .cancel_prepared(request.clone(), "task deadline elapsed")
                            .await
                            .map_err(|_| WorkerError::EventWrite {
                                task_id,
                                seq,
                                error: BusError::TaskChannelClosed,
                            })?;
                        return self
                            .finalize_capability(
                                task_id,
                                seq,
                                TaskStatus::TimedOut,
                                TaskEventPayload::TimedOut { deadline },
                                capability,
                            )
                            .await;
                    }
                }
            }
            match self
                .race(
                    task_id,
                    deadline,
                    &mut cancel_rx,
                    dispatcher.execute(request),
                )
                .await
            {
                Raced::Done(Ok(DispatchOutcome::Completed { output })) => {
                    return self.completed_task(task_id, seq, output).await;
                }
                // A deterministic verdict from the adapter is terminal: retrying
                // it would repeat side effects, which is the submitter's policy.
                Raced::Done(Ok(DispatchOutcome::Failed { error })) => {
                    return self.fail_task(task_id, seq, error).await;
                }
                Raced::Done(Err(err)) => {
                    match self
                        .refused(task_id, attempt, &err, deadline, &mut cancel_rx)
                        .await
                    {
                        Raced::Done(Some(next_attempt)) => {
                            attempt = next_attempt;
                            continue;
                        }
                        Raced::Done(None) => {
                            return self.fail_task(task_id, seq, err.to_string()).await;
                        }
                        Raced::Cancelled(reason) => {
                            return self.cancel_task(task_id, seq, reason).await;
                        }
                        Raced::TimedOut(deadline) => {
                            return self.timed_out_task(task_id, seq, deadline).await;
                        }
                    }
                }
                Raced::Cancelled(reason) => return self.cancel_task(task_id, seq, reason).await,
                Raced::TimedOut(deadline) => {
                    return self.timed_out_task(task_id, seq, deadline).await;
                }
            }
        }
    }

    /// Race one dispatch stage against cancellation and the deadline.
    ///
    /// The order is part of the contract: `biased` polls the stage first, so an
    /// already-available result is never overwritten by a signal that arrived in
    /// the same poll, and cancellation then outranks the clock. Both losing
    /// futures are dropped with the `select!`, which is why this is preemption
    /// and not [`tokio::task::JoinHandle::abort`]: the worker spawns nothing, so
    /// there is no task to abort and no state left behind.
    async fn race<T>(
        &self,
        task_id: TaskId,
        deadline: Option<DateTime<Utc>>,
        cancel_rx: &mut Option<watch::Receiver<u64>>,
        stage: impl Future<Output = T>,
    ) -> Raced<T> {
        let cancelled =
            wait_for_cancellation(self.config.cancellation.as_ref(), task_id, cancel_rx);
        let expired = wait_for_deadline(self.config.timer.as_deref(), deadline);

        tokio::select! {
            biased;
            result = stage => Raced::Done(result),
            reason = cancelled => Raced::Cancelled(reason),
            deadline = expired => Raced::TimedOut(deadline),
        }
    }

    /// Handle a refused stage for `attempt`.
    ///
    /// Returns the next attempt number once the backoff wait (if there is one)
    /// completed, `None` when the failure is terminal for this task, or the
    /// outcome of the race that the backoff wait lost.
    async fn refused(
        &self,
        task_id: TaskId,
        attempt: u32,
        error: &DispatchError,
        deadline: Option<DateTime<Utc>>,
        cancel_rx: &mut Option<watch::Receiver<u64>>,
    ) -> Raced<Option<u32>> {
        // Only a refused stage is transient. This `match` is exhaustive on
        // purpose: it is the single place a future `DispatchError` variant must
        // be classified as retryable or not.
        let retryable = match error {
            // The task never entered `Running`, so redelivery cannot duplicate
            // execution.
            DispatchError::NotAccepted { .. } => true,
            // Acceptance was lost before a terminal result existed.
            DispatchError::ExecutionFailed { .. } => true,
            DispatchError::UnsupportedPrepared
            | DispatchError::AcceptanceUnknown
            | DispatchError::RecoveryNeeded => false,
        };
        if !retryable || !self.config.retry.allows_another_attempt_after(attempt) {
            return Raced::Done(None);
        }
        // CP-3: retrying past the deadline would only race another timeout, so
        // the failure is reported as it stands instead of being retried.
        if self.expired(deadline).is_some() {
            return Raced::Done(None);
        }

        let next_attempt = attempt + 1;
        // A non-terminal attempt deliberately leaves no event behind: the model
        // has no “retrying” status, and a `Failed` event would read as terminal
        // and as a state regression. The reason is recorded here instead —
        // `DispatchError`'s `Display` carries no address or credential.
        warn!(
            task_id = %task_id,
            attempt,
            reason = %error,
            "retrying a refused dispatch attempt"
        );

        match self
            .wait_backoff(task_id, attempt, deadline, cancel_rx)
            .await
        {
            Raced::Done(()) => Raced::Done(Some(next_attempt)),
            Raced::Cancelled(reason) => Raced::Cancelled(reason),
            Raced::TimedOut(deadline) => Raced::TimedOut(deadline),
        }
    }

    /// Wait out the configured backoff before the next attempt.
    ///
    /// The wait races cancellation and the deadline like any other stage, so a
    /// signal or an expiry during a backoff still lands deterministically.
    /// Without a configured delay — or without an injected timer — the next
    /// attempt starts immediately; that is documented behaviour, not an error.
    async fn wait_backoff(
        &self,
        task_id: TaskId,
        failed_attempt: u32,
        deadline: Option<DateTime<Utc>>,
        cancel_rx: &mut Option<watch::Receiver<u64>>,
    ) -> Raced<()> {
        let delay = self.config.retry.backoff.delay_for(failed_attempt);
        if delay.is_zero() {
            return Raced::Done(());
        }
        let Some(timer) = self.config.timer.as_deref() else {
            return Raced::Done(());
        };

        let until = deadline_after(self.clock.now(), delay);
        self.race(task_id, deadline, cancel_rx, wait_until(timer, until))
            .await
    }

    /// The cancellation reason that applies to `task_id`, if it is cancelled.
    fn cancel_reason(&self, task_id: TaskId) -> Option<String> {
        self.config
            .cancellation
            .as_ref()
            .and_then(|cancellation| cancellation.reason_for(task_id))
    }

    /// `Some(deadline)` when the task has already passed it.
    ///
    /// `Option::filter` never calls the predicate for `None`, so a task without a
    /// deadline does not even read the clock here.
    fn expired(&self, deadline: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
        deadline.filter(|deadline| *deadline <= self.clock.now())
    }

    /// Write a `Failed` transition, atomically, and classify a write failure.
    async fn fail_task(&self, task_id: TaskId, seq: u64, error: String) -> Result<(), WorkerError> {
        self.terminate(
            task_id,
            seq,
            TaskStatus::Failed,
            TaskEventPayload::Failed { error },
        )
        .await
    }

    /// Write a `Completed` transition.
    async fn completed_task(
        &self,
        task_id: TaskId,
        seq: u64,
        output: String,
    ) -> Result<(), WorkerError> {
        self.terminate(
            task_id,
            seq,
            TaskStatus::Completed,
            TaskEventPayload::Completed { output },
        )
        .await
    }

    /// Write a `TimedOut` transition.
    async fn timed_out_task(
        &self,
        task_id: TaskId,
        seq: u64,
        deadline: DateTime<Utc>,
    ) -> Result<(), WorkerError> {
        self.terminate(
            task_id,
            seq,
            TaskStatus::TimedOut,
            TaskEventPayload::TimedOut { deadline },
        )
        .await
    }

    /// Write a `Cancelled` transition.
    async fn cancel_task(
        &self,
        task_id: TaskId,
        seq: u64,
        reason: String,
    ) -> Result<(), WorkerError> {
        self.terminate(
            task_id,
            seq,
            TaskStatus::Cancelled,
            TaskEventPayload::Cancelled { reason },
        )
        .await
    }

    /// Write one terminal transition.
    ///
    /// Every terminal status is written here and nowhere else, so one call to
    /// `process` cannot produce two terminal events for a task. That also makes
    /// this the single place a task's cancellation record is retired: once the
    /// task is terminal, a targeted cancellation for its id can no longer affect
    /// anything, so the handle drops it instead of retaining it.
    async fn terminate(
        &self,
        task_id: TaskId,
        seq: u64,
        status: TaskStatus,
        payload: TaskEventPayload,
    ) -> Result<(), WorkerError> {
        let event = TaskEvent {
            id: EventId::generate(),
            task_id,
            seq,
            status,
            timestamp: self.clock.now(),
            payload,
        };
        let result = self.emit(event, task_id, seq).await;
        // Released even when the write failed: a fail-stop stops the worker, so
        // this task can never be retried and the record must not be kept.
        if let Some(cancellation) = &self.config.cancellation {
            cancellation.retire(task_id);
        }
        result
    }

    async fn finalize_prepared(
        &self,
        task_id: TaskId,
        seq: u64,
        prepared: PreparedExecution,
    ) -> Result<(), WorkerError> {
        let (status, payload) = match prepared.outcome {
            DispatchOutcome::Completed { output } => (
                TaskStatus::Completed,
                TaskEventPayload::Completed { output },
            ),
            DispatchOutcome::Failed { error } => {
                (TaskStatus::Failed, TaskEventPayload::Failed { error })
            }
        };
        self.finalize_capability(task_id, seq, status, payload, prepared.capability)
            .await
    }

    async fn finalize_capability(
        &self,
        task_id: TaskId,
        seq: u64,
        status: TaskStatus,
        payload: TaskEventPayload,
        mut capability: FinalizationCapability,
    ) -> Result<(), WorkerError> {
        let event = TaskEvent {
            id: EventId::generate(),
            task_id,
            seq,
            status,
            timestamp: self.clock.now(),
            payload,
        };
        let sender = capability.command.take().ok_or(WorkerError::EventWrite {
            task_id,
            seq,
            error: BusError::TaskChannelClosed,
        })?;
        let (response, response_rx) = tokio::sync::oneshot::channel();
        sender
            .send(FinalizeCommand {
                task_id,
                event: event.clone(),
                response,
            })
            .map_err(|_| WorkerError::EventWrite {
                task_id,
                seq,
                error: BusError::TaskChannelClosed,
            })?;
        match response_rx.await {
            Ok(FinalizeResult::Committed | FinalizeResult::AlreadyTerminal) => {
                self.emit(event, task_id, seq).await?
            }
            Ok(FinalizeResult::Stale | FinalizeResult::Fenced | FinalizeResult::RecoveryNeeded)
            | Err(_) => {
                return Err(WorkerError::EventWrite {
                    task_id,
                    seq,
                    error: BusError::TaskChannelClosed,
                });
            }
        }
        if let Some(cancellation) = &self.config.cancellation {
            cancellation.retire(task_id);
        }
        Ok(())
    }

    /// Write exactly one event; map a write failure onto the fail-stop reasons.
    async fn emit(&self, event: TaskEvent, task_id: TaskId, seq: u64) -> Result<(), WorkerError> {
        if let Some(lifecycle) = &self.lifecycle {
            match lifecycle.transition(&event).await {
                Ok(LifecycleResult::Committed | LifecycleResult::Replay) => {}
                Ok(LifecycleResult::Stale) | Err(_) => {
                    return Err(WorkerError::EventWrite {
                        task_id,
                        seq,
                        error: BusError::TaskChannelClosed,
                    });
                }
            }
        }
        self.events.emit(event).await.map_err(|error| match error {
            BusError::EventSinkClosed => WorkerError::EventStreamClosed { task_id, seq },
            BusError::EventBufferFull => WorkerError::EventBackpressure { task_id, seq },
            other => WorkerError::EventWrite {
                task_id,
                seq,
                error: other,
            },
        })
    }
}

/// Which arm of a raced stage produced the outcome.
///
/// Private: it exists so the three-arm `select!` lives in exactly one place
/// instead of being repeated at every stage.
enum Raced<T> {
    /// The stage produced its own result.
    Done(T),
    /// A cancellation for this task won the race; carries its reason.
    Cancelled(String),
    /// The deadline won the race; carries the deadline that was exceeded.
    TimedOut(DateTime<Utc>),
}

/// Resolve once `task_id` has a cancellation reason, with that reason.
///
/// Wakes on every version bump and re-reads the reason, so a bump that was meant
/// for another task does not end the wait. The bump is level-triggered by
/// version, so a signal recorded before the first poll of this future is still
/// observed.
///
/// A closed channel is structurally impossible: the worker holds a
/// [`Cancellation`] clone, which keeps the `watch` sender alive for as long as
/// anything can be waiting. If it were observed anyway, the wait parks instead of
/// inventing a cancellation — the worker never fabricates a terminal reason.
async fn wait_for_cancellation(
    cancellation: Option<&Cancellation>,
    task_id: TaskId,
    cancel_rx: &mut Option<watch::Receiver<u64>>,
) -> String {
    let (Some(cancellation), Some(rx)) = (cancellation, cancel_rx.as_mut()) else {
        return std::future::pending::<String>().await;
    };

    loop {
        if rx.changed().await.is_err() {
            return std::future::pending::<String>().await;
        }
        if let Some(reason) = cancellation.reason_for(task_id) {
            return reason;
        }
    }
}

/// Resolve with `deadline` once the injected timer reaches it.
///
/// With no deadline, or with no injected timer, the wait never resolves and
/// `select!` behaves as if the branch were absent: only the checkpoints enforce
/// the deadline in that configuration.
async fn wait_for_deadline(
    timer: Option<&dyn Timer>,
    deadline: Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    match (timer, deadline) {
        (Some(timer), Some(deadline)) => {
            timer.sleep_until(deadline).await;
            deadline
        }
        _ => std::future::pending::<DateTime<Utc>>().await,
    }
}

/// Wait out a backoff delay through `timer`, resolving at `until`.
async fn wait_until(timer: &dyn Timer, until: DateTime<Utc>) {
    timer.sleep_until(until).await;
}

/// The absolute instant `delay` after `now`.
///
/// Saturates rather than panicking, so an extreme (operator-supplied) backoff
/// cannot turn a retry into a worker panic.
fn deadline_after(now: DateTime<Utc>, delay: Duration) -> DateTime<Utc> {
    let delay = chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::MAX);
    now.checked_add_signed(delay)
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::registry::derive_endpoint_id;
    use crate::config::Config;
    use crate::models::{ConversationId, EndpointId};
    use std::collections::BTreeMap;

    fn load(toml: &str) -> Config {
        let mut env = BTreeMap::new();
        env.insert("HOME".to_string(), "/home/tester".to_string());
        crate::config::load_from_str_with_env(toml, &env).expect("test config must be valid")
    }

    fn task(label: &str, priority: u8) -> AgentTask {
        let task_id = TaskId::generate();
        AgentTask {
            task_id,
            root_task_id: task_id,
            parent_task_id: None,
            from_agent: EndpointId::generate(),
            to_agent: EndpointId::generate(),
            conversation_id: ConversationId::generate(),
            reply_to: None,
            text: label.to_string(),
            priority: Priority::new(priority).expect("priority in range"),
            depth: 0,
            hops: 0,
            deadline: None,
            version: 0,
        }
    }

    /// Pop `labels` and return the order they came out in.
    fn drain(queue: &mut PriorityQueue) -> Vec<String> {
        let mut order = Vec::new();
        while let Some(task) = queue.pop() {
            order.push(task.text);
        }
        order
    }

    #[test]
    fn priority_queue_pops_highest_priority_first() {
        let mut queue = PriorityQueue::default();
        queue.push(task("low", 1), 0);
        queue.push(task("high", 9), 1);
        queue.push(task("mid", 5), 2);

        assert_eq!(drain(&mut queue), ["high", "mid", "low"]);
    }

    #[test]
    fn priority_queue_is_fifo_within_one_priority() {
        let mut queue = PriorityQueue::default();
        for (label, arrival) in [("a", 0), ("b", 1), ("c", 2), ("d", 3)] {
            queue.push(task(label, 5), arrival);
        }

        assert_eq!(drain(&mut queue), ["a", "b", "c", "d"]);
    }

    #[test]
    fn priority_queue_interleaves_priorities_with_stable_fifo() {
        let mut queue = PriorityQueue::default();
        // Arrival order: low1, mid1, low2, high1, mid2, high2.
        queue.push(task("low1", 1), 0);
        queue.push(task("mid1", 5), 1);
        queue.push(task("low2", 1), 2);
        queue.push(task("high1", 9), 3);
        queue.push(task("mid2", 5), 4);
        queue.push(task("high2", 9), 5);

        assert_eq!(
            drain(&mut queue),
            ["high1", "high2", "mid1", "mid2", "low1", "low2"]
        );
    }

    #[test]
    fn priority_queue_handles_boundary_priorities_and_alternating_push_pop() {
        let mut queue = PriorityQueue::default();
        assert_eq!(queue.pop(), None, "an empty queue yields nothing");

        queue.push(task("min", 0), 0);
        queue.push(task("max", 10), 1);
        assert_eq!(queue.pop().map(|t| t.text), Some("max".to_string()));
        assert_eq!(queue.pop().map(|t| t.text), Some("min".to_string()));
        assert_eq!(queue.pop(), None);

        // A single remaining task is still popped after further pushes.
        queue.push(task("only", 5), 2);
        queue.push(task("default", 5), 3);
        assert_eq!(queue.pop().map(|t| t.text), Some("only".to_string()));
        assert_eq!(queue.pop().map(|t| t.text), Some("default".to_string()));
    }

    /// Fill every free slot of `channel` (a bounded `try_send` producer).
    fn refill(channel: &mpsc::Sender<AgentTask>, label: &str, capacity: usize) {
        for index in 0..capacity {
            if channel
                .try_send(task(&format!("{label}-{index}"), 5))
                .is_err()
            {
                break;
            }
        }
    }

    #[test]
    fn one_admission_step_reads_at_most_the_channel_capacity() {
        const CAPACITY: usize = 4;
        let (tx, mut rx) = mpsc::channel::<AgentTask>(CAPACITY);
        for index in 0..CAPACITY {
            tx.try_send(task(&format!("pre-{index}"), 5)).expect("fits");
        }
        let mut queue = PriorityQueue::default();
        let mut arrival = 0u64;

        // A full channel is read out and the step returns: the budget comes from
        // the channel, so the step never depends on observing `Empty`.
        assert!(!admit_ready(&mut rx, &mut queue, &mut arrival));
        assert_eq!(queue.len(), CAPACITY);
        assert_eq!(arrival, CAPACITY as u64);

        // A step on a full local queue has no budget left: the now-idle channel is
        // left alone until a pop frees a slot.
        assert!(!admit_ready(&mut rx, &mut queue, &mut arrival));
        assert_eq!(
            queue.len(),
            CAPACITY,
            "a step with no budget admits nothing"
        );
        assert_eq!(arrival, CAPACITY as u64);
    }

    #[test]
    fn a_channel_refilled_at_every_step_cannot_grow_the_local_queue_past_capacity() {
        // The regression this guards: a step that drains until `try_recv` reports
        // `Empty` is unbounded whenever producers refill the slots it frees, so it
        // can move arbitrarily many tasks out of the (bounded) channel without ever
        // reaching the pop side — growing the local queue without bound and
        // starving dispatch.
        //
        // Worst case modelled here: every free slot is refilled before the next
        // step, so the channel is full at each step and the step can never rely on
        // observing `Empty` to terminate. The channel is then genuinely bounded
        // only if the local queue is bounded by the channel.
        const CAPACITY: usize = 4;
        const ROUNDS: usize = 64;

        let (tx, mut rx) = mpsc::channel::<AgentTask>(CAPACITY);
        let mut queue = PriorityQueue::default();
        let mut arrival = 0u64;

        for round in 0..ROUNDS {
            let before = arrival;
            refill(&tx, &format!("round-{round}"), CAPACITY);
            admit_ready(&mut rx, &mut queue, &mut arrival);

            assert!(
                arrival - before <= CAPACITY as u64,
                "round {round}: one step admitted {} tasks, more than the channel capacity",
                arrival - before
            );
            assert!(
                queue.len() <= CAPACITY,
                "round {round}: local queue {} exceeded the channel capacity {CAPACITY}",
                queue.len()
            );
            // The step always returns to the pop side, so a channel that is never
            // observed as empty cannot starve dispatch.
            queue
                .pop()
                .expect("every round has an admitted task to dispatch");
        }

        assert!(
            arrival >= ROUNDS as u64,
            "dispatch advanced every round instead of accumulating a backlog"
        );
    }

    #[test]
    fn a_step_with_a_backlog_admits_only_the_slack_left_in_the_channel() {
        // Deterministic form of the same defect, with no scheduler involved: the
        // local queue already holds `CAPACITY - 1` tasks and the channel is full
        // again, which is the refill a producer performs while the worker drains.
        // A step that kept reading until the channel *looked* empty would move the
        // whole channel's worth on top of the backlog; a step bounded by the
        // channel capacity admits only the free slot and leaves the rest.
        const CAPACITY: usize = 4;
        let (tx, mut rx) = mpsc::channel::<AgentTask>(CAPACITY);
        let mut queue = PriorityQueue::default();
        let mut arrival = 0u64;

        for _ in 1..CAPACITY {
            queue.push(task("already-admitted", 5), arrival);
            arrival += 1;
        }
        assert_eq!(queue.len(), CAPACITY - 1, "backlog before the step");
        refill(&tx, "refill", CAPACITY);

        admit_ready(&mut rx, &mut queue, &mut arrival);

        assert_eq!(
            queue.len(),
            CAPACITY,
            "the local queue can never hold more unprocessed tasks than the channel capacity"
        );
        assert_eq!(arrival, CAPACITY as u64, "exactly one slot was free");
        assert_eq!(
            std::iter::from_fn(|| rx.try_recv().ok()).count(),
            CAPACITY - 1,
            "the step left the rest of the refilled channel alone"
        );
    }

    #[test]
    fn a_closed_channel_is_drained_before_the_close_is_reported() {
        let (tx, mut rx) = mpsc::channel::<AgentTask>(4);
        tx.try_send(task("buffered", 5)).expect("fits");
        drop(tx);

        let mut queue = PriorityQueue::default();
        let mut arrival = 0u64;

        // The buffered task is still admitted, and the close is reported by the
        // same step: the read budget never strands a queued task.
        assert!(admit_ready(&mut rx, &mut queue, &mut arrival));
        assert_eq!(
            queue.pop().map(|task| task.text),
            Some("buffered".to_string())
        );

        // A later step has nothing left to read and keeps reporting the close.
        assert!(admit_ready(&mut rx, &mut queue, &mut arrival));
        assert!(queue.pop().is_none());
    }

    #[test]
    fn dispatcher_registry_reports_hits_misses_and_replacements() {
        let dispatcher: Arc<dyn TaskDispatcher> = Arc::new(NoopDispatcher);
        let other: Arc<dyn TaskDispatcher> = Arc::new(NoopDispatcher);

        let registry = DispatcherRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.get(TransportType::Acp).is_none());

        let registry = registry.with(TransportType::Acp, Arc::clone(&dispatcher));
        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());
        assert!(Arc::ptr_eq(
            registry.get(TransportType::Acp).expect("registered"),
            &dispatcher
        ));
        assert!(registry.get(TransportType::Matrix).is_none());

        // Registering the same transport again replaces the entry.
        let replaced = registry.with(TransportType::Acp, Arc::clone(&other));
        assert_eq!(replaced.len(), 1);
        assert!(Arc::ptr_eq(
            replaced.get(TransportType::Acp).expect("registered"),
            &other
        ));
    }

    /// A dispatcher that accepts and completes everything; only used to prove
    /// object safety and trait ergonomics at compile time and in registry tests.
    struct NoopDispatcher;

    impl TaskDispatcher for NoopDispatcher {
        fn deliver<'a>(
            &'a self,
            _request: DispatchRequest<'a>,
        ) -> BusFuture<'a, Result<(), DispatchError>> {
            Box::pin(async { Ok(()) })
        }

        fn execute<'a>(
            &'a self,
            _request: DispatchRequest<'a>,
        ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
            Box::pin(async {
                Ok(DispatchOutcome::Completed {
                    output: "ok".into(),
                })
            })
        }
    }

    #[test]
    fn worker_handles_are_send_sync_and_object_safe() {
        // The worker must be spawnable (`run`'s future is `Send`) and its
        // injections must be usable behind `Arc<dyn _>`: this is the boundary
        // T007/T009/T012/T014 build against.
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_send<T: Send>() {}
        assert_send_sync::<Worker>();
        assert_send_sync::<DispatcherRegistry>();
        assert_send_sync::<WorkerError>();
        assert_send_sync::<DispatchError>();
        assert_send_sync::<Arc<dyn TaskDispatcher>>();
        assert_send::<tokio::task::JoinHandle<Result<(), WorkerError>>>();
    }

    #[test]
    fn worker_assembles_with_the_single_dispatcher_convenience_constructor() {
        let registry = Arc::new(EndpointRegistry::from_config(&load(
            r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
workspace = "/tmp"
enabled = true
"#,
        )));
        let (sink, _events_rx) = crate::bus::MpscEventSink::new(8);
        let sink: Arc<dyn EventSink> = Arc::new(sink);
        let (_bus, tasks_rx) = crate::bus::MemoryBus::with_event_sink(
            Arc::clone(&registry),
            8,
            Clock::default(),
            Arc::clone(&sink),
        );

        let worker = Worker::with_dispatcher(
            Arc::clone(&registry),
            tasks_rx,
            sink,
            Clock::default(),
            TransportType::Acp,
            Arc::new(NoopDispatcher),
        );

        assert!(worker.dispatchers.get(TransportType::Acp).is_some());
        assert!(worker.dispatchers.get(TransportType::Matrix).is_none());
        assert_eq!(
            worker.registry.resolve_agent_id("worker"),
            Some(derive_endpoint_id("worker"))
        );
    }

    #[test]
    fn error_display_is_fixed_text_without_addresses_or_credentials() {
        let task_id = TaskId::generate();

        // Each variant renders a fixed prefix, names the task, and leaks nothing
        // that could carry an address or a credential.
        let cases: [(WorkerError, &str, bool); 3] = [
            (
                WorkerError::EventStreamClosed { task_id, seq: 2 },
                "event stream is closed",
                false,
            ),
            (
                WorkerError::EventBackpressure { task_id, seq: 3 },
                "event buffer is full",
                false,
            ),
            (
                WorkerError::EventWrite {
                    task_id,
                    seq: 4,
                    error: BusError::QueueFull,
                },
                "event write failed",
                true,
            ),
        ];
        for (err, prefix, leaks_error_text) in cases {
            let rendered = err.to_string();
            assert!(rendered.starts_with(prefix), "got: {rendered}");
            assert!(rendered.contains(&task_id.to_string()), "got: {rendered}");
            for leaked in ["command", "args", "token", "password", "user_id", "http"] {
                assert!(
                    !rendered.contains(leaked),
                    "worker errors must not leak {leaked}: {rendered}"
                );
            }
            // Only the defensive catch-all renders an underlying bus error, and
            // `BusError`'s own `Display` is already pinned to fixed text.
            assert_eq!(
                rendered.contains(&BusError::QueueFull.to_string()),
                leaks_error_text
            );
        }

        assert_eq!(
            DispatchError::NotAccepted {
                reason: "no ack".into()
            }
            .to_string(),
            "delivery was not accepted: no ack"
        );
        assert_eq!(
            DispatchError::ExecutionFailed {
                reason: "boom".into()
            }
            .to_string(),
            "execution failed after acceptance: boom"
        );
    }

    #[test]
    fn dispatch_outcome_and_request_are_value_types() {
        let task = task("payload", 5);
        let registry = EndpointRegistry::from_config(&load(
            r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
workspace = "/tmp"
enabled = true
"#,
        ));
        let target = registry
            .get_by_agent_id("worker")
            .expect("worker is declared");
        let delivery_id = DeliveryId::generate();
        let request = DispatchRequest {
            task: &task,
            target,
            delivery_id,
            attempt: 1,
        };
        let clone = request.clone();
        assert_eq!(clone.delivery_id, delivery_id);
        assert_eq!(clone.attempt, 1);
        assert_eq!(clone.task.text, "payload");
        // The worker never rewrites the reserved OCC counter (D9).
        assert_eq!(clone.task.version, 0);
        assert_eq!(clone.target.transport(), TransportType::Acp);

        assert_ne!(
            DispatchOutcome::Completed { output: "a".into() },
            DispatchOutcome::Failed { error: "a".into() }
        );
    }

    // -----------------------------------------------------------------------
    // T006: runtime-control configuration
    // -----------------------------------------------------------------------

    #[test]
    fn default_loop_limits_match_the_config_defaults() {
        // D13's condition: the worker's fallback limits and `Config.bridge`'s
        // defaults must be the same policy, not two that drift apart.
        let config = load("");
        assert_eq!(
            LoopLimits::default(),
            LoopLimits::from_bridge(&config.bridge)
        );
        assert_eq!(LoopLimits::default().max_depth, 8);
        assert_eq!(LoopLimits::default().max_hops, 16);
    }

    #[test]
    fn loop_limits_take_the_configured_values() {
        let config = load(
            r#"
[bridge]
max_task_depth = 3
max_task_hops = 7
"#,
        );
        assert_eq!(
            LoopLimits::from_bridge(&config.bridge),
            LoopLimits {
                max_depth: 3,
                max_hops: 7
            }
        );
    }

    #[test]
    fn loop_limits_block_only_values_above_the_ceiling() {
        let limits = LoopLimits {
            max_depth: 2,
            max_hops: 3,
        };
        let at_the_limit = AgentTask {
            depth: 2,
            hops: 3,
            ..task("at the limit", 5)
        };
        assert_eq!(
            limits.violation(&at_the_limit),
            None,
            "the ceiling itself is allowed, so a root task passes at max_depth = 0"
        );

        let over_both = AgentTask {
            depth: 3,
            hops: 4,
            ..at_the_limit.clone()
        };
        let message = limits.violation(&over_both).expect("both limits exceeded");
        assert!(message.contains("hit: depth, hops"), "got: {message}");

        let over_depth = AgentTask {
            depth: 3,
            ..at_the_limit.clone()
        };
        assert!(
            limits
                .violation(&over_depth)
                .expect("depth exceeded")
                .contains("hit: depth")
        );

        let over_hops = AgentTask {
            hops: 4,
            ..at_the_limit
        };
        assert!(
            limits
                .violation(&over_hops)
                .expect("hops exceeded")
                .contains("hit: hops")
        );

        assert_eq!(
            LoopLimits {
                max_depth: 0,
                max_hops: 0
            }
            .violation(&task("root", 5)),
            None,
            "a root task (depth 0, hops 0) is within a zero ceiling"
        );
    }

    #[test]
    fn a_cycle_limit_message_names_only_bounded_identifiers() {
        let limits = LoopLimits {
            max_depth: 0,
            max_hops: 0,
        };
        let over_limit = AgentTask {
            depth: 1,
            hops: 2,
            text: "token=secret run the thing".into(),
            ..task("sensitive", 5)
        };

        let message = limits.violation(&over_limit).expect("both limits exceeded");

        assert!(
            message.starts_with("cycle limit exceeded"),
            "got: {message}"
        );
        for named in [
            over_limit.task_id.to_string(),
            over_limit.root_task_id.to_string(),
            over_limit.from_agent.to_string(),
            over_limit.to_agent.to_string(),
        ] {
            assert!(message.contains(&named), "{message} must name {named}");
        }
        assert!(message.contains("parent_task_id=none"), "got: {message}");
        assert!(message.contains("depth=1 (max 0)"), "got: {message}");
        assert!(message.contains("hops=2 (max 0)"), "got: {message}");
        for leaked in [
            "secret",
            "token",
            "run the thing",
            "command",
            "args",
            "password",
            "user_id",
            "http",
        ] {
            assert!(
                !message.contains(leaked),
                "a cycle-limit message must not leak {leaked}: {message}"
            );
        }
    }

    #[test]
    fn a_cycle_limit_message_names_the_parent_when_there_is_one() {
        let parent = TaskId::generate();
        let child = AgentTask {
            parent_task_id: Some(parent),
            depth: 9,
            ..task("child", 5)
        };

        let message = LoopLimits::default()
            .violation(&child)
            .expect("depth 9 exceeds the default 8");

        assert!(
            message.contains(&format!("parent_task_id={parent}")),
            "got: {message}"
        );
        assert!(message.contains("hit: depth"), "got: {message}");
    }

    #[test]
    fn retry_policy_defaults_to_a_single_attempt() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 1);
        assert_eq!(policy.backoff, Backoff::None);
        assert!(
            !policy.allows_another_attempt_after(1),
            "the default must never retry (T005 behaviour)"
        );
    }

    #[test]
    fn the_attempt_ceiling_bounds_the_loop() {
        let three = RetryPolicy {
            max_attempts: 3,
            backoff: Backoff::None,
        };
        assert!(three.allows_another_attempt_after(1));
        assert!(three.allows_another_attempt_after(2));
        assert!(!three.allows_another_attempt_after(3));

        // `0` would otherwise allow no attempt at all: the task must still run
        // once, so the worker treats it as `1`.
        let zero = RetryPolicy {
            max_attempts: 0,
            backoff: Backoff::None,
        };
        assert!(!zero.allows_another_attempt_after(1));
    }

    #[test]
    fn backoff_delays_are_deterministic_and_capped() {
        assert_eq!(Backoff::default(), Backoff::None);
        assert_eq!(Backoff::None.delay_for(1), Duration::ZERO);
        assert_eq!(
            Backoff::Fixed(Duration::from_millis(50)).delay_for(7),
            Duration::from_millis(50),
            "a fixed delay does not depend on the attempt"
        );

        let exponential = Backoff::Exponential {
            base: Duration::from_millis(10),
            max: Duration::from_millis(40),
        };
        assert_eq!(exponential.delay_for(1), Duration::from_millis(10));
        assert_eq!(exponential.delay_for(2), Duration::from_millis(20));
        assert_eq!(exponential.delay_for(3), Duration::from_millis(40));
        assert_eq!(
            exponential.delay_for(4),
            Duration::from_millis(40),
            "the curve is capped, not wrapped"
        );
        assert_eq!(
            exponential.delay_for(u32::MAX),
            Duration::from_millis(40),
            "an extreme attempt count must not overflow"
        );

        // A `base` that cannot be doubled safely still saturates at `max`.
        let saturated = Backoff::Exponential {
            base: Duration::MAX,
            max: Duration::from_secs(1),
        };
        assert_eq!(saturated.delay_for(u32::MAX), Duration::from_secs(1));
    }

    #[test]
    fn worker_config_defaults_reproduce_the_t005_behaviour() {
        let config = WorkerConfig::default();
        assert_eq!(config.limits, LoopLimits::default());
        assert_eq!(config.retry, RetryPolicy::default());
        assert!(config.cancellation.is_none());
        assert!(config.timer.is_none(), "preemption is opt-in");

        let rendered = format!("{config:?}");
        assert!(rendered.contains("cancellation: false"), "got: {rendered}");
        assert!(rendered.contains("timer: false"), "got: {rendered}");
    }

    #[test]
    fn the_builder_applies_the_config_and_registers_a_dispatcher() {
        let registry = Arc::new(EndpointRegistry::from_config(&load(
            r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
workspace = "/tmp"
enabled = true
"#,
        )));
        let (sink, _events_rx) = crate::bus::MpscEventSink::new(8);
        let sink: Arc<dyn EventSink> = Arc::new(sink);
        let (_bus, tasks_rx) = crate::bus::MemoryBus::with_event_sink(
            Arc::clone(&registry),
            8,
            Clock::default(),
            Arc::clone(&sink),
        );

        // `.build()` without any call is indistinguishable from `Worker::new`.
        let plain = Worker::builder(
            Arc::clone(&registry),
            tasks_rx,
            Arc::clone(&sink),
            Clock::default(),
            DispatcherRegistry::new(),
        )
        .build();
        assert_eq!(plain.config.limits, LoopLimits::default());
        assert_eq!(plain.config.retry, RetryPolicy::default());
        assert!(plain.config.cancellation.is_none());
        assert!(plain.config.timer.is_none());

        let registry_again = Arc::new(EndpointRegistry::from_config(&load(
            r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
workspace = "/tmp"
enabled = true
"#,
        )));
        let (_bus, tasks_rx) = crate::bus::MemoryBus::with_event_sink(
            Arc::clone(&registry_again),
            8,
            Clock::default(),
            Arc::clone(&sink),
        );
        let configured = WorkerConfig {
            limits: LoopLimits {
                max_depth: 1,
                max_hops: 2,
            },
            retry: RetryPolicy {
                max_attempts: 4,
                backoff: Backoff::Fixed(Duration::from_millis(5)),
            },
            cancellation: Some(Cancellation::new()),
            timer: Some(Arc::new(crate::bus::control::TokioTimer)),
        };

        let worker = Worker::builder(
            registry_again,
            tasks_rx,
            sink,
            Clock::default(),
            DispatcherRegistry::new(),
        )
        .config(configured.clone())
        .with_dispatcher(TransportType::Acp, Arc::new(NoopDispatcher))
        .build();

        assert_eq!(worker.config.retry, configured.retry);
        assert_eq!(worker.config.limits, configured.limits);
        assert!(worker.config.cancellation.is_some());
        assert!(worker.config.timer.is_some());
        assert!(worker.dispatchers.get(TransportType::Acp).is_some());
        assert!(worker.dispatchers.get(TransportType::Matrix).is_none());
    }

    #[test]
    fn control_configuration_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<WorkerConfig>();
        assert_send_sync::<WorkerBuilder>();
        assert_send_sync::<LoopLimits>();
        assert_send_sync::<RetryPolicy>();
        assert_send_sync::<Backoff>();
        assert_send_sync::<Cancellation>();
        assert_send_sync::<Arc<dyn Timer>>();
    }
}
