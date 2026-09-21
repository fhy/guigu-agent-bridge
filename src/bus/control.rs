//! Runtime control boundaries for the bus worker: cancellation and timers.
//!
//! [`Worker`](crate::bus::Worker) is given two injectable resources here, both
//! optional and both introduced by T006:
//!
//! - [`Cancellation`] — an external "stop this task" signal source, which the
//!   worker races against every dispatch stage. Operators (T013 `/cancel`,
//!   T017 assembly) hold the handle; the worker only observes it.
//! - [`Timer`] — an asynchronous wait boundary, which makes a deadline
//!   *preemptive*: a hung `deliver`/`execute` can be abandoned while it is still
//!   in flight. The production implementation is [`TokioTimer`]; tests inject a
//!   deterministic double.
//!
//! Both are injected through
//! [`WorkerConfig`](crate::bus::WorkerConfig) and
//! [`Worker::builder`](crate::bus::Worker::builder), so the frozen
//! [`Worker::new`](crate::bus::Worker::new) /
//! [`Worker::with_dispatcher`](crate::bus::Worker::with_dispatcher) signatures
//! and their semantics (never cancelled, never preempted) are untouched.
//!
//! # Cancellation contract
//!
//! - **Two scopes.** [`Cancellation::cancel`] targets one `task_id`;
//!   [`Cancellation::cancel_all`] cancels every task. A targeted reason is more
//!   specific, so [`Cancellation::reason_for`] reports it in preference to a
//!   global one.
//! - **First reason wins and repeats are idempotent.** Cancelling a task that
//!   already carries a reason keeps that reason, and no call ever produces a
//!   second terminal event for a task the worker has already finished.
//! - **Retained state is lifecycle-bounded.** A reason is kept only while it can
//!   still change an outcome, and every table has a hard ceiling, so a stream of
//!   distinct `task_id`s cannot grow memory without bound:
//!   - reasons for tasks the worker *owns* live in an active table bounded by the
//!     worker's in-flight tasks (one in v1) and are dropped by
//!     [`Cancellation::retire`], which the worker calls at the task's terminal
//!     transition;
//!   - a reason recorded *before* ownership (an operator cancelling a queued
//!     task, or an id the worker never sees) waits in a pre-arm table capped at
//!     [`MAX_PENDING_CANCELLATIONS`], where the oldest entry is displaced first;
//!   - an id whose task reached a terminal state is remembered in a bounded
//!     retirement table, so a late `cancel` for a finished task is **dropped**
//!     rather than retained (and still writes nothing);
//!   - [`Cancellation::tracked`] reports how many ids currently hold a reason, so
//!     the bound stays observable.
//! - **Reasons are truncated.** At most [`MAX_REASON_BYTES`] bytes of any one
//!   reason are retained (cut at a character boundary), bounding the per-entry
//!   cost as well as the entry count. The `Cancelled` event carries the retained
//!   reason.
//! - **A wakeup is a version bump, not a value change.** Waiters are notified
//!   through an internal `watch` counter that every call bumps, so two calls
//!   carrying the *same* reason still wake every waiter. The watched value is an
//!   opaque counter: a waiter must re-read [`Cancellation::reason_for`] after
//!   each wakeup, which is also what makes a signal recorded *before* the waiter
//!   subscribed observable (the worker's synchronous checkpoints cover that
//!   case).
//! - **One lock, never held across an `.await`.** A single `Mutex` guards the
//!   plain tables and is only acquired inside synchronous methods, so
//!   `Cancellation` adds no lock ordering and no deadlock cycle to the worker
//!   path.
//! - **The sender outlives every waiter.** The `watch` sender lives inside the
//!   shared `Arc`; a `Cancellation` clone is held by the worker for as long as it
//!   can be waiting, so a waiter's `changed()` cannot fail because the signal
//!   source disappeared.
//! - **Cancellation is not fail-stop.** It terminates one task with a
//!   `Cancelled` event; the worker keeps consuming. Only a failed event write
//!   stops the worker.
//!
//! # Timer contract
//!
//! `sleep_until(deadline)` must resolve *no earlier* than `deadline` and must not
//! resolve at all once it has been dropped. Implementations must not spawn
//! background tasks that outlive the returned future, must not block the
//! executor, and must be `Send + Sync` so an `Arc<dyn Timer>` can be shared
//! across worker threads.
//!
//! The worker polls the timer inline inside a `select!`, so dropping the future
//! (because the stage or the cancellation won the race) frees the wait: the
//! worker spawns nothing and nothing is leaked.
//!
//! # Boundaries
//!
//! - **No protocol-level cancellation.** Dropping the in-flight stage future
//!   ends the *worker's* interest in the delivery; telling the far side (an ACP
//!   `cancel` notification, subprocess reaping) needs a method on the frozen
//!   [`TaskDispatcher`](crate::bus::TaskDispatcher) and belongs to T014/T015.
//! - **No concurrency limiting.** The worker is still one task at a time
//!   (T016); a `Cancellation` handle does not admit or reject work.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{DateTime, Utc};
use tokio::sync::watch;

use crate::bus::BusFuture;
use crate::models::TaskId;

/// The ceiling on pre-arm cancellation entries.
///
/// A reason that arrives *before* the worker owns its task is retained so it is
/// still observed later (see the module docs). That table is the only one an
/// operator can grow with arbitrary distinct ids, so it is capped: when the cap
/// is reached the *oldest* entry is displaced, which keeps the most recent — and
/// therefore most likely still relevant — cancellations.
pub const MAX_PENDING_CANCELLATIONS: usize = 1024;

/// The number of bytes retained from any one reason.
///
/// Reasons are operator-supplied (T013 `/cancel`), so their length is outside
/// this module's control. Retaining at most this many bytes per entry bounds the
/// per-entry cost; a longer reason is cut at a character boundary, and the
/// `Cancelled` event carries the retained reason.
pub const MAX_REASON_BYTES: usize = 128;

/// The ceiling on the retirement memory that makes a late `cancel` a no-op.
///
/// Deliberately private: it is a hygiene bound, not part of the operator
/// contract. Exceeding it only means a `cancel` for a task retired long ago is
/// treated as unknown again.
const MAX_RETIRED_TASKS: usize = 1024;

/// A cancellation signal source, shared by the worker and its operators.
///
/// Cheap to clone: every clone refers to the same recorded reasons and the same
/// wakeup counter. See the module docs for the full contract.
#[derive(Clone)]
pub struct Cancellation {
    inner: Arc<CancellationInner>,
}

struct CancellationInner {
    /// Every recorded reason plus the retirement memory, under a single lock —
    /// so there is no lock ordering to get wrong.
    state: Mutex<CancellationState>,
    /// Bumped by every `cancel`/`cancel_all` call. Waiters wait for a *version*
    /// change rather than a value change, so repeated identical reasons still
    /// wake them.
    version: watch::Sender<u64>,
}

#[derive(Default)]
struct CancellationState {
    /// Global reason, set once by `cancel_all`.
    global: Option<String>,
    /// Tasks the worker currently owns. Presence marks the task active — so a
    /// later `cancel` applies here instead of the bounded pre-arm table — and the
    /// value becomes its reason once one is recorded. Bounded by the worker's
    /// in-flight tasks (one in v1), because [`Cancellation::retire`] removes each
    /// entry at its task's terminal transition.
    active: HashMap<TaskId, Option<String>>,
    /// Reasons recorded before the task was activated, plus ids the worker never
    /// activates. Bounded by [`MAX_PENDING_CANCELLATIONS`]; each value carries an
    /// insertion counter used to find the oldest entry to displace.
    pending: HashMap<TaskId, (String, u64)>,
    next_pending_seq: u64,
    /// Ids whose task already reached a terminal state, so a late `cancel` is
    /// dropped instead of retained. Bounded by `MAX_RETIRED_TASKS`.
    retired: HashMap<TaskId, u64>,
    next_retired_seq: u64,
}

impl CancellationState {
    /// Retain a pre-arm reason, displacing the oldest entry when the cap is
    /// reached.
    fn remember_pending(&mut self, task_id: TaskId, reason: String) {
        if self.pending.len() >= MAX_PENDING_CANCELLATIONS {
            self.displace_oldest_pending();
        }
        let seq = self.next_pending_seq;
        self.next_pending_seq = seq.wrapping_add(1);
        self.pending.insert(task_id, (reason, seq));
    }

    /// Drop the pre-arm entry that was inserted first.
    fn displace_oldest_pending(&mut self) {
        let oldest = self
            .pending
            .iter()
            .min_by_key(|(_, (_, seq))| *seq)
            .map(|(task_id, _)| *task_id);
        if let Some(task_id) = oldest {
            self.pending.remove(&task_id);
        }
    }

    /// Remember that `task_id` is finished, displacing the oldest memory when the
    /// cap is reached.
    fn remember_retired(&mut self, task_id: TaskId) {
        if self.retired.len() >= MAX_RETIRED_TASKS {
            let oldest = self
                .retired
                .iter()
                .min_by_key(|(_, seq)| **seq)
                .map(|(task_id, _)| *task_id);
            if let Some(oldest) = oldest {
                self.retired.remove(&oldest);
            }
        }
        let seq = self.next_retired_seq;
        self.next_retired_seq = seq.wrapping_add(1);
        self.retired.insert(task_id, seq);
    }
}

impl Cancellation {
    /// Create a signal source with nothing cancelled.
    ///
    /// The handle is inert until a `cancel`/`cancel_all` call: a worker that is
    /// given it never observes a cancellation unless somebody asks for one.
    pub fn new() -> Self {
        // The receiving end is intentionally dropped: `send_modify` notifies
        // unconditionally (even with zero receivers) and a later subscriber
        // starts from the then-current version, so nothing is lost or leaked.
        let (version, _) = watch::channel(0u64);
        Self {
            inner: Arc::new(CancellationInner {
                state: Mutex::new(CancellationState::default()),
                version,
            }),
        }
    }

    /// Cancel one task, recording `reason` as its cancellation reason.
    ///
    /// The first reason recorded for a task wins; later calls for the same task
    /// are no-ops that keep the original reason. A reason only becomes
    /// observable through [`Cancellation::reason_for`], which is what the worker
    /// reads before writing a `Cancelled` event.
    ///
    /// A call for a task that has already reached a terminal state (see
    /// [`Cancellation::retire`]) records nothing: it cannot affect a finished
    /// task, so the id is not retained.
    pub fn cancel(&self, task_id: TaskId, reason: impl Into<String>) {
        {
            let mut state = self.state();
            if state.active.contains_key(&task_id) {
                // The worker owns this task, so the reason belongs to the owned
                // record — it can never be displaced by the pre-arm cap.
                if let Some(slot) = state.active.get_mut(&task_id)
                    && slot.is_none()
                {
                    *slot = Some(bounded_reason(reason.into()));
                }
            } else if !state.retired.contains_key(&task_id) && !state.pending.contains_key(&task_id)
            {
                state.remember_pending(task_id, bounded_reason(reason.into()));
            }
            // A retired id records nothing: the task is past its terminal state,
            // so no reason could affect it and none is retained.
        }
        // Bumped outside the critical section so a wakeup never happens while a
        // lock is held, and unconditionally so a repeated reason still wakes
        // waiters.
        self.bump();
    }

    /// Cancel every task, recording `reason` as the global cancellation reason.
    ///
    /// Intended for shutdown or a manual emergency stop. Like
    /// [`Cancellation::cancel`], the first reason wins.
    pub fn cancel_all(&self, reason: impl Into<String>) {
        {
            let mut state = self.state();
            state
                .global
                .get_or_insert_with(|| bounded_reason(reason.into()));
        }
        self.bump();
    }

    /// The cancellation reason that applies to `task_id`, if any.
    ///
    /// A reason recorded for this exact task takes precedence over the global
    /// one; `None` means this task has not been cancelled.
    pub fn reason_for(&self, task_id: TaskId) -> Option<String> {
        let state = self.state();
        if let Some(reason) = state.active.get(&task_id).and_then(|slot| slot.clone()) {
            return Some(reason);
        }
        if let Some((reason, _)) = state.pending.get(&task_id) {
            return Some(reason.clone());
        }
        state.global.clone()
    }

    /// Take ownership of `task_id` and move any pre-arm reason with it.
    ///
    /// Called by the worker when it starts processing a task. Ownership stops the
    /// reason from being displaced by the pre-arm cap and guarantees it is retired
    /// exactly once, at the task's terminal transition. A task with no reason is
    /// still marked active, so a later `cancel` applies to it directly.
    pub(crate) fn activate(&self, task_id: TaskId) {
        let mut state = self.state();
        // A duplicate submission may legitimately reuse an id the worker already
        // retired (T005 D6: two submissions are two independent state machines),
        // so taking ownership clears the retirement memory.
        state.retired.remove(&task_id);
        let promoted = state.pending.remove(&task_id).map(|(reason, _)| reason);
        state.active.entry(task_id).or_insert(promoted);
    }

    /// Release the record for a finished task, reporting whether anything was
    /// released.
    ///
    /// The worker calls this at every terminal transition, so a finished task
    /// retains nothing and a later `cancel` for it is dropped. It is public so an
    /// owner (T013's `/cancel`, T017's assembly) can release a record itself — for
    /// a task that will never reach a worker, say. Calling it for a *live* task
    /// **discards that task's cancellation**: the task then runs to completion
    /// unless a new cancellation is recorded.
    pub fn retire(&self, task_id: TaskId) -> bool {
        let mut state = self.state();
        let active = state.active.remove(&task_id).is_some();
        let pending = state.pending.remove(&task_id).is_some();
        state.remember_retired(task_id);
        active || pending
    }

    /// True once the worker has completed cancellation/shutdown handling for a
    /// task. Control ingress uses this as the positive handoff before durable
    /// pause; it is not inferred from the cancellation request itself.
    pub fn is_retired(&self, task_id: TaskId) -> bool {
        self.state().retired.contains_key(&task_id)
    }

    /// How many task ids currently hold a cancellation reason.
    ///
    /// Excludes the global reason. Bounded by the worker's in-flight tasks plus
    /// [`MAX_PENDING_CANCELLATIONS`], so an operator can observe the bound instead
    /// of inferring it.
    pub fn tracked(&self) -> usize {
        let state = self.state();
        state.active.len() + state.pending.len()
    }

    /// Subscribe to the internal change signal.
    ///
    /// The watched value is an opaque, monotonically bumped counter — never a
    /// reason — so a subscriber must re-read [`Cancellation::reason_for`] after
    /// every wakeup. Crate-private: the encoding is an implementation detail and
    /// the worker is its only consumer.
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.version.subscribe()
    }

    /// Notify every waiter, even when the recorded reasons did not change.
    fn bump(&self) {
        self.inner
            .version
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    /// The state lock, recovering from poisoning.
    ///
    /// The guard protects plain tables with no cross-field invariant and no user
    /// code runs inside the critical section, so a poisoned lock cannot describe
    /// inconsistent data — recovering is strictly safer than panicking inside the
    /// worker.
    fn state(&self) -> MutexGuard<'_, CancellationState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for Cancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Cancellation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Reasons may carry operator-supplied text, so only the shape is
        // reported — never a reason, and never an address or credential.
        let state = self.state();
        f.debug_struct("Cancellation")
            .field("globally_cancelled", &state.global.is_some())
            .field(
                "cancelled_tasks",
                &(state.active.len() + state.pending.len()),
            )
            .finish()
    }
}

/// Retain at most [`MAX_REASON_BYTES`] bytes of `reason`, cut at a character
/// boundary so the result is always valid UTF-8.
fn bounded_reason(reason: String) -> String {
    if reason.len() <= MAX_REASON_BYTES {
        return reason;
    }
    let mut end = MAX_REASON_BYTES;
    while end > 0 && !reason.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = reason;
    truncated.truncate(end);
    truncated
}

/// An asynchronous wait boundary used to preempt a deadline.
///
/// The worker polls `sleep_until` inline inside a `select!` race against the
/// in-flight dispatch stage and the cancellation signal, so a hung
/// `deliver`/`execute` can be abandoned without `JoinHandle::abort()` and
/// without spawning anything. The same boundary drives retry backoff.
///
/// # Implementation contract
///
/// - Object-safe (`Arc<dyn Timer>`) and `Send + Sync`: the worker may run on
///   another thread.
/// - Resolves **no earlier** than `deadline`, according to the same wall clock
///   the worker's `Clock` reads, and resolves *immediately* for a deadline that
///   has already passed when it is called.
/// - Spawns nothing that outlives the returned future; dropping the future
///   cancels the wait. Never blocks the executor.
pub trait Timer: Send + Sync {
    /// Resolve once `deadline` has been reached.
    fn sleep_until(&self, deadline: DateTime<Utc>) -> BusFuture<'_, ()>;
}

/// The production [`Timer`], backed by `tokio::time`.
///
/// `AgentTask::deadline` is an absolute UTC instant, while `tokio` schedules
/// relative delays, so the conversion reads the system clock. Production wires
/// [`Clock::system`](crate::bus::Clock::system), so the timer and the worker's
/// deadline checkpoints read the same clock; a test that pins the worker clock
/// must inject its own timer instead of this one.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioTimer;

impl Timer for TokioTimer {
    fn sleep_until(&self, deadline: DateTime<Utc>) -> BusFuture<'_, ()> {
        Box::pin(async move {
            // A deadline in the past yields a zero delay (`to_std` rejects a
            // negative one), which `sleep` completes on the first poll. The
            // worker's checkpoints already terminalise such tasks before this
            // boundary is ever reached.
            let delay = (deadline - Utc::now()).to_std().unwrap_or_default();
            tokio::time::sleep(delay).await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_is_cancelled_until_a_cancel_call() {
        let cancellation = Cancellation::new();
        let task_id = TaskId::generate();

        assert_eq!(cancellation.reason_for(task_id), None);
        assert_eq!(Cancellation::default().reason_for(task_id), None);

        cancellation.cancel_all("shutdown");
        assert_eq!(
            cancellation.reason_for(TaskId::generate()).as_deref(),
            Some("shutdown"),
            "a global cancellation applies to every task"
        );
    }

    #[test]
    fn a_targeted_reason_takes_precedence_over_the_global_one() {
        let cancellation = Cancellation::new();
        let target = TaskId::generate();
        let other = TaskId::generate();

        cancellation.cancel_all("shutdown");
        cancellation.cancel(target, "operator stop");

        assert_eq!(
            cancellation.reason_for(target).as_deref(),
            Some("operator stop")
        );
        assert_eq!(cancellation.reason_for(other).as_deref(), Some("shutdown"));
    }

    #[test]
    fn the_first_reason_wins_and_repeats_are_idempotent() {
        let cancellation = Cancellation::new();
        let task_id = TaskId::generate();

        cancellation.cancel(task_id, "first");
        cancellation.cancel(task_id, "second");
        cancellation.cancel_all("global-first");
        cancellation.cancel_all("global-second");

        assert_eq!(cancellation.reason_for(task_id).as_deref(), Some("first"));
        assert_eq!(
            cancellation.reason_for(TaskId::generate()).as_deref(),
            Some("global-first")
        );
    }

    #[test]
    fn clones_share_one_recorded_state() {
        let cancellation = Cancellation::new();
        let clone = cancellation.clone();
        let task_id = TaskId::generate();

        clone.cancel(task_id, "from the clone");

        assert_eq!(
            cancellation.reason_for(task_id).as_deref(),
            Some("from the clone")
        );
        assert_eq!(
            format!("{cancellation:?}"),
            "Cancellation { globally_cancelled: false, cancelled_tasks: 1 }"
        );
    }

    #[test]
    fn cancellation_debug_never_renders_a_reason() {
        let cancellation = Cancellation::new();
        cancellation.cancel(TaskId::generate(), "token=secret");
        cancellation.cancel_all("password=secret");

        let rendered = format!("{cancellation:?}");
        assert!(!rendered.contains("secret"), "got: {rendered}");
        assert!(!rendered.contains("token"), "got: {rendered}");
        assert!(
            rendered.contains("globally_cancelled: true"),
            "got: {rendered}"
        );
    }

    #[test]
    fn control_handles_are_send_sync_and_object_safe() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_send<T: Send>() {}
        assert_send_sync::<Cancellation>();
        assert_send_sync::<TokioTimer>();
        assert_send_sync::<Arc<dyn Timer>>();
        assert_send::<BusFuture<'_, ()>>();
    }

    // -- Lifecycle bounds (T006 review r1 P1) --------------------------------

    #[test]
    fn an_unknown_id_is_retained_only_in_the_bounded_pre_arm_table() {
        // The worker never activates this id, so the reason waits in the pre-arm
        // table — the one an operator can grow with arbitrary ids.
        let cancellation = Cancellation::new();
        let unknown = TaskId::generate();

        cancellation.cancel(unknown, "operator stop");

        assert_eq!(
            cancellation.reason_for(unknown).as_deref(),
            Some("operator stop")
        );
        assert_eq!(cancellation.tracked(), 1);
    }

    #[test]
    fn the_pre_arm_table_displaces_the_oldest_entry_when_it_is_full() {
        let cancellation = Cancellation::new();
        let ids: Vec<TaskId> = (0..MAX_PENDING_CANCELLATIONS + 8)
            .map(|_| TaskId::generate())
            .collect();

        for (index, task_id) in ids.iter().enumerate() {
            cancellation.cancel(*task_id, format!("reason {index}"));
        }

        assert_eq!(
            cancellation.tracked(),
            MAX_PENDING_CANCELLATIONS,
            "the pre-arm table cannot grow past its cap"
        );
        assert_eq!(
            cancellation.reason_for(ids[0]),
            None,
            "the oldest entry is the one displaced"
        );
        assert_eq!(
            cancellation.reason_for(*ids.last().expect("non-empty")),
            Some(format!("reason {}", ids.len() - 1)),
            "the newest entry is still observed"
        );
    }

    #[test]
    fn ownership_keeps_a_reason_out_of_the_pre_arm_cap() {
        // A live task's cancellation must never be displaced by unrelated
        // traffic filling the pre-arm table.
        let cancellation = Cancellation::new();
        let owned = TaskId::generate();
        cancellation.activate(owned);
        cancellation.cancel(owned, "operator stop");

        for _ in 0..MAX_PENDING_CANCELLATIONS + 8 {
            cancellation.cancel(TaskId::generate(), "unrelated");
        }

        assert_eq!(
            cancellation.reason_for(owned).as_deref(),
            Some("operator stop"),
            "an owned task keeps its cancellation under pre-arm pressure"
        );
        assert_eq!(
            cancellation.tracked(),
            MAX_PENDING_CANCELLATIONS + 1,
            "one owned record plus the bounded pre-arm table"
        );
    }

    #[test]
    fn activating_a_task_promotes_its_pre_arm_reason() {
        let cancellation = Cancellation::new();
        let task_id = TaskId::generate();

        cancellation.cancel(task_id, "cancelled while queued");
        assert_eq!(cancellation.tracked(), 1);

        cancellation.activate(task_id);

        assert_eq!(
            cancellation.reason_for(task_id).as_deref(),
            Some("cancelled while queued"),
            "a signal recorded before ownership survives it"
        );
        assert_eq!(cancellation.tracked(), 1, "promotion is not duplication");
    }

    #[test]
    fn retiring_a_task_releases_its_record_and_drops_a_late_cancel() {
        // The r1 regression: a cancel issued after the task finished must not
        // retain anything.
        let cancellation = Cancellation::new();
        let task_id = TaskId::generate();
        cancellation.activate(task_id);
        cancellation.cancel(task_id, "operator stop");

        assert!(cancellation.retire(task_id), "the record was released");
        assert_eq!(cancellation.reason_for(task_id), None);
        assert_eq!(cancellation.tracked(), 0);

        cancellation.cancel(task_id, "too late");

        assert_eq!(
            cancellation.reason_for(task_id),
            None,
            "a finished task's id is not retained again"
        );
        assert_eq!(cancellation.tracked(), 0, "nothing accumulates");
    }

    #[test]
    fn retiring_also_releases_a_pre_arm_entry() {
        let cancellation = Cancellation::new();
        let queued = TaskId::generate();
        cancellation.cancel(queued, "cancelled while queued");

        assert!(cancellation.retire(queued));
        assert_eq!(cancellation.tracked(), 0);
        assert_eq!(cancellation.reason_for(queued), None);
    }

    #[test]
    fn retire_reports_whether_it_released_anything_and_is_idempotent() {
        let cancellation = Cancellation::new();
        let known = TaskId::generate();

        assert!(
            !cancellation.retire(TaskId::generate()),
            "an unknown id releases nothing"
        );

        cancellation.cancel(known, "operator stop");
        assert!(cancellation.retire(known), "a recorded id is released");
        assert!(!cancellation.retire(known), "a second retire is a no-op");
    }

    #[test]
    fn taking_ownership_again_makes_a_retired_id_cancellable() {
        // Two submissions of one `task_id` are two independent state machines
        // (T005 D6), so a second run must be cancellable again.
        let cancellation = Cancellation::new();
        let task_id = TaskId::generate();

        cancellation.activate(task_id);
        cancellation.retire(task_id);
        cancellation.cancel(task_id, "for the first run");
        assert_eq!(cancellation.reason_for(task_id), None);

        cancellation.activate(task_id);
        cancellation.cancel(task_id, "for the second run");

        assert_eq!(
            cancellation.reason_for(task_id).as_deref(),
            Some("for the second run")
        );
    }

    #[test]
    fn the_retirement_memory_is_bounded() {
        let cancellation = Cancellation::new();

        for _ in 0..MAX_RETIRED_TASKS + 8 {
            let task_id = TaskId::generate();
            cancellation.activate(task_id);
            cancellation.retire(task_id);
        }

        assert_eq!(cancellation.tracked(), 0);
        assert!(
            cancellation.state().retired.len() <= MAX_RETIRED_TASKS,
            "the retirement memory cannot grow without bound"
        );
    }

    #[test]
    fn a_long_reason_is_truncated_at_a_character_boundary() {
        let cancellation = Cancellation::new();
        let ascii = TaskId::generate();
        let multibyte = TaskId::generate();

        cancellation.cancel(ascii, "a".repeat(MAX_REASON_BYTES * 4));
        // Three-byte characters, so `MAX_REASON_BYTES` is not a char boundary.
        cancellation.cancel(multibyte, "€".repeat(MAX_REASON_BYTES));

        assert_eq!(
            cancellation.reason_for(ascii).expect("retained").len(),
            MAX_REASON_BYTES
        );
        let truncated = cancellation.reason_for(multibyte).expect("retained");
        assert_eq!(truncated.len(), MAX_REASON_BYTES - 2, "cut at a boundary");
        assert!(truncated.chars().all(|c| c == '€'));
    }

    #[test]
    fn a_global_reason_is_bounded_too() {
        let cancellation = Cancellation::new();
        cancellation.cancel_all("x".repeat(MAX_REASON_BYTES * 4));

        assert_eq!(
            cancellation
                .reason_for(TaskId::generate())
                .expect("global")
                .len(),
            MAX_REASON_BYTES
        );
    }

    #[tokio::test]
    async fn a_version_bump_wakes_a_subscriber_even_for_an_identical_reason() {
        // The regression this guards: a `watch` channel that carried the reason
        // as its value would not wake a waiter when the same reason was recorded
        // twice, so a second `cancel` would be silently lost.
        let cancellation = Cancellation::new();
        let task_id = TaskId::generate();
        let mut rx = cancellation.subscribe();

        cancellation.cancel(task_id, "stop");
        rx.changed().await.expect("the sender is kept alive");

        cancellation.cancel(task_id, "stop");
        rx.changed()
            .await
            .expect("an identical reason still wakes waiters");

        cancellation.cancel_all("stop");
        rx.changed()
            .await
            .expect("a global cancel still wakes waiters");
    }

    #[tokio::test]
    async fn a_bump_that_happened_before_the_first_poll_is_not_lost() {
        // Level-triggered by version: the waiter subscribes, the signal arrives,
        // and the waiter observes it on its first poll without another bump.
        let cancellation = Cancellation::new();
        let task_id = TaskId::generate();
        let mut rx = cancellation.subscribe();

        cancellation.cancel(task_id, "stop");

        rx.changed().await.expect("the sender is kept alive");
        assert_eq!(cancellation.reason_for(task_id).as_deref(), Some("stop"));
    }

    #[tokio::test]
    async fn a_waiter_is_woken_for_another_task_and_must_re_read_the_reason() {
        let cancellation = Cancellation::new();
        let watched = TaskId::generate();
        let other = TaskId::generate();
        let mut rx = cancellation.subscribe();

        cancellation.cancel(other, "unrelated");

        rx.changed().await.expect("every bump wakes every waiter");
        assert_eq!(
            cancellation.reason_for(watched),
            None,
            "the watched value is a version, so the waiter must re-read the reason"
        );
        assert_eq!(cancellation.reason_for(other).as_deref(), Some("unrelated"));
    }

    #[tokio::test]
    async fn tokio_timer_resolves_immediately_for_a_deadline_in_the_past() {
        let timer: Arc<dyn Timer> = Arc::new(TokioTimer);
        let past = Utc::now() - chrono::Duration::seconds(1);

        // No `sleep` is used as a synchronisation device: a past deadline is a
        // zero delay, so this completes on the first poll.
        timer.sleep_until(past).await;
    }

    #[tokio::test]
    async fn a_dropped_timer_wait_is_abandoned_without_side_effects() {
        // The worker drops the timer future whenever the stage or the
        // cancellation wins the race. Using a far-future deadline makes this
        // deterministic: the wait cannot have elapsed, and dropping it must
        // return control immediately and leave nothing running.
        let timer: Arc<dyn Timer> = Arc::new(TokioTimer);
        let far = Utc::now() + chrono::Duration::hours(1);

        let future = timer.sleep_until(far);
        drop(future);
    }
}
