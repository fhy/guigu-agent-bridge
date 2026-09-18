//! Integration tests for the T005 bus worker and event fan-out.
//!
//! Every test drives the real path — TOML → validated `Config` (T003) →
//! `EndpointRegistry` → `MpscEventSink`/`MemoryBus::with_event_sink` → `submit` →
//! `Worker` → `EventBroadcaster` → recording consumer — with injected dispatcher
//! and consumer doubles. Assertions are made on the real `TaskEvent` values
//! (`seq`, `status`, `payload`, `delivery_id`, `attempt`, timestamps), never on
//! strings or global state.
//!
//! Determinism rules followed here:
//!
//! - `Clock::fixed` pins every timestamp, so equality is asserted exactly. The
//!   T006 cases step the clock through `Clock::new` when a test needs the
//!   deadline to pass mid-stage, and never busy-wait for wall-clock time.
//! - No `sleep` and no timeout: progress is awaited through `JoinHandle`s,
//!   `try_recv` and a bounded `yield_now` loop that fails the test rather than
//!   hanging it. The T006 cases fire an injected [`ManualTimer`] and cancel
//!   through [`Cancellation`] instead of waiting on a clock, so `tokio`'s `time`
//!   feature is enabled for the production timer but no test depends on it.
//! - Priority/ordering cases preload the task channel and only then run the
//!   worker, so the first drain sees every task in submission order.
//! - Ordering assertions never run the submitter concurrently with the worker.
//!   That matters: `submit` enqueues the task before writing `Queued`, so on a
//!   multi-thread runtime `Queued(seq = 1)` may arrive *after* `Dispatched(seq =
//!   2)` (Q1 = A). `seq` is the per-task ordering key — never arrival order. The
//!   one test that deliberately submits while the worker runs (the
//!   refill-during-drain regression) asserts counts and bounds only.
//! - Channels are closed by dropping handles explicitly. The event channel only
//!   closes once the bus *and* the worker dropped every clone of the shared sink,
//!   so each test drops its own sink handle instead of waiting on a timer.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, watch};

use guigu_agent_bridge::bus::{
    Backoff, Bus, BusError, BusFuture, Cancellation, Clock, ConsumerError, DispatchError,
    DispatchOutcome, DispatchRequest, DispatcherRegistry, EndpointRegistry, EventBroadcaster,
    EventConsumer, EventSink, LoopLimits, MAX_PENDING_CANCELLATIONS, MemoryBus, MpscEventSink,
    RetryPolicy, TaskDispatcher, Timer, Worker, WorkerConfig, WorkerError, derive_endpoint_id,
};
use guigu_agent_bridge::config::{Config, load_from_str_with_env};
use guigu_agent_bridge::models::{
    AgentTask, ConversationId, DeliveryId, EndpointId, EventId, Priority, TaskEvent,
    TaskEventPayload, TaskId, TaskStatus, TransportType,
};

const TS: &str = "2026-09-15T12:00:00Z";

/// `worker` is the addressable target; `idle` is declared disabled; `matrix-bot`
/// is declared enabled but has no derivable address (T010/T014).
const CONFIG: &str = r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
args = ["--stdio"]
workspace = "/tmp"
enabled = true

[agents.idle]
transport = "acp"
command = "idle-acp"
enabled = false

[agents.matrix-bot]
transport = "matrix"
enabled = true
"#;

/// Same agent id, declared disabled: re-validation must report `TargetDisabled`.
const WORKER_DISABLED: &str = r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
enabled = false
"#;

/// Same agent id, on a transport with no derivable address: re-validation must
/// report `AddressUnavailable`.
const WORKER_UNADDRESSABLE: &str = r#"
[agents.worker]
transport = "matrix"
enabled = true
"#;

/// A registry that declares nothing: re-validation must report `UnknownTarget`.
const NO_AGENTS: &str = "";

fn load(toml: &str) -> Config {
    let mut env = BTreeMap::new();
    env.insert("HOME".to_string(), "/home/tester".to_string());
    load_from_str_with_env(toml, &env).expect("test config must be valid")
}

fn fixed_ts() -> DateTime<Utc> {
    TS.parse().expect("valid timestamp")
}

fn fixed_clock() -> Clock {
    Clock::fixed(fixed_ts())
}

fn worker_endpoint() -> EndpointId {
    derive_endpoint_id("worker")
}

/// A root task (no parent, `root_task_id == task_id`) targeting `to_agent`.
fn root_task(to_agent: EndpointId, priority: u8) -> AgentTask {
    let task_id = TaskId::generate();
    AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: None,
        from_agent: EndpointId::generate(),
        to_agent,
        conversation_id: ConversationId::generate(),
        reply_to: None,
        text: "do the thing".into(),
        priority: Priority::new(priority).expect("priority in range"),
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    }
}

fn labelled(text: &str, priority: u8) -> AgentTask {
    AgentTask {
        text: text.into(),
        ..root_task(worker_endpoint(), priority)
    }
}

// ---------------------------------------------------------------------------
// Test doubles (built through the public API only)
// ---------------------------------------------------------------------------

/// One recorded dispatcher call. The reference-carrying [`DispatchRequest`] is
/// flattened into owned values so it can be asserted after the call returned.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SeenCall {
    stage: &'static str,
    task_id: TaskId,
    delivery_id: DeliveryId,
    attempt: u32,
    transport: TransportType,
    text: String,
    /// The reserved OCC counter as the dispatcher received it (D9: passthrough).
    version: u64,
}

/// A scripted [`TaskDispatcher`]: records the exact call sequence and returns
/// pre-set results.
struct ScriptedDispatcher {
    calls: Mutex<Vec<SeenCall>>,
    delivery: Option<DispatchError>,
    outcome: Result<DispatchOutcome, DispatchError>,
}

impl ScriptedDispatcher {
    fn new(
        delivery: Option<DispatchError>,
        outcome: Result<DispatchOutcome, DispatchError>,
    ) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            delivery,
            outcome,
        }
    }

    /// Accepts, then completes with `output`.
    fn succeeding(output: &str) -> Self {
        Self::new(
            None,
            Ok(DispatchOutcome::Completed {
                output: output.into(),
            }),
        )
    }

    /// Accepts, then reports a terminal failure from `execute`.
    fn reporting_failure(error: &str) -> Self {
        Self::new(
            None,
            Ok(DispatchOutcome::Failed {
                error: error.into(),
            }),
        )
    }

    /// Never accepts: the task must not reach `Running`.
    fn not_accepted(reason: &str) -> Self {
        Self::new(
            Some(DispatchError::NotAccepted {
                reason: reason.into(),
            }),
            Ok(DispatchOutcome::Completed {
                output: "unreachable".into(),
            }),
        )
    }

    /// Accepts, then fails while executing.
    fn execution_failed(reason: &str) -> Self {
        Self::new(
            None,
            Err(DispatchError::ExecutionFailed {
                reason: reason.into(),
            }),
        )
    }

    fn calls(&self) -> Vec<SeenCall> {
        self.calls.lock().expect("not poisoned").clone()
    }

    fn stages(&self) -> Vec<&'static str> {
        self.calls().into_iter().map(|call| call.stage).collect()
    }

    fn delivered_texts(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter(|call| call.stage == "deliver")
            .map(|call| call.text)
            .collect()
    }

    fn recorded(&self, stage: &'static str) -> SeenCall {
        self.calls()
            .into_iter()
            .find(|call| call.stage == stage)
            .unwrap_or_else(|| panic!("a {stage} call must have been recorded"))
    }

    fn record(&self, stage: &'static str, request: &DispatchRequest<'_>) {
        record_call(&self.calls, stage, request);
    }
}

/// Flatten a request into an owned [`SeenCall`].
///
/// Shared by the doubles: the reference-carrying request cannot outlive the call
/// it was passed to, so observations are copied out immediately.
fn record_call(calls: &Mutex<Vec<SeenCall>>, stage: &'static str, request: &DispatchRequest<'_>) {
    calls.lock().expect("not poisoned").push(SeenCall {
        stage,
        task_id: request.task.task_id,
        delivery_id: request.delivery_id,
        attempt: request.attempt,
        transport: request.target.transport(),
        text: request.task.text.clone(),
        version: request.task.version,
    });
}

impl TaskDispatcher for ScriptedDispatcher {
    fn deliver<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move {
            self.record("deliver", &request);
            match &self.delivery {
                Some(err) => Err(err.clone()),
                None => Ok(()),
            }
        })
    }

    fn execute<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
        Box::pin(async move {
            self.record("execute", &request);
            self.outcome.clone()
        })
    }
}

/// Coerce a concrete double into the trait object the worker holds, keeping the
/// concrete handle for assertions.
fn as_dispatcher<S: TaskDispatcher + 'static>(dispatcher: &Arc<S>) -> Arc<dyn TaskDispatcher> {
    let concrete: Arc<S> = Arc::clone(dispatcher);
    concrete
}

/// A dispatcher that samples the producer's progress when each task reaches the
/// dispatch stage.
///
/// It accepts and completes everything; the only thing it records is *how many*
/// submissions had succeeded by the time the worker took a task out of its local
/// queue. That count is the observable proxy for the worker's backlog: it is the
/// cheapest way for an integration test (which cannot see the private heap) to
/// show that a drain was bounded instead of accumulating tasks forever.
struct ProgressDispatcher {
    submitted: Arc<AtomicUsize>,
    seen_at_deliver: Mutex<Vec<usize>>,
}

impl ProgressDispatcher {
    fn new(submitted: Arc<AtomicUsize>) -> Self {
        Self {
            submitted,
            seen_at_deliver: Mutex::new(Vec::new()),
        }
    }

    fn seen_at_deliver(&self) -> Vec<usize> {
        self.seen_at_deliver.lock().expect("not poisoned").clone()
    }
}

impl TaskDispatcher for ProgressDispatcher {
    fn deliver<'a>(
        &'a self,
        _request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move {
            self.seen_at_deliver
                .lock()
                .expect("not poisoned")
                .push(self.submitted.load(Ordering::SeqCst));
            Ok(())
        })
    }

    fn execute<'a>(
        &'a self,
        _request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
        Box::pin(async move {
            Ok(DispatchOutcome::Completed {
                output: "done".into(),
            })
        })
    }
}

/// An [`EventConsumer`] that records every event it is handed.
#[derive(Default)]
struct RecordingConsumer {
    events: Mutex<Vec<TaskEvent>>,
}

impl RecordingConsumer {
    fn events(&self) -> Vec<TaskEvent> {
        self.events.lock().expect("not poisoned").clone()
    }

    fn len(&self) -> usize {
        self.events.lock().expect("not poisoned").len()
    }
}

impl EventConsumer for RecordingConsumer {
    fn consume<'a>(&'a self, event: &'a TaskEvent) -> BusFuture<'a, Result<(), ConsumerError>> {
        Box::pin(async move {
            self.events
                .lock()
                .expect("not poisoned")
                .push(event.clone());
            Ok(())
        })
    }
}

/// An [`EventConsumer`] that always fails, to prove per-consumer isolation.
struct FailingConsumer;

impl EventConsumer for FailingConsumer {
    fn consume<'a>(&'a self, _event: &'a TaskEvent) -> BusFuture<'a, Result<(), ConsumerError>> {
        Box::pin(async {
            Err(ConsumerError::Failed {
                reason: "projection is down".into(),
            })
        })
    }
}

fn as_consumer<C: EventConsumer + 'static>(consumer: &Arc<C>) -> Arc<dyn EventConsumer> {
    let concrete: Arc<C> = Arc::clone(consumer);
    concrete
}

/// A non-`mpsc` [`EventSink`] shared by the bus and the worker.
///
/// It records every *attempted* write and can be told to fail once at a given
/// call index, which is how a test reaches the fail-stop paths deterministically
/// (no timers, no capacity guesswork).
struct ScriptedSink {
    events: Mutex<Vec<TaskEvent>>,
    fail_at: Option<usize>,
    failure: BusError,
}

impl ScriptedSink {
    fn recording() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            fail_at: None,
            failure: BusError::EventSinkClosed,
        })
    }

    /// Succeeds for call indices `< fail_at`, then fails every call with `failure`.
    fn failing_after(fail_at: usize, failure: BusError) -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            fail_at: Some(fail_at),
            failure,
        })
    }

    /// Every attempted write, including the one that failed.
    fn attempted(&self) -> Vec<TaskEvent> {
        self.events.lock().expect("not poisoned").clone()
    }

    /// What a downstream consumer would actually observe: the writes that
    /// returned `Ok`. A failed transition is not observable at all.
    fn observed(&self) -> Vec<TaskEvent> {
        let events = self.events.lock().expect("not poisoned");
        let end = self.fail_at.unwrap_or(events.len()).min(events.len());
        events[..end].to_vec()
    }
}

impl EventSink for ScriptedSink {
    fn emit<'a>(&'a self, event: TaskEvent) -> BusFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            let mut events = self.events.lock().expect("not poisoned");
            let index = events.len();
            events.push(event);
            if Some(index) == self.fail_at {
                return Err(self.failure);
            }
            Ok(())
        })
    }
}

fn as_sink<S: EventSink + 'static>(sink: &Arc<S>) -> Arc<dyn EventSink> {
    let concrete: Arc<S> = Arc::clone(sink);
    concrete
}

// ---------------------------------------------------------------------------
// Assembly helpers
// ---------------------------------------------------------------------------

/// The bus half of an assembly, wired through `MemoryBus::with_event_sink` so the
/// bus and the worker can share one sink — and therefore one event stream.
struct Wiring {
    bus: MemoryBus,
    /// The exact sink the bus writes through. The worker gets a clone; the test
    /// holds this handle only to drop it (the event channel closes when the bus
    /// *and* the worker are gone).
    sink: Arc<dyn EventSink>,
    tasks_rx: mpsc::Receiver<AgentTask>,
    events_rx: mpsc::Receiver<TaskEvent>,
    registry: Arc<EndpointRegistry>,
}

fn wiring(config: &Config, task_capacity: usize, event_capacity: usize) -> Wiring {
    wiring_with_clock(config, task_capacity, event_capacity, fixed_clock())
}

/// The same wiring with an explicit clock.
///
/// The T006 cases need a clock they can pin *or* step (a deadline that passes
/// mid-stage), while every other case keeps the pinned clock through [`wiring`].
fn wiring_with_clock(
    config: &Config,
    task_capacity: usize,
    event_capacity: usize,
    clock: Clock,
) -> Wiring {
    let registry = Arc::new(EndpointRegistry::from_config(config));
    let (mpsc_sink, events_rx) = MpscEventSink::new(event_capacity);
    let sink: Arc<dyn EventSink> = Arc::new(mpsc_sink);
    let (bus, tasks_rx) = MemoryBus::with_event_sink(
        Arc::clone(&registry),
        task_capacity,
        clock,
        Arc::clone(&sink),
    );
    Wiring {
        bus,
        sink,
        tasks_rx,
        events_rx,
        registry,
    }
}

/// Move the task receiver out of its slot (`mpsc::Receiver` is deliberately not
/// cloneable).
fn take_tasks_rx(slot: &mut mpsc::Receiver<AgentTask>) -> mpsc::Receiver<AgentTask> {
    let (tx, fresh) = mpsc::channel(1);
    drop(tx);
    std::mem::replace(slot, fresh)
}

fn seqs(events: &[TaskEvent]) -> Vec<u64> {
    events.iter().map(|event| event.seq).collect()
}

/// One task's `seq` values, sorted.
///
/// Grouping by `task_id` and sorting by `seq` is exactly what the Q1 contract
/// requires of a consumer: when the submitter races the worker, `Queued(seq = 1)`
/// may arrive *after* `Dispatched(seq = 2)`, so arrival order must never be
/// compared against `seq` order.
fn sorted_seqs(events: &[TaskEvent]) -> Vec<u64> {
    let mut seqs = seqs(events);
    seqs.sort_unstable();
    seqs
}

fn statuses(events: &[TaskEvent]) -> Vec<TaskStatus> {
    events.iter().map(|event| event.status).collect()
}

fn for_task(events: &[TaskEvent], task_id: TaskId) -> Vec<TaskEvent> {
    events
        .iter()
        .filter(|event| event.task_id == task_id)
        .cloned()
        .collect()
}

fn dispatched_payload(event: &TaskEvent) -> (DeliveryId, u32) {
    match &event.payload {
        TaskEventPayload::Dispatched {
            delivery_id,
            attempt,
        } => (*delivery_id, *attempt),
        other => panic!("expected a Dispatched payload, got {other:?}"),
    }
}

fn failed_payload(event: &TaskEvent) -> String {
    match &event.payload {
        TaskEventPayload::Failed { error } => error.clone(),
        other => panic!("expected a Failed payload, got {other:?}"),
    }
}

/// Yield to the runtime until `done` holds.
///
/// The budget is not a timeout: the condition must become true, so a broken
/// worker fails the test with a message instead of hanging the suite.
async fn yield_until(mut done: impl FnMut() -> bool) {
    const MAX_YIELDS: usize = 256;
    for _ in 0..MAX_YIELDS {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the worker made no progress within {MAX_YIELDS} yields");
}

// ---------------------------------------------------------------------------
// 1. Success path: Queued(1) → Dispatched(2) → Running(3) → Completed(4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn successful_dispatch_records_queued_dispatched_running_completed() {
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("done"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    assert_eq!(broadcaster.consumer_count(), 1);
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    let task = root_task(worker_endpoint(), 5);
    wiring.bus.submit(task.clone()).await.expect("submission");
    drop(wiring.bus);
    drop(wiring.sink);

    worker
        .run()
        .await
        .expect("clean drain-then-terminate shutdown");
    broadcaster.run().await;

    let events = recording.events();
    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed
        ]
    );
    assert!(
        events.iter().all(|event| event.task_id == task.task_id),
        "one task, one event stream"
    );
    assert_eq!(events[0].payload, TaskEventPayload::Queued);
    assert_eq!(
        events[3].payload,
        TaskEventPayload::Completed {
            output: "done".into()
        }
    );

    // Every event carries its own identity and the injected clock's instant.
    let event_ids: HashSet<EventId> = events.iter().map(|event| event.id).collect();
    assert_eq!(event_ids.len(), 4, "EventId must be unique per event");
    assert!(events.iter().all(|event| event.timestamp == fixed_ts()));

    // The delivery identity is created once and shared by both stages.
    let (delivery_id, attempt) = dispatched_payload(&events[1]);
    assert_eq!(attempt, 1, "T005 always dispatches as attempt 1");
    let deliver = dispatcher.recorded("deliver");
    let execute = dispatcher.recorded("execute");
    assert_eq!(deliver.delivery_id, delivery_id);
    assert_eq!(execute.delivery_id, delivery_id);
    assert_eq!(deliver.task_id, task.task_id);
    assert_eq!(execute.task_id, task.task_id);
    assert_eq!(deliver.transport, TransportType::Acp);
    assert_eq!(dispatcher.calls().len(), 2, "one task, exactly two stages");

    // `Running.started_at` is the event's own timestamp.
    match &events[2].payload {
        TaskEventPayload::Running { started_at } => {
            assert_eq!(*started_at, events[2].timestamp);
        }
        other => panic!("expected a Running payload, got {other:?}"),
    }
}

#[tokio::test]
async fn the_reserved_version_counter_is_passed_through_untouched() {
    // D9: the worker neither reads nor rewrites `AgentTask::version`; conditional
    // updates are T009's job.
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("done"));
    let sink = ScriptedSink::recording();
    let registry = Arc::new(EndpointRegistry::from_config(&load(CONFIG)));
    let (bus, tasks_rx) =
        MemoryBus::with_event_sink(Arc::clone(&registry), 4, fixed_clock(), as_sink(&sink));
    let worker = Worker::with_dispatcher(
        registry,
        tasks_rx,
        as_sink(&sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    let task = AgentTask {
        version: 42,
        ..root_task(worker_endpoint(), 5)
    };
    bus.submit(task.clone()).await.expect("submission");
    drop(bus);
    worker.run().await.expect("clean shutdown");

    assert_eq!(dispatcher.recorded("deliver").version, 42);
    assert_eq!(dispatcher.recorded("execute").version, 42);
    assert_eq!(task.version, 42, "the worker never rewrites the task");
}

// ---------------------------------------------------------------------------
// 2. Delivery not accepted → Failed(seq = 3), never Running
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delivery_not_accepted_fails_at_seq_three_without_running() {
    let dispatcher = Arc::new(ScriptedDispatcher::not_accepted(
        "target never acknowledged",
    ));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");
    drop(wiring.bus);
    drop(wiring.sink);

    worker
        .run()
        .await
        .expect("a dispatch failure is not a worker failure");
    broadcaster.run().await;

    let events = recording.events();
    assert_eq!(seqs(&events), [1, 2, 3], "no Running and no gap");
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Failed
        ]
    );
    assert_eq!(
        failed_payload(&events[2]),
        "delivery was not accepted: target never acknowledged"
    );

    // `execute` must never run for a delivery that was not accepted.
    assert_eq!(dispatcher.stages(), ["deliver"]);
}

// ---------------------------------------------------------------------------
// 3. Execution failure → Running(3) then Failed(4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execution_error_fails_at_seq_four_after_running() {
    let dispatcher = Arc::new(ScriptedDispatcher::execution_failed("adapter crashed"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");
    drop(wiring.bus);
    drop(wiring.sink);

    worker
        .run()
        .await
        .expect("worker survives a dispatch failure");
    broadcaster.run().await;

    let events = recording.events();
    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Failed
        ]
    );
    assert_eq!(
        failed_payload(&events[3]),
        "execution failed after acceptance: adapter crashed"
    );
    // `execute` runs only after `deliver` was accepted.
    assert_eq!(dispatcher.stages(), ["deliver", "execute"]);
}

#[tokio::test]
async fn a_terminal_failed_outcome_is_recorded_at_seq_four() {
    let dispatcher = Arc::new(ScriptedDispatcher::reporting_failure(
        "agent reported an error",
    ));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");
    drop(wiring.bus);
    drop(wiring.sink);

    worker
        .run()
        .await
        .expect("worker survives a failed outcome");
    broadcaster.run().await;

    let events = recording.events();
    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    assert_eq!(events[3].status, TaskStatus::Failed);
    assert_eq!(failed_payload(&events[3]), "agent reported an error");
}

// ---------------------------------------------------------------------------
// 4. Re-validation failure: worker registry != submit registry
// ---------------------------------------------------------------------------

/// Run one re-validation scenario: the *submit* registry accepts the target (so
/// `submit` succeeds), while the worker holds a different registry where the same
/// endpoint id classifies differently. That is the deterministic, reachable way
/// to exercise the defensive branch without writing unreachable code.
async fn assert_revalidation_failure(worker_config: &str, expected_prefix: &str) {
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("never reached"));
    let submit_registry = Arc::new(EndpointRegistry::from_config(&load(CONFIG)));
    let (mpsc_sink, events_rx) = MpscEventSink::new(16);
    let sink: Arc<dyn EventSink> = Arc::new(mpsc_sink);
    let (bus, tasks_rx) = MemoryBus::with_event_sink(
        Arc::clone(&submit_registry),
        4,
        fixed_clock(),
        Arc::clone(&sink),
    );
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(events_rx, vec![as_consumer(&recording)]);

    let worker = Worker::with_dispatcher(
        Arc::new(EndpointRegistry::from_config(&load(worker_config))),
        tasks_rx,
        Arc::clone(&sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    bus.submit(root_task(worker_endpoint(), 5))
        .await
        .expect("the submit registry accepts the target");
    drop(bus);
    drop(sink);

    worker
        .run()
        .await
        .expect("a rejected target is not a worker failure");
    broadcaster.run().await;

    assert_eq!(
        dispatcher.stages(),
        Vec::<&'static str>::new(),
        "a target rejected before dispatch must never reach the dispatcher"
    );

    let events = recording.events();
    assert_eq!(
        seqs(&events),
        [1, 2],
        "nothing was dispatched, so there is no Dispatched event"
    );
    assert_eq!(events[1].status, TaskStatus::Failed);
    let error = failed_payload(&events[1]);
    assert!(
        error.starts_with(expected_prefix),
        "unexpected error text: {error}"
    );
    assert!(
        error.contains(&worker_endpoint().to_string()),
        "the failure must name the endpoint: {error}"
    );
    assert!(
        !error.contains("worker-acp"),
        "an address must never reach an event payload: {error}"
    );
}

#[tokio::test]
async fn revalidation_rejects_an_unknown_target_at_seq_two() {
    assert_revalidation_failure(NO_AGENTS, "unknown target endpoint").await;
}

#[tokio::test]
async fn revalidation_rejects_a_disabled_target_at_seq_two() {
    assert_revalidation_failure(WORKER_DISABLED, "target endpoint is disabled").await;
}

#[tokio::test]
async fn revalidation_rejects_an_unaddressable_target_at_seq_two() {
    assert_revalidation_failure(WORKER_UNADDRESSABLE, "endpoint address is not available").await;
}

// ---------------------------------------------------------------------------
// 5. No dispatcher for the target's transport
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_transport_without_a_dispatcher_fails_at_seq_two() {
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("never reached"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);

    // An explicitly empty registry: nothing can dispatch, and nothing is dropped.
    let worker = Worker::new(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        DispatcherRegistry::new(),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");
    drop(wiring.bus);
    drop(wiring.sink);

    worker
        .run()
        .await
        .expect("a missing dispatcher is not a worker failure");
    broadcaster.run().await;

    let events = recording.events();
    assert_eq!(seqs(&events), [1, 2]);
    assert_eq!(events[1].status, TaskStatus::Failed);
    assert_eq!(
        failed_payload(&events[1]),
        "no dispatcher registered for transport Acp"
    );
    assert_eq!(dispatcher.stages(), Vec::<&'static str>::new());
}

// ---------------------------------------------------------------------------
// 6. Priority dequeue: priority descending, then FIFO
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tasks_are_dequeued_by_priority_then_fifo() {
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("ok"));
    // Capacity >= number of preloaded tasks, so the worker's first drain sees all
    // of them: the priority order is then deterministic instead of a race.
    let mut wiring = wiring(&load(CONFIG), 8, 64);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    for (label, priority) in [("L", 1), ("H", 9), ("M", 5), ("L2", 1)] {
        wiring
            .bus
            .submit(labelled(label, priority))
            .await
            .expect("preload");
    }
    drop(wiring.bus);
    drop(wiring.sink);

    worker.run().await.expect("clean shutdown");
    broadcaster.run().await;

    assert_eq!(
        dispatcher.delivered_texts(),
        ["H", "M", "L", "L2"],
        "priority descending, FIFO within one priority"
    );

    // Every task still produced its own complete, gap-free sequence.
    let events = recording.events();
    assert_eq!(events.len(), 16);
    let task_ids: HashSet<TaskId> = events.iter().map(|event| event.task_id).collect();
    assert_eq!(task_ids.len(), 4);
    for task_id in task_ids {
        assert_eq!(seqs(&for_task(&events, task_id)), [1, 2, 3, 4]);
    }
}

// ---------------------------------------------------------------------------
// 7. Fan-out: arrival order preserved, nothing lost
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fan_out_preserves_arrival_order_and_drops_nothing() {
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("ok"));
    let mut wiring = wiring(&load(CONFIG), 8, 64);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    let mut submitted = Vec::new();
    for label in ["a", "b", "c"] {
        let task = labelled(label, 5);
        submitted.push(task.task_id);
        wiring.bus.submit(task).await.expect("preload");
    }
    drop(wiring.bus);
    drop(wiring.sink);

    worker.run().await.expect("clean shutdown");
    broadcaster.run().await;

    let events = recording.events();
    assert_eq!(events.len(), 12, "four events per task, none lost");
    for task_id in &submitted {
        assert_eq!(
            seqs(&for_task(&events, *task_id)),
            [1, 2, 3, 4],
            "per-task order matches `seq` when the submitter never races the worker"
        );
    }

    // The broadcaster neither reorders nor interleaves — but the stream is still
    // not per-task contiguous, because the bus and the worker are two independent
    // producers writing into one channel. Preloading put all three `Queued`
    // events in first, so the arrival order is:
    //
    //   a1 b1 c1 | a2 a3 a4 | b2 b3 b4 | c2 c3 c4
    //
    // This is exactly the Q1 contract in one assertion: a downstream consumer that
    // wants a task's history must group by `task_id` and sort by `seq`; it must
    // never assume arrival order equals `seq` order, and never assume one task's
    // events are contiguous.
    let observed: Vec<TaskId> = events.iter().map(|event| event.task_id).collect();
    let worker_batch: Vec<TaskId> = submitted
        .iter()
        .flat_map(|task_id| std::iter::repeat_n(*task_id, 3))
        .collect();
    let expected: Vec<TaskId> = submitted.iter().copied().chain(worker_batch).collect();
    assert_eq!(observed, expected);
}

// ---------------------------------------------------------------------------
// 8. Consumer error isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_failing_consumer_does_not_stop_the_others_or_the_broadcast() {
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("ok"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let first = Arc::new(RecordingConsumer::default());
    let third = Arc::new(RecordingConsumer::default());
    let consumers: Vec<Arc<dyn EventConsumer>> = vec![
        as_consumer(&first),
        Arc::new(FailingConsumer),
        Arc::new(FailingConsumer),
        as_consumer(&third),
    ];
    let broadcaster = EventBroadcaster::new(wiring.events_rx, consumers);
    assert_eq!(broadcaster.consumer_count(), 4);
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");
    drop(wiring.bus);
    drop(wiring.sink);

    worker.run().await.expect("clean shutdown");
    broadcaster.run().await;

    // Both surviving consumers saw every event, in arrival order: a failure in
    // the middle neither aborted the loop nor rolled back task state.
    assert_eq!(first.len(), 4);
    assert_eq!(third.len(), 4);
    assert_eq!(seqs(&first.events()), [1, 2, 3, 4]);
    assert_eq!(seqs(&third.events()), [1, 2, 3, 4]);
    assert_eq!(
        first.events().last().map(|event| event.status),
        Some(TaskStatus::Completed)
    );
}

// ---------------------------------------------------------------------------
// 9. One shared, injected sink → one event stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bus_and_worker_write_one_single_event_stream_through_the_injected_sink() {
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("done"));
    let sink = ScriptedSink::recording();
    let registry = Arc::new(EndpointRegistry::from_config(&load(CONFIG)));
    let (bus, tasks_rx) =
        MemoryBus::with_event_sink(Arc::clone(&registry), 4, fixed_clock(), as_sink(&sink));
    let worker = Worker::with_dispatcher(
        registry,
        tasks_rx,
        as_sink(&sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    let task = root_task(worker_endpoint(), 5);
    bus.submit(task.clone()).await.expect("submission");
    drop(bus);
    worker.run().await.expect("clean shutdown");

    // One record, written by the bus and the worker through the same sink: the
    // task's `Queued` and every `seq > 1` event, in order, with no second stream
    // and no extra sink construction.
    let events = sink.attempted();
    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed
        ]
    );
    assert_eq!(events, sink.observed(), "no write failed");
    assert!(events.iter().all(|event| event.task_id == task.task_id));
    assert_eq!(events[0].timestamp, fixed_ts());
    assert_eq!(events[2].timestamp, fixed_ts());
}

// ---------------------------------------------------------------------------
// 10. Event-write failure is fail-stop
// ---------------------------------------------------------------------------

/// What one fail-stop scenario produced.
struct FailStop {
    error: WorkerError,
    /// Writes a downstream consumer would observe (the successful prefix).
    observed: Vec<TaskEvent>,
    /// Every attempted write, including the one that failed.
    attempted: Vec<TaskEvent>,
    stages: Vec<&'static str>,
}

/// Run a scenario where the bus's own `Queued` write succeeds and the worker's
/// write at index `fail_at` fails with `failure`.
async fn run_until_event_write_failure(at: usize, failure: BusError) -> FailStop {
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("outcome was reached"));
    let sink = ScriptedSink::failing_after(at, failure);
    let registry = Arc::new(EndpointRegistry::from_config(&load(CONFIG)));
    let (bus, tasks_rx) =
        MemoryBus::with_event_sink(Arc::clone(&registry), 4, fixed_clock(), as_sink(&sink));
    let worker = Worker::with_dispatcher(
        Arc::clone(&registry),
        tasks_rx,
        as_sink(&sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    bus.submit(root_task(worker_endpoint(), 5))
        .await
        .expect("the bus's Queued write succeeds");
    // A second handle keeps the task channel open; the worker's exit closes the
    // receiving side, which is what makes later submissions report `Closed`.
    let still_open = bus.clone();
    drop(bus);

    let error = worker
        .run()
        .await
        .expect_err("an event write failure is fail-stop");

    // The worker dropped its receiver: the producer side now sees a closed channel.
    assert_eq!(
        still_open
            .submit(root_task(worker_endpoint(), 5))
            .await
            .expect_err("the task channel is closed after fail-stop"),
        BusError::TaskChannelClosed
    );

    FailStop {
        error,
        observed: sink.observed(),
        attempted: sink.attempted(),
        stages: dispatcher.stages(),
    }
}

#[tokio::test]
async fn a_full_event_buffer_fails_stop_with_event_backpressure() {
    // Index 3 is the terminal `Completed` write: everything before it succeeded.
    let fail_stop = run_until_event_write_failure(3, BusError::EventBufferFull).await;
    match fail_stop.error {
        WorkerError::EventBackpressure { seq, .. } => assert_eq!(seq, 4),
        other => panic!("expected EventBackpressure, got {other:?}"),
    }
    assert_eq!(seqs(&fail_stop.observed), [1, 2, 3]);
    assert_eq!(seqs(&fail_stop.attempted), [1, 2, 3, 4]);
    assert_eq!(fail_stop.stages, ["deliver", "execute"]);
    assert!(
        fail_stop
            .observed
            .iter()
            .all(|event| event.status != TaskStatus::Completed),
        "a failed write leaves no terminal state behind"
    );
}

#[tokio::test]
async fn a_closed_event_stream_fails_stop_with_event_stream_closed() {
    // Index 1 is the `Dispatched` write: the dispatcher is never reached.
    let fail_stop = run_until_event_write_failure(1, BusError::EventSinkClosed).await;
    match fail_stop.error {
        WorkerError::EventStreamClosed { seq, .. } => assert_eq!(seq, 2),
        other => panic!("expected EventStreamClosed, got {other:?}"),
    }
    assert_eq!(seqs(&fail_stop.observed), [1]);
    assert_eq!(seqs(&fail_stop.attempted), [1, 2]);
    assert_eq!(
        fail_stop.stages,
        Vec::<&'static str>::new(),
        "a task is never delivered once its state can no longer be recorded"
    );
}

#[tokio::test]
async fn an_unexpected_bus_error_lands_in_the_defensive_variant() {
    let fail_stop = run_until_event_write_failure(1, BusError::QueueFull).await;
    match fail_stop.error {
        WorkerError::EventWrite { seq, error, .. } => {
            assert_eq!(seq, 2);
            assert_eq!(error, BusError::QueueFull);
        }
        other => panic!("expected EventWrite, got {other:?}"),
    }
    assert_eq!(seqs(&fail_stop.observed), [1]);
}

// ---------------------------------------------------------------------------
// 11. Lifecycle: drain-then-terminate, late submissions, no leaks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dropping_every_bus_handle_drains_the_heap_and_shuts_down_cleanly() {
    const N: usize = 3;
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("ok"));
    let mut wiring = wiring(&load(CONFIG), 8, 64);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    let mut submitted = Vec::new();
    for index in 0..N {
        let task = labelled(&format!("task-{index}"), (index as u8) + 1);
        submitted.push(task.task_id);
        wiring.bus.submit(task).await.expect("submission");
    }

    // Close the channels and then run the worker: it must drain its heap and
    // return `Ok`, and the broadcaster must terminate once both sink holders are
    // gone. No sleep, no timeout — just explicit drops.
    drop(wiring.bus);
    drop(wiring.sink);
    worker.run().await.expect("drain-then-terminate returns Ok");
    broadcaster.run().await;

    let events = recording.events();
    assert_eq!(
        events.len(),
        4 * N,
        "no accepted task is dropped on shutdown"
    );
    for task_id in &submitted {
        let own = for_task(&events, *task_id);
        assert_eq!(seqs(&own), [1, 2, 3, 4]);
        assert_eq!(
            own.last().map(|event| event.status),
            Some(TaskStatus::Completed)
        );
    }
    assert_eq!(
        dispatcher
            .calls()
            .into_iter()
            .filter(|call| call.stage == "execute")
            .count(),
        N
    );
}

#[tokio::test]
async fn the_worker_picks_up_tasks_submitted_after_it_started() {
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("ok"));
    let mut wiring = wiring(&load(CONFIG), 4, 32);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    // Both halves run as real tasks, so the worker parks in `recv().await` when
    // the queue is empty rather than only exercising the drain path.
    let worker_handle = worker.spawn();
    let broadcaster_handle = broadcaster.spawn();

    let first = root_task(worker_endpoint(), 5);
    wiring
        .bus
        .submit(first.clone())
        .await
        .expect("first submit");
    yield_until(|| dispatcher.calls().len() >= 2).await;
    assert_eq!(
        dispatcher.delivered_texts().len(),
        1,
        "the first task was taken from the blocked receive, not from a drain"
    );

    let second = root_task(worker_endpoint(), 5);
    wiring
        .bus
        .submit(second.clone())
        .await
        .expect("second submit");

    drop(wiring.bus);
    drop(wiring.sink);

    worker_handle
        .await
        .expect("worker task must not panic")
        .expect("clean shutdown");
    broadcaster_handle.await.expect("broadcaster task");

    let events = recording.events();
    for task_id in [first.task_id, second.task_id] {
        assert_eq!(seqs(&for_task(&events, task_id)), [1, 2, 3, 4]);
    }
}

// ---------------------------------------------------------------------------
// 12. Boundaries T004 fixed and T005 must not change
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_duplicate_task_id_is_processed_twice_independently() {
    // v1 has no dedup (T004 D6) and the worker keeps no cross-task state, so two
    // submissions of the same `task_id` are two independent state machines, each
    // with its own `1..4` sequence.
    let dispatcher = Arc::new(ScriptedDispatcher::succeeding("ok"));
    let sink = ScriptedSink::recording();
    let registry = Arc::new(EndpointRegistry::from_config(&load(CONFIG)));
    let (bus, tasks_rx) =
        MemoryBus::with_event_sink(Arc::clone(&registry), 4, fixed_clock(), as_sink(&sink));
    let worker = Worker::with_dispatcher(
        registry,
        tasks_rx,
        as_sink(&sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    let task = root_task(worker_endpoint(), 5);
    bus.submit(task.clone()).await.expect("first");
    bus.submit(task.clone()).await.expect("duplicate");
    drop(bus);
    worker.run().await.expect("clean shutdown");

    let events = sink.attempted();
    assert_eq!(seqs(&events), [1, 1, 2, 3, 4, 2, 3, 4]);
    let event_ids: HashSet<_> = events.iter().map(|event| event.id).collect();
    assert_eq!(event_ids.len(), events.len(), "event ids stay unique");
    assert_eq!(
        dispatcher.calls().len(),
        4,
        "the duplicate is dispatched independently"
    );
}

// ---------------------------------------------------------------------------
// 13. Bounded admission: a channel refilled while the worker drains neither
//     starves dispatch nor lets the local queue outgrow the channel
// ---------------------------------------------------------------------------

/// A channel that producers keep refilling while the worker drains it is exactly
/// the condition that made an unconditional "read until `Empty`" drain loop
/// unbounded: the bounded channel admits an unbounded number of tasks into the
/// worker's local queue, and dispatch is starved because the pop side is never
/// reached.
///
/// This drives the real path under genuine concurrency (`multi_thread`): the
/// workload is twenty times the channel capacity, so the producers cannot finish
/// without the worker draining, and the worker repeatedly finds the channel full
/// again while it drains.
///
/// The bound asserted below follows from the admission rule alone, so it holds for
/// *every* schedule: the test can never fail spuriously in the passing direction.
/// Showing the defect is what needs an actual interleaving, so the producers stamp
/// on the full queue from dedicated blocking threads instead of yielding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_channel_refilled_while_the_worker_drains_stays_bounded_and_makes_progress() {
    // The task channel is deliberately much smaller than the workload, and the
    // event stream is large enough to hold every event of the whole run, so a busy
    // worker is never stopped by the event backpressure T005 documents.
    const CAPACITY: usize = 2;
    const PRODUCERS: usize = 8;
    const N: usize = 40;
    // A compile-time assertion (the constants above are fixed): this is the
    // `clippy::assertions_on_constants` deferred from T005.
    const {
        assert!(
            N > 2 * CAPACITY,
            "the workload must exceed the admission ceiling, or the bound below is vacuous"
        )
    };
    assert_eq!(N % PRODUCERS, 0, "the workload must split evenly");

    let submitted = Arc::new(AtomicUsize::new(0));
    let dispatcher = Arc::new(ProgressDispatcher::new(Arc::clone(&submitted)));
    let mut wiring = wiring(&load(CONFIG), CAPACITY, 4 * N + 4);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    let worker_handle = worker.spawn();
    let broadcaster_handle = broadcaster.spawn();

    // Several concurrent producers keep the small channel refilled, so the worker
    // repeatedly finds it full again while it drains. They run on dedicated
    // blocking threads and never park: an async producer would yield on `QueueFull`,
    // letting the worker drain an empty channel and win the race by accident.
    let mut producers = Vec::new();
    for _ in 0..PRODUCERS {
        let bus = wiring.bus.clone();
        let submitted = Arc::clone(&submitted);
        let handle = tokio::runtime::Handle::current();
        producers.push(tokio::task::spawn_blocking(move || {
            let mut task_ids = Vec::new();
            for _ in 0..N / PRODUCERS {
                let task = root_task(worker_endpoint(), 5);
                task_ids.push(task.task_id);
                // A full queue is retried immediately, which is what refills the
                // channel while the worker drains. The retry budget fails the test
                // instead of hanging it; it is not a timeout.
                let mut retries = 0;
                loop {
                    match handle.block_on(bus.submit(task.clone())) {
                        Ok(()) => {
                            submitted.fetch_add(1, Ordering::SeqCst);
                            break;
                        }
                        Err(BusError::QueueFull) => {
                            retries += 1;
                            assert!(
                                retries < 100_000,
                                "the worker stopped draining a channel that producers refill"
                            );
                        }
                        Err(other) => panic!("unexpected submit error: {other:?}"),
                    }
                }
            }
            task_ids
        }));
    }

    let mut task_ids = Vec::new();
    for producer in producers {
        task_ids.extend(producer.await.expect("producer task must not panic"));
    }
    assert_eq!(
        submitted.load(Ordering::SeqCst),
        N,
        "the whole workload was submitted"
    );

    drop(wiring.bus);
    drop(wiring.sink);
    worker_handle
        .await
        .expect("worker task must not panic")
        .expect("clean drain-then-terminate shutdown");
    broadcaster_handle.await.expect("broadcaster task");

    // The bound, asserted end to end. Before the first task is dispatched the
    // worker has performed exactly one admission step, which admits at most
    // `CAPACITY` tasks, and the channel itself holds at most `CAPACITY` more — so
    // the first `deliver` cannot observe more than `2 * CAPACITY` successful
    // submissions, however the producers are scheduled. An unbounded drain has no
    // such ceiling: it keeps reading a refilled channel and only reaches the
    // dispatch side once submissions stop.
    let progress = dispatcher.seen_at_deliver();
    assert_eq!(progress.len(), N, "every task was dispatched exactly once");
    let earliest = progress
        .iter()
        .copied()
        .min()
        .expect("at least one dispatch was recorded");
    assert!(
        earliest <= 2 * CAPACITY,
        "the local queue outgrew the {CAPACITY}-task channel: the first dispatch already saw \
         {earliest} submissions (ceiling {})",
        2 * CAPACITY
    );

    // Forward progress and no loss: a refilled channel was drained to the end,
    // every task ran, and each produced one complete, gap-free sequence. The
    // per-task events are grouped and sorted by `seq` — arrival order is not `seq`
    // order here, because the producers genuinely race the worker (Q1 = A).
    let events = recording.events();
    assert_eq!(events.len(), 4 * N, "four events per task, none lost");
    for task_id in task_ids {
        let own = for_task(&events, task_id);
        assert_eq!(
            sorted_seqs(&own),
            [1, 2, 3, 4],
            "each task advanced through every state exactly once"
        );
        let terminal = own
            .iter()
            .find(|event| event.seq == 4)
            .expect("every task reached a terminal state");
        assert_eq!(terminal.status, TaskStatus::Completed);
    }
}

// ---------------------------------------------------------------------------
// 14. T006 runtime control: timeout, cancellation, retry, cycle limits
// ---------------------------------------------------------------------------

/// A deterministic [`Timer`] double: a wait resolves only when the test fires it.
///
/// `fire` bumps a shared version, and each wait captures the version it started
/// from, so a fire resolves exactly the waits that were armed before it. That is
/// why the tests wait for [`ManualTimer::waiting`] to become non-zero (the
/// worker has entered the race) before firing: nothing here sleeps, reads the
/// wall clock, or depends on the scheduler.
struct ManualTimer {
    /// Bumped by `fire`; waits compare it against their own baseline.
    version: watch::Sender<u64>,
    /// Waits created and not yet finished.
    waiting: AtomicUsize,
    /// Number of `fire` calls, for assertions.
    fires: AtomicUsize,
}

impl ManualTimer {
    fn new() -> Arc<Self> {
        let (version, _) = watch::channel(0u64);
        Arc::new(Self {
            version,
            waiting: AtomicUsize::new(0),
            fires: AtomicUsize::new(0),
        })
    }

    /// Resolve every wait that is currently armed.
    fn fire(&self) {
        self.fires.fetch_add(1, Ordering::SeqCst);
        self.version
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    /// How many waits the worker is parked on right now.
    fn waiting(&self) -> usize {
        self.waiting.load(Ordering::SeqCst)
    }

    /// How many times the test fired this timer.
    fn fires(&self) -> usize {
        self.fires.load(Ordering::SeqCst)
    }
}

/// Counts a wait for exactly as long as the wait future exists.
struct WaitGuard<'a> {
    timer: &'a ManualTimer,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        self.timer.waiting.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Timer for ManualTimer {
    fn sleep_until(&self, _deadline: DateTime<Utc>) -> BusFuture<'_, ()> {
        // Captured synchronously, before `select!` polls anything, so a fire that
        // happens after this call resolves this wait.
        let baseline = *self.version.borrow();
        self.waiting.fetch_add(1, Ordering::SeqCst);
        let guard = WaitGuard { timer: self };
        let mut rx = self.version.subscribe();

        Box::pin(async move {
            // Held for the lifetime of the wait, so `waiting()` is an exact
            // count of armed waits.
            let _guard = guard;
            loop {
                // Read the version into a local: the borrow guard must never be
                // alive across the await, or `fire` could block on the write lock.
                let current = *rx.borrow();
                if current != baseline {
                    return;
                }
                if rx.changed().await.is_err() {
                    // The sender lives in the timer the test owns; if it were
                    // gone, resolving is the safe failure mode (the worker would
                    // report a timeout) rather than parking forever.
                    return;
                }
            }
        })
    }
}

/// A clock pinned to `base`, stepped by publishing a second offset into the
/// returned handle.
fn stepping_clock(base: DateTime<Utc>) -> (Clock, Arc<AtomicI64>) {
    let offset = Arc::new(AtomicI64::new(0));
    let clock = {
        let offset = Arc::clone(&offset);
        Clock::new(move || base + chrono::Duration::seconds(offset.load(Ordering::SeqCst)))
    };
    (clock, offset)
}

/// One scripted `deliver` step, selected by attempt number.
#[derive(Debug, Clone)]
enum DeliverStep {
    Accept,
    Refuse(&'static str),
    /// Records the call and then never resolves: the worker must reach its
    /// timeout/cancellation boundary instead of waiting forever.
    Hang,
}

/// One scripted `execute` step, selected by attempt number.
#[derive(Debug, Clone)]
enum ExecuteStep {
    Complete(&'static str),
    /// A deterministic terminal failure from the adapter.
    Report(&'static str),
    /// A transient failure with no terminal result.
    Fail(&'static str),
    Hang,
}

/// A dispatcher whose per-attempt behaviour is scripted.
///
/// Each attempt index picks its step; an attempt beyond the script repeats the
/// last step, so an unexpected extra attempt shows up as an extra dispatched
/// event (and a failed assertion) instead of a panic inside the worker task.
struct ScriptedAttempts {
    calls: Mutex<Vec<SeenCall>>,
    delivers: Vec<DeliverStep>,
    executes: Vec<ExecuteStep>,
}

impl ScriptedAttempts {
    fn new(delivers: Vec<DeliverStep>, executes: Vec<ExecuteStep>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            delivers,
            executes,
        }
    }

    /// Accepts every delivery, then completes with `output`.
    fn succeeding(output: &'static str) -> Self {
        Self::new(
            vec![DeliverStep::Accept],
            vec![ExecuteStep::Complete(output)],
        )
    }

    fn deliver_step(&self, attempt: u32) -> DeliverStep {
        step_at(&self.delivers, DeliverStep::Accept, attempt)
    }

    fn execute_step(&self, attempt: u32) -> ExecuteStep {
        step_at(
            &self.executes,
            ExecuteStep::Complete("unreachable"),
            attempt,
        )
    }

    fn calls(&self) -> Vec<SeenCall> {
        self.calls.lock().expect("not poisoned").clone()
    }

    fn stages(&self) -> Vec<&'static str> {
        self.calls().into_iter().map(|call| call.stage).collect()
    }

    /// Attempt numbers as the dispatcher received them, per stage.
    fn attempts(&self, stage: &'static str) -> Vec<u32> {
        self.calls()
            .into_iter()
            .filter(|call| call.stage == stage)
            .map(|call| call.attempt)
            .collect()
    }

    fn record(&self, stage: &'static str, request: &DispatchRequest<'_>) {
        record_call(&self.calls, stage, request);
    }
}

/// The step for a 1-based `attempt`, clamped to the script.
fn step_at<T: Clone>(steps: &[T], fallback: T, attempt: u32) -> T {
    match steps.len() {
        0 => fallback,
        len => steps[(attempt as usize).saturating_sub(1).min(len - 1)].clone(),
    }
}

impl TaskDispatcher for ScriptedAttempts {
    fn deliver<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move {
            self.record("deliver", &request);
            match self.deliver_step(request.attempt) {
                DeliverStep::Accept => Ok(()),
                DeliverStep::Refuse(reason) => Err(DispatchError::NotAccepted {
                    reason: reason.into(),
                }),
                DeliverStep::Hang => std::future::pending().await,
            }
        })
    }

    fn execute<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
        Box::pin(async move {
            self.record("execute", &request);
            match self.execute_step(request.attempt) {
                ExecuteStep::Complete(output) => Ok(DispatchOutcome::Completed {
                    output: output.into(),
                }),
                ExecuteStep::Report(error) => Ok(DispatchOutcome::Failed {
                    error: error.into(),
                }),
                ExecuteStep::Fail(reason) => Err(DispatchError::ExecutionFailed {
                    reason: reason.into(),
                }),
                ExecuteStep::Hang => std::future::pending().await,
            }
        })
    }
}

/// A dispatcher that moves the injected clock past the task's deadline while
/// `deliver` runs, then accepts or refuses that delivery.
///
/// This is the deterministic way to prove that the *checkpoints* enforce the
/// deadline: those assemblies configure no timer at all, and the clock only
/// changes while `deliver` runs.
struct DeadlineCrossingDispatcher {
    offset: Arc<AtomicI64>,
    seconds: i64,
    refuse: bool,
    delivers: AtomicUsize,
    executes: AtomicUsize,
}

impl DeadlineCrossingDispatcher {
    /// Accepts every delivery, after crossing the deadline.
    fn accepting(offset: Arc<AtomicI64>, seconds: i64) -> Self {
        Self::new(offset, seconds, false)
    }

    /// Refuses every delivery, after crossing the deadline.
    fn refusing(offset: Arc<AtomicI64>, seconds: i64) -> Self {
        Self::new(offset, seconds, true)
    }

    fn new(offset: Arc<AtomicI64>, seconds: i64, refuse: bool) -> Self {
        Self {
            offset,
            seconds,
            refuse,
            delivers: AtomicUsize::new(0),
            executes: AtomicUsize::new(0),
        }
    }

    fn delivers(&self) -> usize {
        self.delivers.load(Ordering::SeqCst)
    }

    fn executes(&self) -> usize {
        self.executes.load(Ordering::SeqCst)
    }
}

impl TaskDispatcher for DeadlineCrossingDispatcher {
    fn deliver<'a>(
        &'a self,
        _request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move {
            self.delivers.fetch_add(1, Ordering::SeqCst);
            self.offset.store(self.seconds, Ordering::SeqCst);
            if self.refuse {
                Err(DispatchError::NotAccepted {
                    reason: "crossed the deadline".into(),
                })
            } else {
                Ok(())
            }
        })
    }

    fn execute<'a>(
        &'a self,
        _request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
        Box::pin(async move {
            self.executes.fetch_add(1, Ordering::SeqCst);
            Ok(DispatchOutcome::Completed {
                output: "unreachable".into(),
            })
        })
    }
}

/// Build a T006 worker over `wiring`: an injected [`WorkerConfig`] and a single
/// `acp` dispatcher.
fn controlled_worker(
    wiring: &Wiring,
    tasks_rx: mpsc::Receiver<AgentTask>,
    clock: Clock,
    config: WorkerConfig,
    dispatcher: Arc<dyn TaskDispatcher>,
) -> Worker {
    Worker::builder(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        clock,
        DispatcherRegistry::new(),
    )
    .config(config)
    .with_dispatcher(TransportType::Acp, dispatcher)
    .build()
}

/// Run one worker to a clean shutdown and return what the recording consumer saw.
///
/// The bus and the extra sink handle are dropped inside, so the drain-then-
/// terminate exit happens without a sleep or a timeout.
async fn drain_and_collect(
    worker: Worker,
    wiring: Wiring,
    recording: &Arc<RecordingConsumer>,
) -> Vec<TaskEvent> {
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(recording)]);
    drop(wiring.bus);
    drop(wiring.sink);

    worker
        .run()
        .await
        .expect("clean drain-then-terminate shutdown");
    broadcaster.run().await;
    recording.events()
}

fn timed_out_payload(event: &TaskEvent) -> DateTime<Utc> {
    match &event.payload {
        TaskEventPayload::TimedOut { deadline } => *deadline,
        other => panic!("expected a TimedOut payload, got {other:?}"),
    }
}

fn cancelled_payload(event: &TaskEvent) -> String {
    match &event.payload {
        TaskEventPayload::Cancelled { reason } => reason.clone(),
        other => panic!("expected a Cancelled payload, got {other:?}"),
    }
}

/// The `(attempt, delivery_id)` of every `Dispatched` event, in `seq` order.
fn dispatched_attempts(events: &[TaskEvent]) -> Vec<(u32, DeliveryId)> {
    events
        .iter()
        .filter(|event| event.status == TaskStatus::Dispatched)
        .map(|event| {
            let (delivery_id, attempt) = dispatched_payload(event);
            (attempt, delivery_id)
        })
        .collect()
}

/// Wait until the worker is parked on a fired-by-hand timer.
async fn yield_until_armed(timer: &ManualTimer) {
    yield_until(|| timer.waiting() > 0).await;
}

/// A task with a deadline, relative to the injected clock's base instant.
fn task_with_deadline(deadline: Option<DateTime<Utc>>) -> AgentTask {
    AgentTask {
        deadline,
        ..root_task(worker_endpoint(), 5)
    }
}

// -- Timeout -----------------------------------------------------------------

#[tokio::test]
async fn t6_an_expired_deadline_times_out_before_any_dispatch() {
    // D2: an already-expired task is never delivered. The checkpoint alone
    // enforces this — no timer is configured in this assembly.
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("unreachable"));
    let deadline = fixed_ts() - chrono::Duration::seconds(1);
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig::default(),
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(task_with_deadline(Some(deadline)))
        .await
        .expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(
        seqs(&events),
        [1, 2],
        "nothing was dispatched, so no seq = 3"
    );
    assert_eq!(
        statuses(&events),
        [TaskStatus::Queued, TaskStatus::TimedOut]
    );
    assert_eq!(
        timed_out_payload(&events[1]),
        deadline,
        "the payload carries the deadline that was exceeded"
    );
    assert_eq!(
        dispatcher.stages(),
        Vec::<&'static str>::new(),
        "an expired task must never reach the dispatcher"
    );
}

#[tokio::test]
async fn t6_a_deadline_that_passes_while_delivering_times_out_at_seq_three() {
    // The in-flight `deliver` is abandoned by the injected timer: this is the
    // preemption the checkpoints alone cannot provide (B1).
    let dispatcher = Arc::new(ScriptedAttempts::new(
        vec![DeliverStep::Hang],
        vec![ExecuteStep::Complete("unreachable")],
    ));
    let timer = ManualTimer::new();
    let deadline = fixed_ts() + chrono::Duration::seconds(30);
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            timer: Some(timer.clone()),
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(task_with_deadline(Some(deadline)))
        .await
        .expect("submission");

    let worker_handle = worker.spawn();
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let broadcaster_handle = broadcaster.spawn();

    yield_until(|| dispatcher.stages().len() == 1).await;
    yield_until(|| recording.events().len() >= 2).await;
    yield_until_armed(&timer).await;
    assert_eq!(
        seqs(&recording.events()),
        [1, 2],
        "the task is still in flight: no terminal event yet"
    );

    timer.fire();

    drop(wiring.bus);
    drop(wiring.sink);
    worker_handle
        .await
        .expect("worker task must not panic")
        .expect("clean shutdown");
    broadcaster_handle.await.expect("broadcaster task");

    let events = recording.events();
    assert_eq!(seqs(&events), [1, 2, 3], "no Running and no gap");
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::TimedOut
        ]
    );
    assert_eq!(timed_out_payload(&events[2]), deadline);
    assert_eq!(
        dispatcher.stages(),
        ["deliver"],
        "an abandoned delivery must never reach execute"
    );
    assert_eq!(timer.fires(), 1);
    assert_eq!(
        timer.waiting(),
        0,
        "the abandoned wait was dropped with the race, not leaked"
    );
}

#[tokio::test]
async fn t6_a_deadline_that_passes_while_executing_times_out_at_seq_four() {
    // The same preemption after acceptance (C1): `Running` is already recorded.
    let dispatcher = Arc::new(ScriptedAttempts::new(
        vec![DeliverStep::Accept],
        vec![ExecuteStep::Hang],
    ));
    let timer = ManualTimer::new();
    let deadline = fixed_ts() + chrono::Duration::seconds(30);
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            timer: Some(timer.clone()),
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(task_with_deadline(Some(deadline)))
        .await
        .expect("submission");

    let worker_handle = worker.spawn();
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let broadcaster_handle = broadcaster.spawn();

    yield_until(|| dispatcher.stages().len() == 2).await;
    yield_until(|| recording.events().len() >= 3).await;
    yield_until_armed(&timer).await;
    assert_eq!(seqs(&recording.events()), [1, 2, 3]);

    timer.fire();

    drop(wiring.bus);
    drop(wiring.sink);
    worker_handle
        .await
        .expect("worker task must not panic")
        .expect("clean shutdown");
    broadcaster_handle.await.expect("broadcaster task");

    let events = recording.events();
    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::TimedOut
        ]
    );
    assert_eq!(timed_out_payload(&events[3]), deadline);
    assert_eq!(dispatcher.stages(), ["deliver", "execute"]);
}

#[tokio::test]
async fn t6_without_a_timer_the_deadline_is_still_enforced_at_the_checkpoints() {
    // The documented degradation: with no `Timer` the race is disabled, but
    // CP-2 still notices that the deadline passed while `deliver` ran (B1).
    let (clock, offset) = stepping_clock(fixed_ts());
    let dispatcher = Arc::new(DeadlineCrossingDispatcher::accepting(
        Arc::clone(&offset),
        60,
    ));
    let deadline = fixed_ts() + chrono::Duration::seconds(30);
    let mut wiring = wiring_with_clock(&load(CONFIG), 4, 16, clock.clone());
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        clock,
        WorkerConfig::default(),
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(task_with_deadline(Some(deadline)))
        .await
        .expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(
        seqs(&events),
        [1, 2, 3],
        "the delivery was accepted, then the deadline was noticed at CP-2"
    );
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::TimedOut
        ]
    );
    assert_eq!(timed_out_payload(&events[2]), deadline);
    assert_eq!(
        dispatcher.executes(),
        0,
        "a task that is already past its deadline must not be executed"
    );
    assert_eq!(dispatcher.delivers(), 1);
}

#[tokio::test]
async fn t6_a_deadline_that_passes_during_a_refusal_suppresses_the_retry() {
    // CP-3: once the deadline has passed, another attempt would only race
    // another timeout, so a refusable failure terminates the task instead of
    // being retried — even though the policy allows three attempts.
    let (clock, offset) = stepping_clock(fixed_ts());
    let dispatcher = Arc::new(DeadlineCrossingDispatcher::refusing(
        Arc::clone(&offset),
        60,
    ));
    let deadline = fixed_ts() + chrono::Duration::seconds(30);
    let mut wiring = wiring_with_clock(&load(CONFIG), 4, 32, clock.clone());
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        clock,
        WorkerConfig {
            retry: RetryPolicy {
                max_attempts: 3,
                backoff: Backoff::None,
            },
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(task_with_deadline(Some(deadline)))
        .await
        .expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2, 3]);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Failed
        ]
    );
    assert_eq!(
        failed_payload(&events[2]),
        "delivery was not accepted: crossed the deadline"
    );
    assert_eq!(
        dispatcher.delivers(),
        1,
        "a refused attempt past the deadline must not be retried"
    );
}

// -- Cancellation ------------------------------------------------------------

#[tokio::test]
async fn t6_a_global_cancellation_stops_a_task_before_any_dispatch() {
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("unreachable"));
    let cancellation = Cancellation::new();
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            cancellation: Some(cancellation.clone()),
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    cancellation.cancel_all("bridge is shutting down");
    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2]);
    assert_eq!(
        statuses(&events),
        [TaskStatus::Queued, TaskStatus::Cancelled]
    );
    assert_eq!(
        cancelled_payload(&events[1]),
        "bridge is shutting down",
        "the operator's reason reaches the event unchanged"
    );
    assert_eq!(
        dispatcher.stages(),
        Vec::<&'static str>::new(),
        "a task cancelled before dispatch must never reach the dispatcher"
    );
}

#[tokio::test]
async fn t6_cancelling_while_delivering_lands_at_seq_three_without_running() {
    let dispatcher = Arc::new(ScriptedAttempts::new(
        vec![DeliverStep::Hang],
        vec![ExecuteStep::Complete("unreachable")],
    ));
    let cancellation = Cancellation::new();
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            cancellation: Some(cancellation.clone()),
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    let task = root_task(worker_endpoint(), 5);
    wiring.bus.submit(task.clone()).await.expect("submission");

    let worker_handle = worker.spawn();
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let broadcaster_handle = broadcaster.spawn();

    // The delivery is in flight (`Dispatched(2)` is already written), which is
    // what makes `seq = 3` the deterministic landing point.
    yield_until(|| dispatcher.stages().len() == 1).await;
    cancellation.cancel(task.task_id, "operator stop");

    drop(wiring.bus);
    drop(wiring.sink);
    worker_handle
        .await
        .expect("worker task must not panic")
        .expect("clean shutdown");
    broadcaster_handle.await.expect("broadcaster task");

    let events = for_task(&recording.events(), task.task_id);
    assert_eq!(seqs(&events), [1, 2, 3], "no Running and no gap");
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Cancelled
        ]
    );
    assert_eq!(cancelled_payload(&events[2]), "operator stop");
    assert_eq!(
        dispatcher.stages(),
        ["deliver"],
        "the abandoned delivery must never reach execute"
    );
    assert_eq!(
        cancellation.tracked(),
        0,
        "the terminal transition retires the record"
    );
}

#[tokio::test]
async fn t6_cancelling_while_executing_lands_at_seq_four_and_is_terminal() {
    let dispatcher = Arc::new(ScriptedAttempts::new(
        vec![DeliverStep::Accept],
        vec![ExecuteStep::Hang],
    ));
    let cancellation = Cancellation::new();
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            cancellation: Some(cancellation.clone()),
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    let task = root_task(worker_endpoint(), 5);
    wiring.bus.submit(task.clone()).await.expect("submission");

    let worker_handle = worker.spawn();
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let broadcaster_handle = broadcaster.spawn();

    yield_until(|| dispatcher.stages().len() == 2).await;
    cancellation.cancel(task.task_id, "operator stop");
    // A second, different reason is recorded nowhere: the first one wins and the
    // task has already reached its terminal state.
    cancellation.cancel(task.task_id, "a later reason");

    drop(wiring.bus);
    drop(wiring.sink);
    worker_handle
        .await
        .expect("worker task must not panic")
        .expect("clean shutdown");
    broadcaster_handle.await.expect("broadcaster task");

    let events = for_task(&recording.events(), task.task_id);
    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Cancelled
        ]
    );
    assert_eq!(
        cancelled_payload(&events[3]),
        "operator stop",
        "a repeated cancel must not rewrite the reason or add an event"
    );
    assert_eq!(dispatcher.stages(), ["deliver", "execute"]);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.status == TaskStatus::Cancelled)
            .count(),
        1,
        "exactly one terminal event"
    );
    assert_eq!(
        cancellation.tracked(),
        0,
        "the terminal transition retires the record, so a repeated cancel \
         leaves nothing behind"
    );
}

/// A cancellation recorded *before* the worker ever sees the task still stops it:
/// the reason waits for the task, and taking ownership moves it along.
#[tokio::test]
async fn t6_a_targeted_cancellation_recorded_before_dispatch_stops_the_task() {
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("unreachable"));
    let cancellation = Cancellation::new();
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            cancellation: Some(cancellation.clone()),
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    let task = root_task(worker_endpoint(), 5);
    wiring.bus.submit(task.clone()).await.expect("submission");
    // The worker has not started, so the task is still queued and nothing owns
    // its record yet.
    cancellation.cancel(task.task_id, "operator stop");
    assert_eq!(cancellation.tracked(), 1);

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2], "no Dispatched: the gate hit first");
    assert_eq!(
        statuses(&events),
        [TaskStatus::Queued, TaskStatus::Cancelled]
    );
    assert_eq!(cancelled_payload(&events[1]), "operator stop");
    assert_eq!(
        dispatcher.stages(),
        Vec::<&'static str>::new(),
        "a task cancelled before dispatch must never reach the dispatcher"
    );
    assert_eq!(
        cancellation.tracked(),
        0,
        "the record is retired once the task is terminal"
    );
}

/// Every task's record is bound to its lifetime, even when nothing cancels it:
/// a task that runs to completion retains no cancellation state.
#[tokio::test]
async fn t6_a_task_that_completes_retains_no_cancellation_record() {
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("done"));
    let cancellation = Cancellation::new();
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            cancellation: Some(cancellation.clone()),
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    let task = root_task(worker_endpoint(), 5);
    wiring.bus.submit(task.clone()).await.expect("submission");
    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed
        ]
    );
    assert_eq!(
        cancellation.tracked(),
        0,
        "a completed task is not tracked: nothing was cancelled and the owned \
         record was retired"
    );
    assert_eq!(cancellation.reason_for(task.task_id), None);
}

/// An unbounded stream of distinct ids cannot grow the retained state: the
/// pre-arm table is capped, and a real task is unaffected by the pressure.
#[tokio::test]
async fn t6_distinct_unknown_cancellations_remain_bounded() {
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("done"));
    let cancellation = Cancellation::new();
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            cancellation: Some(cancellation.clone()),
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    for _ in 0..MAX_PENDING_CANCELLATIONS + 64 {
        cancellation.cancel(TaskId::generate(), "unknown task");
    }
    assert!(
        cancellation.tracked() <= MAX_PENDING_CANCELLATIONS,
        "got {} tracked ids",
        cancellation.tracked()
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");
    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed
        ],
        "pressure from unknown ids must not affect a real task"
    );
    assert!(
        cancellation.tracked() <= MAX_PENDING_CANCELLATIONS,
        "the bound still holds after the task ran"
    );
}

#[tokio::test]
async fn t6_cancelling_a_task_that_already_finished_writes_nothing_and_retains_nothing() {
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("done"));
    let cancellation = Cancellation::new();
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            cancellation: Some(cancellation.clone()),
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    let task = root_task(worker_endpoint(), 5);
    wiring.bus.submit(task.clone()).await.expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed
        ]
    );
    assert_eq!(
        cancellation.tracked(),
        0,
        "the finished task retained no record"
    );

    cancellation.cancel(task.task_id, "too late");
    assert_eq!(
        recording.events(),
        events,
        "cancelling a finished task must not produce a second terminal event"
    );
    assert_eq!(
        cancellation.tracked(),
        0,
        "and it must not retain the id either (r1 P1)"
    );
    assert_eq!(cancellation.reason_for(task.task_id), None);
}

// -- Retry -------------------------------------------------------------------

#[tokio::test]
async fn t6_a_refused_delivery_is_retried_with_a_new_identity_and_a_higher_attempt() {
    let dispatcher = Arc::new(ScriptedAttempts::new(
        vec![
            DeliverStep::Refuse("handshake lost"),
            DeliverStep::Refuse("handshake lost"),
            DeliverStep::Accept,
        ],
        vec![ExecuteStep::Complete("done")],
    ));
    let mut wiring = wiring(&load(CONFIG), 4, 32);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            retry: RetryPolicy {
                max_attempts: 3,
                backoff: Backoff::None,
            },
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    // The failed attempts leave no event behind: the retry is visible as the
    // `Dispatched` that follows, and `seq` stays contiguous and monotonic.
    assert_eq!(seqs(&events), [1, 2, 3, 4, 5, 6]);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Dispatched,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed
        ]
    );

    let dispatched = dispatched_attempts(&events);
    assert_eq!(
        dispatched
            .iter()
            .map(|(attempt, _)| *attempt)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    let deliveries: HashSet<DeliveryId> = dispatched.iter().map(|(_, id)| *id).collect();
    assert_eq!(
        deliveries.len(),
        3,
        "every attempt is a new delivery record"
    );

    // Each `Dispatched` describes exactly the attempt the dispatcher was given.
    let deliver_calls: Vec<SeenCall> = dispatcher
        .calls()
        .into_iter()
        .filter(|call| call.stage == "deliver")
        .collect();
    assert_eq!(deliver_calls.len(), 3);
    for (event_attempt, call) in dispatched.iter().zip(&deliver_calls) {
        assert_eq!(event_attempt.0, call.attempt);
        assert_eq!(event_attempt.1, call.delivery_id);
    }
    let execute = dispatcher
        .calls()
        .into_iter()
        .find(|call| call.stage == "execute")
        .expect("the accepted attempt was executed");
    assert_eq!(
        execute.delivery_id, dispatched[2].1,
        "both stages of one attempt share its identity"
    );
    assert_eq!(execute.attempt, 3);
    assert_eq!(
        failed_or_completed(&events),
        "done",
        "the retried delivery completed"
    );
}

#[tokio::test]
async fn t6_retries_stop_at_the_attempt_ceiling_with_the_last_reason() {
    let dispatcher = Arc::new(ScriptedAttempts::new(
        vec![
            DeliverStep::Refuse("first refusal"),
            DeliverStep::Refuse("second refusal"),
            DeliverStep::Refuse("third refusal"),
        ],
        vec![ExecuteStep::Complete("unreachable")],
    ));
    let mut wiring = wiring(&load(CONFIG), 4, 32);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            retry: RetryPolicy {
                max_attempts: 3,
                backoff: Backoff::None,
            },
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2, 3, 4, 5]);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Dispatched,
            TaskStatus::Dispatched,
            TaskStatus::Failed
        ]
    );
    assert_eq!(
        dispatched_attempts(&events)
            .iter()
            .map(|(attempt, _)| *attempt)
            .collect::<Vec<_>>(),
        [1, 2, 3],
        "exactly one delivery per allowed attempt"
    );
    assert_eq!(dispatcher.attempts("deliver"), [1, 2, 3]);
    assert_eq!(
        dispatcher.attempts("execute"),
        Vec::<u32>::new(),
        "a delivery that was never accepted must never be executed"
    );
    assert_eq!(
        failed_payload(&events[4]),
        "delivery was not accepted: third refusal",
        "the terminal failure reports the last attempt's reason"
    );
}

#[tokio::test]
async fn t6_a_deterministic_terminal_failure_is_never_retried() {
    // `Ok(Failed)` is the adapter's verdict, not a transient transport failure:
    // even with attempts to spare, it terminates the task immediately.
    let dispatcher = Arc::new(ScriptedAttempts::new(
        vec![DeliverStep::Accept],
        vec![ExecuteStep::Report("the agent refused the work")],
    ));
    let mut wiring = wiring(&load(CONFIG), 4, 32);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            retry: RetryPolicy {
                max_attempts: 5,
                backoff: Backoff::None,
            },
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    assert_eq!(events[3].status, TaskStatus::Failed);
    assert_eq!(failed_payload(&events[3]), "the agent refused the work");
    assert_eq!(dispatcher.stages(), ["deliver", "execute"]);
}

#[tokio::test]
async fn t6_an_execution_failure_is_retried_across_a_recorded_running_state() {
    // D5: `ExecutionFailed` is transient, so the retry may produce
    // `Running -> Dispatched`. Each `Dispatched` carries a new identity and a
    // higher attempt, which is what makes that sequence unambiguous (T012 must
    // group by `task_id` + `attempt`).
    let dispatcher = Arc::new(ScriptedAttempts::new(
        vec![DeliverStep::Accept],
        vec![
            ExecuteStep::Fail("adapter crashed"),
            ExecuteStep::Complete("second time lucky"),
        ],
    ));
    let mut wiring = wiring(&load(CONFIG), 4, 32);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            retry: RetryPolicy {
                max_attempts: 2,
                backoff: Backoff::None,
            },
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2, 3, 4, 5, 6]);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed
        ]
    );
    assert_eq!(
        dispatched_attempts(&events)
            .iter()
            .map(|(attempt, _)| *attempt)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(dispatcher.attempts("deliver"), [1, 2]);
    assert_eq!(dispatcher.attempts("execute"), [1, 2]);
}

#[tokio::test]
async fn t6_a_retry_waits_for_the_configured_backoff_before_dispatching_again() {
    let dispatcher = Arc::new(ScriptedAttempts::new(
        vec![DeliverStep::Refuse("transient"), DeliverStep::Accept],
        vec![ExecuteStep::Complete("done")],
    ));
    let timer = ManualTimer::new();
    let mut wiring = wiring(&load(CONFIG), 4, 32);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            retry: RetryPolicy {
                max_attempts: 2,
                backoff: Backoff::Fixed(Duration::from_secs(30)),
            },
            timer: Some(timer.clone()),
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");

    let worker_handle = worker.spawn();
    let broadcaster = EventBroadcaster::new(wiring.events_rx, vec![as_consumer(&recording)]);
    let broadcaster_handle = broadcaster.spawn();

    yield_until(|| dispatcher.stages().len() == 1).await;
    yield_until(|| recording.events().len() >= 2).await;
    // The worker is parked on the backoff wait: the next `Dispatched` cannot be
    // written until the test fires the timer.
    yield_until_armed(&timer).await;
    assert_eq!(
        seqs(&recording.events()),
        [1, 2],
        "no second Dispatched before the backoff elapses"
    );
    assert_eq!(timer.fires(), 0);

    timer.fire();

    drop(wiring.bus);
    drop(wiring.sink);
    worker_handle
        .await
        .expect("worker task must not panic")
        .expect("clean shutdown");
    broadcaster_handle.await.expect("broadcaster task");

    let events = recording.events();
    assert_eq!(seqs(&events), [1, 2, 3, 4, 5]);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed
        ]
    );
    assert_eq!(dispatcher.attempts("deliver"), [1, 2]);
}

// -- Cycle limits ------------------------------------------------------------

#[tokio::test]
async fn t6_a_depth_over_the_limit_blocks_delivery_at_seq_two() {
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("unreachable"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            limits: LoopLimits {
                max_depth: 2,
                max_hops: 3,
            },
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    let task = AgentTask {
        depth: 3,
        text: "SECRET-BODY".into(),
        ..root_task(worker_endpoint(), 5)
    };
    wiring.bus.submit(task.clone()).await.expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2], "a blocked task is never dispatched");
    assert_eq!(events[1].status, TaskStatus::Failed);
    let error = failed_payload(&events[1]);
    assert!(error.starts_with("cycle limit exceeded"), "got: {error}");
    assert!(error.contains("hit: depth"), "got: {error}");
    assert!(error.contains("depth=3 (max 2)"), "got: {error}");
    assert!(error.contains(&task.task_id.to_string()), "got: {error}");
    assert!(
        error.contains(&task.root_task_id.to_string()),
        "got: {error}"
    );
    assert!(
        !error.contains("SECRET-BODY"),
        "the failure text must never carry the task body: {error}"
    );
    for leaked in ["command", "args", "token", "password", "http"] {
        assert!(!error.contains(leaked), "got: {error}");
    }
    assert_eq!(
        dispatcher.stages(),
        Vec::<&'static str>::new(),
        "a cycle hit blocks the delivery entirely"
    );
}

#[tokio::test]
async fn t6_a_hop_count_over_the_limit_blocks_delivery_at_seq_two() {
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("unreachable"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            limits: LoopLimits {
                max_depth: 2,
                max_hops: 3,
            },
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    let task = AgentTask {
        hops: 4,
        ..root_task(worker_endpoint(), 5)
    };
    wiring.bus.submit(task).await.expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2]);
    let error = failed_payload(&events[1]);
    assert!(error.contains("hit: hops"), "got: {error}");
    assert!(error.contains("hops=4 (max 3)"), "got: {error}");
    assert_eq!(dispatcher.stages(), Vec::<&'static str>::new());
}

#[tokio::test]
async fn t6_the_default_constructor_enforces_the_configured_limits() {
    // `Worker::new`'s default limits must be the `Config.bridge` defaults
    // (depth 8, hops 16), so the frozen constructor is not a hole in the check.
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("unreachable"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = Worker::with_dispatcher(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        TransportType::Acp,
        as_dispatcher(&dispatcher),
    );

    let task = AgentTask {
        depth: 9,
        hops: 17,
        ..root_task(worker_endpoint(), 5)
    };
    wiring.bus.submit(task).await.expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2]);
    let error = failed_payload(&events[1]);
    assert!(error.contains("hit: depth, hops"), "got: {error}");
    assert!(error.contains("depth=9 (max 8)"), "got: {error}");
    assert!(error.contains("hops=17 (max 16)"), "got: {error}");
    assert_eq!(dispatcher.stages(), Vec::<&'static str>::new());
}

#[tokio::test]
async fn t6_a_task_exactly_at_the_limit_is_dispatched_normally() {
    // The comparison is `>`, not `>=`: the ceiling itself is allowed, so a root
    // task passes at `max_depth = 0` and this task passes at both ceilings.
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("done"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            limits: LoopLimits {
                max_depth: 2,
                max_hops: 3,
            },
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    let task = AgentTask {
        depth: 2,
        hops: 3,
        ..root_task(worker_endpoint(), 5)
    };
    wiring.bus.submit(task).await.expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    assert_eq!(events[3].status, TaskStatus::Completed);
    assert_eq!(dispatcher.stages(), ["deliver", "execute"]);
}

#[tokio::test]
async fn t6_a_zero_ceiling_still_admits_a_root_task() {
    let dispatcher = Arc::new(ScriptedAttempts::succeeding("done"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = controlled_worker(
        &wiring,
        tasks_rx,
        fixed_clock(),
        WorkerConfig {
            limits: LoopLimits {
                max_depth: 0,
                max_hops: 0,
            },
            ..WorkerConfig::default()
        },
        as_dispatcher(&dispatcher),
    );

    wiring
        .bus
        .submit(root_task(worker_endpoint(), 5))
        .await
        .expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;

    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    assert_eq!(events[3].status, TaskStatus::Completed);
}

/// The `Completed` output or the `Failed` error of the last event.
fn failed_or_completed(events: &[TaskEvent]) -> String {
    match &events.last().expect("at least one event").payload {
        TaskEventPayload::Completed { output } => output.clone(),
        TaskEventPayload::Failed { error } => error.clone(),
        other => panic!("expected a terminal payload, got {other:?}"),
    }
}
