//! End-to-end tests for the T007 mock agent.
//!
//! Every test drives the real path — TOML → validated `Config` (T003) →
//! `EndpointRegistry` → `MpscEventSink`/`MemoryBus::with_event_sink` → `submit` →
//! `Worker` → `MockAgent` → `EventBroadcaster` → recording consumer — and asserts
//! on real `TaskEvent` values plus the mock's own observation table. Nothing is
//! stubbed between the bus and the mock, and no assertion is made on a string that
//! the production path does not produce.
//!
//! Determinism rules (same as `tests/bus_worker.rs`):
//!
//! - `Clock::fixed` pins every timestamp; no wall-clock dependency anywhere.
//! - No `sleep` and no timeout: the mock's futures are immediately ready, tasks
//!   are submitted *before* the worker runs, and the run ends through the
//!   drain-then-terminate path (`drop(bus)` + `drop(sink)` + `await` the worker).
//! - Events are compared per `task_id` in `seq` order, never in arrival order.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use guigu_agent_bridge::agents::{DEFAULT_OUTPUT, MockAgent, MockReply, MockStage, ReplySource};
use guigu_agent_bridge::bus::{
    Backoff, Bus, BusFuture, Cancellation, Clock, ConsumerError, DispatchError, DispatcherRegistry,
    EndpointRegistry, EventBroadcaster, EventConsumer, EventSink, MemoryBus, MpscEventSink,
    RetryPolicy, TaskDispatcher, Worker, WorkerConfig, derive_endpoint_id,
};
use guigu_agent_bridge::config::{Config, load_from_str_with_env};
use guigu_agent_bridge::models::{
    AgentTask, ConversationId, DeliveryId, EndpointId, Priority, TaskEvent, TaskEventPayload,
    TaskId, TaskStatus, TransportType,
};

const TS: &str = "2026-09-15T12:00:00Z";

/// `worker` is the addressable `acp` target the mock impersonates.
const CONFIG: &str = r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
args = ["--stdio"]
workspace = "/tmp"
enabled = true
"#;

fn load(toml: &str) -> Config {
    let mut env = BTreeMap::new();
    env.insert("HOME".to_string(), "/home/tester".to_string());
    load_from_str_with_env(toml, &env).expect("test config must be valid")
}

fn fixed_clock() -> Clock {
    Clock::fixed(TS.parse::<DateTime<Utc>>().expect("valid timestamp"))
}

fn mock_for(agent: MockAgent) -> (Arc<MockAgent>, Arc<dyn TaskDispatcher>) {
    let mock = Arc::new(agent);
    let dispatcher: Arc<dyn TaskDispatcher> = mock.clone();
    (mock, dispatcher)
}

/// Move the task receiver out of `wiring`, leaving a closed placeholder, so the
/// rest of `wiring` can still be passed by value to the drain helper.
///
/// (`BusReceivers` hands the receiver out exactly once; the placeholder is never
/// read.)
fn take_tasks_rx(slot: &mut mpsc::Receiver<AgentTask>) -> mpsc::Receiver<AgentTask> {
    let (tx, fresh) = mpsc::channel(1);
    drop(tx);
    std::mem::replace(slot, fresh)
}

fn worker_endpoint() -> EndpointId {
    derive_endpoint_id("worker")
}

fn root_task(text: &str) -> AgentTask {
    let task_id = TaskId::generate();
    AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: None,
        from_agent: EndpointId::generate(),
        to_agent: worker_endpoint(),
        conversation_id: ConversationId::generate(),
        reply_to: None,
        text: text.into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    }
}

// ---------------------------------------------------------------------------
// Assembly (the public path only)
// ---------------------------------------------------------------------------

/// An [`EventConsumer`] that records every event it is handed.
#[derive(Default)]
struct RecordingConsumer {
    events: Mutex<Vec<TaskEvent>>,
}

impl RecordingConsumer {
    fn events(&self) -> Vec<TaskEvent> {
        self.events.lock().expect("not poisoned").clone()
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

fn as_consumer<C: EventConsumer + 'static>(consumer: &Arc<C>) -> Arc<dyn EventConsumer> {
    let concrete: Arc<C> = Arc::clone(consumer);
    concrete
}

/// The bus half of an assembly: one sink shared by the bus and the worker, so
/// both write into one event stream.
struct Wiring {
    bus: MemoryBus,
    /// Held only to drop it: the event channel closes once the bus *and* the
    /// worker are gone.
    sink: Arc<dyn EventSink>,
    tasks_rx: mpsc::Receiver<AgentTask>,
    events_rx: mpsc::Receiver<TaskEvent>,
    registry: Arc<EndpointRegistry>,
}

fn wiring(config: &Config, task_capacity: usize, event_capacity: usize) -> Wiring {
    let registry = Arc::new(EndpointRegistry::from_config(config));
    let (mpsc_sink, events_rx) = MpscEventSink::new(event_capacity);
    let sink: Arc<dyn EventSink> = Arc::new(mpsc_sink);
    let (bus, tasks_rx) = MemoryBus::with_event_sink(
        Arc::clone(&registry),
        task_capacity,
        fixed_clock(),
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

/// Build a worker over `wiring` with the given dispatcher table and controls.
fn worker_with(
    wiring: &Wiring,
    tasks_rx: mpsc::Receiver<AgentTask>,
    dispatchers: DispatcherRegistry,
    config: WorkerConfig,
) -> Worker {
    Worker::builder(
        Arc::clone(&wiring.registry),
        tasks_rx,
        Arc::clone(&wiring.sink),
        fixed_clock(),
        dispatchers,
    )
    .config(config)
    .build()
}

/// Run one worker to a clean shutdown and return what the recording consumer saw.
///
/// The bus and the extra sink handle are dropped here, so the drain-then-
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

// ---------------------------------------------------------------------------
// Event assertions
// ---------------------------------------------------------------------------

fn for_task(events: &[TaskEvent], task_id: TaskId) -> Vec<TaskEvent> {
    events
        .iter()
        .filter(|event| event.task_id == task_id)
        .cloned()
        .collect()
}

fn seqs(events: &[TaskEvent]) -> Vec<u64> {
    events.iter().map(|event| event.seq).collect()
}

fn statuses(events: &[TaskEvent]) -> Vec<TaskStatus> {
    events.iter().map(|event| event.status).collect()
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

fn completed_payload(event: &TaskEvent) -> String {
    match &event.payload {
        TaskEventPayload::Completed { output } => output.clone(),
        other => panic!("expected a Completed payload, got {other:?}"),
    }
}

fn failed_payload(event: &TaskEvent) -> String {
    match &event.payload {
        TaskEventPayload::Failed { error } => error.clone(),
        other => panic!("expected a Failed payload, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// M1: the happy path — Queued(1) → Dispatched(2) → Running(3) → Completed(4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_mocked_task_completes_through_the_real_path() {
    let (mock, dispatcher) = mock_for(MockAgent::completing("mock output"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = worker_with(
        &wiring,
        tasks_rx,
        DispatcherRegistry::new().with(TransportType::Acp, dispatcher),
        WorkerConfig::default(),
    );

    let task = root_task("do the thing");
    wiring.bus.submit(task.clone()).await.expect("submission");

    let events = drain_and_collect(worker, wiring, &recording).await;
    let events = for_task(&events, task.task_id);

    assert_eq!(seqs(&events), [1, 2, 3, 4], "contiguous per-task sequence");
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed
        ]
    );
    assert_eq!(completed_payload(&events[3]), "mock output");

    let calls = mock.calls();
    assert_eq!(calls.len(), 2, "one deliver and one execute");
    assert_eq!(calls[0].task_id, task.task_id);
    assert_eq!(calls[0].text, "do the thing");
    assert_eq!(calls[0].attempt, 1);
    assert_eq!(
        calls[1].delivery_id, calls[0].delivery_id,
        "both stages of one attempt must share the delivery identity"
    );
    assert_eq!(calls[1].attempt, calls[0].attempt);
    assert_eq!(
        dispatched_payload(&events[1]),
        (calls[0].delivery_id, 1),
        "the recorded delivery identity is the one the worker wrote"
    );
}

#[tokio::test]
async fn an_unscripted_mock_uses_the_documented_default() {
    let (mock, dispatcher) = mock_for(MockAgent::new());
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = worker_with(
        &wiring,
        tasks_rx,
        DispatcherRegistry::new().with(TransportType::Acp, dispatcher),
        WorkerConfig::default(),
    );

    let task = root_task("anything");
    wiring.bus.submit(task.clone()).await.expect("submission");
    let events = drain_and_collect(worker, wiring, &recording).await;
    let events = for_task(&events, task.task_id);

    assert_eq!(completed_payload(&events[3]), DEFAULT_OUTPUT);
    assert!(
        mock.calls()
            .iter()
            .all(|call| call.source == ReplySource::Default),
        "an unscripted call must be recorded as default-sourced, never silent"
    );
}

// ---------------------------------------------------------------------------
// M2–M4: refusals and terminal failures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_refused_delivery_fails_at_seq_three_and_never_runs() {
    let (mock, dispatcher) = mock_for(MockAgent::refusing("target is busy"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = worker_with(
        &wiring,
        tasks_rx,
        DispatcherRegistry::new().with(TransportType::Acp, dispatcher),
        WorkerConfig::default(),
    );

    let task = root_task("do the thing");
    wiring.bus.submit(task.clone()).await.expect("submission");
    let events = drain_and_collect(worker, wiring, &recording).await;
    let events = for_task(&events, task.task_id);

    assert_eq!(
        seqs(&events),
        [1, 2, 3],
        "no Running: the refusal is seq = 3"
    );
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
        DispatchError::NotAccepted {
            reason: "target is busy".into()
        }
        .to_string(),
        "the mock's reason reaches the event through the frozen Display"
    );
    assert_eq!(
        mock.execute_calls().len(),
        0,
        "execute must never be called after a refused delivery"
    );
    assert_eq!(mock.deliver_calls().len(), 1);
}

#[tokio::test]
async fn a_reported_terminal_failure_is_recorded_verbatim() {
    let (mock, dispatcher) = mock_for(MockAgent::reporting_failure("adapter rejected the work"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = worker_with(
        &wiring,
        tasks_rx,
        DispatcherRegistry::new().with(TransportType::Acp, dispatcher),
        WorkerConfig::default(),
    );

    let task = root_task("do the thing");
    wiring.bus.submit(task.clone()).await.expect("submission");
    let events = drain_and_collect(worker, wiring, &recording).await;
    let events = for_task(&events, task.task_id);

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
        "adapter rejected the work",
        "a DispatchOutcome::Failed error is recorded exactly as given"
    );
    assert_eq!(mock.call_count(), 2);
}

#[tokio::test]
async fn an_execution_failure_is_recorded_through_the_frozen_display() {
    let (mock, dispatcher) = mock_for(MockAgent::failing_execution("connection dropped"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = worker_with(
        &wiring,
        tasks_rx,
        DispatcherRegistry::new().with(TransportType::Acp, dispatcher),
        WorkerConfig::default(),
    );

    let task = root_task("do the thing");
    wiring.bus.submit(task.clone()).await.expect("submission");
    let events = drain_and_collect(worker, wiring, &recording).await;
    let events = for_task(&events, task.task_id);

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
        DispatchError::ExecutionFailed {
            reason: "connection dropped".into()
        }
        .to_string()
    );
    assert_eq!(
        mock.call_count(),
        2,
        "one accepted delivery and one failed execution"
    );
}

// ---------------------------------------------------------------------------
// M5: attempt scripting drives the worker's T006 retry loop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_attempt_script_drives_a_retry_to_completion() {
    let (mock, dispatcher) = mock_for(
        MockAgent::completing("second attempt won")
            .on_attempt(1, MockReply::refused("handshake lost")),
    );
    let mut wiring = wiring(&load(CONFIG), 4, 32);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = worker_with(
        &wiring,
        tasks_rx,
        DispatcherRegistry::new().with(TransportType::Acp, dispatcher),
        WorkerConfig {
            retry: RetryPolicy {
                max_attempts: 2,
                backoff: Backoff::None,
            },
            ..WorkerConfig::default()
        },
    );

    let task = root_task("do the thing");
    wiring.bus.submit(task.clone()).await.expect("submission");
    let events = drain_and_collect(worker, wiring, &recording).await;
    let events = for_task(&events, task.task_id);

    assert_eq!(
        seqs(&events),
        [1, 2, 3, 4, 5],
        "a refused attempt writes no event of its own"
    );
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
    let (first_delivery, first_attempt) = dispatched_payload(&events[1]);
    let (second_delivery, second_attempt) = dispatched_payload(&events[2]);
    assert_eq!([first_attempt, second_attempt], [1, 2]);
    assert_ne!(
        first_delivery, second_delivery,
        "a retry is a new delivery, not a rewrite"
    );
    assert_eq!(completed_payload(&events[4]), "second attempt won");

    let calls = mock.calls();
    assert_eq!(calls.len(), 3, "deliver(1), deliver(2), execute(2)");
    assert_eq!(
        calls
            .iter()
            .map(|call| (call.attempt, call.stage))
            .collect::<Vec<_>>(),
        [
            (1, MockStage::Deliver),
            (2, MockStage::Deliver),
            (2, MockStage::Execute),
        ]
    );
    assert_eq!(
        calls[1].delivery_id, second_delivery,
        "the second attempt's recorded identity is the one the worker wrote"
    );
}

// ---------------------------------------------------------------------------
// M6: routing by content, and observation order
// ---------------------------------------------------------------------------

#[tokio::test]
async fn text_rules_route_two_tasks_to_different_outcomes() {
    let (mock, dispatcher) = mock_for(
        MockAgent::completing("default output")
            .on_text("alpha", MockReply::completed("alpha done"))
            .on_text("beta", MockReply::reporting_failure("beta failed")),
    );
    let mut wiring = wiring(&load(CONFIG), 4, 32);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = worker_with(
        &wiring,
        tasks_rx,
        DispatcherRegistry::new().with(TransportType::Acp, dispatcher),
        WorkerConfig::default(),
    );

    let alpha = root_task("alpha");
    let beta = root_task("beta");
    wiring.bus.submit(alpha.clone()).await.expect("alpha");
    wiring.bus.submit(beta.clone()).await.expect("beta");

    let events = drain_and_collect(worker, wiring, &recording).await;

    let alpha_events = for_task(&events, alpha.task_id);
    let beta_events = for_task(&events, beta.task_id);
    assert_eq!(completed_payload(&alpha_events[3]), "alpha done");
    assert_eq!(failed_payload(&beta_events[3]), "beta failed");
    assert_eq!(statuses(&beta_events)[3], TaskStatus::Failed);

    assert_eq!(
        mock.calls()
            .iter()
            .map(|call| call.text.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "alpha", "beta", "beta"],
        "one worker consumes in submission order, so the observation order is fixed"
    );
}

// ---------------------------------------------------------------------------
// M7: the assembly key and its replace semantics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_later_registration_for_the_same_transport_replaces_the_earlier_one() {
    let (first, first_dispatcher) = mock_for(MockAgent::completing("first"));
    let (second, second_dispatcher) = mock_for(MockAgent::completing("second"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = worker_with(
        &wiring,
        tasks_rx,
        DispatcherRegistry::new()
            .with(TransportType::Acp, first_dispatcher)
            .with(TransportType::Acp, second_dispatcher),
        WorkerConfig::default(),
    );

    let task = root_task("do the thing");
    wiring.bus.submit(task.clone()).await.expect("submission");
    let events = drain_and_collect(worker, wiring, &recording).await;
    let events = for_task(&events, task.task_id);

    assert_eq!(
        completed_payload(&events[3]),
        "second",
        "DispatcherRegistry::with replaces per transport (frozen T005 semantics)"
    );
    assert_eq!(first.call_count(), 0, "the replaced mock is never called");
    assert_eq!(second.call_count(), 2);
}

#[tokio::test]
async fn a_missing_dispatcher_fails_the_task_without_reaching_any_mock() {
    let (mock, _dispatcher) = mock_for(MockAgent::completing("unreachable"));
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = worker_with(
        &wiring,
        tasks_rx,
        DispatcherRegistry::new(),
        WorkerConfig::default(),
    );

    let task = root_task("do the thing");
    wiring.bus.submit(task.clone()).await.expect("submission");
    let events = drain_and_collect(worker, wiring, &recording).await;
    let events = for_task(&events, task.task_id);

    assert_eq!(seqs(&events), [1, 2]);
    assert_eq!(statuses(&events)[1], TaskStatus::Failed);
    assert!(
        failed_payload(&events[1]).contains("no dispatcher"),
        "the failure names the missing dispatcher"
    );
    assert_eq!(
        mock.call_count(),
        0,
        "a mock that was not registered must not be a silent fallback"
    );
}

// ---------------------------------------------------------------------------
// M8: the mock does not interfere with the T006 gates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_cancelled_task_never_reaches_the_mock() {
    let (mock, dispatcher) = mock_for(MockAgent::completing("unreachable"));
    let cancellation = Cancellation::new();
    let mut wiring = wiring(&load(CONFIG), 4, 16);
    let tasks_rx = take_tasks_rx(&mut wiring.tasks_rx);
    let recording = Arc::new(RecordingConsumer::default());
    let worker = worker_with(
        &wiring,
        tasks_rx,
        DispatcherRegistry::new().with(TransportType::Acp, dispatcher),
        WorkerConfig {
            cancellation: Some(cancellation.clone()),
            ..WorkerConfig::default()
        },
    );

    cancellation.cancel_all("bridge is shutting down");
    let task = root_task("do the thing");
    wiring.bus.submit(task.clone()).await.expect("submission");
    let events = drain_and_collect(worker, wiring, &recording).await;
    let events = for_task(&events, task.task_id);

    assert_eq!(seqs(&events), [1, 2]);
    assert_eq!(
        statuses(&events),
        [TaskStatus::Queued, TaskStatus::Cancelled]
    );
    assert_eq!(
        mock.call_count(),
        0,
        "a task blocked at CP-0 must never be dispatched to the mock"
    );
}
