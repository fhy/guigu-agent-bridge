//! Integration tests for the T004 Agent Bus submission path.
//!
//! Every test drives the real path — TOML → validated `Config` (T003 pipeline) →
//! `EndpointRegistry` → `MemoryBus` → `submit` → channel consumption — and never
//! spawns a worker (T005 does not exist yet). Because both channels are written
//! with `try_send`, no test can block on a stalled consumer, so the suite is
//! deterministic and safe to repeat.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinSet;

use guigu_agent_bridge::bus::{
    Bus, BusError, BusFuture, BusReceivers, Clock, EndpointRegistry, EventSink, MemoryBus,
    MpscEventSink, derive_endpoint_id,
};
use guigu_agent_bridge::config::{Config, load_from_str_with_env};
use guigu_agent_bridge::models::{
    AgentTask, ConversationId, EndpointId, EventId, Priority, TaskEvent, TaskEventPayload, TaskId,
    TaskStatus,
};

/// A fixed instant, so event timestamps are asserted exactly rather than within a
/// fragile wall-clock window.
const TS: &str = "2026-09-15T12:00:00Z";

/// One enabled `acp` agent, one disabled `acp` agent, and the two transports whose
/// address cannot be derived from config yet.
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

[agents.disabled-matrix]
transport = "matrix"
enabled = false

[agents.http-svc]
transport = "http"
enabled = true
"#;

fn load(toml: &str) -> Config {
    let mut env = BTreeMap::new();
    env.insert("HOME".to_string(), "/home/tester".to_string());
    load_from_str_with_env(toml, &env).expect("test config must be valid")
}

fn test_config() -> Config {
    load(CONFIG)
}

/// A bus with a fixed clock, so timestamps are exact.
fn fixed_clock_bus(config: &Config, capacity: usize) -> (MemoryBus, BusReceivers) {
    let ts: DateTime<Utc> = TS.parse().expect("valid timestamp");
    let registry = Arc::new(EndpointRegistry::from_config(config));
    MemoryBus::with_clock(registry, capacity, Clock::fixed(ts))
}

fn fixed_ts() -> DateTime<Utc> {
    TS.parse().expect("valid timestamp")
}

/// A root task (no parent, `root_task_id == task_id`) targeting `to_agent`.
fn root_task(to_agent: EndpointId) -> AgentTask {
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
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    }
}

fn assert_nothing_queued(receivers: &mut BusReceivers) {
    assert_eq!(
        receivers.tasks.try_recv(),
        Err(TryRecvError::Empty),
        "a rejected submission must not enqueue a task"
    );
    assert_eq!(
        receivers.events.try_recv(),
        Err(TryRecvError::Empty),
        "a rejected submission must not write an event"
    );
}

#[tokio::test]
async fn valid_submission_enqueues_the_task_and_emits_queued_seq_one() {
    let config = test_config();
    let worker = derive_endpoint_id("worker");
    let (bus, mut receivers) = fixed_clock_bus(&config, 4);

    let task = root_task(worker);
    bus.submit(task.clone()).await.expect("valid submission");

    // The whole task is enqueued, unchanged (priority included).
    let queued = receivers.tasks.recv().await.expect("task must be queued");
    assert_eq!(queued, task);
    assert_eq!(queued.to_agent, worker);
    assert_eq!(queued.priority, Priority::DEFAULT);

    // Exactly one immutable Queued event at seq = 1.
    let event = receivers
        .events
        .recv()
        .await
        .expect("event must be emitted");
    assert_eq!(event.task_id, task.task_id);
    assert_eq!(event.seq, 1, "T004 owns seq = 1");
    assert_eq!(event.status, TaskStatus::Queued);
    assert_eq!(event.payload, TaskEventPayload::Queued);
    assert_eq!(event.timestamp, fixed_ts(), "the injected clock is used");

    // Nothing else was produced.
    assert_eq!(receivers.tasks.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(receivers.events.try_recv(), Err(TryRecvError::Empty));
}

#[tokio::test]
async fn from_config_builds_the_registry_and_the_system_clock_is_used() {
    let config = test_config();
    let (bus, mut receivers) = MemoryBus::from_config(&config, 4);
    assert!(bus.registry().get_by_agent_id("worker").is_some());

    bus.submit(root_task(derive_endpoint_id("worker")))
        .await
        .expect("valid submission");

    let event = receivers
        .events
        .recv()
        .await
        .expect("event must be emitted");
    let delta = (Utc::now() - event.timestamp).num_seconds().abs();
    assert!(delta < 60, "system clock timestamp looks wrong: {event:?}");
}

#[tokio::test]
async fn unknown_target_is_rejected_without_enqueue_or_event() {
    let config = test_config();
    let (bus, mut receivers) = fixed_clock_bus(&config, 4);

    let unknown = EndpointId::generate();
    let err = bus
        .submit(root_task(unknown))
        .await
        .expect_err("unknown target must be rejected");
    assert_eq!(
        err,
        BusError::UnknownTarget {
            endpoint_id: unknown
        }
    );
    assert_nothing_queued(&mut receivers);
}

#[tokio::test]
async fn disabled_target_is_rejected_without_enqueue_or_event() {
    let config = test_config();
    let (bus, mut receivers) = fixed_clock_bus(&config, 4);

    let idle = derive_endpoint_id("idle");
    let err = bus
        .submit(root_task(idle))
        .await
        .expect_err("disabled target must be rejected");
    assert_eq!(err, BusError::TargetDisabled { endpoint_id: idle });
    assert_nothing_queued(&mut receivers);

    // The rejection names no address detail (T003/T004 never render the command).
    let rendered = err.to_string();
    assert!(
        !rendered.contains("idle-acp"),
        "address must never be rendered: {rendered}"
    );
}

#[tokio::test]
async fn declared_but_unaddressable_target_is_rejected_explicitly() {
    let config = test_config();
    let registry = EndpointRegistry::from_config(&config);
    let (bus, mut receivers) = fixed_clock_bus(&config, 4);

    for agent_id in ["matrix-bot", "http-svc"] {
        let id = derive_endpoint_id(agent_id);
        // Identity resolution still covers these declarations: T010/T011 can
        // normalize an external identity to this endpoint id.
        assert_eq!(registry.resolve_agent_id(agent_id), Some(id));
        assert_eq!(registry.get(id).map(|e| e.is_addressable()), Some(false));

        let err = bus
            .submit(root_task(id))
            .await
            .expect_err("declared but unaddressable targets must be rejected");
        assert_eq!(err, BusError::AddressUnavailable { endpoint_id: id });
    }
    assert_nothing_queued(&mut receivers);
}

#[tokio::test]
async fn target_rejections_follow_the_fixed_priority_order() {
    let config = test_config();
    let (bus, mut receivers) = fixed_clock_bus(&config, 4);

    // 1. Unknown wins first: `to_agent` is not declared at all.
    let unknown = EndpointId::generate();
    assert_eq!(
        bus.submit(root_task(unknown)).await.unwrap_err(),
        BusError::UnknownTarget {
            endpoint_id: unknown
        }
    );

    // 2. Disabled is reported before unaddressable, even for a matrix endpoint.
    let disabled_matrix = derive_endpoint_id("disabled-matrix");
    assert_eq!(
        bus.submit(root_task(disabled_matrix)).await.unwrap_err(),
        BusError::TargetDisabled {
            endpoint_id: disabled_matrix
        }
    );

    // 3. Enabled but unaddressable.
    let matrix = derive_endpoint_id("matrix-bot");
    assert_eq!(
        bus.submit(root_task(matrix)).await.unwrap_err(),
        BusError::AddressUnavailable {
            endpoint_id: matrix
        }
    );

    // 4. Addressable: accepted.
    bus.submit(root_task(derive_endpoint_id("worker")))
        .await
        .expect("worker is addressable");
    assert!(receivers.tasks.try_recv().is_ok());
    assert!(receivers.events.try_recv().is_ok());
    assert_nothing_queued(&mut receivers);
}

#[tokio::test]
async fn full_task_queue_reports_queue_full_and_writes_no_event() {
    let config = test_config();
    // Capacity 1: the first submission fills both channels.
    let (bus, mut receivers) = fixed_clock_bus(&config, 1);
    let worker = derive_endpoint_id("worker");

    bus.submit(root_task(worker)).await.expect("first fits");

    let err = bus
        .submit(root_task(worker))
        .await
        .expect_err("queue is full");
    assert_eq!(err, BusError::QueueFull);

    // Exactly one task and one event exist; the rejected submission added neither.
    assert!(receivers.tasks.try_recv().is_ok());
    assert_eq!(receivers.tasks.try_recv(), Err(TryRecvError::Empty));
    assert!(receivers.events.try_recv().is_ok());
    assert_eq!(receivers.events.try_recv(), Err(TryRecvError::Empty));
}

#[tokio::test]
async fn full_event_buffer_reports_event_buffer_full_after_the_task_is_queued() {
    let config = test_config();
    let (bus, mut receivers) = fixed_clock_bus(&config, 2);
    let worker = derive_endpoint_id("worker");

    // Drain the task queue between submissions so it stays below capacity while
    // the event buffer fills up: this reaches the step-3 failure window.
    let mut queued_task_ids = Vec::new();
    for submitted in 0..2 {
        let task = root_task(worker);
        queued_task_ids.push(task.task_id);
        bus.submit(task)
            .await
            .unwrap_or_else(|e| panic!("submission {submitted} must fit, got {e}"));
        assert!(receivers.tasks.try_recv().is_ok(), "task was queued");
    }

    let rejected = root_task(worker);
    let err = bus
        .submit(rejected.clone())
        .await
        .expect_err("event buffer is full");
    assert_eq!(err, BusError::EventBufferFull);

    // Documented partial commit: the task was already enqueued, its Queued event
    // was not written. Callers must treat this error as "queued, event missing".
    assert!(
        receivers.tasks.try_recv().is_ok(),
        "the task stays queued even though the event write failed"
    );

    // The two accepted submissions have their events; the rejected one has none.
    let mut event_task_ids = Vec::new();
    while let Ok(event) = receivers.events.try_recv() {
        event_task_ids.push(event.task_id);
    }
    assert_eq!(event_task_ids, queued_task_ids);
    assert!(!event_task_ids.contains(&rejected.task_id));
}

#[tokio::test]
async fn dropping_every_bus_handle_closes_the_channels_after_draining() {
    let config = test_config();
    let (bus, mut receivers) = fixed_clock_bus(&config, 2);
    let worker = derive_endpoint_id("worker");

    bus.submit(root_task(worker)).await.expect("first fits");
    bus.submit(root_task(worker)).await.expect("second fits");

    let clone = bus.clone();
    drop(bus);
    assert!(
        receivers.tasks.try_recv().is_ok(),
        "a clone keeps the channel open"
    );
    drop(clone);

    // mpsc drains buffered items before signalling completion: close is
    // "drain, then None", not "discard".
    assert!(receivers.tasks.recv().await.is_some());
    assert_eq!(receivers.tasks.recv().await, None);
    assert!(receivers.events.recv().await.is_some());
    assert!(receivers.events.recv().await.is_some());
    assert_eq!(receivers.events.recv().await, None);
}

#[tokio::test]
async fn submission_reports_closed_once_the_consumer_is_gone() {
    let config = test_config();
    let (bus, receivers) = fixed_clock_bus(&config, 4);
    drop(receivers);

    let err = bus
        .submit(root_task(derive_endpoint_id("worker")))
        .await
        .expect_err("channel is closed");
    assert_eq!(err, BusError::TaskChannelClosed);
}

#[tokio::test]
async fn priority_is_preserved_and_arrival_order_is_kept() {
    let config = test_config();
    let (bus, mut receivers) = fixed_clock_bus(&config, 4);
    let worker = derive_endpoint_id("worker");

    let urgent = AgentTask {
        priority: Priority::MAX,
        text: "urgent".into(),
        ..root_task(worker)
    };
    let low = AgentTask {
        priority: Priority::MIN,
        text: "low".into(),
        ..root_task(worker)
    };
    bus.submit(urgent.clone()).await.expect("urgent fits");
    bus.submit(low.clone()).await.expect("low fits");

    // v1 is arrival-order (channel FIFO): priority is carried, not reordered.
    // Priority *ordering* is the T005 consumer's contract.
    let first = receivers.tasks.recv().await.expect("first task");
    let second = receivers.tasks.recv().await.expect("second task");
    assert_eq!(first.task_id, urgent.task_id);
    assert_eq!(first.priority, Priority::MAX);
    assert_eq!(second.task_id, low.task_id);
    assert_eq!(second.priority, Priority::MIN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_producers_queue_every_task_exactly_once() {
    const N: usize = 16;
    let config = test_config();
    let (bus, mut receivers) = MemoryBus::new(Arc::new(EndpointRegistry::from_config(&config)), N);
    // Object-safe handle: producers depend on `dyn Bus`, not on MemoryBus.
    let bus: Arc<dyn Bus> = Arc::new(bus);
    let worker = derive_endpoint_id("worker");

    let mut set = JoinSet::new();
    let mut submitted = HashSet::new();
    for _ in 0..N {
        let bus = Arc::clone(&bus);
        let task = root_task(worker);
        submitted.insert(task.task_id);
        set.spawn(async move { bus.submit(task).await });
    }
    while let Some(result) = set.join_next().await {
        result.expect("submit task must not panic").expect("submit");
    }

    let mut queued = HashSet::new();
    while let Ok(task) = receivers.tasks.try_recv() {
        assert_eq!(task.to_agent, worker);
        assert!(queued.insert(task.task_id), "a task was queued twice");
    }
    assert_eq!(queued, submitted, "every task must be queued exactly once");

    let mut event_ids = HashSet::new();
    let mut event_tasks = HashSet::new();
    while let Ok(event) = receivers.events.try_recv() {
        assert!(
            event_ids.insert(event.id),
            "EventId must be globally unique"
        );
        assert_eq!(event.seq, 1);
        assert_eq!(event.status, TaskStatus::Queued);
        assert!(event_tasks.insert(event.task_id));
    }
    assert_eq!(event_ids.len(), N);
    assert_eq!(event_tasks, submitted, "one Queued event per submission");
}

#[tokio::test]
async fn resubmitting_the_same_task_id_is_not_deduplicated_in_v1() {
    // Documented boundary (D6): v1 keeps no seen-task_id set, so a duplicate
    // submission produces a second task and a second `seq = 1` event. Durable
    // idempotency is T009/T018.
    let config = test_config();
    let (bus, mut receivers) = fixed_clock_bus(&config, 4);
    let task = root_task(derive_endpoint_id("worker"));

    bus.submit(task.clone()).await.expect("first submission");
    bus.submit(task.clone())
        .await
        .expect("duplicate submission");

    let first = receivers.tasks.recv().await.expect("first task");
    let second = receivers.tasks.recv().await.expect("second task");
    assert_eq!(first.task_id, second.task_id);

    let first_event = receivers.events.recv().await.expect("first event");
    let second_event = receivers.events.recv().await.expect("second event");
    assert_eq!(first_event.seq, 1);
    assert_eq!(second_event.seq, 1);
    assert_ne!(
        first_event.id, second_event.id,
        "duplicate submissions still get distinct event identities"
    );
}

/// A non-`Mpsc` [`EventSink`] defined outside the crate: it records every event
/// it is asked to write, then either succeeds (a recording sink) or fails with
/// the supplied error (a sink whose write fails *after* the task was enqueued).
///
/// Implementing it here with only the public API proves the replaceability the
/// handoff claims — and, because it is injected into a real `MemoryBus` below, it
/// exercises the genuine `submit -> EventSink::emit` path rather than a
/// standalone trait object.
struct RecordingSink {
    events: Mutex<Vec<TaskEvent>>,
    fail_with: Option<BusError>,
}

impl RecordingSink {
    fn recording() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            fail_with: None,
        })
    }

    fn failing(err: BusError) -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            fail_with: Some(err),
        })
    }

    fn recorded(&self) -> Vec<TaskEvent> {
        self.events.lock().expect("not poisoned").clone()
    }
}

/// Coerce a concrete `Arc<S>` into the trait object the bus holds, so the test
/// keeps its concrete handle for assertions.
fn as_sink<S: EventSink + 'static>(sink: &Arc<S>) -> Arc<dyn EventSink> {
    let concrete: Arc<S> = Arc::clone(sink);
    concrete
}

impl EventSink for RecordingSink {
    fn emit<'a>(&'a self, event: TaskEvent) -> BusFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            self.events.lock().expect("not poisoned").push(event);
            match self.fail_with {
                Some(err) => Err(err),
                None => Ok(()),
            }
        })
    }
}

#[tokio::test]
async fn injected_recording_sink_receives_the_queued_event_through_submit() {
    let config = test_config();
    let worker = derive_endpoint_id("worker");
    let sink = RecordingSink::recording();

    let (bus, mut tasks) = MemoryBus::with_event_sink(
        Arc::new(EndpointRegistry::from_config(&config)),
        4,
        Clock::fixed(fixed_ts()),
        as_sink(&sink),
    );

    let task = root_task(worker);
    bus.submit(task.clone()).await.expect("valid submission");

    // The real submit path enqueued the task...
    let queued = tasks.recv().await.expect("task must be queued");
    assert_eq!(queued, task);

    // ...and drove the injected (non-mpsc) sink with exactly one event.
    let recorded = sink.recorded();
    assert_eq!(recorded.len(), 1, "exactly one Queued event");
    let event = &recorded[0];
    assert_eq!(event.task_id, task.task_id);
    assert_eq!(event.seq, 1, "T004 owns seq = 1");
    assert_eq!(event.status, TaskStatus::Queued);
    assert_eq!(event.payload, TaskEventPayload::Queued);
    assert_eq!(event.timestamp, fixed_ts(), "the injected clock is used");

    assert_eq!(tasks.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(sink.recorded().len(), 1, "no extra event was written");
}

#[tokio::test]
async fn injected_sink_error_propagates_after_the_task_is_queued() {
    let config = test_config();
    let worker = derive_endpoint_id("worker");
    let sink = RecordingSink::failing(BusError::EventBufferFull);

    let (bus, mut tasks) = MemoryBus::with_event_sink(
        Arc::new(EndpointRegistry::from_config(&config)),
        4,
        Clock::fixed(fixed_ts()),
        as_sink(&sink),
    );

    let task = root_task(worker);
    let err = bus
        .submit(task.clone())
        .await
        .expect_err("the injected sink failed");
    assert_eq!(err, BusError::EventBufferFull);

    // Documented partial commit: the task is queued before the event write fails,
    // and the failure surfaced from the injected sink (not from a channel).
    let queued = tasks.recv().await.expect("the task stays queued");
    assert_eq!(queued, task);

    let recorded = sink.recorded();
    assert_eq!(recorded.len(), 1, "the sink saw the attempted event");
    assert_eq!(recorded[0].task_id, task.task_id);
    assert_eq!(recorded[0].seq, 1);
    assert_eq!(recorded[0].status, TaskStatus::Queued);
}

#[tokio::test]
async fn event_sink_is_object_safe_and_mpsc_sink_stays_available() {
    // The default sink remains usable through the same trait object the bus holds.
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: TaskId::generate(),
        seq: 1,
        status: TaskStatus::Queued,
        timestamp: fixed_ts(),
        payload: TaskEventPayload::Queued,
    };
    let (mpsc_sink, mut rx) = MpscEventSink::new(2);
    let mpsc_sink: Arc<dyn EventSink> = Arc::new(mpsc_sink);
    mpsc_sink.emit(event.clone()).await.expect("emit");
    assert_eq!(rx.recv().await, Some(event));
}
