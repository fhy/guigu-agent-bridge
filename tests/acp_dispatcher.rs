//! T014 end-to-end tests: `submit` → `Worker` → `AcpDispatcher` → a real ACP child
//! process → events and durable rows.
//!
//! Each test builds the whole stack — a real SQLite pool with the real migrations,
//! the T009 repository, a configuration whose `acp` agent points at the mock backend
//! example, the worker and a recording consumer — so the assertions are about the
//! events, the `deliveries` row and the `sessions` row a real run produces. The child
//! process is spawned by the adapter from the registry address, not injected.
//!
//! No test sleeps: a mock that hangs is bounded by the adapter's own deadlines, and
//! every run ends through the drain-then-terminate path with the child shut down.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::SqlitePool;
use tokio::sync::mpsc;

use guigu_agent_bridge::acp::{
    AcpDispatcher, AcpLimits, SessionState, SqliteSessionStore, TransportLimits,
};
use guigu_agent_bridge::app::AcpDispatcherRouter;
use guigu_agent_bridge::bus::{
    Bus, BusFuture, Clock, ConsumerError, DispatcherRegistry, EndpointRegistry, EventBroadcaster,
    EventConsumer, EventSink, MemoryBus, MpscEventSink, TaskDispatcher, Worker, WorkerConfig,
    derive_endpoint_id,
};
use guigu_agent_bridge::config::{Config, load_from_str_with_env};
use guigu_agent_bridge::matrix::{
    AdminHandler, AdminPermissionPolicy, CommandLedger, InboundMatrixEvent, MatrixSender,
    ReplyContext, ReplyFuture,
};
use guigu_agent_bridge::models::{
    AgentTask, Conversation, ConversationId, DeliveryId, EndpointId, EventId, Priority, TaskEvent,
    TaskEventPayload, TaskId, TaskStatus, TransportType,
};
use guigu_agent_bridge::runtime::{
    AcquireOutcome, Continuation, ContinuationPolicy, ExecutionResourceKey, LeasedAcpDispatcher,
    RuntimeClock, RuntimeMetrics, SqliteRuntimeStore, SqliteTaskLifecycle, TokioRuntimeTimer,
    WorkspaceId,
};
use guigu_agent_bridge::storage::plan_recovery;
use guigu_agent_bridge::storage::{Repository, SqliteRepository, connect, migrate, sync_agents};

const TS: &str = "2026-09-16T10:00:00.000000000Z";
const SECRET_BODY: &str = "SECRET-BODY-DO-NOT-LEAK";

fn ts() -> DateTime<Utc> {
    TS.parse().expect("valid timestamp")
}

/// The mock backend, rebuilt once so a filtered test run cannot spawn a stale one.
fn mock_backend() -> PathBuf {
    static BUILT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BUILT
        .get_or_init(|| {
            let current = std::env::current_exe().expect("current test binary");
            let mut path = current
                .parent()
                .expect("the test binary has a directory")
                .to_path_buf();
            path.pop();
            path.push("examples");
            path.push(format!("acp-mock-backend{}", std::env::consts::EXE_SUFFIX));
            if !path.exists() {
                let cargo = option_env!("CARGO").unwrap_or("cargo");
                let status = std::process::Command::new(cargo)
                    .args(["build", "--example", "acp-mock-backend"])
                    .status()
                    .expect("run cargo build --example");
                assert!(status.success(), "building the mock backend failed");
            }
            assert!(
                path.exists(),
                "the mock backend is missing at {}",
                path.display()
            );
            path
        })
        .clone()
}

/// A configuration whose one `acp` agent is the mock backend in `scenario`.
fn config(scenario: &str) -> Config {
    let mut parts = scenario.splitn(2, ':');
    let scenario_name = parts.next().unwrap();
    let extra = parts.next();
    let args = match extra {
        Some(extra) => format!("{}, {}", json!(scenario_name), json!(extra)),
        None => json!(scenario_name).to_string(),
    };
    let toml = format!(
        "[agents.worker]\ntransport = \"acp\"\ncommand = {}\nargs = [{}]\nworkspace = \"/tmp\"\nenabled = true\n",
        json!(mock_backend().to_string_lossy()),
        args,
    );
    let mut env = BTreeMap::new();
    env.insert("HOME".to_string(), "/home/tester".to_string());
    load_from_str_with_env(&toml, &env).expect("the test config must be valid")
}

#[tokio::test]
async fn a_crashed_prompt_is_resumed_without_replaying_the_accepted_delivery() {
    let marker =
        std::env::temp_dir().join(format!("guigu-acp-recovery-{}.log", uuid::Uuid::now_v7()));
    let scenario = format!("crash-prompt-once:{}", marker.display());
    let harness = Harness::new("recovery", &scenario, default_limits(), true).await;
    let task = harness.task("do the thing");
    let outcome = harness.run(vec![task.clone()]).await;
    let events = outcome.task_events(task.task_id);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Failed
        ]
    );
    assert!(failed_payload(&events[3]).contains("backend recovered"));
    let delivery = outcome
        .repository
        .get_delivery(dispatched_delivery(&events[1]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.attempt(), 1);
    assert!(delivery.is_acknowledged());
    let log = std::fs::read_to_string(&marker).expect("recovery log");
    assert_eq!(log.matches("prompt\n").count(), 1);
    assert_eq!(log.matches("resume\n").count(), 1);
    outcome.cleanup().await;
    let _ = std::fs::remove_file(marker);
}

fn remove_db_files(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut candidate = path.as_os_str().to_owned();
        candidate.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(candidate));
    }
}

#[derive(Default)]
struct PauseReplies(Mutex<Vec<String>>);
impl MatrixSender for PauseReplies {
    fn send_reply<'a>(&'a self, _: &'a ReplyContext, body: &'a str) -> ReplyFuture<'a> {
        self.0.lock().unwrap().push(body.to_owned());
        Box::pin(async { Ok(()) })
    }
}

struct FixedClock;
impl RuntimeClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        ts()
    }
}
fn default_policy() -> ContinuationPolicy {
    ContinuationPolicy {
        max_turns: 2,
        max_wall_time: Duration::from_secs(60),
        max_inactivity: Duration::from_secs(60),
        max_consecutive_no_progress: 2,
        max_observed_output_bytes: 1024,
        lease_ttl: Duration::from_secs(30),
    }
}

#[tokio::test]
async fn authenticated_pause_uses_real_router_and_marks_running_queue_paused() {
    let harness = Harness::new("t022-pause", "hang-prompt", default_limits(), true).await;
    let task = harness.task("pause me");
    harness
        .repository_impl
        .insert_task_and_event(
            &task,
            &TaskEvent {
                id: EventId::generate(),
                task_id: task.task_id,
                seq: 1,
                status: TaskStatus::Running,
                timestamp: ts(),
                payload: TaskEventPayload::Running { started_at: ts() },
            },
        )
        .await
        .unwrap();
    let delivery = DeliveryId::generate();
    sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,?)")
        .bind(delivery.to_string()).bind(task.task_id.to_string()).bind(task.to_agent.to_string()).bind(TS).execute(&harness.pool).await.unwrap();
    let reliability = guigu_agent_bridge::storage::ReliabilityStore::new(harness.pool.clone());
    let queue = reliability
        .reserve_queue(
            &task.task_id.to_string(),
            &delivery.to_string(),
            &task.to_agent.to_string(),
            "!room",
            None,
            "@admin:example",
            "pause-idem",
            "hash",
            4,
            TS,
        )
        .await
        .unwrap();
    sqlx::query("UPDATE agent_work_queue SET state='running',runtime_owner='owner',owner_fence=7,send_started=1 WHERE queue_id=?").bind(&queue.queue_id).execute(&harness.pool).await.unwrap();
    let runtime = SqliteRuntimeStore::new(harness.pool.clone());
    let leased = Arc::new(LeasedAcpDispatcher::new(
        (*harness.dispatcher).clone(),
        runtime,
        WorkspaceId::from_canonical_path(std::env::temp_dir()).unwrap(),
        default_policy(),
        Arc::new(FixedClock),
        Arc::new(TokioRuntimeTimer),
        Arc::new(RuntimeMetrics::default()),
    ));
    let target = harness.registry.get(task.to_agent).unwrap().clone();
    let task_for_run = task.clone();
    let delivery_for_run = delivery;
    let leased_for_run = Arc::clone(&leased);
    let run = tokio::spawn(async move {
        let request = guigu_agent_bridge::bus::DispatchRequest {
            task: &task_for_run,
            target: &target,
            delivery_id: delivery_for_run,
            attempt: 1,
        };
        let _ = leased_for_run.deliver(request.clone()).await;
        let _ = leased_for_run.execute_prepared(request).await;
    });
    let (owner, fence) = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(value) = sqlx::query_as::<_, (String, i64)>(
                "SELECT owner_token,fence FROM execution_leases WHERE task_id=? AND state='active'",
            )
            .bind(task.task_id.to_string())
            .fetch_optional(&harness.pool)
            .await
            .unwrap()
            {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("active lease established");
    {
        sqlx::query("UPDATE agent_work_queue SET runtime_owner=?,owner_fence=? WHERE queue_id=?")
            .bind(owner)
            .bind(fence)
            .bind(&queue.queue_id)
            .execute(&harness.pool)
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let state: Option<String> =
                sqlx::query_scalar("SELECT state FROM task_continuations WHERE task_id=?")
                    .bind(task.task_id.to_string())
                    .fetch_optional(&harness.pool)
                    .await
                    .unwrap();
            if state.as_deref() == Some("in_flight") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("prepared execution entered an in-flight turn");
    let mut endpoints = std::collections::HashMap::new();
    endpoints.insert(task.to_agent, leased);
    let router = Arc::new(AcpDispatcherRouter::new(endpoints));
    let sender = Arc::new(PauseReplies::default());
    let admin = AdminHandler::new(
        harness.repository.clone(),
        guigu_agent_bridge::bus::Cancellation::new(),
        sender.clone(),
        AdminPermissionPolicy::new()
            .allow_user("@admin:example")
            .restrict_rooms(["!room"]),
        Arc::new(CommandLedger::new(8)),
    )
    .with_queue_control(Arc::new(reliability))
    .with_reap_control(router);
    admin
        .handle(&InboundMatrixEvent {
            event_id: "$pause".into(),
            room_id: "!room".into(),
            thread_root: None,
            sender: "@admin:example".into(),
            body: format!("/pause {}", task.task_id),
        })
        .await
        .unwrap();
    let state: String = sqlx::query_scalar("SELECT state FROM agent_work_queue WHERE queue_id=?")
        .bind(&queue.queue_id)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
    assert_eq!(state, "paused");
    run.abort();
    harness.pool.close().await;
    remove_db_files(&harness.path);
}

#[tokio::test]
async fn missing_router_reap_marks_recovery_without_paused_state() {
    let harness = Harness::new_reliable("t022-recovery", "happy", default_limits(), true).await;
    let task = harness.task("recover me");
    harness
        .repository_impl
        .insert_task_and_event(
            &task,
            &TaskEvent {
                id: EventId::generate(),
                task_id: task.task_id,
                seq: 1,
                status: TaskStatus::Running,
                timestamp: ts(),
                payload: TaskEventPayload::Running { started_at: ts() },
            },
        )
        .await
        .unwrap();
    let delivery = DeliveryId::generate();
    sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,?)")
        .bind(delivery.to_string()).bind(task.task_id.to_string()).bind(task.to_agent.to_string()).bind(TS).execute(&harness.pool).await.unwrap();
    let reliability = guigu_agent_bridge::storage::ReliabilityStore::new(harness.pool.clone());
    let queue = reliability
        .reserve_queue(
            &task.task_id.to_string(),
            &delivery.to_string(),
            &task.to_agent.to_string(),
            "!room",
            None,
            "@admin:example",
            "recover-idem",
            "hash",
            4,
            TS,
        )
        .await
        .unwrap();
    sqlx::query("UPDATE agent_work_queue SET state='running',runtime_owner='owner',owner_fence=7,send_started=1 WHERE queue_id=?").bind(&queue.queue_id).execute(&harness.pool).await.unwrap();
    let admin = AdminHandler::new(
        harness.repository.clone(),
        guigu_agent_bridge::bus::Cancellation::new(),
        Arc::new(PauseReplies::default()),
        AdminPermissionPolicy::new()
            .allow_user("@admin:example")
            .restrict_rooms(["!room"]),
        Arc::new(CommandLedger::new(8)),
    )
    .with_queue_control(Arc::new(reliability))
    .with_reap_control(Arc::new(AcpDispatcherRouter::new(
        std::collections::HashMap::new(),
    )));
    admin
        .handle(&InboundMatrixEvent {
            event_id: "$pause-recovery".into(),
            room_id: "!room".into(),
            thread_root: None,
            sender: "@admin:example".into(),
            body: format!("/pause {}", task.task_id),
        })
        .await
        .unwrap();
    let state: String = sqlx::query_scalar("SELECT state FROM agent_work_queue WHERE queue_id=?")
        .bind(&queue.queue_id)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
    assert_eq!(state, "recovery_needed");
    assert_ne!(state, "paused");
    harness.pool.close().await;
    remove_db_files(&harness.path);
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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

/// Everything a test asserts against after a run.
struct Outcome {
    events: Vec<TaskEvent>,
    repository: Arc<dyn Repository>,
    store: SqliteSessionStore,
    dispatcher: Arc<AcpDispatcher>,
    pool: SqlitePool,
    path: PathBuf,
}

impl Outcome {
    /// One task's events, in arrival order (the task is submitted before the worker
    /// starts, so arrival order is `seq` order here).
    fn task_events(&self, task_id: TaskId) -> Vec<TaskEvent> {
        self.events
            .iter()
            .filter(|event| event.task_id == task_id)
            .cloned()
            .collect()
    }

    async fn cleanup(self) {
        let Outcome {
            repository,
            store,
            dispatcher,
            pool,
            path,
            ..
        } = self;
        dispatcher.shutdown().await;
        pool.close().await;
        drop(dispatcher);
        drop(store);
        drop(repository);
        drop(pool);
        remove_db_files(&path);
    }
}

/// The stack before a task runs.
struct Harness {
    bus: MemoryBus,
    sink: Arc<dyn EventSink>,
    registry: Arc<EndpointRegistry>,
    tasks_rx: mpsc::Receiver<AgentTask>,
    events_rx: mpsc::Receiver<TaskEvent>,
    repository: Arc<dyn Repository>,
    repository_impl: SqliteRepository,
    store: SqliteSessionStore,
    dispatcher: Arc<AcpDispatcher>,
    pool: SqlitePool,
    path: PathBuf,
    conversation: ConversationId,
}

impl Harness {
    /// Assemble the stack. `with_agents` controls whether T009's startup snapshot
    /// runs — the foreign keys that sessions and deliveries depend on.
    async fn new(tag: &str, scenario: &str, limits: AcpLimits, with_agents: bool) -> Self {
        Self::assemble(tag, scenario, limits, with_agents, false).await
    }

    async fn new_reliable(tag: &str, scenario: &str, limits: AcpLimits, with_agents: bool) -> Self {
        Self::assemble(tag, scenario, limits, with_agents, true).await
    }

    async fn assemble(
        tag: &str,
        scenario: &str,
        limits: AcpLimits,
        with_agents: bool,
        reliable: bool,
    ) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!("guigu-acp-{tag}-{}.db", uuid::Uuid::now_v7()));
        let pool = connect(&path).await.expect("connect");
        migrate(&pool).await.expect("migrate");

        let registry = Arc::new(EndpointRegistry::from_config(&config(scenario)));
        let repository_impl = SqliteRepository::new(pool.clone());
        let repository: Arc<dyn Repository> = Arc::new(repository_impl.clone());
        if with_agents {
            sync_agents(&*repository, &registry)
                .await
                .expect("sync the endpoint snapshot");
        }

        let conversation = Conversation {
            id: ConversationId::generate(),
            participants: vec![derive_endpoint_id("worker")],
            external_ref: None,
        };
        repository
            .insert_conversation(&conversation)
            .await
            .expect("insert the conversation");

        let store = SqliteSessionStore::new(pool.clone());
        let mut dispatcher = AcpDispatcher::builder()
            .cwd(std::env::temp_dir())
            .clock(Clock::fixed(ts()))
            .limits(limits)
            .sessions(store.clone())
            .repository(Arc::clone(&repository))
            .legacy_results(scenario == "legacy-end-turn-optin");
        if reliable {
            dispatcher = dispatcher.reliability(
                guigu_agent_bridge::storage::ReliabilityStore::new(pool.clone()),
            );
        }
        let dispatcher = Arc::new(dispatcher.build().expect("the adapter must build"));

        let (mpsc_sink, events_rx) = MpscEventSink::new(64);
        let sink: Arc<dyn EventSink> = Arc::new(mpsc_sink);
        let (bus, tasks_rx) = MemoryBus::with_event_sink(
            Arc::clone(&registry),
            8,
            Clock::fixed(ts()),
            Arc::clone(&sink),
        );

        Self {
            bus,
            sink,
            registry,
            tasks_rx,
            events_rx,
            repository,
            repository_impl,
            store,
            dispatcher,
            pool,
            path,
            conversation: conversation.id,
        }
    }

    /// A task aimed at the configured `worker` endpoint, in this harness's conversation.
    fn task(&self, text: &str) -> AgentTask {
        let task_id = TaskId::generate();
        AgentTask {
            task_id,
            root_task_id: task_id,
            parent_task_id: None,
            from_agent: EndpointId::generate(),
            to_agent: derive_endpoint_id("worker"),
            conversation_id: self.conversation,
            reply_to: None,
            text: text.to_owned(),
            priority: Priority::DEFAULT,
            depth: 0,
            hops: 0,
            deadline: None,
            version: 0,
        }
    }

    /// Submit every task, then run the worker to a clean shutdown.
    async fn run(self, tasks: Vec<AgentTask>) -> Outcome {
        let Harness {
            bus,
            sink,
            registry,
            tasks_rx,
            events_rx,
            repository,
            repository_impl,
            store,
            dispatcher,
            pool,
            path,
            ..
        } = self;

        for task in tasks {
            let queued = TaskEvent {
                id: EventId::generate(),
                task_id: task.task_id,
                seq: 1,
                status: TaskStatus::Queued,
                timestamp: ts(),
                payload: TaskEventPayload::Queued,
            };
            repository_impl
                .insert_task_and_event(&task, &queued)
                .await
                .expect("persist task and queued event before dispatch");
            bus.submit(task).await.expect("the bus accepts the task");
        }

        let worker = Worker::builder(
            Arc::clone(&registry),
            tasks_rx,
            Arc::clone(&sink),
            Clock::fixed(ts()),
            DispatcherRegistry::new().with(
                TransportType::Acp,
                Arc::clone(&dispatcher) as Arc<dyn TaskDispatcher>,
            ),
        )
        .config(WorkerConfig::default())
        .build();

        let recording = Arc::new(RecordingConsumer::default());
        let consumer: Arc<dyn EventConsumer> = Arc::clone(&recording) as Arc<_>;
        let broadcaster = EventBroadcaster::new(events_rx, vec![consumer]);

        // The bus and the extra sink handle must go before the drain can finish.
        drop(bus);
        drop(sink);
        worker
            .run()
            .await
            .expect("clean drain-then-terminate shutdown");
        broadcaster.run().await;

        Outcome {
            events: recording.events(),
            repository,
            store,
            dispatcher,
            pool,
            path,
        }
    }

    async fn run_leased(
        self,
        tasks: Vec<AgentTask>,
        policy: ContinuationPolicy,
        cp2_action: Cp2Action,
    ) -> (Outcome, Vec<Continuation>, bool) {
        let Harness {
            bus,
            sink,
            registry,
            tasks_rx,
            events_rx,
            repository,
            repository_impl,
            store,
            dispatcher,
            pool,
            path,
            ..
        } = self;
        let task_ids: Vec<_> = tasks.iter().map(|task| task.task_id).collect();
        for task in tasks {
            let queued = TaskEvent {
                id: EventId::generate(),
                task_id: task.task_id,
                seq: 1,
                status: TaskStatus::Queued,
                timestamp: ts(),
                payload: TaskEventPayload::Queued,
            };
            repository_impl
                .insert_task_and_event(&task, &queued)
                .await
                .expect("persist before dispatch");
            sqlx::query("INSERT INTO task_admissions(task_id,state,revision,reply_room,reply_thread_root,reply_event_id,monitor_room,monitor_generation,render_version,created_at,updated_at) VALUES(?,'enqueued',0,'!reply:example','$thread','$event','!monitor:example',7,'v1',?,?)")
                .bind(task.task_id.to_string())
                .bind(TS)
                .bind(TS)
                .execute(&pool)
                .await
                .expect("persist admission before dispatch");
            bus.submit(task).await.expect("submit");
        }
        struct FixedRuntimeClock;
        impl RuntimeClock for FixedRuntimeClock {
            fn now(&self) -> DateTime<Utc> {
                ts()
            }
        }
        let runtime_store = SqliteRuntimeStore::new(pool.clone());
        let leased = Arc::new(LeasedAcpDispatcher::new(
            (*dispatcher).clone(),
            runtime_store.clone(),
            WorkspaceId::from_canonical_path(std::env::temp_dir()).unwrap(),
            policy,
            Arc::new(FixedRuntimeClock),
            Arc::new(TokioRuntimeTimer),
            Arc::new(RuntimeMetrics::default()),
        ));
        let cancellation = guigu_agent_bridge::bus::Cancellation::new();
        struct Cp2Dispatcher {
            inner: Arc<LeasedAcpDispatcher>,
            cancellation: guigu_agent_bridge::bus::Cancellation,
            action: Cp2Action,
            deadline_elapsed: Arc<std::sync::atomic::AtomicBool>,
        }
        impl TaskDispatcher for Cp2Dispatcher {
            fn deliver<'a>(
                &'a self,
                request: guigu_agent_bridge::bus::DispatchRequest<'a>,
            ) -> BusFuture<'a, Result<(), guigu_agent_bridge::bus::DispatchError>> {
                Box::pin(async move {
                    self.inner.deliver(request.clone()).await?;
                    match self.action {
                        Cp2Action::None => {}
                        Cp2Action::Cancel => self
                            .cancellation
                            .cancel(request.task.task_id, "operator cancelled"),
                        Cp2Action::Deadline => self
                            .deadline_elapsed
                            .store(true, std::sync::atomic::Ordering::Release),
                    }
                    Ok(())
                })
            }
            fn execute<'a>(
                &'a self,
                request: guigu_agent_bridge::bus::DispatchRequest<'a>,
            ) -> BusFuture<
                'a,
                Result<
                    guigu_agent_bridge::bus::DispatchOutcome,
                    guigu_agent_bridge::bus::DispatchError,
                >,
            > {
                self.inner.execute(request)
            }
            fn supports_prepared(&self) -> bool {
                true
            }
            fn execute_prepared<'a>(
                &'a self,
                request: guigu_agent_bridge::bus::DispatchRequest<'a>,
            ) -> BusFuture<
                'a,
                Result<
                    guigu_agent_bridge::bus::PreparedExecution,
                    guigu_agent_bridge::bus::DispatchError,
                >,
            > {
                self.inner.execute_prepared(request)
            }
            fn cancel_prepared<'a>(
                &'a self,
                request: guigu_agent_bridge::bus::DispatchRequest<'a>,
                reason: &'a str,
            ) -> BusFuture<
                'a,
                Result<
                    guigu_agent_bridge::bus::FinalizationCapability,
                    guigu_agent_bridge::bus::DispatchError,
                >,
            > {
                self.inner.cancel_prepared(request, reason)
            }
            fn finalize_delivery_failure<'a>(
                &'a self,
                request: guigu_agent_bridge::bus::DispatchRequest<'a>,
                event: TaskEvent,
            ) -> BusFuture<'a, Result<(), guigu_agent_bridge::bus::DispatchError>> {
                self.inner.finalize_delivery_failure(request, event)
            }
        }
        let deadline_elapsed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let selected: Arc<dyn TaskDispatcher> = if cp2_action != Cp2Action::None {
            Arc::new(Cp2Dispatcher {
                inner: Arc::clone(&leased),
                cancellation: cancellation.clone(),
                action: cp2_action,
                deadline_elapsed: Arc::clone(&deadline_elapsed),
            })
        } else {
            // Exercise the production routing path, including the router's
            // prepared-delivery finalization forwarding.
            let mut endpoints = HashMap::new();
            endpoints.insert(derive_endpoint_id("worker"), Arc::clone(&leased));
            Arc::new(AcpDispatcherRouter::new(endpoints))
        };
        let worker_clock = Clock::new(move || {
            if deadline_elapsed.load(std::sync::atomic::Ordering::Acquire) {
                ts() + chrono::Duration::seconds(1)
            } else {
                ts()
            }
        });
        let worker = Worker::builder(
            Arc::clone(&registry),
            tasks_rx,
            Arc::clone(&sink),
            worker_clock,
            DispatcherRegistry::new().with(TransportType::Acp, selected),
        )
        .lifecycle(Arc::new(SqliteTaskLifecycle::new(pool.clone())))
        .config(WorkerConfig {
            cancellation: Some(cancellation),
            ..WorkerConfig::default()
        })
        .build();
        let recording = Arc::new(RecordingConsumer::default());
        let broadcaster = EventBroadcaster::new(
            events_rx,
            vec![Arc::clone(&recording) as Arc<dyn EventConsumer>],
        );
        drop(bus);
        worker.run().await.expect("worker");
        drop(sink);
        broadcaster.run().await;
        let backend_reaped_before_shutdown = dispatcher.backend_id().is_none();
        leased.shutdown().await;
        let mut continuations = Vec::new();
        for id in task_ids {
            continuations.push(runtime_store.continuation(id).await.unwrap().unwrap());
        }
        (
            Outcome {
                events: recording.events(),
                repository,
                store,
                dispatcher,
                pool,
                path,
            },
            continuations,
            backend_reaped_before_shutdown,
        )
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cp2Action {
    None,
    Cancel,
    Deadline,
}

fn default_limits() -> AcpLimits {
    AcpLimits {
        initialize_deadline: Duration::from_secs(5),
        request_deadline: Duration::from_secs(5),
        transport: TransportLimits::default(),
    }
}

fn seqs(events: &[TaskEvent]) -> Vec<u64> {
    events.iter().map(|event| event.seq).collect()
}

fn statuses(events: &[TaskEvent]) -> Vec<TaskStatus> {
    events.iter().map(|event| event.status).collect()
}

fn failed_payload(event: &TaskEvent) -> String {
    match &event.payload {
        TaskEventPayload::Failed { error } => error.clone(),
        other => panic!("expected a Failed payload, got {other:?}"),
    }
}

fn completed_payload(event: &TaskEvent) -> String {
    match &event.payload {
        TaskEventPayload::Completed { output } => output.clone(),
        other => panic!("expected a Completed payload, got {other:?}"),
    }
}

fn dispatched_delivery(event: &TaskEvent) -> DeliveryId {
    match &event.payload {
        TaskEventPayload::Dispatched { delivery_id, .. } => *delivery_id,
        other => panic!("expected a Dispatched payload, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The happy path and its durable record
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_task_completes_through_the_real_acp_adapter() {
    let harness = Harness::new("happy", "happy", default_limits(), true).await;
    let task = harness.task("do the thing");
    let outcome = harness.run(vec![task.clone()]).await;

    let events = outcome.task_events(task.task_id);
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
    assert_eq!(
        completed_payload(&events[3]),
        "Hello, world",
        "the turn text comes from the backend's streamed chunks"
    );

    // The acceptance is durable: the delivery the worker dispatched is acknowledged.
    let delivery_id = dispatched_delivery(&events[1]);
    let delivery = outcome
        .repository
        .get_delivery(delivery_id)
        .await
        .expect("get_delivery")
        .expect("the delivery was recorded");
    assert!(
        delivery.is_acknowledged(),
        "an accepted delivery is acknowledged"
    );

    // And the session is recorded with the negotiated backend identity.
    let session = outcome
        .store
        .live_session(
            derive_endpoint_id("worker"),
            task.conversation_id,
            &std::env::temp_dir().to_string_lossy(),
        )
        .await
        .expect("live_session")
        .expect("a live session");
    assert_eq!(session.session_id(), "session-1");
    assert_eq!(session.state(), SessionState::Ready);
    assert_eq!(session.backend_id(), "worker/acp-mock-backend@0.1.0");
    assert_eq!(session.protocol_version(), 1);

    outcome.cleanup().await;
}

#[tokio::test]
async fn structured_continue_reuses_the_real_session_and_delivery_until_completed() {
    let harness = Harness::new_reliable("continue", "continue-once", default_limits(), true).await;
    let task = harness.task("start");
    let (outcome, continuations, _) = harness
        .run_leased(
            vec![task.clone()],
            ContinuationPolicy {
                max_turns: 4,
                max_wall_time: Duration::from_secs(60),
                max_inactivity: Duration::from_secs(60),
                max_consecutive_no_progress: 3,
                max_observed_output_bytes: 4096,
                lease_ttl: Duration::from_secs(30),
            },
            Cp2Action::None,
        )
        .await;
    let events = outcome.task_events(task.task_id);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Running,
            TaskStatus::Completed
        ]
    );
    assert_eq!(completed_payload(&events[3]), "Hello, world");
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.payload, TaskEventPayload::Dispatched { .. }))
            .count(),
        1
    );
    assert_eq!(continuations[0].completed_turns, 2);
    assert_eq!(
        continuations[0].state,
        guigu_agent_bridge::runtime::ContinuationState::Terminal
    );
    let lease_state: String =
        sqlx::query_scalar("SELECT state FROM execution_leases WHERE task_id = ?")
            .bind(task.task_id.to_string())
            .fetch_one(&outcome.pool)
            .await
            .expect("terminal execution lease");
    assert_eq!(
        lease_state, "released",
        "a terminal turn must make the workspace available to the next task"
    );
    let admission: (String, i64) =
        sqlx::query_as("SELECT state,revision FROM task_admissions WHERE task_id=?")
            .bind(task.task_id.to_string())
            .fetch_one(&outcome.pool)
            .await
            .expect("terminal admission");
    assert_eq!(admission.0, "terminal");
    assert_eq!(admission.1, 3);
    let durable: Vec<String> =
        sqlx::query_scalar("SELECT status FROM task_events WHERE task_id=? ORDER BY seq")
            .bind(task.task_id.to_string())
            .fetch_all(&outcome.pool)
            .await
            .expect("durable lifecycle events");
    assert_eq!(durable, ["queued", "dispatched", "running", "completed"]);
    let version: i64 = sqlx::query_scalar("SELECT version FROM tasks WHERE task_id=?")
        .bind(task.task_id.to_string())
        .fetch_one(&outcome.pool)
        .await
        .expect("terminal version");
    assert_eq!(version, 3);
    let disposition: String =
        sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE delivery_id=?")
            .bind(dispatched_delivery(&events[1]).to_string())
            .fetch_one(&outcome.pool)
            .await
            .expect("terminal delivery disposition");
    assert_eq!(disposition, "terminal");
    let acknowledged: Option<String> =
        sqlx::query_scalar("SELECT acknowledged_at FROM deliveries WHERE delivery_id=?")
            .bind(dispatched_delivery(&events[1]).to_string())
            .fetch_one(&outcome.pool)
            .await
            .expect("terminal delivery acknowledgement");
    assert!(acknowledged.is_some());
    type ProjectionRow = (String, String, Option<String>, Option<String>, String);
    let projections: Vec<ProjectionRow> = sqlx::query_as(
        "SELECT projection,room_id,thread_root,reply_event_id,body FROM projection_outbox WHERE source_kind='task_event' AND source_id=? ORDER BY projection",
    )
    .bind(events[3].id.to_string())
    .fetch_all(&outcome.pool)
    .await
    .expect("terminal projection siblings");
    assert_eq!(projections.len(), 2);
    assert_eq!(projections[0].0, "observer");
    assert_eq!(projections[0].1, "!monitor:example");
    assert_eq!(projections[0].2, None);
    assert_eq!(projections[0].3, None);
    assert!(projections[0].4.contains("status=completed"));
    assert_eq!(projections[1].0, "terminal_reply");
    assert_eq!(projections[1].1, "!reply:example");
    assert_eq!(projections[1].2.as_deref(), Some("$thread"));
    assert_eq!(projections[1].3.as_deref(), Some("$event"));
    assert_eq!(projections[1].4, "Hello, world");
    outcome.cleanup().await;
}

#[tokio::test]
async fn active_prepared_cancellation_reaps_then_commits_one_cancelled_terminal() {
    let harness = Harness::new_reliable(
        "cancel-prepared",
        "hang-after-session-new",
        default_limits(),
        true,
    )
    .await;
    let task = harness.task("start");
    let (outcome, continuations, backend_reaped) = harness
        .run_leased(
            vec![task.clone()],
            ContinuationPolicy {
                max_turns: 4,
                max_wall_time: Duration::from_secs(60),
                max_inactivity: Duration::from_secs(60),
                max_consecutive_no_progress: 3,
                max_observed_output_bytes: 4096,
                lease_ttl: Duration::from_secs(30),
            },
            Cp2Action::Cancel,
        )
        .await;
    let events = outcome.task_events(task.task_id);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Cancelled,
        ]
    );
    assert!(matches!(
        &events[2].payload,
        TaskEventPayload::Cancelled { reason } if reason == "operator cancelled"
    ));
    assert_eq!(
        continuations[0].state,
        guigu_agent_bridge::runtime::ContinuationState::Terminal
    );
    assert!(backend_reaped);
    let terminals: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_events WHERE task_id=? AND status IN ('completed','failed','timed_out','cancelled')",
    )
    .bind(task.task_id.to_string())
    .fetch_one(&outcome.pool)
    .await
    .unwrap();
    assert_eq!(terminals, 1);
    let disposition: String =
        sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE task_id=?")
            .bind(task.task_id.to_string())
            .fetch_one(&outcome.pool)
            .await
            .unwrap();
    assert_eq!(disposition, "terminal");
    outcome.cleanup().await;
}

#[tokio::test]
async fn active_prepared_deadline_reaps_signal_exit_then_commits_one_timed_out_terminal() {
    let harness = Harness::new_reliable(
        "deadline-prepared",
        "hang-after-session-new",
        default_limits(),
        true,
    )
    .await;
    let mut task = harness.task("start");
    let deadline = ts() + chrono::Duration::milliseconds(500);
    task.deadline = Some(deadline);
    let (outcome, continuations, backend_reaped) = harness
        .run_leased(
            vec![task.clone()],
            ContinuationPolicy {
                max_turns: 4,
                max_wall_time: Duration::from_secs(60),
                max_inactivity: Duration::from_secs(60),
                max_consecutive_no_progress: 3,
                max_observed_output_bytes: 4096,
                lease_ttl: Duration::from_secs(30),
            },
            Cp2Action::Deadline,
        )
        .await;
    let events = outcome.task_events(task.task_id);
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::TimedOut,
        ]
    );
    assert!(matches!(
        &events[2].payload,
        TaskEventPayload::TimedOut { deadline: actual } if actual == &deadline
    ));
    assert_eq!(
        continuations[0].state,
        guigu_agent_bridge::runtime::ContinuationState::Terminal
    );
    assert!(backend_reaped);
    let disposition: String =
        sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE task_id=?")
            .bind(task.task_id.to_string())
            .fetch_one(&outcome.pool)
            .await
            .unwrap();
    assert_eq!(disposition, "terminal");
    outcome.cleanup().await;
}

#[tokio::test]
async fn active_chunks_do_not_extend_the_hard_deadline_and_the_child_is_reaped() {
    let harness =
        Harness::new_reliable("hard-deadline", "chunks-then-hang", default_limits(), true).await;
    let mut task = harness.task("start");
    task.deadline = Some(ts() + chrono::Duration::milliseconds(100));
    let (outcome, continuations, backend_reaped) = harness
        .run_leased(
            vec![task.clone()],
            ContinuationPolicy {
                max_turns: 4,
                max_wall_time: Duration::from_secs(60),
                max_inactivity: Duration::from_secs(60),
                max_consecutive_no_progress: 3,
                max_observed_output_bytes: 4096,
                lease_ttl: Duration::from_secs(30),
            },
            Cp2Action::None,
        )
        .await;
    let events = outcome.task_events(task.task_id);
    assert_eq!(events.last().unwrap().status, TaskStatus::Failed);
    assert!(
        failed_payload(events.last().unwrap()).contains("deadline"),
        "the earlier task deadline, not inactivity, wall time, or the ACP deadline, must end the turn"
    );
    assert_eq!(
        continuations[0].state,
        guigu_agent_bridge::runtime::ContinuationState::RecoveryNeeded
    );
    assert!(
        backend_reaped,
        "the hard-deadline path must reap the child before recording its outcome"
    );
    let disposition: String =
        sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE delivery_id=?")
            .bind(dispatched_delivery(&events[1]).to_string())
            .fetch_one(&outcome.pool)
            .await
            .expect("uncertain delivery disposition");
    assert_eq!(disposition, "outcome_unknown");
    outcome.cleanup().await;
}

#[tokio::test]
async fn lease_conflict_is_rejected_before_the_acp_process_starts() {
    let harness = Harness::new("lease-conflict", "happy", default_limits(), true).await;
    let task = harness.task("must not start");
    let queued = TaskEvent {
        id: EventId::generate(),
        task_id: task.task_id,
        seq: 1,
        status: TaskStatus::Queued,
        timestamp: ts(),
        payload: TaskEventPayload::Queued,
    };
    harness
        .repository_impl
        .insert_task_and_event(&task, &queued)
        .await
        .unwrap();
    let runtime = SqliteRuntimeStore::new(harness.pool.clone());
    let workspace = WorkspaceId::from_canonical_path(std::env::temp_dir()).unwrap();
    let resource = ExecutionResourceKey::new(task.to_agent, workspace);
    assert!(matches!(
        runtime
            .acquire(resource, task.task_id, ts(), Duration::from_secs(30))
            .await
            .unwrap(),
        AcquireOutcome::Acquired(_)
    ));
    struct FixedRuntimeClock;
    impl RuntimeClock for FixedRuntimeClock {
        fn now(&self) -> DateTime<Utc> {
            ts()
        }
    }
    let leased = LeasedAcpDispatcher::new(
        (*harness.dispatcher).clone(),
        runtime,
        workspace,
        ContinuationPolicy {
            max_turns: 2,
            max_wall_time: Duration::from_secs(60),
            max_inactivity: Duration::from_secs(60),
            max_consecutive_no_progress: 2,
            max_observed_output_bytes: 1024,
            lease_ttl: Duration::from_secs(30),
        },
        Arc::new(FixedRuntimeClock),
        Arc::new(TokioRuntimeTimer),
        Arc::new(RuntimeMetrics::default()),
    );
    let target = harness
        .registry
        .resolve_agent_id("worker")
        .and_then(|id| harness.registry.get(id))
        .unwrap();
    let request = guigu_agent_bridge::bus::DispatchRequest {
        task: &task,
        target,
        delivery_id: DeliveryId::generate(),
        attempt: 1,
    };
    assert!(matches!(
        leased.deliver(request).await,
        Err(guigu_agent_bridge::bus::DispatchError::NotAccepted { .. })
    ));
    assert_eq!(harness.dispatcher.backend_id(), None);
    harness.dispatcher.shutdown().await;
    harness.pool.close().await;
    remove_db_files(&harness.path);
}

#[tokio::test]
async fn the_adapter_reports_the_backend_it_negotiated() {
    let harness = Harness::new("identity", "happy", default_limits(), true).await;
    let task = harness.task("do the thing");
    let outcome = harness.run(vec![task]).await;

    assert_eq!(
        outcome.dispatcher.backend_id().as_deref(),
        Some("worker/acp-mock-backend@0.1.0"),
        "ADR-001: the compatibility identity is versioned"
    );
    outcome.cleanup().await;
}

// ---------------------------------------------------------------------------
// Failures before and after acceptance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_bad_protocol_version_fails_before_the_task_runs() {
    let harness = Harness::new(
        "bad-version",
        "bad-protocol-version",
        default_limits(),
        true,
    )
    .await;
    let task = harness.task("do the thing");
    let outcome = harness.run(vec![task.clone()]).await;

    let events = outcome.task_events(task.task_id);
    assert_eq!(
        seqs(&events),
        [1, 2, 3],
        "the refusal lands at seq = 3: no Running, no prompt"
    );
    assert_eq!(
        statuses(&events),
        [
            TaskStatus::Queued,
            TaskStatus::Dispatched,
            TaskStatus::Failed
        ]
    );
    let reason = failed_payload(&events[2]);
    assert!(reason.contains("protocol version"), "got: {reason}");
    assert!(reason.len() <= 512, "a reason stays bounded: {reason}");

    // Nothing was accepted, so no delivery or session row exists.
    assert!(
        outcome
            .repository
            .get_delivery(dispatched_delivery(&events[1]))
            .await
            .expect("get_delivery")
            .is_none()
    );
    assert!(
        outcome
            .store
            .live_session(
                derive_endpoint_id("worker"),
                task.conversation_id,
                &std::env::temp_dir().to_string_lossy(),
            )
            .await
            .expect("live_session")
            .is_none()
    );

    outcome.cleanup().await;
}

#[tokio::test]
async fn pre_acceptance_child_exit_atomically_closes_delivery_and_restart_recovery() {
    let harness = Harness::new_reliable(
        "pre-acceptance-exit",
        "exit-after-session",
        default_limits(),
        true,
    )
    .await;
    let task = harness.task("bounded smoke");
    let (outcome, _, _) = harness
        .run_leased(vec![task.clone()], default_policy(), Cp2Action::None)
        .await;

    let events = outcome.task_events(task.task_id);
    assert_eq!(statuses(&events).last(), Some(&TaskStatus::Failed));
    let delivery_id = dispatched_delivery(&events[1]);
    let acknowledged: Option<String> =
        sqlx::query_scalar("SELECT acknowledged_at FROM deliveries WHERE delivery_id=?")
            .bind(delivery_id.to_string())
            .fetch_one(&outcome.pool)
            .await
            .unwrap();
    assert!(acknowledged.is_some());
    let disposition: String =
        sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE delivery_id=?")
            .bind(delivery_id.to_string())
            .fetch_one(&outcome.pool)
            .await
            .unwrap();
    assert_eq!(disposition, "terminal");

    let plan = plan_recovery(outcome.repository.as_ref()).await.unwrap();
    assert!(!plan.unfinished.contains(&task.task_id));
    assert!(
        !plan
            .unacknowledged
            .iter()
            .any(|d| d.delivery_id() == delivery_id)
    );
    assert!(
        !plan
            .awaiting_outcome
            .iter()
            .any(|d| d.delivery_id() == delivery_id)
    );
    outcome.cleanup().await;
}

#[tokio::test]
async fn router_finalization_missing_endpoint_and_second_consumption_are_recovery_needed() {
    let harness = Harness::new_reliable(
        "router-finalization-errors",
        "exit-after-session",
        default_limits(),
        true,
    )
    .await;
    let task = harness.task("router finalization probe");
    let target = harness
        .registry
        .resolve_agent_id("worker")
        .and_then(|id| harness.registry.get(id))
        .unwrap();
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: task.task_id,
        seq: 3,
        status: TaskStatus::Failed,
        timestamp: ts(),
        payload: TaskEventPayload::Failed {
            error: "pre-acceptance".into(),
        },
    };
    let request = guigu_agent_bridge::bus::DispatchRequest {
        task: &task,
        target,
        delivery_id: DeliveryId::generate(),
        attempt: 1,
    };
    let empty = AcpDispatcherRouter::new(HashMap::new());
    assert!(matches!(
        empty
            .finalize_delivery_failure(request.clone(), event.clone())
            .await,
        Err(guigu_agent_bridge::bus::DispatchError::RecoveryNeeded)
    ));

    struct FixedRuntimeClock;
    impl RuntimeClock for FixedRuntimeClock {
        fn now(&self) -> DateTime<Utc> {
            ts()
        }
    }
    let leased = Arc::new(LeasedAcpDispatcher::new(
        (*harness.dispatcher).clone(),
        SqliteRuntimeStore::new(harness.pool.clone()),
        WorkspaceId::from_canonical_path(std::env::temp_dir()).unwrap(),
        default_policy(),
        Arc::new(FixedRuntimeClock),
        Arc::new(TokioRuntimeTimer),
        Arc::new(RuntimeMetrics::default()),
    ));
    let mut endpoints = HashMap::new();
    endpoints.insert(target.id(), leased);
    let router = AcpDispatcherRouter::new(endpoints);
    assert!(matches!(
        router.finalize_delivery_failure(request, event).await,
        Err(guigu_agent_bridge::bus::DispatchError::RecoveryNeeded)
    ));
    harness.dispatcher.shutdown().await;
    harness.pool.close().await;
    remove_db_files(&harness.path);
}

#[tokio::test]
async fn a_refusal_after_acceptance_is_a_terminal_failure() {
    let harness = Harness::new("refuse", "refuse", default_limits(), true).await;
    let task = harness.task(SECRET_BODY);
    let outcome = harness.run(vec![task.clone()]).await;

    let events = outcome.task_events(task.task_id);
    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    assert_eq!(statuses(&events)[3], TaskStatus::Failed);
    let reason = failed_payload(&events[3]);
    assert!(reason.contains("refusal"), "got: {reason}");
    assert!(
        !reason.contains(SECRET_BODY),
        "a task body must never reach an event: {reason}"
    );

    // The delivery was still accepted before the turn failed.
    let delivery = outcome
        .repository
        .get_delivery(dispatched_delivery(&events[1]))
        .await
        .expect("get_delivery")
        .expect("recorded");
    assert!(delivery.is_acknowledged());

    outcome.cleanup().await;
}

#[tokio::test]
async fn an_unknown_stop_reason_is_not_reported_as_success() {
    let harness = Harness::new("unknown", "unknown-stop", default_limits(), true).await;
    let task = harness.task("do the thing");
    let outcome = harness.run(vec![task.clone()]).await;

    let events = outcome.task_events(task.task_id);
    assert_eq!(statuses(&events)[3], TaskStatus::Failed);
    let reason = failed_payload(&events[3]);
    assert!(
        reason.contains("some_future_reason"),
        "an unmodelled stop reason is reported verbatim: {reason}"
    );
    outcome.cleanup().await;
}

#[tokio::test]
async fn end_turn_without_a_structured_result_is_not_completed() {
    let harness = Harness::new("legacy", "legacy-end-turn", default_limits(), true).await;
    let task = harness.task("do the thing");
    let outcome = harness.run(vec![task.clone()]).await;
    let events = outcome.task_events(task.task_id);
    assert_eq!(events.last().unwrap().status, TaskStatus::Failed);
    assert!(failed_payload(events.last().unwrap()).contains("execution failed after acceptance"));
    assert!(failed_payload(events.last().unwrap()).contains("missing taskResult"));
    outcome.cleanup().await;
}

#[tokio::test]
async fn explicit_legacy_opt_in_preserves_end_turn_completion() {
    let harness = Harness::new(
        "legacy-optin",
        "legacy-end-turn-optin",
        default_limits(),
        true,
    )
    .await;
    let task = harness.task("do the thing");
    let outcome = harness.run(vec![task.clone()]).await;
    assert_eq!(
        outcome.task_events(task.task_id).last().unwrap().status,
        TaskStatus::Completed
    );
    outcome.cleanup().await;
}

#[tokio::test]
async fn a_prompt_that_never_finishes_is_bounded_by_the_request_deadline() {
    let limits = AcpLimits {
        initialize_deadline: Duration::from_secs(5),
        request_deadline: Duration::from_millis(150),
        transport: TransportLimits::default(),
    };
    let harness = Harness::new("hang", "hang-prompt", limits, true).await;
    let task = harness.task("do the thing");
    let outcome = harness.run(vec![task.clone()]).await;

    let events = outcome.task_events(task.task_id);
    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    let reason = failed_payload(&events[3]);
    assert!(reason.contains("timed out"), "got: {reason}");
    outcome.cleanup().await;
}

/// The sessions and deliveries tables have foreign keys, so the adapter must be
/// assembled after T009's startup snapshot.
#[tokio::test]
async fn a_missing_endpoint_snapshot_fails_the_delivery_explicitly() {
    let harness = Harness::new("no-agents", "happy", default_limits(), false).await;
    let task = harness.task("do the thing");
    let outcome = harness.run(vec![task.clone()]).await;

    let events = outcome.task_events(task.task_id);
    assert_eq!(seqs(&events), [1, 2, 3], "the failure precedes Running");
    let reason = failed_payload(&events[2]);
    assert!(
        reason.contains("integrity") || reason.contains("foreign key"),
        "the missing precondition is reported: {reason}"
    );
    outcome.cleanup().await;
}

// ---------------------------------------------------------------------------
// Sessions across tasks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_second_task_in_one_conversation_reuses_its_session() {
    let harness = Harness::new("resume", "happy", default_limits(), true).await;
    let first = harness.task("first");
    let second = harness.task("second");
    let (conversation, endpoint) = (first.conversation_id, derive_endpoint_id("worker"));
    let outcome = harness.run(vec![first.clone(), second.clone()]).await;

    for task in [&first, &second] {
        let events = outcome.task_events(task.task_id);
        assert_eq!(
            statuses(&events),
            [
                TaskStatus::Queued,
                TaskStatus::Dispatched,
                TaskStatus::Running,
                TaskStatus::Completed
            ],
            "both tasks must complete"
        );
    }

    // One live session, the first one: a second `session/new` would have produced a
    // different id and collided with the live-session unique index, so completing both
    // tasks proves the second delivery resumed rather than created.
    let session = outcome
        .store
        .live_session(
            endpoint,
            conversation,
            &std::env::temp_dir().to_string_lossy(),
        )
        .await
        .expect("live_session")
        .expect("a live session");
    assert_eq!(session.session_id(), "session-1");

    outcome.cleanup().await;
}

#[tokio::test]
async fn a_permission_request_is_denied_and_the_turn_reflects_it() {
    let harness = Harness::new("permission", "permission", default_limits(), true).await;
    let task = harness.task("do the thing");
    let outcome = harness.run(vec![task.clone()]).await;

    let events = outcome.task_events(task.task_id);
    assert_eq!(seqs(&events), [1, 2, 3, 4]);
    let reason = failed_payload(&events[3]);
    assert_eq!(
        reason, "agent stopped: refusal",
        "the backend ends the turn with a refusal only when the permission was denied"
    );
    outcome.cleanup().await;
}
