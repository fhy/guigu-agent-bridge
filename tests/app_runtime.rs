use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use chrono::DateTime;
use guigu_agent_bridge::{
    app::{
        AppRuntime, ConfigSource, HealthServer, HealthState, MatrixIngress, OwnerState,
        PersistenceFirstProjection, PersistingBus, ProjectionMetrics, ReloadController,
        ReloadOutcome, ReplyRegistry,
    },
    bus::{
        AdmissionContext, Bus, BusError, BusFuture, Cancellation, Clock, EndpointRegistry,
        EventConsumer, EventSink,
    },
    config::{Config, ConfigError, load_from_str_with_env},
    matrix::{
        CommandLedger, DurableMatrixAdmission, InboundMatrixEvent, MatrixAdmissionFuture,
        MatrixSender, ReplyContext, ReplyFuture,
    },
    models::{
        AgentTask, Conversation, ConversationId, EventId, Priority, TaskEvent, TaskEventPayload,
        TaskId, TaskStatus,
    },
    runtime::{RuntimeMetrics, SqliteRuntimeStore},
    storage::{ReliabilityStore, Repository, SqliteRepository, connect, migrate, sync_agents},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("guigu-t017-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config(extra: &str) -> Config {
    let mut env = BTreeMap::new();
    env.insert("HOME".into(), "/tmp".into());
    load_from_str_with_env(
        &format!(
            r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
workspace = "/tmp"
enabled = true
{extra}
"#
        ),
        &env,
    )
    .unwrap()
}

fn task(conversation: ConversationId) -> AgentTask {
    let id = TaskId::generate();
    AgentTask {
        task_id: id,
        root_task_id: id,
        parent_task_id: None,
        from_agent: guigu_agent_bridge::bus::derive_endpoint_id("sender"),
        to_agent: guigu_agent_bridge::bus::derive_endpoint_id("worker"),
        conversation_id: conversation,
        reply_to: None,
        text: "work".into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    }
}

#[tokio::test]
async fn persisting_bus_reserves_before_commit_and_emits_exact_queued_event() {
    let temp = TempDir::new();
    let pool = connect(temp.0.join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let repository = SqliteRepository::new(pool.clone());
    let registry = Arc::new(EndpointRegistry::from_config(&config("")));
    sync_agents(&repository, &registry).await.unwrap();
    let conversation = Conversation {
        id: ConversationId::generate(),
        participants: vec![],
        external_ref: None,
    };
    repository.insert_conversation(&conversation).await.unwrap();
    let at: DateTime<chrono::Utc> = "2026-09-18T00:00:00Z".parse().unwrap();
    let (sink, mut events) = guigu_agent_bridge::bus::MpscEventSink::new(2);
    let sink: Arc<dyn EventSink> = Arc::new(sink);
    let (bus, mut tasks) =
        PersistingBus::new(registry, 1, sink, repository.clone(), Clock::fixed(at));
    let bus = bus.with_default_timeout(chrono::Duration::seconds(30));

    let first = task(conversation.id);
    bus.submit(first.clone()).await.unwrap();
    let queued = events.recv().await.unwrap();
    assert_eq!(queued.task_id, first.task_id);
    assert_eq!(queued.seq, 1);
    assert_eq!(
        repository.latest_event(first.task_id).await.unwrap(),
        Some(queued)
    );

    let second = task(conversation.id);
    assert_eq!(bus.submit(second.clone()).await, Err(BusError::QueueFull));
    assert!(repository.get_task(second.task_id).await.unwrap().is_none());
    let mut expected = first;
    expected.deadline = Some(at + chrono::Duration::seconds(30));
    assert_eq!(tasks.recv().await.unwrap(), expected);
    pool.close().await;
}

#[tokio::test]
async fn matrix_admission_atomically_freezes_receipt_and_projection_destinations() {
    let temp = TempDir::new();
    let pool = connect(temp.0.join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let repository = SqliteRepository::new(pool.clone());
    let registry = Arc::new(EndpointRegistry::from_config(&config("")));
    sync_agents(&repository, &registry).await.unwrap();
    let conversation = Conversation {
        id: ConversationId::generate(),
        participants: vec![],
        external_ref: None,
    };
    repository.insert_conversation(&conversation).await.unwrap();
    let reliability = ReliabilityStore::new(pool.clone());
    reliability
        .begin_runtime("runtime-1", "pid-1", "2026-09-18T00:00:00.000000000Z")
        .await
        .unwrap();
    let at: DateTime<chrono::Utc> = "2026-09-18T00:00:00Z".parse().unwrap();
    let (sink, mut events) = guigu_agent_bridge::bus::MpscEventSink::new(2);
    let (bus, mut tasks) =
        PersistingBus::new(registry, 2, Arc::new(sink), repository, Clock::fixed(at));
    let bus = bus.with_reliability(reliability, "runtime-1".into());
    let admitted = task(conversation.id);
    let context = AdmissionContext {
        transport: "matrix".into(),
        external_event_id: "$event".into(),
        room_id: "!room:example".into(),
        thread_root: Some("$thread".into()),
        reply_event_id: "$event".into(),
        monitor_room: Some("!monitor:example".into()),
        monitor_generation: 7,
    };
    bus.submit_with_context(admitted.clone(), context.clone())
        .await
        .unwrap();
    assert_eq!(tasks.recv().await.unwrap().task_id, admitted.task_id);
    assert_eq!(events.recv().await.unwrap().task_id, admitted.task_id);
    let receipt: (String, String) = sqlx::query_as("SELECT state,task_id FROM transport_receipts WHERE transport='matrix' AND external_event_id='$event'").fetch_one(&pool).await.unwrap();
    assert_eq!(receipt, ("admitted".into(), admitted.task_id.to_string()));
    let snapshot: (String, String, String, i64) = sqlx::query_as("SELECT reply_room,reply_thread_root,monitor_room,monitor_generation FROM task_admissions WHERE task_id=?")
        .bind(admitted.task_id.to_string()).fetch_one(&pool).await.unwrap();
    assert_eq!(
        snapshot,
        (
            "!room:example".into(),
            "$thread".into(),
            "!monitor:example".into(),
            7
        )
    );
    let replay = task(conversation.id);
    let winner = bus.admit(replay.clone(), context).await.unwrap();
    assert_eq!(winner.task_id, admitted.task_id);
    assert!(tasks.try_recv().is_err());
    assert!(events.try_recv().is_err());
    assert!(
        sqlx::query_scalar::<_, String>("SELECT task_id FROM tasks WHERE task_id=?")
            .bind(replay.task_id.to_string())
            .fetch_optional(&pool)
            .await
            .unwrap()
            .is_none()
    );
    pool.close().await;
}

struct RecordingConsumer {
    name: &'static str,
    fail: bool,
    calls: Arc<Mutex<Vec<&'static str>>>,
}
impl EventConsumer for RecordingConsumer {
    fn consume<'a>(
        &'a self,
        _event: &'a TaskEvent,
    ) -> BusFuture<'a, Result<(), guigu_agent_bridge::bus::ConsumerError>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(self.name);
            if self.fail {
                Err(guigu_agent_bridge::bus::ConsumerError::Failed {
                    reason: "injected".into(),
                })
            } else {
                Ok(())
            }
        })
    }
}

fn event() -> TaskEvent {
    TaskEvent {
        id: EventId::generate(),
        task_id: TaskId::generate(),
        seq: 1,
        status: TaskStatus::Queued,
        timestamp: "2026-09-18T00:00:00Z".parse().unwrap(),
        payload: TaskEventPayload::Queued,
    }
}

#[tokio::test]
async fn persistence_failure_short_circuits_but_sibling_failures_do_not() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let metrics = Arc::new(ProjectionMetrics::default());
    let projection = PersistenceFirstProjection::new(
        Arc::new(RecordingConsumer {
            name: "persistence",
            fail: false,
            calls: Arc::clone(&calls),
        }),
        vec![
            Arc::new(RecordingConsumer {
                name: "reply",
                fail: true,
                calls: Arc::clone(&calls),
            }),
            Arc::new(RecordingConsumer {
                name: "observer",
                fail: false,
                calls: Arc::clone(&calls),
            }),
        ],
        Arc::clone(&metrics),
    );
    assert!(projection.consume(&event()).await.is_err());
    assert_eq!(*calls.lock().unwrap(), ["persistence", "reply", "observer"]);
    assert_eq!(metrics.failures(), 1);

    calls.lock().unwrap().clear();
    let projection = PersistenceFirstProjection::new(
        Arc::new(RecordingConsumer {
            name: "persistence",
            fail: true,
            calls: Arc::clone(&calls),
        }),
        vec![Arc::new(RecordingConsumer {
            name: "observer",
            fail: false,
            calls: Arc::clone(&calls),
        })],
        metrics,
    );
    assert!(projection.consume(&event()).await.is_err());
    assert_eq!(*calls.lock().unwrap(), ["persistence"]);
}

struct MutableSource(Mutex<Config>);
impl ConfigSource for MutableSource {
    fn load(&self) -> Result<Config, ConfigError> {
        Ok(self.0.lock().unwrap().clone())
    }
}

#[derive(Default)]
struct RecordingMatrixSender(Mutex<Vec<(ReplyContext, String)>>);
impl MatrixSender for RecordingMatrixSender {
    fn send_reply<'a>(&'a self, context: &'a ReplyContext, body: &'a str) -> ReplyFuture<'a> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .push((context.clone(), body.to_owned()));
            Ok(())
        })
    }
}

struct AdmissionProbe {
    inner: Arc<PersistingBus>,
    winner_calls: AtomicUsize,
    winner_called: tokio::sync::Notify,
    fail_after_commit: AtomicBool,
    post_commit_error_reported: tokio::sync::Notify,
}
impl AdmissionProbe {
    async fn wait_for_winner_calls(&self, expected: usize) {
        while self.winner_calls.load(Ordering::Acquire) < expected {
            self.winner_called.notified().await;
        }
    }

    async fn wait_for_post_commit_error(&self) {
        while self.fail_after_commit.load(Ordering::Acquire) {
            self.post_commit_error_reported.notified().await;
        }
    }
}
impl DurableMatrixAdmission for AdmissionProbe {
    fn winner<'a>(
        &'a self,
        external_event_id: &'a str,
    ) -> MatrixAdmissionFuture<'a, Result<Option<AgentTask>, BusError>> {
        Box::pin(async move {
            let result = self.inner.winner(external_event_id).await;
            self.winner_calls.fetch_add(1, Ordering::Release);
            self.winner_called.notify_waiters();
            result
        })
    }

    fn admit<'a>(
        &'a self,
        task: AgentTask,
        context: AdmissionContext,
    ) -> MatrixAdmissionFuture<'a, Result<AgentTask, BusError>> {
        Box::pin(async move {
            let result = self.inner.admit(task, context).await;
            if result.is_ok() && self.fail_after_commit.swap(false, Ordering::AcqRel) {
                self.post_commit_error_reported.notify_waiters();
                return Err(BusError::TaskChannelClosed);
            }
            result
        })
    }
}

#[tokio::test]
async fn matrix_ingress_reads_back_unknown_commit_before_changed_routing_and_permission() {
    let temp = TempDir::new();
    let pool = connect(temp.0.join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let repository = Arc::new(SqliteRepository::new(pool.clone()));
    let repository_trait: Arc<dyn Repository> = repository.clone();
    let mut initial = config("");
    initial
        .transports
        .matrix
        .allowed_users
        .push("@alice:example".into());
    initial
        .transports
        .matrix
        .routes
        .rooms
        .insert("!room:example".into(), "worker".into());
    let registry = Arc::new(EndpointRegistry::from_config(&initial));
    sync_agents(repository.as_ref(), &registry).await.unwrap();
    let reliability = ReliabilityStore::new(pool.clone());
    reliability
        .begin_runtime("runtime-1", "pid-1", "2026-09-18T00:00:00.000000000Z")
        .await
        .unwrap();
    let (sink, mut events) = guigu_agent_bridge::bus::MpscEventSink::new(4);
    let (bus, mut tasks) = PersistingBus::new(
        Arc::clone(&registry),
        4,
        Arc::new(sink),
        repository.as_ref().clone(),
        Clock::fixed("2026-09-18T00:00:00Z".parse().unwrap()),
    );
    let admission = Arc::new(AdmissionProbe {
        inner: Arc::new(bus.with_reliability(reliability, "runtime-1".into())),
        winner_calls: AtomicUsize::new(0),
        winner_called: tokio::sync::Notify::new(),
        fail_after_commit: AtomicBool::new(true),
        post_commit_error_reported: tokio::sync::Notify::new(),
    });
    let source = Arc::new(MutableSource(Mutex::new(initial.clone())));
    let reload =
        Arc::new(ReloadController::new(initial, source.clone() as Arc<dyn ConfigSource>).unwrap());
    let sender = Arc::new(RecordingMatrixSender::default());
    let replies = Arc::new(ReplyRegistry::default());
    let incoming = InboundMatrixEvent {
        event_id: "$event:example".into(),
        room_id: "!room:example".into(),
        thread_root: None,
        sender: "@alice:example".into(),
        body: "run once".into(),
    };

    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let ingress = MatrixIngress::new(
        rx,
        repository_trait.clone(),
        Arc::clone(&registry),
        admission.clone(),
        sender.clone(),
        Arc::clone(&reload),
        Cancellation::new(),
        Arc::new(CommandLedger::new(8)),
        Arc::clone(&replies),
        8,
    )
    .start();
    tx.send(incoming.clone()).await.unwrap();
    let winner = tasks.recv().await.unwrap();
    assert_eq!(events.recv().await.unwrap().task_id, winner.task_id);
    admission.wait_for_post_commit_error().await;
    ingress.shutdown().await;

    source
        .0
        .lock()
        .unwrap()
        .transports
        .matrix
        .allowed_users
        .clear();
    source
        .0
        .lock()
        .unwrap()
        .transports
        .matrix
        .routes
        .rooms
        .clear();
    assert_eq!(
        reload.reload().await.unwrap(),
        ReloadOutcome::Applied { generation: 2 }
    );
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let ingress = MatrixIngress::new(
        rx,
        repository_trait,
        registry,
        admission.clone(),
        sender.clone(),
        reload,
        Cancellation::new(),
        Arc::new(CommandLedger::new(8)),
        replies,
        8,
    )
    .start();
    tx.send(incoming).await.unwrap();
    admission.wait_for_winner_calls(2).await;
    ingress.shutdown().await;

    assert!(tasks.try_recv().is_err());
    assert!(events.try_recv().is_err());
    assert!(sender.0.lock().unwrap().is_empty());
    let task_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(task_count, 1);
    let queued_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_events WHERE task_id=? AND status='queued'")
            .bind(winner.task_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(queued_count, 1);
    let receipt: (String, String, String) = sqlx::query_as(
        "SELECT state,result_code,task_id FROM transport_receipts
         WHERE transport='matrix' AND external_event_id='$event:example'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        receipt,
        (
            "admitted".into(),
            "admitted".into(),
            winner.task_id.to_string()
        )
    );
    let admission_row: (String, String, String, i64) = sqlx::query_as(
        "SELECT state,reply_room,reply_event_id,monitor_generation
         FROM task_admissions WHERE task_id=?",
    )
    .bind(winner.task_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        admission_row,
        (
            "enqueued".into(),
            "!room:example".into(),
            "$event:example".into(),
            1
        )
    );
    pool.close().await;
}

#[tokio::test]
async fn reload_publishes_hot_generation_and_rejects_restart_changes_atomically() {
    let initial = config("");
    let source = Arc::new(MutableSource(Mutex::new(initial.clone())));
    let source_trait: Arc<dyn ConfigSource> = source.clone();
    let controller = ReloadController::new(initial, source_trait).unwrap();
    source.0.lock().unwrap().transports.matrix.monitor_room = "!monitor:x".into();
    assert_eq!(
        controller.reload().await.unwrap(),
        ReloadOutcome::Applied { generation: 2 }
    );
    assert_eq!(
        controller.snapshot().hot.monitor_room.as_deref(),
        Some("!monitor:x")
    );
    source.0.lock().unwrap().bridge.queue_capacity += 1;
    assert_eq!(
        controller.reload().await.unwrap(),
        ReloadOutcome::RestartRequired
    );
    assert_eq!(controller.snapshot().generation, 2);
}

#[tokio::test]
async fn loopback_health_reports_readiness_and_releases_listener() {
    let temp = TempDir::new();
    let pool = connect(temp.0.join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let state = Arc::new(HealthState::new(
        SqliteRuntimeStore::new(pool.clone()),
        Arc::new(RuntimeMetrics::default()),
        Arc::new(ProjectionMetrics::default()),
    ));
    state.set_owner(OwnerState::Running);
    state.set_adapter_ready(true);
    let required_owner = Arc::new(AtomicBool::new(true));
    state.register_required(Arc::clone(&required_owner));
    let server = HealthServer::start("127.0.0.1:0".parse().unwrap(), Arc::clone(&state))
        .await
        .unwrap();
    let address = server.local_addr();
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("ready=true\n"));

    required_owner.store(false, Ordering::Release);
    assert_eq!(
        state.snapshot().await.readiness,
        guigu_agent_bridge::runtime::Readiness::OwnerStopped
    );
    server.shutdown().await.unwrap();
    let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
    drop(rebound);
    pool.close().await;
}

#[tokio::test]
async fn production_runtime_starts_and_shuts_down_without_resource_residue() {
    let temp = TempDir::new();
    let config_path = temp.0.join("bridge.toml");
    let database = temp.0.join("state.db");
    let sessions = temp.0.join("sessions");
    std::fs::write(
        &config_path,
        format!(
            "[bridge]\ndatabase = {:?}\nsession_root = {:?}\nhealth_bind = \"127.0.0.1:0\"\n",
            database.to_string_lossy(),
            sessions.to_string_lossy()
        ),
    )
    .unwrap();
    let runtime = AppRuntime::start(&config_path).await.unwrap();
    let address = runtime.health_addr().unwrap();
    assert_eq!(
        runtime.health_state().snapshot().await.readiness,
        guigu_agent_bridge::runtime::Readiness::Ready
    );
    runtime.shutdown().await.unwrap();
    let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
    drop(rebound);
    let pool = connect(&database).await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn startup_recovery_backlog_blocks_readiness_without_spawning_acp() {
    let temp = TempDir::new();
    let config_path = temp.0.join("bridge.toml");
    let database = temp.0.join("state.db");
    let sessions = temp.0.join("sessions");
    std::fs::write(
        &config_path,
        format!(
            "[bridge]\ndatabase = {:?}\nsession_root = {:?}\n\n[agents.worker]\ntransport = \"acp\"\ncommand = \"definitely-not-started\"\nworkspace = \"/tmp\"\nenabled = true\n",
            database.to_string_lossy(),
            sessions.to_string_lossy()
        ),
    )
    .unwrap();
    let loaded = guigu_agent_bridge::config::load(&config_path).unwrap();
    let registry = EndpointRegistry::from_config(&loaded);
    let pool = connect(&database).await.unwrap();
    migrate(&pool).await.unwrap();
    let repository = SqliteRepository::new(pool.clone());
    sync_agents(&repository, &registry).await.unwrap();
    let conversation = Conversation {
        id: ConversationId::generate(),
        participants: vec![],
        external_ref: None,
    };
    repository.insert_conversation(&conversation).await.unwrap();
    let task = task(conversation.id);
    let queued = TaskEvent {
        id: EventId::generate(),
        task_id: task.task_id,
        seq: 1,
        status: TaskStatus::Queued,
        timestamp: "2026-09-18T00:00:00Z".parse().unwrap(),
        payload: TaskEventPayload::Queued,
    };
    repository
        .insert_task_and_event(&task, &queued)
        .await
        .unwrap();
    pool.close().await;

    let runtime = AppRuntime::start(&config_path).await.unwrap();
    assert_eq!(
        runtime.health_state().snapshot().await.readiness,
        guigu_agent_bridge::runtime::Readiness::RecoveryBlocked
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_start_failure_rolls_back_runtime_epoch_before_pool_close() {
    let temp = TempDir::new();
    let config_path = temp.0.join("bridge.toml");
    let database = temp.0.join("state.db");
    let sessions = temp.0.join("sessions");
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    std::fs::write(
        &config_path,
        format!(
            "[bridge]\ndatabase = {:?}\nsession_root = {:?}\nhealth_bind = {:?}\n",
            database.to_string_lossy(),
            sessions.to_string_lossy(),
            address.to_string()
        ),
    )
    .unwrap();
    assert!(AppRuntime::start(&config_path).await.is_err());
    let pool = connect(&database).await.unwrap();
    let states: Vec<String> = sqlx::query_scalar("SELECT state FROM runtime_instances")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(states, vec!["stopped"]);
    pool.close().await;
    drop(occupied);
}
