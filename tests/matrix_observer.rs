use chrono::{DateTime, Utc};
use guigu_agent_bridge::{
    bus::{EventBroadcaster, EventConsumer, EventSink, MpscEventSink, derive_endpoint_id},
    models::{
        AgentEndpoint, AgentTask, Capability, Conversation, ConversationId, DeliveryId,
        EndpointAddress, EventId, Priority, TaskEvent, TaskEventPayload, TaskId, TaskStatus,
        TransportType,
    },
    observer::{
        MAX_BODY_BYTES, MatrixObserver, MessageCategory, MonitorSender, ObserverFuture,
        ObserverMessage, ObserverSendError, Severity,
    },
    storage::{Repository, RepositoryEventConsumer, SqliteRepository, connect, migrate},
};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

fn at() -> DateTime<Utc> {
    "2026-09-17T12:00:00Z".parse().unwrap()
}
fn endpoint(name: &str) -> AgentEndpoint {
    AgentEndpoint {
        id: derive_endpoint_id(name),
        transport: TransportType::Acp,
        address: EndpointAddress::Acp {
            command: name.into(),
            args: vec![],
        },
        enabled: true,
        capabilities: vec![Capability::new("chat")],
    }
}
fn event(task: TaskId, seq: u64, status: TaskStatus, payload: TaskEventPayload) -> TaskEvent {
    TaskEvent {
        id: EventId::generate(),
        task_id: task,
        seq,
        status,
        timestamp: at(),
        payload,
    }
}

struct Db {
    repo: Arc<SqliteRepository>,
    pool: sqlx::SqlitePool,
    path: PathBuf,
    from: AgentEndpoint,
    to: AgentEndpoint,
    conversation: ConversationId,
}
impl Db {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!("observer-{}.db", uuid::Uuid::now_v7()));
        let pool = connect(&path).await.unwrap();
        migrate(&pool).await.unwrap();
        let repo = Arc::new(SqliteRepository::new(pool.clone()));
        let from = endpoint("observer-from");
        let to = endpoint("observer-to");
        repo.upsert_agent("observer-from", &from).await.unwrap();
        repo.upsert_agent("observer-to", &to).await.unwrap();
        let conversation = ConversationId::generate();
        repo.insert_conversation(&Conversation {
            id: conversation,
            participants: vec![from.id, to.id],
            external_ref: None,
        })
        .await
        .unwrap();
        Self {
            repo,
            pool,
            path,
            from,
            to,
            conversation,
        }
    }
    fn task(&self, parent: Option<TaskId>, root: Option<TaskId>) -> AgentTask {
        let id = TaskId::generate();
        AgentTask {
            task_id: id,
            root_task_id: root.unwrap_or(id),
            parent_task_id: parent,
            from_agent: self.from.id,
            to_agent: self.to.id,
            conversation_id: self.conversation,
            reply_to: None,
            text: "SECRET PROMPT".into(),
            priority: Priority::DEFAULT,
            depth: 0,
            hops: 0,
            deadline: None,
            version: 0,
        }
    }
    async fn insert(&self, task: &AgentTask) {
        self.repo
            .insert_task_and_event(
                task,
                &event(
                    task.task_id,
                    1,
                    TaskStatus::Queued,
                    TaskEventPayload::Queued,
                ),
            )
            .await
            .unwrap();
    }
    async fn close(self) {
        let Self {
            repo, pool, path, ..
        } = self;
        drop(repo);
        pool.close().await;
        drop(pool);
        for s in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{}", path.display(), s));
        }
    }
}

#[derive(Default)]
struct Recorder {
    messages: Mutex<Vec<(String, ObserverMessage)>>,
    failures: Mutex<usize>,
}
impl Recorder {
    fn messages(&self) -> Vec<(String, ObserverMessage)> {
        self.messages.lock().unwrap().clone()
    }
    fn fail_next(&self) {
        *self.failures.lock().unwrap() += 1;
    }
}
impl MonitorSender for Recorder {
    fn send<'a>(&'a self, room: &'a str, message: &'a ObserverMessage) -> ObserverFuture<'a> {
        Box::pin(async move {
            let mut failures = self.failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                return Err(ObserverSendError);
            }
            drop(failures);
            self.messages
                .lock()
                .unwrap()
                .push((room.into(), message.clone()));
            Ok(())
        })
    }
}

#[tokio::test]
async fn real_broadcaster_persists_before_projecting_and_stops_cleanly() {
    let db = Db::new().await;
    let task = db.task(None, None);
    db.insert(&task).await;
    let update = event(
        task.task_id,
        2,
        TaskStatus::Running,
        TaskEventPayload::Running { started_at: at() },
    );
    let sender = Arc::new(Recorder::default());
    let (sink, rx) = MpscEventSink::new(8);
    let repository: Arc<dyn Repository> = db.repo.clone();
    let consumers: Vec<Arc<dyn EventConsumer>> = vec![
        Arc::new(RepositoryEventConsumer::new(repository.clone())),
        Arc::new(MatrixObserver::new(
            repository,
            sender.clone(),
            "!ops:example",
        )),
    ];
    let handle = EventBroadcaster::new(rx, consumers).spawn();
    sink.emit(update.clone()).await.unwrap();
    drop(sink);
    handle.await.unwrap();
    assert_eq!(
        db.repo.latest_event(task.task_id).await.unwrap(),
        Some(update)
    );
    let got = sender.messages();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, "!ops:example");
    assert_eq!(got[0].1.category, MessageCategory::Summary);
    assert_eq!(got[0].1.severity, Severity::Info);
    db.close().await;
}

#[tokio::test]
async fn every_status_is_bounded_and_sensitive_payloads_are_omitted() {
    let db = Db::new().await;
    let sender = Arc::new(Recorder::default());
    let repository: Arc<dyn Repository> = db.repo.clone();
    let observer = Arc::new(MatrixObserver::new(repository, sender.clone(), "ops"));
    let cases = vec![
        (
            TaskStatus::Queued,
            TaskEventPayload::Queued,
            MessageCategory::Summary,
            Severity::Info,
        ),
        (
            TaskStatus::Dispatched,
            TaskEventPayload::Dispatched {
                delivery_id: DeliveryId::generate(),
                attempt: 999,
            },
            MessageCategory::Summary,
            Severity::Info,
        ),
        (
            TaskStatus::Running,
            TaskEventPayload::Running { started_at: at() },
            MessageCategory::Summary,
            Severity::Info,
        ),
        (
            TaskStatus::Completed,
            TaskEventPayload::Completed {
                output: "SECRET OUTPUT".into(),
            },
            MessageCategory::Summary,
            Severity::Info,
        ),
        (
            TaskStatus::Failed,
            TaskEventPayload::Failed {
                error: "SECRET ERROR".into(),
            },
            MessageCategory::Alert,
            Severity::Error,
        ),
        (
            TaskStatus::TimedOut,
            TaskEventPayload::TimedOut { deadline: at() },
            MessageCategory::Alert,
            Severity::Error,
        ),
        (
            TaskStatus::Cancelled,
            TaskEventPayload::Cancelled {
                reason: "SECRET REASON".into(),
            },
            MessageCategory::Alert,
            Severity::Warning,
        ),
    ];
    for (status, payload, _, _) in &cases {
        let task = db.task(None, None);
        db.insert(&task).await;
        observer
            .consume(&event(task.task_id, 2, *status, payload.clone()))
            .await
            .unwrap();
    }
    let messages = sender.messages();
    assert_eq!(messages.len(), cases.len());
    for ((_, msg), (_, _, category, severity)) in messages.iter().zip(cases) {
        assert_eq!(msg.category, category);
        assert_eq!(msg.severity, severity);
        assert!(msg.body.len() <= MAX_BODY_BYTES);
        assert!(
            msg.body.contains(&format!("from_agent={}", db.from.id)),
            "{}",
            msg.body
        );
        assert!(
            msg.body.contains(&format!("to_agent={}", db.to.id)),
            "{}",
            msg.body
        );
        for secret in [
            "SECRET PROMPT",
            "SECRET OUTPUT",
            "SECRET ERROR",
            "SECRET REASON",
            "attempt=999",
            "observer-from",
            "observer-to",
        ] {
            assert!(!msg.body.contains(secret), "{}", msg.body);
        }
    }
    db.close().await;
}

#[tokio::test]
async fn chain_is_root_to_current_and_limit_is_marked() {
    let db = Db::new().await;
    let root = db.task(None, None);
    db.insert(&root).await;
    let child = db.task(Some(root.task_id), Some(root.task_id));
    db.insert(&child).await;
    let leaf = db.task(Some(child.task_id), Some(root.task_id));
    db.insert(&leaf).await;
    let sender = Arc::new(Recorder::default());
    let repository: Arc<dyn Repository> = db.repo.clone();
    let observer = MatrixObserver::new(repository, sender.clone(), "ops");
    observer
        .consume(&event(
            leaf.task_id,
            2,
            TaskStatus::Completed,
            TaskEventPayload::Completed { output: "x".into() },
        ))
        .await
        .unwrap();
    let body = &sender.messages()[0].1.body;
    let a = body.find(&root.task_id.to_string()).unwrap();
    let b = body.rfind(&child.task_id.to_string()).unwrap();
    let c = body.rfind(&leaf.task_id.to_string()).unwrap();
    assert!(a < b && b < c);
    assert!(body.contains("chain_state=complete"));
    let limited = MatrixObserver::new(db.repo.clone(), sender.clone(), "ops").with_limits(1, 4);
    limited
        .consume(&event(
            leaf.task_id,
            2,
            TaskStatus::Running,
            TaskEventPayload::Running { started_at: at() },
        ))
        .await
        .unwrap();
    assert!(
        sender.messages()[1]
            .1
            .body
            .contains("chain_state=truncated")
    );
    db.close().await;
}

#[tokio::test]
async fn corrupt_parent_chains_are_bounded_and_explicit() {
    let db = Db::new().await;
    let root = db.task(None, None);
    db.insert(&root).await;
    let child = db.task(Some(root.task_id), Some(root.task_id));
    db.insert(&child).await;
    let sender = Arc::new(Recorder::default());

    sqlx::query("UPDATE tasks SET parent_task_id = ? WHERE task_id = ?")
        .bind(child.task_id.to_string())
        .bind(root.task_id.to_string())
        .execute(&db.pool)
        .await
        .unwrap();
    let observer = MatrixObserver::new(db.repo.clone(), sender.clone(), "ops");
    observer
        .consume(&event(
            root.task_id,
            2,
            TaskStatus::Running,
            TaskEventPayload::Running { started_at: at() },
        ))
        .await
        .unwrap();
    assert!(sender.messages()[0].1.body.contains("chain_state=cycle"));

    let mut connection = db.pool.acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    let missing = TaskId::generate();
    sqlx::query("UPDATE tasks SET parent_task_id = ? WHERE task_id = ?")
        .bind(missing.to_string())
        .bind(root.task_id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    observer
        .consume(&event(
            root.task_id,
            3,
            TaskStatus::Failed,
            TaskEventPayload::Failed {
                error: "hidden".into(),
            },
        ))
        .await
        .unwrap();
    let body = &sender.messages()[1].1.body;
    assert!(body.contains("chain_state=missing_parent"));
    assert!(body.contains(&missing.to_string()));
    db.close().await;
}

#[tokio::test]
async fn persistence_and_projection_failures_remain_isolated() {
    let db = Db::new().await;
    let task = db.task(None, None);
    db.insert(&task).await;
    let sender = Arc::new(Recorder::default());
    let repository: Arc<dyn Repository> = db.repo.clone();
    let observer = Arc::new(MatrixObserver::new(
        repository.clone(),
        sender.clone(),
        "ops",
    ));
    let persistence = Arc::new(RepositoryEventConsumer::new(repository));

    let conflicting = event(
        task.task_id,
        1,
        TaskStatus::Queued,
        TaskEventPayload::Queued,
    );
    let (sink, receiver) = MpscEventSink::new(2);
    let consumers: Vec<Arc<dyn EventConsumer>> = vec![persistence.clone(), observer.clone()];
    let handle = EventBroadcaster::new(receiver, consumers).spawn();
    sink.emit(conflicting).await.unwrap();
    drop(sink);
    handle.await.unwrap();
    assert_eq!(sender.messages().len(), 1, "live projection still runs");

    sender.fail_next();
    let persisted = event(
        task.task_id,
        2,
        TaskStatus::Completed,
        TaskEventPayload::Completed {
            output: "hidden".into(),
        },
    );
    persistence.consume(&persisted).await.unwrap();
    assert!(observer.consume(&persisted).await.is_err());
    assert_eq!(
        db.repo.latest_event(task.task_id).await.unwrap(),
        Some(persisted)
    );
    db.close().await;
}

#[tokio::test]
async fn missing_tasks_fail_without_sending_and_seq_arrival_is_not_reordered() {
    let db = Db::new().await;
    let sender = Arc::new(Recorder::default());
    let observer = MatrixObserver::new(db.repo.clone(), sender.clone(), "ops");
    let missing = event(
        TaskId::generate(),
        1,
        TaskStatus::Queued,
        TaskEventPayload::Queued,
    );
    assert!(observer.consume(&missing).await.is_err());
    assert!(sender.messages().is_empty());

    let task = db.task(None, None);
    db.insert(&task).await;
    let high = event(
        task.task_id,
        9,
        TaskStatus::Running,
        TaskEventPayload::Running { started_at: at() },
    );
    let low = event(
        task.task_id,
        3,
        TaskStatus::Running,
        TaskEventPayload::Running { started_at: at() },
    );
    observer.consume(&high).await.unwrap();
    observer.consume(&low).await.unwrap();
    let messages = sender.messages();
    assert!(messages[0].1.body.contains("seq=9"));
    assert!(messages[1].1.body.contains("seq=3"));
    db.close().await;
}

#[tokio::test]
async fn send_failure_is_retryable_and_successful_ids_are_bounded() {
    let db = Db::new().await;
    let task = db.task(None, None);
    db.insert(&task).await;
    let sender = Arc::new(Recorder::default());
    sender.fail_next();
    let observer = MatrixObserver::new(db.repo.clone(), sender.clone(), "ops").with_limits(16, 1);
    let first = event(
        task.task_id,
        2,
        TaskStatus::Completed,
        TaskEventPayload::Completed {
            output: "hidden".into(),
        },
    );
    assert!(observer.consume(&first).await.is_err());
    observer.consume(&first).await.unwrap();
    observer.consume(&first).await.unwrap();
    assert_eq!(sender.messages().len(), 1);
    let second = event(
        task.task_id,
        3,
        TaskStatus::Failed,
        TaskEventPayload::Failed {
            error: "hidden".into(),
        },
    );
    observer.consume(&second).await.unwrap();
    observer.consume(&first).await.unwrap();
    assert_eq!(sender.messages().len(), 3);
    db.close().await;
}
