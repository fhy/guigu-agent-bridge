use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use chrono::{DateTime, Utc};
use guigu_agent_bridge::{
    bus::{BusFuture, Cancellation, MemoryBus, derive_endpoint_id},
    config::load_from_str,
    matrix::{
        AdminHandler, AdminPermissionPolicy, AdminResult, CommandLedger, EventDedup,
        InboundMatrixEvent, MatrixSender, PermissionPolicy, ReplyContext, ReplyError, ReplyFuture,
        RetryAdmission, RetryReply, RoutePolicy, route_event,
    },
    models::{
        AgentEndpoint, AgentTask, Conversation, ConversationId, EndpointAddress, Priority,
        TaskEvent, TaskEventPayload, TaskId, TaskStatus, TransportType,
    },
    storage::{Repository, SqliteRepository, connect, migrate},
};

#[derive(Default)]
struct RetryProbe(AtomicUsize);
impl RetryAdmission for RetryProbe {
    fn admit<'a>(
        &'a self,
        _: &'a InboundMatrixEvent,
        source: &'a AgentTask,
    ) -> BusFuture<'a, Result<RetryReply, ()>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(RetryReply::Immediate(format!(
                "retry=admitted task_id={}",
                source.task_id
            )))
        })
    }
}

#[derive(Default)]
struct OutboxRetryProbe(AtomicUsize);
impl RetryAdmission for OutboxRetryProbe {
    fn admit<'a>(
        &'a self,
        _: &'a InboundMatrixEvent,
        _: &'a AgentTask,
    ) -> BusFuture<'a, Result<RetryReply, ()>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(RetryReply::OutboxOwned)
        })
    }
}

fn at() -> DateTime<Utc> {
    "2026-09-17T12:00:00Z".parse().unwrap()
}
fn event(task_id: TaskId, seq: u64, status: TaskStatus, payload: TaskEventPayload) -> TaskEvent {
    TaskEvent {
        id: guigu_agent_bridge::models::EventId::generate(),
        task_id,
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
    conversation: ConversationId,
}
impl Db {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!("admin-{}.db", uuid::Uuid::now_v7()));
        let pool = connect(&path).await.unwrap();
        migrate(&pool).await.unwrap();
        let repo = Arc::new(SqliteRepository::new(pool.clone()));
        let from = AgentEndpoint {
            id: derive_endpoint_id("from"),
            transport: TransportType::Acp,
            address: EndpointAddress::Acp {
                command: "SECRET-FROM-COMMAND".into(),
                args: vec![],
            },
            enabled: true,
            capabilities: vec![],
        };
        let to = AgentEndpoint {
            id: derive_endpoint_id("to"),
            transport: TransportType::Acp,
            address: EndpointAddress::Acp {
                command: "SECRET-TO-COMMAND".into(),
                args: vec![],
            },
            enabled: true,
            capabilities: vec![],
        };
        repo.upsert_agent("from", &from).await.unwrap();
        repo.upsert_agent("to", &to).await.unwrap();
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
            conversation,
        }
    }
    async fn task(
        &self,
        parent: Option<TaskId>,
        root: Option<TaskId>,
        status: Option<TaskStatus>,
    ) -> AgentTask {
        let id = TaskId::generate();
        let task = AgentTask {
            task_id: id,
            root_task_id: root.unwrap_or(id),
            parent_task_id: parent,
            from_agent: derive_endpoint_id("from"),
            to_agent: derive_endpoint_id("to"),
            conversation_id: self.conversation,
            reply_to: None,
            text: "SECRET PROMPT".into(),
            priority: Priority::DEFAULT,
            depth: 0,
            hops: 0,
            deadline: None,
            version: 0,
        };
        self.repo.insert_task(&task).await.unwrap();
        if let Some(status) = status {
            let payload = match status {
                TaskStatus::Queued => TaskEventPayload::Queued,
                TaskStatus::Running => TaskEventPayload::Running { started_at: at() },
                TaskStatus::Completed => TaskEventPayload::Completed {
                    output: "SECRET OUTPUT".into(),
                },
                _ => TaskEventPayload::Failed {
                    error: "SECRET ERROR".into(),
                },
            };
            self.repo
                .append_event(&event(id, 1, status, payload))
                .await
                .unwrap();
        }
        task
    }
    async fn close(self) {
        let Self {
            repo, pool, path, ..
        } = self;
        drop(repo);
        pool.close().await;
        drop(pool);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{}", path.display(), suffix));
        }
    }
}

#[derive(Default)]
struct Sender {
    messages: Mutex<Vec<(ReplyContext, String)>>,
    fail: Mutex<bool>,
}
impl Sender {
    fn bodies(&self) -> Vec<String> {
        self.messages
            .lock()
            .unwrap()
            .iter()
            .map(|(_, b)| b.clone())
            .collect()
    }
    fn fail_next(&self) {
        *self.fail.lock().unwrap() = true;
    }
}
impl MatrixSender for Sender {
    fn send_reply<'a>(&'a self, context: &'a ReplyContext, body: &'a str) -> ReplyFuture<'a> {
        Box::pin(async move {
            if std::mem::take(&mut *self.fail.lock().unwrap()) {
                return Err(ReplyError);
            }
            self.messages
                .lock()
                .unwrap()
                .push((context.clone(), body.into()));
            Ok(())
        })
    }
}
fn incoming(id: &str, body: String, sender: &str, room: &str) -> InboundMatrixEvent {
    InboundMatrixEvent {
        event_id: id.into(),
        room_id: room.into(),
        thread_root: Some("$root:x".into()),
        sender: sender.into(),
        body,
    }
}
fn handler(
    db: &Db,
    sender: Arc<Sender>,
    cancellation: Cancellation,
    permissions: AdminPermissionPolicy,
    cap: usize,
) -> AdminHandler {
    AdminHandler::new(
        db.repo.clone(),
        cancellation,
        sender,
        permissions,
        Arc::new(CommandLedger::new(cap)),
    )
}

fn routing_config() -> guigu_agent_bridge::config::Config {
    load_from_str(
        r#"
[agents.worker]
transport = "acp"
command = "worker"
workspace = "/tmp"
enabled = true
"#,
    )
    .unwrap()
}

#[tokio::test]
async fn retry_admission_is_below_admin_authorization_and_terminal_check() {
    let db = Db::new().await;
    let running = db.task(None, None, Some(TaskStatus::Running)).await;
    let terminal = db.task(None, None, Some(TaskStatus::Completed)).await;
    let sender = Arc::new(Sender::default());
    let probe = Arc::new(RetryProbe::default());
    let retry: Arc<dyn RetryAdmission> = probe.clone();
    let admin = handler(
        &db,
        sender.clone(),
        Cancellation::new(),
        AdminPermissionPolicy::new()
            .allow_user("@admin:x")
            .restrict_rooms(["!ops:x"]),
        8,
    )
    .with_retry_admission(retry);

    for (id, task, user) in [
        ("$u", terminal.task_id, "@other:x"),
        ("$n", running.task_id, "@admin:x"),
        ("$t", terminal.task_id, "@admin:x"),
    ] {
        admin
            .handle(&incoming(id, format!("/retry {task}"), user, "!ops:x"))
            .await
            .unwrap();
    }
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
    assert_eq!(sender.bodies()[0], "command=forbidden");
    assert_eq!(sender.bodies()[1], "retry=not_terminal");
    assert!(sender.bodies()[2].starts_with("retry=admitted task_id="));
    db.close().await;
}

#[tokio::test]
async fn duplicate_outbox_owned_retry_never_uses_the_ordinary_sender() {
    let db = Db::new().await;
    let terminal = db.task(None, None, Some(TaskStatus::Completed)).await;
    let sender = Arc::new(Sender::default());
    let probe = Arc::new(OutboxRetryProbe::default());
    let retry: Arc<dyn RetryAdmission> = probe.clone();
    let admin = handler(
        &db,
        sender.clone(),
        Cancellation::new(),
        AdminPermissionPolicy::new()
            .allow_user("@admin:x")
            .restrict_rooms(["!ops:x"]),
        8,
    )
    .with_retry_admission(retry);
    let input = incoming(
        "$same",
        format!("/retry {}", terminal.task_id),
        "@admin:x",
        "!ops:x",
    );
    admin.handle(&input).await.unwrap();
    admin.handle(&input).await.unwrap();
    assert_eq!(probe.0.load(Ordering::SeqCst), 1);
    assert!(sender.bodies().is_empty());
    db.close().await;
}

#[tokio::test]
async fn status_and_trace_use_repository_facts_and_redact_payloads() {
    let db = Db::new().await;
    let root = db.task(None, None, Some(TaskStatus::Queued)).await;
    let child = db
        .task(
            Some(root.task_id),
            Some(root.task_id),
            Some(TaskStatus::Completed),
        )
        .await;
    let sender = Arc::new(Sender::default());
    let admin = handler(
        &db,
        sender.clone(),
        Cancellation::new(),
        AdminPermissionPolicy::new()
            .allow_user("@admin:x")
            .restrict_rooms(["!ops:x"]),
        8,
    );
    admin
        .handle(&incoming(
            "$status:x",
            format!("/status {}", child.task_id),
            "@admin:x",
            "!ops:x",
        ))
        .await
        .unwrap();
    admin
        .handle(&incoming(
            "$trace:x",
            format!("/trace {}", child.task_id),
            "@admin:x",
            "!ops:x",
        ))
        .await
        .unwrap();
    let bodies = sender.bodies();
    assert!(
        bodies[0].contains("status=completed")
            && bodies[0].contains(&format!("from_agent={}", child.from_agent))
            && bodies[0].contains(&format!("to_agent={}", child.to_agent))
    );
    assert!(bodies[1].contains("chain_state=complete"));
    assert!(bodies[1].contains(&format!("chain={}>{}", root.task_id, child.task_id)));
    for body in bodies {
        assert!(body.len() <= 2048);
        for secret in [
            "SECRET PROMPT",
            "SECRET OUTPUT",
            "SECRET ERROR",
            "SECRET-FROM-COMMAND",
            "SECRET-TO-COMMAND",
        ] {
            assert!(!body.contains(secret));
        }
    }
    db.close().await;
}

#[tokio::test]
async fn commands_are_exclusive_strict_and_separately_authorized() {
    let db = Db::new().await;
    let task = db.task(None, None, Some(TaskStatus::Queued)).await;
    let sender = Arc::new(Sender::default());
    let admin = handler(
        &db,
        sender.clone(),
        Cancellation::new(),
        AdminPermissionPolicy::new()
            .allow_user("@admin:x")
            .restrict_rooms(["!ops:x"]),
        8,
    );
    assert_eq!(
        admin
            .handle(&incoming(
                "$ordinary:x",
                "hello".into(),
                "@admin:x",
                "!ops:x"
            ))
            .await
            .unwrap(),
        AdminResult::Ordinary
    );
    for (id, body, user, room, expected) in [
        (
            "$deny:x",
            format!("/status {}", task.task_id),
            "@user:x",
            "!ops:x",
            "command=forbidden",
        ),
        (
            "$room:x",
            format!("/status {}", task.task_id),
            "@admin:x",
            "!other:x",
            "command=forbidden",
        ),
        (
            "$unknown:x",
            format!("/stat {}", task.task_id),
            "@admin:x",
            "!ops:x",
            "command=unknown",
        ),
        (
            "$extra:x",
            format!("/status {} extra", task.task_id),
            "@admin:x",
            "!ops:x",
            "command=invalid",
        ),
    ] {
        admin.handle(&incoming(id, body, user, room)).await.unwrap();
        assert_eq!(sender.bodies().last().unwrap(), expected);
    }
    db.close().await;
}

#[tokio::test]
async fn whitespace_prefixed_commands_never_reach_the_real_bus_route() {
    let db = Db::new().await;
    let live = db.task(None, None, Some(TaskStatus::Running)).await;
    let denied = db.task(None, None, Some(TaskStatus::Running)).await;
    let retry = db.task(None, None, Some(TaskStatus::Failed)).await;
    let sender = Arc::new(Sender::default());
    let cancellation = Cancellation::new();
    let admin = handler(
        &db,
        sender.clone(),
        cancellation.clone(),
        AdminPermissionPolicy::new().allow_user("@admin:x"),
        8,
    );
    let (bus, mut receivers) = MemoryBus::from_config(&routing_config(), 4);
    let route_policy = RoutePolicy::new().bind_room("!ops:x", "worker");
    let route_permissions = PermissionPolicy::new()
        .allow_user("@admin:x")
        .allow_user("@user:x");
    let mut dedup = EventDedup::new(8).unwrap();

    let cases = [
        incoming(
            "$space:x",
            format!(" /cancel {}", live.task_id),
            "@admin:x",
            "!ops:x",
        ),
        incoming(
            "$tab:x",
            format!("\t/cancel {}", denied.task_id),
            "@user:x",
            "!ops:x",
        ),
        incoming(
            "$newline:x",
            format!("\n/retry {}", retry.task_id),
            "@admin:x",
            "!ops:x",
        ),
    ];
    for event in &cases {
        let result = admin.handle(event).await.unwrap();
        if result == AdminResult::Ordinary {
            route_event(
                event,
                db.conversation,
                &route_policy,
                &route_permissions,
                bus.registry(),
                &bus,
                &mut dedup,
            )
            .await
            .unwrap();
        }
        assert_eq!(result, AdminResult::Replied);
    }

    assert!(receivers.tasks.try_recv().is_err());
    assert!(receivers.events.try_recv().is_err());
    assert!(cancellation.reason_for(live.task_id).is_some());
    assert!(cancellation.reason_for(denied.task_id).is_none());
    assert!(cancellation.reason_for(retry.task_id).is_none());
    assert_eq!(
        sender.bodies(),
        vec![
            "cancel_request=registered",
            "command=forbidden",
            "retry=unsupported"
        ]
    );
    db.close().await;
}

#[tokio::test]
async fn cancel_only_registers_nonterminal_tasks_and_replay_does_not_repeat_effect() {
    let db = Db::new().await;
    let live = db.task(None, None, Some(TaskStatus::Running)).await;
    let done = db.task(None, None, Some(TaskStatus::Completed)).await;
    let sender = Arc::new(Sender::default());
    let cancellation = Cancellation::new();
    let admin = handler(
        &db,
        sender.clone(),
        cancellation.clone(),
        AdminPermissionPolicy::new().allow_user("@admin:x"),
        8,
    );
    let cancel = incoming(
        "$cancel:x",
        format!("/cancel {}", live.task_id),
        "@admin:x",
        "!ops:x",
    );
    sender.fail_next();
    assert!(admin.handle(&cancel).await.is_err());
    assert!(cancellation.reason_for(live.task_id).is_some());
    admin.handle(&cancel).await.unwrap();
    assert_eq!(sender.bodies(), vec!["cancel_request=registered"]);
    admin
        .handle(&incoming(
            "$done:x",
            format!("/cancel {}", done.task_id),
            "@admin:x",
            "!ops:x",
        ))
        .await
        .unwrap();
    assert!(cancellation.reason_for(done.task_id).is_none());
    assert_eq!(
        sender.bodies().last().unwrap(),
        "cancel_request=already_terminal"
    );
    db.close().await;
}

#[tokio::test]
async fn retry_is_validated_but_has_no_side_effect() {
    let db = Db::new().await;
    let task = db.task(None, None, Some(TaskStatus::Failed)).await;
    let sender = Arc::new(Sender::default());
    let cancellation = Cancellation::new();
    let admin = handler(
        &db,
        sender.clone(),
        cancellation.clone(),
        AdminPermissionPolicy::new().allow_user("@admin:x"),
        1,
    );
    admin
        .handle(&incoming(
            "$retry:x",
            format!("/retry {}", task.task_id),
            "@admin:x",
            "!ops:x",
        ))
        .await
        .unwrap();
    assert_eq!(sender.bodies(), vec!["retry=unsupported"]);
    assert!(cancellation.reason_for(task.task_id).is_none());
    assert!(db.repo.child_tasks(task.task_id).await.unwrap().is_empty());
    admin
        .handle(&incoming(
            "$missing:x",
            format!("/retry {}", TaskId::generate()),
            "@admin:x",
            "!ops:x",
        ))
        .await
        .unwrap();
    assert_eq!(sender.bodies().last().unwrap(), "task=not_found");
    db.close().await;
}
