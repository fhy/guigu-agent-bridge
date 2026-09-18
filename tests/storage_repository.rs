//! T009 integration tests: the real `SqliteRepository` over a real SQLite pool
//! with the real embedded migrations.
//!
//! Every case drives the public `Repository` surface (or the one inherent
//! submission helper) and asserts on rows and returned models — never on SQL
//! strings. Raw SQL appears only where a test must plant a row the model layer
//! cannot express (a NULL-address `agents` row) or count what is actually stored.
//!
//! Each test owns a database file under the system temporary directory and
//! removes it (plus its `-wal`/`-shm` siblings) at the end; nothing is created
//! inside the repository. No test sleeps or waits on a timer.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;

use guigu_agent_bridge::bus::derive_endpoint_id;
use guigu_agent_bridge::models::{
    AgentEndpoint, AgentTask, Capability, Conversation, ConversationId, DeliveryId,
    EndpointAddress, EndpointId, EventId, ExternalRef, Message, MessageId, Priority, TaskEvent,
    TaskEventPayload, TaskId, TaskStatus, TransportType,
};
use guigu_agent_bridge::storage::{
    AckOutcome, Delivery, Repository, SqliteRepository, StorageError, connect, migrate,
};

const TS: &str = "2026-09-16T10:00:00.000000000Z";

fn ts() -> DateTime<Utc> {
    TS.parse().expect("valid timestamp")
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct TestDb {
    repository: SqliteRepository,
    pool: SqlitePool,
    path: PathBuf,
}

impl TestDb {
    async fn new(tag: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "guigu-storage-repository-{tag}-{}.db",
            uuid::Uuid::now_v7()
        ));
        let pool = connect(&path).await.expect("connect");
        migrate(&pool).await.expect("migrate");
        Self {
            repository: SqliteRepository::new(pool.clone()),
            pool,
            path,
        }
    }

    async fn cleanup(&self) {
        self.pool.close().await;
        remove_db_files(&self.path);
    }

    /// A raw read used only to assert what is physically stored.
    async fn count(&self, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&self.pool)
            .await
            .expect("count rows")
    }
}

fn remove_db_files(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut candidate = path.as_os_str().to_owned();
        candidate.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(candidate));
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn endpoint(agent_id: &str, enabled: bool) -> AgentEndpoint {
    AgentEndpoint {
        id: derive_endpoint_id(agent_id),
        transport: TransportType::Acp,
        address: EndpointAddress::Acp {
            command: format!("{agent_id}-acp"),
            args: vec!["--stdio".into()],
        },
        enabled,
        capabilities: vec![Capability::new("chat")],
    }
}

fn local_conversation() -> Conversation {
    Conversation {
        id: ConversationId::generate(),
        participants: vec![EndpointId::generate()],
        external_ref: None,
    }
}

fn external_conversation(external_id: &str, thread_ref: Option<&str>) -> Conversation {
    Conversation {
        id: ConversationId::generate(),
        participants: vec![EndpointId::generate(), EndpointId::generate()],
        external_ref: Some(ExternalRef {
            transport: TransportType::Matrix,
            external_id: external_id.into(),
            thread_ref: thread_ref.map(str::to_owned),
        }),
    }
}

fn task_in(conversation: ConversationId, text: &str) -> AgentTask {
    let task_id = TaskId::generate();
    AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: None,
        from_agent: EndpointId::generate(),
        to_agent: endpoint("worker", true).id,
        conversation_id: conversation,
        reply_to: None,
        text: text.into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    }
}

fn child_task(parent: &AgentTask) -> AgentTask {
    AgentTask {
        task_id: TaskId::generate(),
        root_task_id: parent.root_task_id,
        parent_task_id: Some(parent.task_id),
        text: "child".into(),
        depth: parent.depth + 1,
        hops: parent.hops + 1,
        ..parent.clone()
    }
}

fn event(task_id: TaskId, seq: u64, status: TaskStatus, payload: TaskEventPayload) -> TaskEvent {
    TaskEvent {
        id: EventId::generate(),
        task_id,
        seq,
        status,
        timestamp: ts(),
        payload,
    }
}

/// A valid first event for `task`.
fn queued(task_id: TaskId) -> TaskEvent {
    event(task_id, 1, TaskStatus::Queued, TaskEventPayload::Queued)
}

fn delivery_for(task: &AgentTask, attempt: u32) -> Delivery {
    Delivery::new(
        DeliveryId::generate(),
        task.task_id,
        attempt,
        task.to_agent,
        ts(),
    )
}

/// Insert a conversation and return it (the foreign key every task needs).
async fn seeded_conversation(db: &TestDb) -> Conversation {
    let conversation = local_conversation();
    db.repository
        .insert_conversation(&conversation)
        .await
        .expect("seed conversation");
    conversation
}

/// Insert a task together with its `Queued` event, then return it.
async fn seeded_task(db: &TestDb, text: &str) -> AgentTask {
    let conversation = seeded_conversation(db).await;
    let task = task_in(conversation.id, text);
    db.repository
        .insert_task_and_event(&task, &queued(task.task_id))
        .await
        .expect("seed task");
    task
}

/// Insert the endpoint every delivery foreign-keys to.
async fn seeded_endpoint(db: &TestDb) -> AgentEndpoint {
    let endpoint = endpoint("worker", true);
    db.repository
        .upsert_agent("worker", &endpoint)
        .await
        .expect("seed endpoint");
    endpoint
}

// ---------------------------------------------------------------------------
// agents
// ---------------------------------------------------------------------------

#[tokio::test]
async fn agents_round_trip_and_upsert_replaces_in_place() {
    let db = TestDb::new("agents").await;
    let endpoint = endpoint("worker", true);

    db.repository
        .upsert_agent("worker", &endpoint)
        .await
        .expect("upsert");
    assert_eq!(
        db.repository.get_agent(endpoint.id).await.expect("get"),
        Some(endpoint.clone())
    );
    assert_eq!(
        db.repository.agents().await.expect("agents"),
        vec![endpoint.clone()]
    );
    assert_eq!(db.count("agents").await, 1);

    let disabled = AgentEndpoint {
        enabled: false,
        capabilities: Vec::new(),
        ..endpoint.clone()
    };
    db.repository
        .upsert_agent("worker", &disabled)
        .await
        .expect("re-upsert");

    let stored = db
        .repository
        .get_agent(endpoint.id)
        .await
        .expect("get")
        .expect("still present");
    assert_eq!(stored, disabled, "the snapshot follows configuration");
    assert_eq!(db.count("agents").await, 1, "upsert never duplicates a row");

    assert_eq!(
        db.repository
            .get_agent(EndpointId::generate())
            .await
            .expect("get"),
        None
    );

    db.cleanup().await;
}

#[tokio::test]
async fn an_agent_id_remapped_to_another_identity_is_reported() {
    let db = TestDb::new("agents-identity").await;
    let endpoint = endpoint("worker", true);
    db.repository
        .upsert_agent("worker", &endpoint)
        .await
        .expect("upsert");

    // ADR-003 derives the identity from the id, so this pair cannot occur for a
    // correct caller; storing it silently would corrupt the snapshot.
    let mismatched = AgentEndpoint {
        id: EndpointId::generate(),
        ..endpoint
    };
    assert!(matches!(
        db.repository.upsert_agent("worker", &mismatched).await,
        Err(StorageError::IntegrityViolation { .. })
    ));
    assert_eq!(db.count("agents").await, 1);

    db.cleanup().await;
}

#[tokio::test]
async fn a_declared_but_unaddressable_agent_row_is_not_surfaced() {
    let db = TestDb::new("agents-null-address").await;

    sqlx::query(
        "INSERT INTO agents (endpoint_id, agent_id, transport, enabled, address_json, \
         capabilities_json) VALUES (?, ?, 'matrix', 1, NULL, '[]')",
    )
    .bind(derive_endpoint_id("matrix-bot").to_string())
    .bind("matrix-bot")
    .execute(&db.pool)
    .await
    .expect("the schema admits a NULL address");

    assert!(
        db.repository.agents().await.expect("agents").is_empty(),
        "a row the model cannot represent must not be surfaced"
    );
    assert_eq!(
        db.repository
            .get_agent(derive_endpoint_id("matrix-bot"))
            .await
            .expect("get"),
        None
    );
    assert_eq!(
        db.count("agents").await,
        1,
        "the row stays in the table; only the model layer hides it"
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// conversations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn conversations_round_trip_for_every_row_shape() {
    let db = TestDb::new("conversations").await;
    let local = local_conversation();
    let unthreaded = external_conversation("!room:matrix.org", None);
    let threaded = external_conversation("!room:matrix.org", Some("$root:matrix.org"));

    for conversation in [&local, &unthreaded, &threaded] {
        db.repository
            .insert_conversation(conversation)
            .await
            .expect("insert");
        assert_eq!(
            db.repository
                .get_conversation(conversation.id)
                .await
                .expect("get"),
            Some(conversation.clone())
        );
    }

    assert_eq!(
        db.repository
            .conversation_by_external_ref(unthreaded.external_ref.as_ref().expect("ref"))
            .await
            .expect("by ref"),
        Some(unthreaded.clone())
    );
    assert_eq!(
        db.repository
            .conversation_by_external_ref(threaded.external_ref.as_ref().expect("ref"))
            .await
            .expect("by ref"),
        Some(threaded.clone()),
        "a room and each of its threads are distinct conversations"
    );
    assert_eq!(
        db.repository
            .get_conversation(unthreaded.id)
            .await
            .expect("get"),
        Some(unthreaded.clone())
    );

    // A local conversation is not reachable by reference, and an unknown one is
    // simply absent.
    let other = external_conversation("!elsewhere:matrix.org", None);
    assert_eq!(
        db.repository
            .conversation_by_external_ref(other.external_ref.as_ref().expect("ref"))
            .await
            .expect("by ref"),
        None
    );
    assert_eq!(
        db.repository
            .get_conversation(ConversationId::generate())
            .await
            .expect("get"),
        None
    );

    db.cleanup().await;
}

#[tokio::test]
async fn local_conversations_coexist_without_limit() {
    let db = TestDb::new("conversations-local").await;

    let locals: Vec<Conversation> = (0..3).map(|_| local_conversation()).collect();
    for conversation in &locals {
        db.repository
            .insert_conversation(conversation)
            .await
            .expect("insert local");
    }
    assert_eq!(db.count("conversations").await, 3);

    db.cleanup().await;
}

#[tokio::test]
async fn a_replayed_conversation_is_a_replay_and_a_competing_reference_is_a_duplicate() {
    let db = TestDb::new("conversations-duplicate").await;
    let conversation = external_conversation("!room:matrix.org", None);

    db.repository
        .insert_conversation(&conversation)
        .await
        .expect("insert");
    db.repository
        .insert_conversation(&conversation)
        .await
        .expect("an identical replay is Ok");
    assert_eq!(db.count("conversations").await, 1);

    // A different conversation claiming the same external reference loses: the
    // caller must read the winner back instead of assuming its id was stored.
    let competitor = external_conversation("!room:matrix.org", None);
    assert!(matches!(
        db.repository.insert_conversation(&competitor).await,
        Err(StorageError::Duplicate { .. })
    ));
    assert_eq!(db.count("conversations").await, 1);
    assert_eq!(
        db.repository
            .get_conversation(competitor.id)
            .await
            .expect("get"),
        None,
        "the losing id was not persisted"
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// messages
// ---------------------------------------------------------------------------

#[tokio::test]
async fn messages_round_trip_with_reply_chain_and_metadata() {
    let db = TestDb::new("messages").await;
    let conversation = seeded_conversation(&db).await;
    let sender = EndpointId::generate();

    let first = Message {
        id: MessageId::generate(),
        conversation: conversation.id,
        sender,
        recipient: EndpointId::generate(),
        body: "first".into(),
        reply_to: None,
        metadata: [("k".to_owned(), "v".to_owned())].into_iter().collect(),
    };
    let second = Message {
        id: MessageId::generate(),
        conversation: conversation.id,
        sender,
        recipient: EndpointId::generate(),
        body: "second".into(),
        reply_to: Some(first.id),
        metadata: Default::default(),
    };

    for message in [&first, &second] {
        db.repository.insert_message(message).await.expect("insert");
        assert_eq!(
            db.repository.get_message(message.id).await.expect("get"),
            Some(message.clone())
        );
    }

    let mut expected = vec![first.clone(), second.clone()];
    expected.sort_by_key(|message| message.id.to_string());
    assert_eq!(
        db.repository
            .messages_in_conversation(conversation.id)
            .await
            .expect("messages"),
        expected,
        "a conversation's messages come back in a stable order"
    );
    assert_eq!(
        db.repository
            .messages_in_conversation(ConversationId::generate())
            .await
            .expect("messages"),
        Vec::new()
    );

    db.cleanup().await;
}

#[tokio::test]
async fn a_replayed_message_is_a_replay_and_a_conflicting_body_is_a_duplicate() {
    let db = TestDb::new("messages-duplicate").await;
    let conversation = seeded_conversation(&db).await;
    let message = Message {
        id: MessageId::generate(),
        conversation: conversation.id,
        sender: EndpointId::generate(),
        recipient: EndpointId::generate(),
        body: "original".into(),
        reply_to: None,
        metadata: Default::default(),
    };

    db.repository
        .insert_message(&message)
        .await
        .expect("insert");
    db.repository
        .insert_message(&message)
        .await
        .expect("an identical replay is Ok");

    let edited = Message {
        body: "rewritten".into(),
        ..message.clone()
    };
    assert!(matches!(
        db.repository.insert_message(&edited).await,
        Err(StorageError::Duplicate { .. })
    ));
    assert_eq!(
        db.repository.get_message(message.id).await.expect("get"),
        Some(message),
        "the stored row is never rewritten by a duplicate"
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// tasks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tasks_round_trip_including_a_self_rooted_root() {
    let db = TestDb::new("tasks").await;
    let conversation = seeded_conversation(&db).await;
    let mut task = task_in(conversation.id, "do the thing");
    task.deadline = Some(ts());
    task.priority = Priority::new(10).expect("in range");
    task.version = 42;

    db.repository.insert_task(&task).await.expect("insert");

    assert_eq!(
        db.repository.get_task(task.task_id).await.expect("get"),
        Some(task.clone()),
        "a root task is its own root and survives the round trip"
    );
    assert_eq!(
        db.repository
            .get_task(TaskId::generate())
            .await
            .expect("get"),
        None
    );

    db.cleanup().await;
}

#[tokio::test]
async fn child_tasks_returns_only_direct_children_in_order() {
    let db = TestDb::new("tasks-children").await;
    let parent = seeded_task(&db, "parent").await;
    let first = child_task(&parent);
    let second = child_task(&parent);
    let grandchild = child_task(&first);

    for task in [&first, &second, &grandchild] {
        db.repository.insert_task(task).await.expect("insert");
    }

    let mut expected = vec![first.task_id, second.task_id];
    expected.sort_by_key(|id| id.to_string());
    assert_eq!(
        db.repository
            .child_tasks(parent.task_id)
            .await
            .expect("children"),
        expected,
        "only direct children, in a stable order"
    );
    assert_eq!(
        db.repository
            .child_tasks(grandchild.task_id)
            .await
            .expect("children"),
        Vec::new()
    );

    db.cleanup().await;
}

#[tokio::test]
async fn a_replayed_task_is_a_replay_and_a_conflicting_task_is_a_duplicate() {
    let db = TestDb::new("tasks-duplicate").await;
    let conversation = seeded_conversation(&db).await;
    let task = task_in(conversation.id, "original");

    db.repository.insert_task(&task).await.expect("insert");
    db.repository
        .insert_task(&task)
        .await
        .expect("an identical replay is Ok");
    assert_eq!(db.count("tasks").await, 1);

    let rewritten = AgentTask {
        text: "rewritten".into(),
        version: 7,
        ..task.clone()
    };
    assert!(matches!(
        db.repository.insert_task(&rewritten).await,
        Err(StorageError::Duplicate { .. })
    ));
    assert_eq!(
        db.repository.get_task(task.task_id).await.expect("get"),
        Some(task),
        "the first writer wins: a replay never rewrites the row"
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// write order and the atomic submission helper
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_event_for_an_unknown_task_is_rejected_and_not_persisted() {
    let db = TestDb::new("order").await;
    let orphan_event = queued(TaskId::generate());

    assert!(matches!(
        db.repository.append_event(&orphan_event).await,
        Err(StorageError::IntegrityViolation { .. })
    ));
    assert_eq!(
        db.count("task_events").await,
        0,
        "the foreign key stops the event instead of dropping it silently later"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn insert_task_and_event_writes_both_or_neither() {
    let db = TestDb::new("order-atomic").await;
    let conversation = seeded_conversation(&db).await;

    // Success: both rows, readable through the frozen queries.
    let task = task_in(conversation.id, "atomic");
    let first = queued(task.task_id);
    db.repository
        .insert_task_and_event(&task, &first)
        .await
        .expect("insert");
    assert_eq!(db.count("tasks").await, 1);
    assert_eq!(db.count("task_events").await, 1);
    assert_eq!(
        db.repository
            .latest_event(task.task_id)
            .await
            .expect("latest"),
        Some(first.clone())
    );

    // An identical replay changes nothing.
    db.repository
        .insert_task_and_event(&task, &first)
        .await
        .expect("replay");
    assert_eq!(db.count("tasks").await, 1);
    assert_eq!(db.count("task_events").await, 1);

    db.cleanup().await;
}

#[tokio::test]
async fn insert_task_and_event_rejects_a_mismatched_event_before_writing() {
    let db = TestDb::new("order-mismatch").await;
    let conversation = seeded_conversation(&db).await;
    let task = task_in(conversation.id, "mismatch");
    let foreign_event = queued(TaskId::generate());

    assert!(matches!(
        db.repository
            .insert_task_and_event(&task, &foreign_event)
            .await,
        Err(StorageError::Malformed { .. })
    ));
    assert_eq!(db.count("tasks").await, 0, "nothing is written");
    assert_eq!(db.count("task_events").await, 0);

    db.cleanup().await;
}

#[tokio::test]
async fn insert_task_and_event_rolls_back_when_the_event_conflicts() {
    let db = TestDb::new("order-rollback").await;
    let existing = seeded_task(&db, "existing").await;

    // A fresh task whose first event reuses an identity that is already stored
    // under another task: the task insert succeeds, the event insert conflicts,
    // and the transaction must undo the task row.
    let seeded_event = db
        .repository
        .latest_event(existing.task_id)
        .await
        .expect("latest")
        .expect("seeded");
    let fresh = child_task(&existing);
    let collision = TaskEvent {
        id: seeded_event.id,
        task_id: fresh.task_id,
        seq: 1,
        status: TaskStatus::Queued,
        timestamp: ts(),
        payload: TaskEventPayload::Queued,
    };

    assert!(matches!(
        db.repository
            .insert_task_and_event(&fresh, &collision)
            .await,
        Err(StorageError::Duplicate { .. })
    ));
    assert_eq!(
        db.repository.get_task(fresh.task_id).await.expect("get"),
        None,
        "the task row rolled back with the failed event"
    );
    assert_eq!(db.count("tasks").await, 1, "only the seeded task remains");

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// task_events
// ---------------------------------------------------------------------------

#[tokio::test]
async fn events_come_back_in_seq_order_regardless_of_arrival_order() {
    let db = TestDb::new("events-order").await;
    let task = seeded_task(&db, "ordering").await;

    // Arrival order is deliberately not seq order (T005 Q1=A).
    let third = event(
        task.task_id,
        3,
        TaskStatus::Running,
        TaskEventPayload::Running { started_at: ts() },
    );
    let second = event(
        task.task_id,
        2,
        TaskStatus::Dispatched,
        TaskEventPayload::Dispatched {
            delivery_id: DeliveryId::generate(),
            attempt: 1,
        },
    );
    for event in [&third, &second] {
        db.repository.append_event(event).await.expect("append");
    }

    let stored = db
        .repository
        .events_for_task(task.task_id)
        .await
        .expect("events");
    assert_eq!(
        stored.iter().map(|event| event.seq).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        db.repository
            .latest_event(task.task_id)
            .await
            .expect("latest"),
        Some(third)
    );
    assert_eq!(
        db.repository
            .latest_event(TaskId::generate())
            .await
            .expect("latest"),
        None
    );

    db.cleanup().await;
}

#[tokio::test]
async fn a_replayed_event_is_a_replay_and_a_conflicting_position_is_a_duplicate() {
    let db = TestDb::new("events-duplicate").await;
    let task = seeded_task(&db, "duplicates").await;
    // Re-append the *stored* event: the seeding already wrote `Queued` at seq 1,
    // so a freshly generated event would be a different event at the same
    // position (which is a conflict, not a replay).
    let queued = db
        .repository
        .latest_event(task.task_id)
        .await
        .expect("latest")
        .expect("seeded");

    db.repository
        .append_event(&queued)
        .await
        .expect("an identical replay is Ok");
    assert_eq!(db.count("task_events").await, 1);

    // Same position, different event: silently accepting this would rewrite
    // per-task history, so it must be visible.
    let impostor = event(
        task.task_id,
        1,
        TaskStatus::Failed,
        TaskEventPayload::Failed {
            error: "not the queued event".into(),
        },
    );
    assert!(matches!(
        db.repository.append_event(&impostor).await,
        Err(StorageError::Duplicate { .. })
    ));

    // Same event identity, different position.
    let moved = TaskEvent {
        seq: 2,
        ..queued.clone()
    };
    assert!(matches!(
        db.repository.append_event(&moved).await,
        Err(StorageError::Duplicate { .. })
    ));

    assert_eq!(db.count("task_events").await, 1);
    assert_eq!(
        db.repository
            .events_for_task(task.task_id)
            .await
            .expect("events"),
        vec![queued]
    );

    db.cleanup().await;
}

#[tokio::test]
async fn unfinished_tasks_covers_orphans_and_excludes_every_terminal_state() {
    let db = TestDb::new("unfinished").await;

    // Orphan: a task row with no events at all.
    let conversation = seeded_conversation(&db).await;
    let orphan = task_in(conversation.id, "orphan");
    db.repository
        .insert_task(&orphan)
        .await
        .expect("insert orphan");

    let in_progress = [
        (TaskStatus::Queued, TaskEventPayload::Queued),
        (
            TaskStatus::Dispatched,
            TaskEventPayload::Dispatched {
                delivery_id: DeliveryId::generate(),
                attempt: 1,
            },
        ),
        (
            TaskStatus::Running,
            TaskEventPayload::Running { started_at: ts() },
        ),
    ];
    let mut expected = vec![orphan.task_id];
    for (status, payload) in in_progress {
        let task = seeded_task(&db, "in progress").await;
        db.repository
            .append_event(&event(task.task_id, 2, status, payload))
            .await
            .expect("append");
        expected.push(task.task_id);
    }

    let terminal = [
        (
            TaskStatus::Completed,
            TaskEventPayload::Completed {
                output: "done".into(),
            },
        ),
        (
            TaskStatus::Failed,
            TaskEventPayload::Failed {
                error: "failed".into(),
            },
        ),
        (
            TaskStatus::TimedOut,
            TaskEventPayload::TimedOut { deadline: ts() },
        ),
        (
            TaskStatus::Cancelled,
            TaskEventPayload::Cancelled {
                reason: "stopped".into(),
            },
        ),
    ];
    for (status, payload) in terminal {
        let task = seeded_task(&db, "terminal").await;
        db.repository
            .append_event(&event(task.task_id, 2, status, payload))
            .await
            .expect("append");
    }

    expected.sort_by_key(|id| id.to_string());
    assert_eq!(
        db.repository.unfinished_tasks().await.expect("unfinished"),
        expected,
        "terminal tasks are absent, orphans and in-flight tasks are present"
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// optimistic concurrency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compare_and_increment_version_is_conditional() {
    let db = TestDb::new("occ").await;
    let task = seeded_task(&db, "occ").await;

    assert_eq!(
        db.repository
            .compare_and_increment_version(task.task_id, 0)
            .await
            .expect("first update"),
        1
    );
    assert_eq!(
        db.repository
            .compare_and_increment_version(task.task_id, 1)
            .await
            .expect("second update"),
        2
    );

    // A stale expected version is a conditional-update conflict, not corruption.
    assert!(matches!(
        db.repository
            .compare_and_increment_version(task.task_id, 0)
            .await,
        Err(StorageError::Duplicate { .. })
    ));
    assert_eq!(
        db.repository
            .get_task(task.task_id)
            .await
            .expect("get")
            .expect("present")
            .version,
        2,
        "a failed compare leaves the version untouched"
    );

    assert!(matches!(
        db.repository
            .compare_and_increment_version(TaskId::generate(), 0)
            .await,
        Err(StorageError::NotFound { .. })
    ));

    db.cleanup().await;
}

#[tokio::test]
async fn the_version_ceiling_is_reported_instead_of_overflowing() {
    let db = TestDb::new("occ-ceiling").await;
    let conversation = seeded_conversation(&db).await;
    let mut task = task_in(conversation.id, "ceiling");
    task.version = i64::MAX as u64;
    db.repository.insert_task(&task).await.expect("insert");

    assert!(matches!(
        db.repository
            .compare_and_increment_version(task.task_id, i64::MAX as u64)
            .await,
        Err(StorageError::OutOfRange { .. })
    ));

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// deliveries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deliveries_round_trip_and_the_attempt_key_is_idempotent() {
    let db = TestDb::new("deliveries").await;
    let endpoint = seeded_endpoint(&db).await;
    let task = seeded_task(&db, "delivery").await;
    let delivery = delivery_for(&task, 1);

    db.repository
        .record_delivery(&delivery)
        .await
        .expect("record");
    assert_eq!(
        db.repository
            .get_delivery(delivery.delivery_id())
            .await
            .expect("get"),
        Some(delivery.clone())
    );
    db.repository
        .record_delivery(&delivery)
        .await
        .expect("an identical replay is Ok");
    assert_eq!(db.count("deliveries").await, 1);

    // Same attempt, different delivery: a genuine conflict.
    let competing = Delivery::new(DeliveryId::generate(), task.task_id, 1, endpoint.id, ts());
    assert!(matches!(
        db.repository.record_delivery(&competing).await,
        Err(StorageError::Duplicate { .. })
    ));
    assert_eq!(db.count("deliveries").await, 1);

    // A retry uses a new attempt, which is a different position in the key.
    let retry = delivery_for(&task, 2);
    db.repository
        .record_delivery(&retry)
        .await
        .expect("a new attempt is accepted");
    assert_eq!(db.count("deliveries").await, 2);

    assert_eq!(
        db.repository
            .get_delivery(DeliveryId::generate())
            .await
            .expect("get"),
        None
    );

    db.cleanup().await;
}

#[tokio::test]
async fn a_delivery_to_an_unsynchronised_endpoint_is_rejected() {
    let db = TestDb::new("deliveries-fk").await;
    let task = seeded_task(&db, "delivery").await;

    assert!(matches!(
        db.repository.record_delivery(&delivery_for(&task, 1)).await,
        Err(StorageError::IntegrityViolation { .. })
    ));
    assert_eq!(db.count("deliveries").await, 0);

    db.cleanup().await;
}

#[tokio::test]
async fn acknowledge_delivery_is_idempotent_and_reports_all_three_states() {
    let db = TestDb::new("deliveries-ack").await;
    let _endpoint = seeded_endpoint(&db).await;
    let task = seeded_task(&db, "ack").await;
    let delivery = delivery_for(&task, 1);
    db.repository
        .record_delivery(&delivery)
        .await
        .expect("record");

    let first_ack = ts() + chrono::Duration::seconds(5);
    assert_eq!(
        db.repository
            .acknowledge_delivery(delivery.delivery_id(), first_ack)
            .await
            .expect("first ack"),
        AckOutcome::Recorded
    );
    assert_eq!(
        db.repository
            .acknowledge_delivery(
                delivery.delivery_id(),
                first_ack + chrono::Duration::seconds(5)
            )
            .await
            .expect("replay"),
        AckOutcome::AlreadyAcknowledged
    );
    assert_eq!(
        db.repository
            .get_delivery(delivery.delivery_id())
            .await
            .expect("get")
            .expect("present")
            .acknowledged_at(),
        Some(first_ack),
        "a replayed acknowledgement never rewrites the first timestamp"
    );

    assert!(matches!(
        db.repository
            .acknowledge_delivery(DeliveryId::generate(), first_ack)
            .await,
        Err(StorageError::NotFound { .. })
    ));

    // The schema refuses an acknowledgement that precedes the dispatch.
    let newer = delivery_for(&task, 2);
    db.repository.record_delivery(&newer).await.expect("record");
    assert!(matches!(
        db.repository
            .acknowledge_delivery(newer.delivery_id(), ts() - chrono::Duration::seconds(1))
            .await,
        Err(StorageError::IntegrityViolation { .. })
    ));

    db.cleanup().await;
}

#[tokio::test]
async fn unacknowledged_and_awaiting_outcome_split_on_acknowledgement_and_terminal_state() {
    let db = TestDb::new("deliveries-recovery").await;
    let _endpoint = seeded_endpoint(&db).await;
    let task = seeded_task(&db, "recovery").await;

    let pending = delivery_for(&task, 1);
    let acknowledged = delivery_for(&task, 2);
    db.repository
        .record_delivery(&pending)
        .await
        .expect("record");
    db.repository
        .record_delivery(&acknowledged)
        .await
        .expect("record");
    db.repository
        .append_event(&event(
            task.task_id,
            2,
            TaskStatus::Dispatched,
            TaskEventPayload::Dispatched {
                delivery_id: acknowledged.delivery_id(),
                attempt: 2,
            },
        ))
        .await
        .expect("append");

    assert_eq!(
        db.repository
            .unacknowledged_deliveries()
            .await
            .expect("unacknowledged"),
        vec![pending.clone(), acknowledged.clone()],
        "nothing is acknowledged yet"
    );
    assert_eq!(
        db.repository
            .deliveries_awaiting_outcome()
            .await
            .expect("awaiting"),
        Vec::new(),
        "an unacknowledged delivery is retry input, not recovery input"
    );

    let ack_at = ts() + chrono::Duration::seconds(1);
    db.repository
        .acknowledge_delivery(acknowledged.delivery_id(), ack_at)
        .await
        .expect("ack");
    assert_eq!(
        db.repository
            .unacknowledged_deliveries()
            .await
            .expect("unacknowledged"),
        vec![pending.clone()],
        "an acknowledged delivery leaves the retry input"
    );
    let awaiting = db
        .repository
        .deliveries_awaiting_outcome()
        .await
        .expect("awaiting");
    assert_eq!(awaiting.len(), 1);
    assert_eq!(awaiting[0].delivery_id(), acknowledged.delivery_id());
    assert!(awaiting[0].is_acknowledged());

    // A terminal latest event retires it from recovery input; the row itself is
    // never deleted, and the unacknowledged one is untouched.
    db.repository
        .append_event(&event(
            task.task_id,
            3,
            TaskStatus::Completed,
            TaskEventPayload::Completed {
                output: "done".into(),
            },
        ))
        .await
        .expect("append");
    assert_eq!(
        db.repository
            .deliveries_awaiting_outcome()
            .await
            .expect("awaiting"),
        Vec::new()
    );
    assert_eq!(
        db.repository
            .unacknowledged_deliveries()
            .await
            .expect("unacknowledged"),
        vec![pending]
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// trait-object compatibility
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_method_is_callable_through_a_trait_object() {
    let db = TestDb::new("trait-object").await;
    let repository: Arc<dyn Repository> = Arc::new(SqliteRepository::new(db.pool.clone()));
    let conversation = seeded_conversation(&db).await;
    let task = task_in(conversation.id, "object safe");
    let endpoint = endpoint("worker", true);

    repository
        .upsert_agent("worker", &endpoint)
        .await
        .expect("upsert_agent");
    repository.get_agent(endpoint.id).await.expect("get_agent");
    repository.agents().await.expect("agents");
    repository
        .insert_conversation(&local_conversation())
        .await
        .expect("insert_conversation");
    repository
        .get_conversation(conversation.id)
        .await
        .expect("get_conversation");
    repository
        .conversation_by_external_ref(&ExternalRef {
            transport: TransportType::Matrix,
            external_id: "!room:matrix.org".into(),
            thread_ref: None,
        })
        .await
        .expect("conversation_by_external_ref");
    repository.insert_task(&task).await.expect("insert_task");
    repository.get_task(task.task_id).await.expect("get_task");
    repository
        .child_tasks(task.task_id)
        .await
        .expect("child_tasks");
    repository
        .unfinished_tasks()
        .await
        .expect("unfinished_tasks");
    repository
        .append_event(&queued(task.task_id))
        .await
        .expect("append_event");
    repository
        .events_for_task(task.task_id)
        .await
        .expect("events_for_task");
    repository
        .latest_event(task.task_id)
        .await
        .expect("latest_event");
    repository
        .compare_and_increment_version(task.task_id, 0)
        .await
        .expect("compare_and_increment_version");
    let delivery = delivery_for(&task, 1);
    repository
        .record_delivery(&delivery)
        .await
        .expect("record_delivery");
    repository
        .acknowledge_delivery(delivery.delivery_id(), ts() + chrono::Duration::seconds(1))
        .await
        .expect("acknowledge_delivery");
    repository
        .get_delivery(delivery.delivery_id())
        .await
        .expect("get_delivery");
    repository
        .unacknowledged_deliveries()
        .await
        .expect("unacknowledged_deliveries");
    repository
        .deliveries_awaiting_outcome()
        .await
        .expect("deliveries_awaiting_outcome");

    let message = Message {
        id: MessageId::generate(),
        conversation: conversation.id,
        sender: EndpointId::generate(),
        recipient: EndpointId::generate(),
        body: "hi".into(),
        reply_to: None,
        metadata: Default::default(),
    };
    repository
        .insert_message(&message)
        .await
        .expect("insert_message");
    repository
        .get_message(message.id)
        .await
        .expect("get_message");
    repository
        .messages_in_conversation(conversation.id)
        .await
        .expect("messages_in_conversation");

    assert_eq!(Arc::strong_count(&repository), 1);
    db.cleanup().await;
}

/// A stray row must never appear because of a failed write.
#[tokio::test]
async fn a_rejected_write_leaves_every_table_untouched() {
    let db = TestDb::new("no-torn-writes").await;
    let task = seeded_task(&db, "torn");
    drop(task);

    let before: Vec<(&str, i64)> = {
        let mut counts = Vec::new();
        for table in [
            "agents",
            "conversations",
            "messages",
            "tasks",
            "task_events",
            "deliveries",
        ] {
            counts.push((table, db.count(table).await));
        }
        counts
    };

    let _ = db
        .repository
        .append_event(&queued(TaskId::generate()))
        .await;
    let _ = db
        .repository
        .record_delivery(&Delivery::new(
            DeliveryId::generate(),
            TaskId::generate(),
            1,
            EndpointId::generate(),
            ts(),
        ))
        .await;

    for (table, expected) in before {
        assert_eq!(db.count(table).await, expected, "{table} changed");
    }

    db.cleanup().await;
}
