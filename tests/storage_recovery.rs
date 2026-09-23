//! T009 restart-recovery tests: what survives a process restart, and what the
//! recovery primitives report about it.
//!
//! "Restart" is a real one: the pool is closed and reopened against the *same*
//! database file, with the migrations re-applied. Nothing is simulated in memory.
//! Every database file lives under the system temporary directory and is removed
//! at the end of its test.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;

use guigu_agent_bridge::bus::{EndpointRegistry, derive_endpoint_id};
use guigu_agent_bridge::config::{Config, load_from_str_with_env};
use guigu_agent_bridge::models::AgentEndpoint;
use guigu_agent_bridge::models::{
    AgentTask, Conversation, ConversationId, DeliveryId, EndpointAddress, EndpointId, EventId,
    ExternalRef, Priority, TaskEvent, TaskEventPayload, TaskId, TaskStatus, TransportType,
};
use guigu_agent_bridge::storage::{
    Delivery, Repository, SqliteRepository, StorageError, connect, migrate, plan_recovery,
    sync_agents,
};

const TS: &str = "2026-09-16T10:00:00.000000000Z";

/// `worker` is addressable (`acp`); `matrix-bot` is declared but has no derivable
/// address, so it must never reach a stored `agents` row.
const CONFIG: &str = r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
args = ["--stdio"]
workspace = "/tmp"
enabled = true

[agents.matrix-bot]
transport = "matrix"
enabled = true
"#;

fn ts() -> DateTime<Utc> {
    TS.parse().expect("valid timestamp")
}

fn load(toml: &str) -> Config {
    let mut env = BTreeMap::new();
    env.insert("HOME".to_string(), "/home/tester".to_string());
    load_from_str_with_env(toml, &env).expect("test config must be valid")
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
            "guigu-storage-recovery-{tag}-{}.db",
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

    /// Close the pool and reopen the same file, as a restarted process would.
    async fn restart(self) -> Self {
        self.pool.close().await;
        let pool = connect(&self.path).await.expect("reopen");
        migrate(&pool).await.expect("migrate the reopened file");
        Self {
            repository: SqliteRepository::new(pool.clone()),
            pool,
            path: self.path,
        }
    }

    async fn cleanup(&self) {
        self.pool.close().await;
        remove_db_files(&self.path);
    }

    async fn count(&self, table: &str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
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

fn worker_endpoint() -> EndpointId {
    derive_endpoint_id("worker")
}

fn registry() -> EndpointRegistry {
    EndpointRegistry::from_config(&load(CONFIG))
}

fn agent_endpoint() -> AgentEndpoint {
    AgentEndpoint {
        id: worker_endpoint(),
        transport: TransportType::Acp,
        address: EndpointAddress::Acp {
            command: "worker-acp".into(),
            args: vec!["--stdio".into()],
        },
        enabled: true,
        capabilities: Vec::new(),
    }
}

fn task(text: &str, conversation: ConversationId) -> AgentTask {
    let task_id = TaskId::generate();
    AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: None,
        from_agent: EndpointId::generate(),
        to_agent: worker_endpoint(),
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

fn queued(task_id: TaskId) -> TaskEvent {
    event(task_id, 1, TaskStatus::Queued, TaskEventPayload::Queued)
}

async fn insert_task(db: &TestDb, text: &str) -> AgentTask {
    let conversation = Conversation {
        id: ConversationId::generate(),
        participants: vec![EndpointId::generate()],
        external_ref: None,
    };
    db.repository
        .insert_conversation(&conversation)
        .await
        .expect("conversation");
    let task = task(text, conversation.id);
    db.repository
        .insert_task_and_event(&task, &queued(task.task_id))
        .await
        .expect("task");
    task
}

// ---------------------------------------------------------------------------
// R1: a restart preserves state and splits the recovery inputs correctly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_restart_keeps_finished_tasks_out_and_acknowledged_deliveries_in() {
    let db = TestDb::new("restart").await;
    sync_agents(&db.repository, &registry())
        .await
        .expect("sync agents");

    // A task that finished before the restart: it must not be re-run.
    let finished = insert_task(&db, "finished").await;
    db.repository
        .append_event(&event(
            finished.task_id,
            2,
            TaskStatus::Completed,
            TaskEventPayload::Completed {
                output: "done".into(),
            },
        ))
        .await
        .expect("completed");

    // A task that was still in flight when the process stopped.
    let in_flight = insert_task(&db, "in flight").await;
    db.repository
        .append_event(&event(
            in_flight.task_id,
            2,
            TaskStatus::Dispatched,
            TaskEventPayload::Dispatched {
                delivery_id: DeliveryId::generate(),
                attempt: 1,
            },
        ))
        .await
        .expect("dispatched");

    // A delivery that was acknowledged but whose task never reached a terminal
    // state, and one that was never acknowledged.
    let awaiting = Delivery::new(
        DeliveryId::generate(),
        in_flight.task_id,
        1,
        worker_endpoint(),
        ts(),
    );
    let never_acknowledged = Delivery::new(
        DeliveryId::generate(),
        in_flight.task_id,
        2,
        worker_endpoint(),
        ts(),
    );
    db.repository
        .record_delivery(&awaiting)
        .await
        .expect("record");
    db.repository
        .record_delivery(&never_acknowledged)
        .await
        .expect("record");
    db.repository
        .acknowledge_delivery(awaiting.delivery_id(), ts() + chrono::Duration::seconds(1))
        .await
        .expect("ack");

    let db = db.restart().await;
    let plan = plan_recovery(&db.repository).await.expect("plan");

    assert_eq!(
        plan.unfinished,
        vec![in_flight.task_id],
        "a finished task is never offered for re-execution"
    );
    assert_eq!(
        plan.unacknowledged
            .iter()
            .map(|delivery| delivery.delivery_id())
            .collect::<Vec<_>>(),
        vec![never_acknowledged.delivery_id()],
        "retry input is exactly the never-acknowledged deliveries"
    );
    assert_eq!(
        plan.awaiting_outcome
            .iter()
            .map(|delivery| delivery.delivery_id())
            .collect::<Vec<_>>(),
        vec![awaiting.delivery_id()],
        "an acknowledged delivery is never lost, even with no terminal state"
    );

    // The event log and the task row survive verbatim.
    assert_eq!(
        db.repository
            .events_for_task(finished.task_id)
            .await
            .expect("events")
            .len(),
        2
    );
    assert_eq!(
        db.repository
            .get_task(finished.task_id)
            .await
            .expect("task")
            .expect("present"),
        finished
    );

    db.cleanup().await;
}

#[tokio::test]
async fn an_orphan_task_row_survives_a_restart_as_unfinished() {
    let db = TestDb::new("orphan").await;
    let conversation = Conversation {
        id: ConversationId::generate(),
        participants: Vec::new(),
        external_ref: None,
    };
    db.repository
        .insert_conversation(&conversation)
        .await
        .expect("conversation");
    // The crash window the atomic submission helper closes: the task row exists
    // but its first event never landed.
    let orphan = task("orphan", conversation.id);
    db.repository.insert_task(&orphan).await.expect("task");

    let db = db.restart().await;
    let plan = plan_recovery(&db.repository).await.expect("plan");

    assert_eq!(
        plan.unfinished,
        vec![orphan.task_id],
        "a task with no events has no terminal state, so it must be surfaced"
    );
    assert_eq!(
        db.repository
            .latest_event(orphan.task_id)
            .await
            .expect("events"),
        None
    );

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// R2: planning is read-only, and nothing is written into the repository
// ---------------------------------------------------------------------------

#[tokio::test]
async fn planning_twice_changes_nothing() {
    let db = TestDb::new("read-only").await;
    sync_agents(&db.repository, &registry())
        .await
        .expect("sync agents");
    let task = insert_task(&db, "in flight").await;
    db.repository
        .record_delivery(&Delivery::new(
            DeliveryId::generate(),
            task.task_id,
            1,
            worker_endpoint(),
            ts(),
        ))
        .await
        .expect("record");

    let before = (
        db.count("agents").await,
        db.count("tasks").await,
        db.count("task_events").await,
        db.count("deliveries").await,
    );
    let first = plan_recovery(&db.repository).await.expect("plan");
    let second = plan_recovery(&db.repository).await.expect("plan again");
    let after = (
        db.count("agents").await,
        db.count("tasks").await,
        db.count("task_events").await,
        db.count("deliveries").await,
    );

    assert_eq!(first, second, "planning is deterministic");
    assert_eq!(before, after, "planning writes nothing");

    db.cleanup().await;
}

/// Nothing in this suite may leave a database behind in the repository itself.
#[test]
fn the_repository_contains_no_database_files() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let stray: Vec<String> = std::fs::read_dir(root)
        .expect("read the repository root")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".db"))
        .collect();

    assert!(stray.is_empty(), "stray database files: {stray:?}");
}

// ---------------------------------------------------------------------------
// R3: startup agents synchronisation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn syncing_agents_writes_addressable_endpoints_only_and_is_idempotent() {
    let db = TestDb::new("agents-sync").await;
    let registry = registry();

    assert_eq!(
        sync_agents(&db.repository, &registry).await.expect("sync"),
        1,
        "only the addressable endpoint is stored"
    );
    let stored = db.repository.agents().await.expect("agents");
    assert_eq!(stored, vec![agent_endpoint()]);
    assert_eq!(
        db.repository
            .get_agent(derive_endpoint_id("matrix-bot"))
            .await
            .expect("get"),
        None,
        "a declared but unaddressable endpoint has no model form to store"
    );

    assert_eq!(
        sync_agents(&db.repository, &registry)
            .await
            .expect("sync again"),
        1
    );
    assert_eq!(db.count("agents").await, 1, "a repeated sync is idempotent");

    db.cleanup().await;
}

#[tokio::test]
async fn a_delivery_before_the_startup_sync_is_rejected() {
    let db = TestDb::new("agents-order").await;
    let task = insert_task(&db, "delivery before sync").await;

    assert!(matches!(
        db.repository
            .record_delivery(&Delivery::new(
                DeliveryId::generate(),
                task.task_id,
                1,
                worker_endpoint(),
                ts(),
            ))
            .await,
        Err(StorageError::IntegrityViolation { .. })
    ));

    sync_agents(&db.repository, &registry())
        .await
        .expect("sync");
    db.repository
        .record_delivery(&Delivery::new(
            DeliveryId::generate(),
            task.task_id,
            1,
            worker_endpoint(),
            ts(),
        ))
        .await
        .expect("recorded once the snapshot exists");

    db.cleanup().await;
}

// ---------------------------------------------------------------------------
// R4: recovery re-dispatch respects the attempt key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn re_dispatching_an_unacknowledged_delivery_needs_a_new_attempt() {
    let db = TestDb::new("redispatch").await;
    sync_agents(&db.repository, &registry())
        .await
        .expect("sync agents");
    let task = insert_task(&db, "retry").await;
    let original = Delivery::new(
        DeliveryId::generate(),
        task.task_id,
        1,
        worker_endpoint(),
        ts(),
    );
    db.repository
        .record_delivery(&original)
        .await
        .expect("record");

    let plan = plan_recovery(&db.repository).await.expect("plan");
    assert_eq!(plan.unacknowledged.len(), 1);

    // Rewriting the same attempt is impossible by construction: `UNIQUE
    // (task_id, attempt)` refuses it (and the row is not identical anyway).
    let same_attempt = Delivery::new(
        DeliveryId::generate(),
        task.task_id,
        1,
        worker_endpoint(),
        ts() + chrono::Duration::seconds(1),
    );
    assert!(matches!(
        db.repository.record_delivery(&same_attempt).await,
        Err(StorageError::Duplicate { .. })
    ));

    // A new attempt is the documented re-dispatch.
    let retry = Delivery::new(
        DeliveryId::generate(),
        task.task_id,
        2,
        worker_endpoint(),
        ts() + chrono::Duration::seconds(1),
    );
    db.repository
        .record_delivery(&retry)
        .await
        .expect("a new attempt is recorded");

    let after = plan_recovery(&db.repository).await.expect("plan");
    assert_eq!(
        after
            .unacknowledged
            .iter()
            .map(|delivery| delivery.attempt())
            .collect::<Vec<_>>(),
        vec![1, 2],
        "the original attempt stays as an audit fact"
    );

    db.cleanup().await;
}

/// A conversation linked to an external room survives a restart too, so recovery
/// never loses the context a task belongs to.
#[tokio::test]
async fn external_conversations_survive_a_restart() {
    let db = TestDb::new("conversation-restart").await;
    let conversation = Conversation {
        id: ConversationId::generate(),
        participants: vec![EndpointId::generate()],
        external_ref: Some(ExternalRef {
            transport: TransportType::Matrix,
            external_id: "!room:matrix.org".into(),
            thread_ref: Some("$root:matrix.org".into()),
        }),
    };
    db.repository
        .insert_conversation(&conversation)
        .await
        .expect("insert");

    let db = db.restart().await;

    assert_eq!(
        db.repository
            .conversation_by_external_ref(conversation.external_ref.as_ref().expect("ref"))
            .await
            .expect("by ref"),
        Some(conversation)
    );

    db.cleanup().await;
}
