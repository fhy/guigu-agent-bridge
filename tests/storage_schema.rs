//! T008 integration tests: the real `SqlitePool`, the real embedded migrations,
//! and real temporary database files.
//!
//! Everything here drives production entry points — [`connect`] and [`migrate`]
//! — and asserts on real SQLite behaviour (constraints, pragmas, checksums)
//! rather than on strings. No `Repository` method body exists yet, so no
//! repository CRUD test is possible or included; that is T009's acceptance.
//!
//! Each test uses its own file under the system temporary directory. `.gitignore`
//! also covers `*.db*`, but nothing is ever created inside the repository.

use std::path::{Path, PathBuf};

use guigu_agent_bridge::models::{ConversationId, DeliveryId, EventId, MessageId, TaskId};
use guigu_agent_bridge::storage::{StorageError, connect, migrate};
use sqlx::{Row, SqlitePool};

/// A canonical, fixed-width UTC nanosecond timestamp (analyse §4.5).
const TIMESTAMP: &str = "2026-09-16T10:00:00.000000000Z";

fn temp_db_path(tag: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "guigu-storage-schema-{tag}-{}.db",
        uuid::Uuid::now_v7()
    ));
    path
}

async fn fresh_pool(tag: &str) -> (SqlitePool, PathBuf) {
    let path = temp_db_path(tag);
    let pool = connect(&path).await.expect("connect");
    migrate(&pool).await.expect("migrate");
    (pool, path)
}

fn remove_db_files(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut candidate = path.as_os_str().to_owned();
        candidate.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(candidate));
    }
}

async fn table_names(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .fetch_all(pool)
        .await
        .expect("read sqlite_master")
}

async fn applied_migration_count(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(pool)
        .await
        .expect("read _sqlx_migrations")
}

async fn row_count(pool: &SqlitePool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .expect("count rows")
}

async fn insert_conversation(pool: &SqlitePool, id: &str) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO conversations (conversation_id, participants_json) VALUES (?, '[]')")
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(StorageError::from)
}

/// Insert a conversation linked to an external room/thread/session reference.
///
/// `thread_ref` is `None` for a non-threaded reference; the table CHECKs reject
/// a `thread_ref` that arrives without a transport/external id.
async fn insert_external_conversation(
    pool: &SqlitePool,
    conversation_id: &str,
    transport: &str,
    external_id: &str,
    thread_ref: Option<&str>,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO conversations (conversation_id, transport, external_id, thread_ref, \
         participants_json) VALUES (?, ?, ?, ?, '[]')",
    )
    .bind(conversation_id)
    .bind(transport)
    .bind(external_id)
    .bind(thread_ref)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(StorageError::from)
}

/// `(name, unique, partial)` for every index on `conversations`.
async fn conversation_indexes(pool: &SqlitePool) -> Vec<(String, bool, bool)> {
    let rows = sqlx::query("PRAGMA index_list(conversations)")
        .fetch_all(pool)
        .await
        .expect("index_list(conversations)");
    rows.iter()
        .map(|row| {
            (
                row.get::<String, _>("name"),
                row.get::<i64, _>("unique") == 1,
                row.get::<i64, _>("partial") == 1,
            )
        })
        .collect()
}

async fn insert_task(
    pool: &SqlitePool,
    task_id: &str,
    conversation_id: &str,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO tasks (task_id, root_task_id, from_agent, to_agent, conversation_id, \
         text, priority, depth, hops, version) VALUES (?, ?, 'from', 'to', ?, 'body', 5, 0, 0, 0)",
    )
    .bind(task_id)
    .bind(task_id)
    .bind(conversation_id)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(StorageError::from)
}

async fn insert_event(
    pool: &SqlitePool,
    event_id: &str,
    task_id: &str,
    seq: i64,
    status: &str,
    payload: &str,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO task_events (event_id, task_id, seq, status, timestamp, payload) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(event_id)
    .bind(task_id)
    .bind(seq)
    .bind(status)
    .bind(TIMESTAMP)
    .bind(payload)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(StorageError::from)
}

async fn insert_agent(
    pool: &SqlitePool,
    endpoint_id: &str,
    agent_id: &str,
    transport: &str,
    enabled: i64,
    address_json: Option<&str>,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO agents (endpoint_id, agent_id, transport, enabled, address_json, \
         capabilities_json) VALUES (?, ?, ?, ?, ?, '[]')",
    )
    .bind(endpoint_id)
    .bind(agent_id)
    .bind(transport)
    .bind(enabled)
    .bind(address_json)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(StorageError::from)
}

async fn insert_delivery(
    pool: &SqlitePool,
    delivery_id: &str,
    task_id: &str,
    attempt: i64,
    target_endpoint_id: &str,
    acknowledged_at: Option<&str>,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO deliveries (delivery_id, task_id, attempt, target_endpoint_id, \
         dispatched_at, acknowledged_at) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(delivery_id)
    .bind(task_id)
    .bind(attempt)
    .bind(target_endpoint_id)
    .bind(TIMESTAMP)
    .bind(acknowledged_at)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(StorageError::from)
}

/// Seed a conversation + task pair and return the task id.
async fn seed_task(pool: &SqlitePool) -> String {
    let conversation_id = ConversationId::generate().to_string();
    insert_conversation(pool, &conversation_id)
        .await
        .expect("conversation");
    let task_id = TaskId::generate().to_string();
    insert_task(pool, &task_id, &conversation_id)
        .await
        .expect("task");
    task_id
}

async fn seed_agent(pool: &SqlitePool) -> String {
    let endpoint_id = guigu_agent_bridge::bus::derive_endpoint_id("alpha").to_string();
    insert_agent(
        pool,
        &endpoint_id,
        "alpha",
        "acp",
        1,
        Some(r#"{"acp":{"command":"codex-acp","args":[]}}"#),
    )
    .await
    .expect("agent");
    endpoint_id
}

// ── 1. empty database + migration idempotence ────────────────────────────────

#[tokio::test]
async fn an_empty_database_initializes_and_migrating_twice_is_idempotent() {
    let path = temp_db_path("init");
    let pool = connect(&path).await.expect("connect");
    migrate(&pool).await.expect("first migrate");

    let tables = table_names(&pool).await;
    for expected in [
        "agents",
        "conversations",
        "messages",
        "tasks",
        "task_events",
        "deliveries",
        "a2a_exchanges",
        "a2a_messages",
        "a2a_message_parts",
        "a2a_artifacts",
        "a2a_artifact_parts",
    ] {
        assert!(
            tables.iter().any(|name| name == expected),
            "table {expected} is missing from {tables:?}"
        );
    }

    let applied = applied_migration_count(&pool).await;
    assert_eq!(applied, 10, "the ten migrations are recorded once each");

    migrate(&pool).await.expect("second migrate is a no-op");
    assert_eq!(
        applied_migration_count(&pool).await,
        applied,
        "re-running the migrator must not record the version again"
    );

    // Cross-connection idempotence: a brand-new pool over the same file.
    pool.close().await;
    let second = connect(&path).await.expect("reconnect");
    migrate(&second).await.expect("migrate from a second pool");
    assert_eq!(applied_migration_count(&second).await, applied);
    second.close().await;

    remove_db_files(&path);
}

// ── 2. connection pragmas, per connection ────────────────────────────────────

#[tokio::test]
async fn connection_pragmas_are_applied_to_every_pooled_connection() {
    let (pool, path) = fresh_pool("pragmas").await;

    let mut connections = Vec::new();
    for _ in 0..3 {
        connections.push(pool.acquire().await.expect("acquire"));
    }

    for connection in &mut connections {
        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&mut **connection)
            .await
            .expect("journal_mode");
        assert_eq!(journal_mode.to_lowercase(), "wal");

        let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&mut **connection)
            .await
            .expect("foreign_keys");
        assert_eq!(foreign_keys, 1, "foreign keys must be ON per connection");

        let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&mut **connection)
            .await
            .expect("busy_timeout");
        assert_eq!(busy_timeout, 5_000);
    }

    drop(connections);
    pool.close().await;
    remove_db_files(&path);
}

// ── 3. uniqueness constraints ────────────────────────────────────────────────

#[tokio::test]
async fn task_event_sequence_and_event_identity_are_unique() {
    let (pool, path) = fresh_pool("uniqueness").await;
    let task_id = seed_task(&pool).await;

    let first_event = EventId::generate().to_string();
    let second_event = EventId::generate().to_string();
    insert_event(&pool, &first_event, &task_id, 1, "queued", "\"queued\"")
        .await
        .expect("first event");

    // Repeated (task_id, seq): the per-task ordering guard.
    let error = insert_event(
        &pool,
        &EventId::generate().to_string(),
        &task_id,
        1,
        "queued",
        "\"queued\"",
    )
    .await
    .expect_err("duplicate (task_id, seq) must fail");
    assert!(matches!(error, StorageError::Duplicate { .. }), "{error:?}");

    insert_event(
        &pool,
        &second_event,
        &task_id,
        2,
        "dispatched",
        "\"queued\"",
    )
    .await
    .expect("second event");

    // Repeated event_id at a different seq: the global dedup guard.
    let error = insert_event(&pool, &first_event, &task_id, 3, "running", "\"queued\"")
        .await
        .expect_err("duplicate event_id must fail");
    assert!(matches!(error, StorageError::Duplicate { .. }), "{error:?}");

    assert_eq!(row_count(&pool, "task_events").await, 2);

    pool.close().await;
    remove_db_files(&path);
}

// ── 4. external conversation references are unique per shape ─────────────────

#[tokio::test]
async fn external_references_are_unique_per_shape_while_local_conversations_coexist() {
    let (pool, path) = fresh_pool("external-refs").await;

    // The two UNIQUE PARTIAL indexes are the enforcing structure, not tuning:
    // `conversation_by_external_ref` resolves a single `Option<Conversation>`.
    // Automatic indexes for the TEXT primary key are ignored here.
    let mut named: Vec<(String, bool, bool)> = conversation_indexes(&pool)
        .await
        .into_iter()
        .filter(|(name, _, _)| !name.starts_with("sqlite_autoindex"))
        .collect();
    named.sort();
    assert_eq!(
        named,
        vec![
            (
                "conversations_external_ref_threaded".to_string(),
                true,
                true
            ),
            (
                "conversations_external_ref_unthreaded".to_string(),
                true,
                true
            ),
        ],
        "conversations must carry exactly the two UNIQUE PARTIAL external-ref indexes"
    );

    // A conversation with no external reference at all: many must coexist. This
    // is what the `transport IS NOT NULL` guard on the unthreaded index protects.
    for _ in 0..3 {
        insert_conversation(&pool, &ConversationId::generate().to_string())
            .await
            .expect("a local conversation");
    }
    assert_eq!(row_count(&pool, "conversations").await, 3);

    // Non-threaded external reference.
    insert_external_conversation(
        &pool,
        &ConversationId::generate().to_string(),
        "matrix",
        "!room:example.org",
        None,
    )
    .await
    .expect("first non-threaded reference");

    // The same reference again — a replay or a concurrent first message — must
    // be rejected, otherwise the room would own two conversations.
    let error = insert_external_conversation(
        &pool,
        &ConversationId::generate().to_string(),
        "matrix",
        "!room:example.org",
        None,
    )
    .await
    .expect_err("a duplicate non-threaded reference must fail");
    assert!(matches!(error, StorageError::Duplicate { .. }), "{error:?}");

    // Another transport, or another external id, is another conversation.
    insert_external_conversation(
        &pool,
        &ConversationId::generate().to_string(),
        "acp",
        "!room:example.org",
        None,
    )
    .await
    .expect("the same external id on another transport");
    insert_external_conversation(
        &pool,
        &ConversationId::generate().to_string(),
        "matrix",
        "!other:example.org",
        None,
    )
    .await
    .expect("another room on the same transport");

    // A thread is its own conversation, distinct from its room.
    let thread = "$thread-1:example.org";
    insert_external_conversation(
        &pool,
        &ConversationId::generate().to_string(),
        "matrix",
        "!room:example.org",
        Some(thread),
    )
    .await
    .expect("first threaded reference");

    let error = insert_external_conversation(
        &pool,
        &ConversationId::generate().to_string(),
        "matrix",
        "!room:example.org",
        Some(thread),
    )
    .await
    .expect_err("a duplicate threaded reference must fail");
    assert!(matches!(error, StorageError::Duplicate { .. }), "{error:?}");

    // A second thread in the same room stays legal.
    insert_external_conversation(
        &pool,
        &ConversationId::generate().to_string(),
        "matrix",
        "!room:example.org",
        Some("$thread-2:example.org"),
    )
    .await
    .expect("a second thread in the same room");

    // 3 local + 3 non-threaded + 2 threaded; the two rejected inserts added none.
    assert_eq!(row_count(&pool, "conversations").await, 8);

    pool.close().await;
    remove_db_files(&path);
}

// ── 5. foreign keys are enforced ─────────────────────────────────────────────

#[tokio::test]
async fn foreign_keys_are_enforced() {
    let (pool, path) = fresh_pool("foreign-keys").await;
    let task_id = seed_task(&pool).await;
    let endpoint_id = seed_agent(&pool).await;

    // An event for a task that does not exist.
    let unknown_task = TaskId::generate().to_string();
    let error = insert_event(
        &pool,
        &EventId::generate().to_string(),
        &unknown_task,
        1,
        "queued",
        "\"queued\"",
    )
    .await
    .expect_err("unknown task must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );

    // A delivery for an unknown task.
    let error = insert_delivery(
        &pool,
        &DeliveryId::generate().to_string(),
        &unknown_task,
        1,
        &endpoint_id,
        None,
    )
    .await
    .expect_err("unknown delivery task must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );

    // A delivery to an unknown endpoint.
    let error = insert_delivery(
        &pool,
        &DeliveryId::generate().to_string(),
        &task_id,
        1,
        &guigu_agent_bridge::bus::derive_endpoint_id("nobody").to_string(),
        None,
    )
    .await
    .expect_err("unknown delivery target must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );

    // A message in an unknown conversation.
    let error = sqlx::query(
        "INSERT INTO messages (message_id, conversation_id, sender, recipient, body, \
         metadata_json) VALUES (?, ?, 's', 'r', 'body', '{}')",
    )
    .bind(MessageId::generate().to_string())
    .bind(ConversationId::generate().to_string())
    .execute(&pool)
    .await
    .map_err(StorageError::from)
    .expect_err("unknown conversation must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );

    // A task cannot be deleted while events still reference it (NO ACTION).
    insert_event(
        &pool,
        &EventId::generate().to_string(),
        &task_id,
        1,
        "queued",
        "\"queued\"",
    )
    .await
    .expect("event");
    let error = sqlx::query("DELETE FROM tasks WHERE task_id = ?")
        .bind(&task_id)
        .execute(&pool)
        .await
        .map_err(StorageError::from)
        .expect_err("deleting a referenced task must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );
    assert_eq!(row_count(&pool, "tasks").await, 1);

    pool.close().await;
    remove_db_files(&path);
}

// ── 6. value domains, JSON integrity, and STRICT typing ──────────────────────

#[tokio::test]
async fn value_domains_json_shape_and_strict_types_are_enforced() {
    let (pool, path) = fresh_pool("domains").await;
    let task_id = seed_task(&pool).await;
    let endpoint_id = seed_agent(&pool).await;

    // Unknown enum value: the CHECK constraint rejects it on any write path.
    let error = insert_event(
        &pool,
        &EventId::generate().to_string(),
        &task_id,
        1,
        "paused",
        "\"paused\"",
    )
    .await
    .expect_err("unknown status must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );

    // Invalid JSON payload.
    let error = insert_event(
        &pool,
        &EventId::generate().to_string(),
        &task_id,
        1,
        "queued",
        "not json",
    )
    .await
    .expect_err("invalid payload JSON must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );

    // Priority outside 0..=10.
    let error = sqlx::query(
        "INSERT INTO tasks (task_id, root_task_id, from_agent, to_agent, conversation_id, \
         text, priority, depth, hops, version) VALUES (?, ?, 'f', 't', \
         (SELECT conversation_id FROM tasks WHERE task_id = ?), 'x', 99, 0, 0, 0)",
    )
    .bind(TaskId::generate().to_string())
    .bind(&task_id)
    .bind(&task_id)
    .execute(&pool)
    .await
    .map_err(StorageError::from)
    .expect_err("priority 99 must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );

    // Boolean domain on the agents table.
    let error = insert_agent(
        &pool,
        &guigu_agent_bridge::bus::derive_endpoint_id("broken").to_string(),
        "broken",
        "acp",
        2,
        Some(r#"{"acp":{"command":"x","args":[]}}"#),
    )
    .await
    .expect_err("enabled = 2 must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );

    // The address JSON tag must match the declared transport.
    let error = insert_agent(
        &pool,
        &guigu_agent_bridge::bus::derive_endpoint_id("mismatch").to_string(),
        "mismatch",
        "http",
        1,
        Some(r#"{"acp":{"command":"x","args":[]}}"#),
    )
    .await
    .expect_err("http transport with an acp address must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );

    // Acknowledgement may not precede dispatch.
    let error = insert_delivery(
        &pool,
        &DeliveryId::generate().to_string(),
        &task_id,
        1,
        &endpoint_id,
        Some("2026-09-16T09:59:59.000000000Z"),
    )
    .await
    .expect_err("an acknowledgement before dispatch must fail");
    assert!(
        matches!(error, StorageError::IntegrityViolation { .. }),
        "{error:?}"
    );

    // STRICT typing. SQLite's STRICT tables reject values that cannot be
    // *losslessly* converted, so the probes below are chosen to be unconvertible
    // and to sit on columns without a CHECK that could mask the type error:
    // `tasks.text` has no CHECK at all, and `version >= 0` is satisfied by both a
    // BLOB and a non-numeric TEXT (they rank above integers), so only STRICT can
    // reject them. Today these surface as `StorageError::Query` because
    // SQLITE_CONSTRAINT_DATATYPE is not one of the kinds sqlx classifies.
    let error = sqlx::query(
        "INSERT INTO tasks (task_id, root_task_id, from_agent, to_agent, conversation_id, \
         text, priority, depth, hops, version) VALUES (?, ?, 'f', 't', \
         (SELECT conversation_id FROM tasks WHERE task_id = ?), ?, 5, 0, 0, 0)",
    )
    .bind(TaskId::generate().to_string())
    .bind(&task_id)
    .bind(&task_id)
    .bind(vec![1_u8, 2, 3])
    .execute(&pool)
    .await
    .map_err(StorageError::from)
    .expect_err("a BLOB in a TEXT column must fail");
    assert!(
        !matches!(error, StorageError::Duplicate { .. }),
        "{error:?}"
    );

    let error = sqlx::query(
        "INSERT INTO tasks (task_id, root_task_id, from_agent, to_agent, conversation_id, \
         text, priority, depth, hops, version) VALUES (?, ?, 'f', 't', \
         (SELECT conversation_id FROM tasks WHERE task_id = ?), 'x', 5, 0, 0, ?)",
    )
    .bind(TaskId::generate().to_string())
    .bind(&task_id)
    .bind(&task_id)
    .bind("not-a-number")
    .execute(&pool)
    .await
    .map_err(StorageError::from)
    .expect_err("text in an INTEGER column must fail");
    assert!(
        !matches!(error, StorageError::Duplicate { .. }),
        "{error:?}"
    );

    assert_eq!(row_count(&pool, "task_events").await, 0);
    assert_eq!(row_count(&pool, "conversations").await, 1);
    assert_eq!(row_count(&pool, "deliveries").await, 0);
    assert_eq!(row_count(&pool, "tasks").await, 1);

    pool.close().await;
    remove_db_files(&path);
}

// ── 7. migration immutability is enforced, not just documented ───────────────

#[tokio::test]
async fn editing_an_applied_migration_is_rejected_by_its_checksum() {
    let (pool, path) = fresh_pool("immutability").await;

    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let tampered_dir = std::env::temp_dir().join(format!(
        "guigu-tampered-migrations-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&tampered_dir).expect("create temp migration dir");

    let mut copied = 0;
    for entry in std::fs::read_dir(&source).expect("read migrations") {
        let entry = entry.expect("dir entry");
        if !entry.file_type().expect("file type").is_file() {
            continue;
        }
        let target = tampered_dir.join(entry.file_name());
        if entry.file_name() == "0001_init.sql" {
            let mut content = std::fs::read_to_string(entry.path()).expect("read migration");
            content.push_str("\n-- tampered by the T008 immutability test\n");
            std::fs::write(&target, content).expect("write tampered migration");
        } else {
            std::fs::copy(entry.path(), &target).expect("copy migration");
        }
        copied += 1;
    }
    assert!(copied >= 1, "at least the initial migration must be copied");

    let tampered = sqlx::migrate::Migrator::new(tampered_dir.as_path())
        .await
        .expect("load the tampered migrator");
    let error = tampered
        .run(&pool)
        .await
        .expect_err("an edited, already-applied migration must be rejected");
    assert!(
        matches!(error, sqlx::migrate::MigrateError::VersionMismatch(1)),
        "{error:?}"
    );

    // The embedded migrations still apply cleanly afterwards.
    migrate(&pool)
        .await
        .expect("the untampered migrator still works");

    pool.close().await;
    let _ = std::fs::remove_dir_all(&tampered_dir);
    remove_db_files(&path);
}

// ── 8. a missing parent directory is an explicit error ──────────────────────

#[tokio::test]
async fn opening_a_database_in_a_missing_directory_is_an_explicit_error() {
    let mut path = std::env::temp_dir();
    path.push(format!("guigu-absent-{}", uuid::Uuid::now_v7()));
    path.push("state.db");

    let error = connect(&path)
        .await
        .expect_err("connect must not create parent directories");
    match &error {
        StorageError::Open { path: reported, .. } => assert_eq!(reported, &path),
        other => panic!("expected StorageError::Open, got {other:?}"),
    }
    assert!(
        error.to_string().contains(&path.display().to_string()),
        "the rendering must name the path: {error}"
    );
    assert!(!path.parent().expect("parent").exists());
}

// ── 9. the schema is a fixed point: a second process-level open sees it ──────

#[tokio::test]
async fn the_schema_survives_a_close_and_reopen() {
    let path = temp_db_path("reopen");
    let pool = connect(&path).await.expect("connect");
    migrate(&pool).await.expect("migrate");
    let task_id = seed_task(&pool).await;
    insert_event(
        &pool,
        &EventId::generate().to_string(),
        &task_id,
        1,
        "queued",
        "\"queued\"",
    )
    .await
    .expect("event");
    pool.close().await;

    let reopened = connect(&path).await.expect("reopen");
    migrate(&reopened).await.expect("migrate on reopen");
    assert_eq!(row_count(&reopened, "task_events").await, 1);
    let row = sqlx::query("SELECT status, timestamp FROM task_events")
        .fetch_one(&reopened)
        .await
        .expect("read event");
    assert_eq!(row.get::<String, _>("status"), "queued");
    assert_eq!(row.get::<String, _>("timestamp"), TIMESTAMP);
    reopened.close().await;

    remove_db_files(&path);
}
