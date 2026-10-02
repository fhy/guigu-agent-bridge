use std::path::{Path, PathBuf};

use guigu_agent_bridge::{
    models::{EndpointAddress, TransportType},
    storage::{Repository, SqliteRepository, connect, migrate},
};
use sqlx::{Row, migrate::Migrator};

static FULL_MIGRATOR: Migrator = sqlx::migrate!("./migrations");

struct TempDir(PathBuf);
impl TempDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!("t049-{label}-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn migration_source(root: &Path, failing_0011: bool) -> PathBuf {
    let source = root.join(if failing_0011 {
        "failing"
    } else {
        "through-0010"
    });
    std::fs::create_dir(&source).unwrap();
    for version in 1..=10 {
        let prefix = format!("{version:04}_");
        let entry = std::fs::read_dir("migrations")
            .unwrap()
            .map(Result::unwrap)
            .find(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
            .unwrap();
        std::fs::copy(entry.path(), source.join(entry.file_name())).unwrap();
    }
    if failing_0011 {
        let mut sql = std::fs::read_to_string("migrations/0011_a2a_transport.sql").unwrap();
        sql.push_str("\nCREATE TEMP TABLE __t049_test_failure(value INTEGER CHECK(value=1)) STRICT;\nINSERT INTO __t049_test_failure VALUES(0);\n");
        std::fs::write(source.join("0011_a2a_transport.sql"), sql).unwrap();
    }
    source
}

async fn seeded_0010(root: &Path) -> (sqlx::SqlitePool, PathBuf) {
    let database = root.join(format!("state-{}.db", uuid::Uuid::now_v7()));
    let pool = connect(&database).await.unwrap();
    let source = migration_source(root, false).await;
    Migrator::new(source)
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(
        "INSERT INTO agents VALUES ('agent-a','agent-a','acp',1,NULL,'[]');
         INSERT INTO agents VALUES ('agent-h','agent-h','http',1,NULL,'[]');
         INSERT INTO agents VALUES ('agent-m','agent-m','matrix',1,NULL,'[]');
         INSERT INTO conversations VALUES ('conversation-local',NULL,NULL,NULL,'[]');
         INSERT INTO conversations VALUES ('conversation-http','http','external-http',NULL,'[]');
         INSERT INTO conversations VALUES ('conversation-matrix','matrix','room-a','thread-a','[]');
         INSERT INTO messages VALUES ('message-a','conversation-local','agent-a','agent-h','body',NULL,'{}');
         INSERT INTO tasks VALUES ('task-a','task-a',NULL,'agent-a','agent-h','conversation-local','message-a','task',5,0,0,NULL,0);
         INSERT INTO sessions (session_id,endpoint_id,conversation_id,cwd,backend_id,protocol_version,state,created_at,updated_at)
           VALUES ('session-a','agent-a','conversation-local','/tmp','backend',1,'closed','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
         INSERT INTO transport_receipts (transport,external_event_id,room_id,conversation_id,selected_endpoint_id,task_id,receipt_kind,route_kind,state,result_code,created_at,updated_at)
           VALUES ('matrix','event-a','room-a','conversation-local','agent-a','task-a','ordinary','message','admitted','admitted','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');",
    ).execute(&pool).await.unwrap();
    (pool, database)
}

async fn schema(pool: &sqlx::SqlitePool) -> Vec<(String, String, String)> {
    sqlx::query("SELECT type,name,sql FROM sqlite_master WHERE name IN ('agents','conversations','conversations_external_ref_unthreaded','conversations_external_ref_threaded') ORDER BY type,name")
        .fetch_all(pool).await.unwrap().into_iter()
        .map(|row| (row.get("type"), row.get("name"), row.get("sql"))).collect()
}

async fn rows(pool: &sqlx::SqlitePool) -> (Vec<(String, String)>, Vec<(String, Option<String>)>) {
    let agents = sqlx::query_as("SELECT endpoint_id,transport FROM agents ORDER BY endpoint_id")
        .fetch_all(pool)
        .await
        .unwrap();
    let conversations = sqlx::query_as(
        "SELECT conversation_id,transport FROM conversations ORDER BY conversation_id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    (agents, conversations)
}

#[derive(Debug, PartialEq, Eq)]
struct DatabaseSnapshot {
    schema: Vec<(String, String, String)>,
    columns: Vec<ColumnSnapshot>,
    foreign_keys: Vec<(String, i64, String, String, String)>,
    rows: RowSnapshot,
    dependent_counts: (i64, i64, i64, i64),
}

type ColumnSnapshot = (String, i64, String, String, i64, Option<String>, i64);
type RowSnapshot = (Vec<(String, String)>, Vec<(String, Option<String>)>);

async fn snapshot(pool: &sqlx::SqlitePool) -> DatabaseSnapshot {
    let mut columns = Vec::new();
    for (table, sql) in [
        ("agents", "PRAGMA table_info(agents)"),
        ("conversations", "PRAGMA table_info(conversations)"),
    ] {
        let values: Vec<(i64, String, String, i64, Option<String>, i64)> =
            sqlx::query_as(sql).fetch_all(pool).await.unwrap();
        columns.extend(values.into_iter().map(|value| {
            (
                table.to_owned(),
                value.0,
                value.1,
                value.2,
                value.3,
                value.4,
                value.5,
            )
        }));
    }
    let mut foreign_keys = Vec::new();
    for (table, sql) in [
        (
            "messages",
            "SELECT id,\"table\",\"from\",\"to\" FROM pragma_foreign_key_list('messages') ORDER BY id",
        ),
        (
            "tasks",
            "SELECT id,\"table\",\"from\",\"to\" FROM pragma_foreign_key_list('tasks') ORDER BY id",
        ),
    ] {
        let values: Vec<(i64, String, String, String)> =
            sqlx::query_as(sql).fetch_all(pool).await.unwrap();
        foreign_keys.extend(
            values
                .into_iter()
                .map(|value| (table.to_owned(), value.0, value.1, value.2, value.3)),
        );
    }
    let dependent_counts = (
        sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(pool)
            .await
            .unwrap(),
        sqlx::query_scalar("SELECT COUNT(*) FROM tasks")
            .fetch_one(pool)
            .await
            .unwrap(),
        sqlx::query_scalar("SELECT COUNT(*) FROM sessions")
            .fetch_one(pool)
            .await
            .unwrap(),
        sqlx::query_scalar("SELECT COUNT(*) FROM transport_receipts")
            .fetch_one(pool)
            .await
            .unwrap(),
    );
    DatabaseSnapshot {
        schema: schema(pool).await,
        columns,
        foreign_keys,
        rows: rows(pool).await,
        dependent_counts,
    }
}

async fn assert_integrity(pool: &sqlx::SqlitePool) {
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(pool)
        .await
        .unwrap();
    assert!(violations.is_empty());
    let indexes: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='conversations' AND sql IS NOT NULL ORDER BY name")
        .fetch_all(pool).await.unwrap();
    assert_eq!(
        indexes,
        vec![
            "conversations_external_ref_threaded",
            "conversations_external_ref_unthreaded"
        ]
    );
}

#[tokio::test]
async fn fresh_and_nonempty_0010_upgrade_preserve_rows_schema_and_foreign_keys() {
    let fresh = TempDir::new("fresh");
    let fresh_pool = connect(fresh.0.join("fresh.db")).await.unwrap();
    migrate(&fresh_pool).await.unwrap();
    assert_integrity(&fresh_pool).await;
    fresh_pool.close().await;

    let upgrade = TempDir::new("upgrade");
    let (pool, _) = seeded_0010(&upgrade.0).await;
    let before = snapshot(&pool).await;
    migrate(&pool).await.unwrap();
    let after = snapshot(&pool).await;
    assert_eq!(after.rows, before.rows);
    assert_eq!(after.foreign_keys, before.foreign_keys);
    assert_eq!(after.dependent_counts, before.dependent_counts);
    assert_eq!(after.columns, before.columns);
    assert_integrity(&pool).await;
    let a2a_id = "00000000-0000-0000-0000-00000000002a";
    sqlx::query("INSERT INTO agents VALUES (?,'agent-2a','a2a',1,?,'[]')")
        .bind(a2a_id)
        .bind(r#"{"a2a":{"peer":"peer-a"}}"#)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO conversations VALUES ('00000000-0000-0000-0000-00000000002c','a2a','peer/context',NULL,'[]')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let repository = SqliteRepository::new(pool.clone());
    let agent = repository
        .get_agent(a2a_id.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(agent.transport, TransportType::A2a);
    assert!(matches!(agent.address, EndpointAddress::A2a { .. }));
    let reference = guigu_agent_bridge::models::ExternalRef {
        transport: TransportType::A2a,
        external_id: "peer/context".into(),
        thread_ref: None,
    };
    assert!(
        repository
            .conversation_by_external_ref(&reference)
            .await
            .unwrap()
            .is_some()
    );
    pool.close().await;
}

#[tokio::test]
async fn explicit_post_rebuild_failure_restores_the_complete_0010_state() {
    let fixture = TempDir::new("transaction-rollback");
    let (pool, _) = seeded_0010(&fixture.0).await;
    let before = snapshot(&pool).await;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("CREATE TEMP TABLE __t049_test_failure(value INTEGER CHECK(value=1)) STRICT")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../migrations/0011_a2a_transport.sql"))
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        sqlx::query("INSERT INTO __t049_test_failure VALUES(0)")
            .execute(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(snapshot(&pool).await, before);
    assert_integrity(&pool).await;
    pool.close().await;
}

#[tokio::test]
async fn sqlx_migrator_rolls_back_rebuild_and_ledger_then_clean_retry_succeeds() {
    let fixture = TempDir::new("migrator-rollback");
    let (pool, _) = seeded_0010(&fixture.0).await;
    let before = snapshot(&pool).await;
    let failing = migration_source(&fixture.0, true).await;
    assert!(
        Migrator::new(failing)
            .await
            .unwrap()
            .run(&pool)
            .await
            .is_err()
    );
    assert_eq!(snapshot(&pool).await, before);
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(versions, (1..=10).collect::<Vec<_>>());
    assert_integrity(&pool).await;
    migrate(&pool).await.unwrap();
    let ledger: Vec<(i64, String, bool, Vec<u8>)> = sqlx::query_as(
        "SELECT version,description,success,checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let expected: Vec<(i64, String, bool, Vec<u8>)> = FULL_MIGRATOR
        .migrations
        .iter()
        .map(|migration| {
            (
                migration.version,
                migration.description.to_string(),
                true,
                migration.checksum.to_vec(),
            )
        })
        .collect();
    assert_eq!(ledger, expected);
    assert_eq!(ledger.last().map(|row| row.0), Some(15));
    assert_integrity(&pool).await;
    pool.close().await;
}
