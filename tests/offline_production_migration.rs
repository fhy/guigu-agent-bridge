use std::{
    fs::{File, OpenOptions},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::Path,
    process::{Command, Output},
    time::Duration,
};

use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};

#[derive(Debug, PartialEq, Eq)]
struct DurableSnapshot {
    ledger: Vec<(i64, Vec<u8>)>,
    owner: (String, String, String, String),
}

#[test]
fn real_cli_rejects_invalid_arguments_without_startup() {
    let temporary = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .current_dir(temporary.path())
        .args(["offline-migrate-production", "--mode", "apply"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stdout).contains("invalid-arguments"));
    assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
}

fn hold_maintenance_lock(root: &Path) -> File {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(root.join(".state.db.offline-migration.lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    lock
}

async fn invoke_selected_recovery(database: &Path) -> Output {
    tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
            .args(["recover-selected-preacceptance", "--database"])
            .arg(database)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("held-lock recovery did not exit within the deadline")
    .unwrap()
}

fn assert_maintenance_refusal(output: Output) {
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(
        output.stdout,
        b"maintenance-exclusion result=busy-or-unresolved\n"
    );
    assert!(output.stderr.is_empty());
}

async fn durable_snapshot(database: &Path) -> DurableSnapshot {
    let mut connection = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(database)
            .create_if_missing(false)
            .read_only(true),
    )
    .await
    .unwrap();
    let ledger = sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
        .fetch_all(&mut connection)
        .await
        .unwrap();
    let owner = sqlx::query_as(
        "SELECT started_at,heartbeat_at,state,process_fingerprint FROM runtime_instances WHERE instance_token='retained'",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();
    DurableSnapshot { ledger, owner }
}

#[tokio::test]
async fn selected_recovery_real_cli_refuses_held_lock_without_durable_mutation() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("state.db");
    let pool = guigu_agent_bridge::storage::connect(&database)
        .await
        .unwrap();
    guigu_agent_bridge::storage::migrate(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO runtime_instances VALUES ('retained','before','before','stopped','synthetic')",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
    let _guard = hold_maintenance_lock(temporary.path());
    let before = durable_snapshot(&database).await;
    let latest = sqlx::migrate!("./migrations")
        .iter()
        .map(|migration| migration.version)
        .max()
        .unwrap();
    assert_eq!(before.ledger.last().unwrap().0, latest);
    let before_bytes = std::fs::read(&database).unwrap();
    assert_maintenance_refusal(invoke_selected_recovery(&database).await);
    assert_eq!(std::fs::read(&database).unwrap(), before_bytes);
    assert_eq!(durable_snapshot(&database).await, before);
}

#[tokio::test]
async fn selected_recovery_held_lock_precedes_database_validation() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("state.db");
    let unusable_database = b"synthetic non-SQLite evidence; must not be opened";
    std::fs::write(&database, unusable_database).unwrap();
    let _guard = hold_maintenance_lock(temporary.path());
    assert_maintenance_refusal(invoke_selected_recovery(&database).await);
    assert_eq!(std::fs::read(&database).unwrap(), unusable_database);
    assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 2);
}
