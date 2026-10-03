use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use guigu_agent_bridge::storage::{connect, migrate};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};

fn proc_view() -> PathBuf {
    let root = std::env::temp_dir().join(format!("t050-migration-proc-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(root.join("net")).unwrap();
    for table in ["tcp", "tcp6"] {
        std::fs::write(root.join("net").join(table), b"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n").unwrap();
    }
    root
}

async fn durable_rows(path: &Path) -> (Vec<(String, String, String)>, (String, i64, String), i64) {
    let mut c = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false),
    )
    .await
    .unwrap();
    let owners = sqlx::query_as(
        "SELECT instance_token,state,heartbeat_at FROM runtime_instances ORDER BY instance_token",
    )
    .fetch_all(&mut c)
    .await
    .unwrap();
    let continuation = sqlx::query_as("SELECT state,revision,heartbeat_at FROM task_continuations")
        .fetch_one(&mut c)
        .await
        .unwrap();
    let receipts = sqlx::query_scalar("SELECT count(*) FROM isolated_fixture_migration_receipts")
        .fetch_one(&mut c)
        .await
        .unwrap();
    c.close().await.unwrap();
    (owners, continuation, receipts)
}

struct Fixture {
    root: PathBuf,
    db: PathBuf,
    config: PathBuf,
    manifest: PathBuf,
    evidence: PathBuf,
    systemctl: PathBuf,
    proc_root: PathBuf,
}

impl Fixture {
    async fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root =
            std::env::temp_dir().join(format!("t050-fixture-migration-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let db = root.join("state.db");
        let pool = connect(&db).await.unwrap();
        migrate(&pool).await.unwrap();
        sqlx::query("INSERT INTO agents VALUES('agent-a','a','acp',1,NULL,'[]'),('agent-b','b','acp',1,NULL,'[]')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO conversations VALUES('conversation',NULL,NULL,NULL,'[]')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tasks VALUES('task','task',NULL,'agent-a','agent-b','conversation',NULL,'closed fixture',5,0,0,NULL,1)").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_events VALUES('event-1','task',1,'running','2026-10-01T00:00:00.000000000Z','{}'),('event-2','task',2,'failed','2026-10-01T00:00:03.000000000Z','{}')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries VALUES('delivery','task',1,'agent-b','2026-10-01T00:00:01.000000000Z','2026-10-01T00:00:03.000000000Z')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO runtime_instances VALUES('owner-s','2026-09-30T23:00:00.000000000Z','2026-09-30T23:30:00.000000000Z','stopping','legacy-stopping'),('owner-a','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','active','legacy-active')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES('task','terminal',4,'owner-a','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES('delivery','task',1,'terminal')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_leases VALUES('resource','task','owner-a',7,'released','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','2026-10-01T00:00:03.000000000Z')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes,runtime_generation,runtime_generation_version) VALUES('task','resource','delivery',7,2,'blocked',1,1,0,'','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','2026-10-01T00:00:03.000000000Z',12,'generation.v1:strict',1)").execute(&pool).await.unwrap();
        let response_hash = uuid::Uuid::new_v5(
            &uuid::Uuid::from_u128(0x7297ec04_1338_57b4_9bc4_f0ff9f84fa5a),
            b"{}",
        )
        .to_string();
        sqlx::query("INSERT INTO continuation_response_receipts VALUES('receipt','task','delivery','resource',7,1,?,'structured',2,'generation.v1:strict','2026-10-01T00:00:03.000000000Z')").bind(response_hash).execute(&pool).await.unwrap();
        pool.close().await;
        let config = root.join("config.toml");
        std::fs::write(&config, b"fixture=true\n").unwrap();
        let systemctl = root.join("systemctl");
        std::fs::write(&systemctl, b"#!/bin/sh\nprintf 'inactive\ndead\n0\n'\n").unwrap();
        std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o700)).unwrap();
        let fixture = Self {
            manifest: root.join("manifest.json"),
            evidence: root.join("evidence.txt"),
            proc_root: proc_view(),
            root,
            db,
            config,
            systemctl,
        };
        let output = fixture.prepare().output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(0),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        fixture
    }
    fn base(&self) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"));
        c.env("T050_RECONCILE_TEST_SYSTEMCTL", &self.systemctl)
            .env("T050_RECONCILE_TEST_PROC_ROOT", &self.proc_root);
        c
    }
    fn prepare(&self) -> Command {
        let mut c = self.base();
        c.args([
            "prepare-isolated-legacy-fixture-migration-manifest",
            "--root",
        ])
        .arg(&self.root)
        .arg("--config")
        .arg(&self.config)
        .arg("--database")
        .arg(&self.db)
        .arg("--unit")
        .arg("guigu-codex-test.service")
        .arg("--manifest")
        .arg(&self.manifest)
        .arg("--evidence")
        .arg(&self.evidence)
        .arg("--operation-id")
        .arg("00000000-0000-7000-8000-000000000150")
        .arg("--commit-timestamp")
        .arg("2026-10-04T00:00:00.000000000Z");
        c
    }
    fn run(&self) -> Command {
        let mut c = self.base();
        c.args(["migrate-isolated-legacy-fixture", "--manifest"])
            .arg(&self.manifest);
        c
    }
}

#[tokio::test]
async fn exact_closed_fixture_migrates_repeats_and_adopts_unknown_commit() {
    let f = Fixture::new().await;
    let first = f.run().output().unwrap();
    assert_eq!(
        first.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&first.stdout)
    );
    let committed = durable_rows(&f.db).await;
    assert_eq!(
        committed.0.iter().filter(|row| row.1 == "stopped").count(),
        2
    );
    assert_eq!(committed.1.0, "terminal");
    assert_eq!(committed.1.1, 3);
    assert_eq!(committed.2, 1);
    std::fs::remove_file(&f.evidence).unwrap();
    let repeat = f.run().output().unwrap();
    assert_eq!(repeat.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&repeat.stdout).contains("already-migrated"));
    assert_eq!(durable_rows(&f.db).await, committed);
    let f = Fixture::new().await;
    let lost = f
        .run()
        .env("T050_FIXTURE_MIGRATION_TEST_LOSE_EVIDENCE", "1")
        .output()
        .unwrap();
    assert_eq!(lost.status.code(), Some(3));
    assert!(!f.evidence.exists());
    let committed = durable_rows(&f.db).await;
    assert_eq!(committed.2, 1);
    assert_eq!(f.run().output().unwrap().status.code(), Some(0));
    assert_eq!(durable_rows(&f.db).await, committed);
}

#[tokio::test]
async fn partial_cas_failpoints_roll_back_all_rows_and_receipt() {
    for stage in ["active", "stopping", "continuation"] {
        let f = Fixture::new().await;
        let before = durable_rows(&f.db).await;
        let rejected = f
            .run()
            .env("T050_FIXTURE_MIGRATION_TEST_FAIL_AFTER", stage)
            .output()
            .unwrap();
        assert_eq!(rejected.status.code(), Some(3));
        assert_eq!(durable_rows(&f.db).await, before);
        assert!(!f.evidence.exists());
    }
}

#[tokio::test]
async fn missing_extra_and_drifted_facts_are_rejected_without_mutation() {
    for sql in [
        "DELETE FROM continuation_response_receipts",
        "DELETE FROM task_admissions",
        "DELETE FROM delivery_dispositions",
        "UPDATE deliveries SET acknowledged_at=NULL",
        "UPDATE task_events SET status='running' WHERE seq=2",
        "UPDATE task_admissions SET runtime_instance='owner-s'",
        "UPDATE task_continuations SET runtime_generation='drift'",
        "UPDATE delivery_dispositions SET state='acknowledged'",
        "INSERT INTO runtime_instances VALUES('extra','2026-09-01T00:00:00Z','2026-09-01T00:00:00Z','stopping','legacy-extra')",
        "INSERT INTO workspace_claims VALUES('claim','owner-s','task','/tmp','[]',7,1,'active','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
    ] {
        let f = Fixture::new().await;
        std::fs::remove_file(&f.manifest).unwrap();
        let pool = connect(&f.db).await.unwrap();
        sqlx::query(sql).execute(&pool).await.unwrap();
        pool.close().await;
        let before = durable_rows(&f.db).await;
        assert_eq!(
            f.prepare().output().unwrap().status.code(),
            Some(3),
            "{sql}"
        );
        assert_eq!(durable_rows(&f.db).await, before);
        assert!(!f.manifest.exists());
    }
    let f = Fixture::new().await;
    let pool = connect(&f.db).await.unwrap();
    sqlx::query("UPDATE execution_leases SET fence=8")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let before = durable_rows(&f.db).await;
    assert_eq!(f.run().output().unwrap().status.code(), Some(3));
    assert_eq!(durable_rows(&f.db).await, before);
}

#[tokio::test]
async fn crash_before_commit_rolls_back_and_retry_succeeds() {
    let f = Fixture::new().await;
    let before = durable_rows(&f.db).await;
    let pause = f.root.join("precommit");
    let mut child = f
        .run()
        .env("T050_FIXTURE_MIGRATION_TEST_PRECOMMIT", &pause)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !pause.exists() {
        assert!(child.try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert_eq!(durable_rows(&f.db).await, before);
    assert_eq!(f.run().output().unwrap().status.code(), Some(0));
}

#[tokio::test]
async fn hostile_targets_and_live_interlock_fail_closed() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new().await;
    let before = durable_rows(&f.db).await;
    let guard =
        guigu_agent_bridge::runtime_admission::Guard::acquire(&f.db, &f.config, "startup").unwrap();
    assert_eq!(f.run().output().unwrap().status.code(), Some(3));
    assert_eq!(durable_rows(&f.db).await, before);
    drop(guard);
    std::fs::write(&f.evidence, b"hostile").unwrap();
    std::fs::set_permissions(&f.evidence, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(f.run().output().unwrap().status.code(), Some(3));
    assert_eq!(durable_rows(&f.db).await, before);
    let f = Fixture::new().await;
    std::fs::remove_file(&f.manifest).unwrap();
    let outside = f.root.join("outside");
    std::fs::write(&outside, b"unchanged").unwrap();
    std::os::unix::fs::symlink(&outside, &f.manifest).unwrap();
    let before = durable_rows(&f.db).await;
    assert_eq!(f.prepare().output().unwrap().status.code(), Some(3));
    assert_eq!(std::fs::read(&outside).unwrap(), b"unchanged");
    assert_eq!(durable_rows(&f.db).await, before);
    let f = Fixture::new().await;
    std::fs::remove_file(&f.manifest).unwrap();
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", f.db.to_string_lossy()));
    }
    let outside = f.root.join("outside");
    std::fs::write(&outside, b"unchanged").unwrap();
    let before = durable_rows(&f.db).await;
    std::os::unix::fs::symlink(&outside, format!("{}-wal", f.db.to_string_lossy())).unwrap();
    assert_eq!(f.prepare().output().unwrap().status.code(), Some(3));
    assert_eq!(std::fs::read(&outside).unwrap(), b"unchanged");
    std::fs::remove_file(format!("{}-wal", f.db.to_string_lossy())).unwrap();
    assert_eq!(durable_rows(&f.db).await, before);
}
