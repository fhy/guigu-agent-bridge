use std::{collections::BTreeMap, path::Path, process::Command};

use guigu_agent_bridge::{
    config::load_from_str_with_env,
    storage::{connect, migrate},
};
use sqlx::{Row, SqlitePool};

static FULL_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

async fn seed_unknown(pool: &SqlitePool) {
    sqlx::raw_sql(
        "INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('endpoint','worker','acp',1,'[]');
         INSERT INTO conversations(conversation_id,participants_json) VALUES('conversation','[]');
         INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('runtime','2026-10-20T00:00:00Z','2026-10-20T00:00:00Z','stopped','fixture');
         INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','endpoint','endpoint','conversation','retained unknown',1,0,0,1);
         INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES('task','terminal',1,'runtime','2026-10-20T00:00:00Z','2026-10-20T00:00:00Z');
         INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES('delivery','task',1,'endpoint','2026-10-20T00:00:00Z');
         INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,session_id,reason_code) VALUES('delivery','task',1,'outcome_unknown','session-fixture','unknown');
         INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource','task','owner',7,'recovery_needed','2026-10-20T00:00:00Z','2026-10-20T00:00:00Z','2099-01-01T00:00:00Z');
         INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes) VALUES('task','resource','delivery',7,1,'recovery_needed',1,0,0,'prompt','2026-10-20T00:00:00Z','2026-10-20T00:00:00Z','2026-10-20T00:00:00Z',0);",
    ).execute(pool).await.unwrap();
}

async fn history(pool: &SqlitePool) -> (String, i64, i64, i64, i64, i64) {
    sqlx::query_as("SELECT text,(SELECT count(*) FROM deliveries),(SELECT count(*) FROM task_events),(SELECT count(*) FROM transport_receipts),(SELECT count(*) FROM projection_outbox),(SELECT count(*) FROM agent_work_queue) FROM tasks WHERE task_id='task'")
        .fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn v0218_upgrade_reopen_preserves_unknown_history_through_0018() {
    let root = tempfile::tempdir().unwrap();
    let legacy = root.path().join("legacy");
    std::fs::create_dir(&legacy).unwrap();
    for entry in
        std::fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations")).unwrap()
    {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let version: u32 = name.to_str().unwrap()[..4].parse().unwrap();
        if version <= 15 {
            std::fs::copy(entry.path(), legacy.join(name)).unwrap();
        }
    }
    let database = root.path().join("upgrade.db");
    let pool = connect(&database).await.unwrap();
    sqlx::migrate::Migrator::new(legacy)
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();
    seed_unknown(&pool).await;
    let before = history(&pool).await;
    migrate(&pool).await.unwrap();
    pool.close().await;
    let pool = connect(&database).await.unwrap();
    migrate(&pool).await.unwrap();
    assert_eq!(history(&pool).await, before);
    let state: String =
        sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE delivery_id='delivery'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "outcome_unknown");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        FULL_MIGRATOR.migrations.len() as i64
    );
    let columns = sqlx::query("PRAGMA table_info(runtime_owner_evidence)")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(
        columns
            .iter()
            .any(|row| row.get::<String, _>("name") == "max_age_seconds")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM historical_unknown_abandonments")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    pool.close().await;
}

#[test]
fn old_config_and_owner_window_boundaries() {
    let env = BTreeMap::from([("HOME".into(), "/tmp/r044-config".into())]);
    let old = load_from_str_with_env("", &env).unwrap();
    assert_eq!(old.bridge.owner_evidence_max_age_seconds, 30);
    for window in [1, 30, 300] {
        let config = load_from_str_with_env(
            &format!("[bridge]\nowner_evidence_max_age_seconds={window}\n"),
            &env,
        )
        .unwrap();
        assert_eq!(config.bridge.owner_evidence_max_age_seconds, window);
    }
    for window in [0, 301] {
        assert!(
            load_from_str_with_env(
                &format!("[bridge]\nowner_evidence_max_age_seconds={window}\n"),
                &env
            )
            .is_err()
        );
    }
}

fn binary() -> std::path::PathBuf {
    std::env::var_os("R044_BINARY")
        .map(Into::into)
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_guigu-agent-bridge").into())
}

fn invoke(root: &Path, mode: &str) -> std::process::Output {
    Command::new(binary())
        .args(["historical-unknown-abandon", "--mode", mode, "--database"])
        .arg(root.join("bridge.db"))
        .arg("--config")
        .arg(root.join("bridge.toml"))
        .args([
            "--decision-id",
            "decision",
            "--task-id",
            "task",
            "--delivery-id",
            "delivery",
            "--resource-key",
            "resource",
            "--owner-token",
            "owner",
            "--fence",
            "7",
            "--revision",
            "1",
            "--generation",
            "generation.v1:strict",
            "--reason",
            "operator decision",
            "--policy-ref",
            "R044",
            "--authorization-ref",
            "fixture",
        ])
        .output()
        .unwrap()
}

#[tokio::test]
async fn executable_exact_target_modes_reopen_repeat_and_no_replay() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("bridge.toml"), "").unwrap();
    let database = root.path().join("bridge.db");
    let pool = connect(&database).await.unwrap();
    migrate(&pool).await.unwrap();
    seed_unknown(&pool).await;
    let before = history(&pool).await;
    pool.close().await;
    let version = Command::new(binary()).arg("--version").output().unwrap();
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8(version.stdout).unwrap(),
        format!("guigu-agent-bridge {}\n", env!("CARGO_PKG_VERSION"))
    );
    for mode in ["dry-run", "apply", "verify", "apply"] {
        let output = invoke(root.path(), mode);
        assert!(
            output.status.success(),
            "mode={mode} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let pool = connect(&database).await.unwrap();
        assert_eq!(history(&pool).await, before);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM historical_unknown_abandonments")
                .fetch_one(&pool)
                .await
                .unwrap(),
            i64::from(mode != "dry-run")
        );
        if mode != "dry-run" {
            let state: (String, String, String) = sqlx::query_as("SELECT c.state,l.state,d.state FROM task_continuations c JOIN execution_leases l USING(task_id) JOIN delivery_dispositions d USING(task_id)")
                .fetch_one(&pool).await.unwrap();
            assert_eq!(
                state,
                ("terminal".into(), "released".into(), "terminal".into())
            );
        }
        pool.close().await;
    }
    let invalid = Command::new(binary())
        .arg("historical-unknown-abandon")
        .output()
        .unwrap();
    assert_eq!(invalid.status.code(), Some(2));
}

#[tokio::test]
async fn executable_missing_schema_refuses_without_migration() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("bridge.toml"), "").unwrap();
    let pool = connect(root.path().join("bridge.db")).await.unwrap();
    pool.close().await;
    assert_eq!(invoke(root.path(), "dry-run").status.code(), Some(6));
    let pool = connect(root.path().join("bridge.db")).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sqlite_master WHERE type='table'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    pool.close().await;
}
