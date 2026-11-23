use guigu_agent_bridge::models::TaskEventPayload;
use guigu_agent_bridge::storage::{connect, migrate};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use std::{
    path::{Path, PathBuf},
    process::Command,
};
use uuid::Uuid;

fn binary() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_guigu-agent-bridge")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/debug/guigu-agent-bridge"))
}

async fn fixture(path: &Path) {
    let pool = connect(path).await.unwrap();
    migrate(&pool).await.unwrap();
    let endpoint = Uuid::now_v7().to_string();
    let conversation = Uuid::now_v7().to_string();
    let task = Uuid::now_v7().to_string();
    let delivery = Uuid::now_v7().to_string();
    let runtime = Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?,'acp',1,'[]')").bind(&endpoint).bind(Uuid::now_v7().to_string()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
        .bind(&conversation)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'secret',1,0,0,0)").bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(&pool).await.unwrap();
    let payload = serde_json::to_string(&TaskEventPayload::Dispatched {
        delivery_id: delivery.parse().unwrap(),
        attempt: 1,
    })
    .unwrap();
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'dispatched','2026-01-01T00:00:00Z',?)").bind(Uuid::now_v7().to_string()).bind(&task).bind(payload).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES(?,'t','t','stopped','secret-fingerprint')").bind(&runtime).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',4,?,'t','t')").bind(&task).bind(&runtime).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2026-01-01T00:00:00Z')").bind(&delivery).bind(&task).bind(&endpoint).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'prepared','fixture')").bind(&delivery).bind(&task).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,'orphan-owner',4,'recovery_needed','t','t','later')").bind(Uuid::now_v7().to_string()).bind(&task).execute(&pool).await.unwrap();
    pool.close().await;
}

fn run(path: &Path) -> std::process::Output {
    Command::new(binary())
        .args([
            "reconcile-orphaned-preacceptance",
            "--database",
            path.to_str().unwrap(),
        ])
        .output()
        .unwrap()
}

fn assert_result(path: &Path, result: &str, status: i32) {
    for _ in 0..2 {
        let before = std::fs::read(path).unwrap();
        let output = run(path);
        assert_eq!(output.status.code(), Some(status));
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            format!("reconcile-orphaned-preacceptance result={result} status={status}\n")
        );
        assert!(output.stderr.is_empty());
        assert_eq!(before, std::fs::read(path).unwrap());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(!stdout.contains(path.to_str().unwrap()));
        assert!(!stdout.contains("secret") && !stdout.contains("orphan-owner"));
    }
}

async fn mutate(path: &Path, sql: &'static str) {
    let pool = connect(path).await.unwrap();
    sqlx::query(sql).execute(&pool).await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn lease_owner_and_event_precedence_is_repeatable_and_redacted() {
    let duplicate =
        std::env::temp_dir().join(format!("t043-duplicate-lease-{}.db", Uuid::now_v7()));
    fixture(&duplicate).await;
    let pool = connect(&duplicate).await.unwrap();
    let task: String = sqlx::query_scalar("SELECT task_id FROM task_admissions LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,'other-secret-owner',5,'recovery_needed','t','t','later')").bind(Uuid::now_v7().to_string()).bind(task).execute(&pool).await.unwrap();
    pool.close().await;
    assert_result(&duplicate, "multiple", 4);

    let missing = std::env::temp_dir().join(format!("t043-missing-lease-{}.db", Uuid::now_v7()));
    fixture(&missing).await;
    mutate(&missing, "DELETE FROM execution_leases").await;
    assert_result(&missing, "fenced", 7);

    let stopped = std::env::temp_dir().join(format!("t043-owner-stopped-{}.db", Uuid::now_v7()));
    fixture(&stopped).await;
    mutate(&stopped, "INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('orphan-owner','t','t','stopped','secret-owner')").await;
    assert_result(&stopped, "fenced", 7);

    let unknown = std::env::temp_dir().join(format!("t043-owner-unknown-{}.db", Uuid::now_v7()));
    fixture(&unknown).await;
    let opts = SqliteConnectOptions::new()
        .filename(&unknown)
        .create_if_missing(false)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&opts).await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints=ON")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('orphan-owner','t','t','unknown','secret-owner')").execute(&mut connection).await.unwrap();
    connection.close().await.unwrap();
    assert_result(&unknown, "fenced", 7);

    let malformed = std::env::temp_dir().join(format!("t043-event-value-{}.db", Uuid::now_v7()));
    fixture(&malformed).await;
    mutate(
        &malformed,
        "UPDATE task_events SET payload='\"secret-invalid-event\"'",
    )
    .await;
    assert_result(&malformed, "malformed", 5);

    let variant = std::env::temp_dir().join(format!("t043-event-variant-{}.db", Uuid::now_v7()));
    fixture(&variant).await;
    let pool = connect(&variant).await.unwrap();
    sqlx::query("UPDATE task_events SET payload=?")
        .bind(serde_json::to_string(&TaskEventPayload::Queued).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert_result(&variant, "malformed", 5);

    let binding = std::env::temp_dir().join(format!("t043-event-binding-{}.db", Uuid::now_v7()));
    fixture(&binding).await;
    let pool = connect(&binding).await.unwrap();
    let payload = serde_json::to_string(&TaskEventPayload::Dispatched {
        delivery_id: Uuid::nil().to_string().parse().unwrap(),
        attempt: 99,
    })
    .unwrap();
    sqlx::query("UPDATE task_events SET payload=?")
        .bind(payload)
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert_result(&binding, "malformed", 5);

    for path in [
        duplicate, missing, stopped, unknown, malformed, variant, binding,
    ] {
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn malformed_closed_candidate_is_not_hidden_by_a_valid_candidate() {
    let path = std::env::temp_dir().join(format!("t043-mixed-closed-{}.db", Uuid::now_v7()));
    fixture(&path).await;
    assert_eq!(run(&path).status.code(), Some(0));
    fixture(&path).await;
    let pool = connect(&path).await.unwrap();
    let task: String =
        sqlx::query_scalar("SELECT task_id FROM task_admissions WHERE state='dispatching'")
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("UPDATE task_admissions SET state='terminal',revision=revision+1 WHERE task_id=?")
        .bind(&task)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE deliveries SET acknowledged_at='t' WHERE task_id=?")
        .bind(&task)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE delivery_dispositions SET state='terminal',reason_code='pre_acceptance_auth_failure' WHERE task_id=?").bind(&task).execute(&pool).await.unwrap();
    sqlx::query("UPDATE execution_leases SET state='released' WHERE task_id=?")
        .bind(&task)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,2,'failed','t','\"secret-invalid-event\"')").bind(Uuid::now_v7().to_string()).bind(&task).execute(&pool).await.unwrap();
    pool.close().await;
    assert_result(&path, "malformed", 5);
    let _ = std::fs::remove_file(path);
}
