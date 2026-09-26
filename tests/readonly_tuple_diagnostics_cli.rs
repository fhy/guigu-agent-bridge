use guigu_agent_bridge::models::TaskEventPayload;
use guigu_agent_bridge::storage::{connect, migrate};
use std::process::Command;
use uuid::Uuid;

#[test]
fn diagnosis_missing_database_is_fixed_and_redacted() {
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args([
            "diagnose-preacceptance-conflict",
            "--database",
            "/sentinel/diagnosis.db",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(9));
    assert_eq!(
        output.stdout,
        b"diagnose-preacceptance-conflict result=database\n"
    );
    assert!(output.stderr.is_empty());
    let output = String::from_utf8_lossy(&output.stdout);
    assert!(!output.contains("sentinel"));
    assert!(!output.contains("SELECT"));
}

#[test]
fn diagnosis_invalid_arguments_are_fixed_and_redacted() {
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args(["diagnose-preacceptance-conflict", "--database"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        output.stdout,
        b"diagnose-preacceptance-conflict result=invalid-arguments\n"
    );
    assert!(output.stderr.is_empty());
}

#[tokio::test]
async fn diagnosis_empty_is_fixed_and_read_only() {
    let path = std::env::temp_dir().join(format!("t041-empty-cli-{}.db", Uuid::now_v7()));
    let pool = connect(&path).await.unwrap();
    migrate(&pool).await.unwrap();
    pool.close().await;
    let before = std::fs::read(&path).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args(["diagnose-preacceptance-conflict", "--database"])
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(
        output.stdout,
        b"diagnose-preacceptance-conflict result=empty\n"
    );
    assert!(output.stderr.is_empty());
    assert_eq!(before, std::fs::read(&path).unwrap());
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn diagnosis_conflict_output_is_fixed_canonical_and_redacted() {
    let path = std::env::temp_dir().join(format!("t041-cli-{}.db", Uuid::now_v7()));
    let pool = connect(&path).await.unwrap();
    migrate(&pool).await.unwrap();
    let endpoint = Uuid::now_v7().to_string();
    let conversation = Uuid::now_v7().to_string();
    let task = Uuid::now_v7().to_string();
    let delivery = Uuid::now_v7().to_string();
    let runtime = Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?, 'acp',1,'[]')").bind(&endpoint).bind(Uuid::now_v7().to_string()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
        .bind(&conversation)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'fixture',1,0,0,0)").bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(&pool).await.unwrap();
    let payload = serde_json::to_string(&TaskEventPayload::Dispatched {
        delivery_id: delivery.parse().unwrap(),
        attempt: 1,
    })
    .unwrap();
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'dispatched','t',?)").bind(Uuid::now_v7().to_string()).bind(&task).bind(payload).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES(?,'t','t','active','fp')").bind(&runtime).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',0,?,'t','t')").bind(&task).bind(&runtime).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'t')").bind(&delivery).bind(&task).bind(&endpoint).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'prepared','x')").bind(&delivery).bind(&task).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,1,'recovery_needed','t','t','2099')").bind("resource").bind(&task).bind(&runtime).execute(&pool).await.unwrap();
    pool.close().await;
    let before = std::fs::read(&path).unwrap();
    let eligible = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args(["diagnose-preacceptance-conflict", "--database"])
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(eligible.status.code(), Some(0));
    assert_eq!(
        eligible.stdout,
        b"diagnose-preacceptance-conflict result=eligible count=1\n"
    );
    assert!(eligible.stderr.is_empty());
    assert_eq!(before, std::fs::read(&path).unwrap());
    let pool = connect(&path).await.unwrap();
    sqlx::query("UPDATE execution_leases SET owner_token='other-owner' WHERE task_id=?")
        .bind(&task)
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let before_conflict = std::fs::read(&path).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args(["diagnose-preacceptance-conflict", "--database"])
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(6));
    assert_eq!(
        output.stdout,
        b"diagnose-preacceptance-conflict result=conflicting causes=lease-owner-mismatch count=1\n"
    );
    assert!(output.stderr.is_empty());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(&task));
    assert_eq!(before_conflict, std::fs::read(&path).unwrap());
    let _ = std::fs::remove_file(path);
}
