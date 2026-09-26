use guigu_agent_bridge::models::TaskEventPayload;
use guigu_agent_bridge::storage::{connect, migrate};
use std::process::Command;
use uuid::Uuid;

#[test]
fn select_preacceptance_cli_has_fixed_success_and_rejection_shapes() {
    let bin = env!("CARGO_BIN_EXE_guigu-agent-bridge");
    let rejected = Command::new(bin)
        .args([
            "select-preacceptance",
            "--database",
            "/definitely/missing.db",
        ])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&rejected.stdout),
        String::from_utf8_lossy(&rejected.stderr)
    );
    assert!(!combined.contains("/definitely/missing.db"));
    assert!(!combined.contains("SELECT"));
    assert!(!combined.contains("delivery"));

    let invalid = Command::new(bin)
        .args(["select-preacceptance", "--database"])
        .output()
        .unwrap();
    assert!(!invalid.status.success());
    let invalid_text = format!(
        "{}{}",
        String::from_utf8_lossy(&invalid.stdout),
        String::from_utf8_lossy(&invalid.stderr)
    );
    assert!(!invalid_text.contains("--database"));
}

#[test]
fn combined_cli_rejection_is_fixed_and_redacted() {
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args([
            "recover-selected-preacceptance",
            "--database",
            "/missing/t039.db",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!text.contains("/missing/t039.db"));
    assert!(!text.contains("SELECT"));
    assert!(!text.contains("delivery"));
}

#[tokio::test]
async fn select_preacceptance_cli_success_is_fixed_and_redacted() {
    let path = std::env::temp_dir().join(format!("t038-cli-success-{}.db", Uuid::now_v7()));
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
    sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES(?,'t','t','active','fixture')").bind(&runtime).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',4,?,'t','t')").bind(&task).bind(&runtime).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'t')").bind(&delivery).bind(&task).bind(&endpoint).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'prepared','fixture')").bind(&delivery).bind(&task).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,7,'recovery_needed','t','t','2000')").bind(format!("resource-{task}" )).bind(&task).bind(&runtime).execute(&pool).await.unwrap();
    pool.close().await;
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args(["select-preacceptance", "--database"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "select-preacceptance result=selected count=1\n"
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains(&task));
    assert!(output.stderr.is_empty());
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn recover_selected_preacceptance_cli_success_is_fixed_and_redacted() {
    let path = std::env::temp_dir().join(format!("t039-cli-success-{}.db", Uuid::now_v7()));
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
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'dispatched','2026-01-01T00:00:00Z',?)").bind(Uuid::now_v7().to_string()).bind(&task).bind(payload).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES(?,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','active','fixture')").bind(&runtime).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',0,?,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").bind(&task).bind(&runtime).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2026-01-01T00:00:00Z')").bind(&delivery).bind(&task).bind(&endpoint).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'prepared','fixture')").bind(&delivery).bind(&task).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,1,'recovery_needed','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2099-01-01T00:00:00Z')").bind(format!("resource-{task}")).bind(&task).bind(&runtime).execute(&pool).await.unwrap();
    pool.close().await;
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args(["recover-selected-preacceptance", "--database"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "recover-selected-preacceptance result=recovered count=1\n"
    );
    assert!(output.stderr.is_empty());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(!text.contains(&task));
    assert!(!text.contains(path.to_string_lossy().as_ref()));
    let _ = std::fs::remove_file(path);
}
