use guigu_agent_bridge::models::TaskEventPayload;
use guigu_agent_bridge::storage::{connect, migrate};
use std::path::PathBuf;
use std::process::Command;
use uuid::Uuid;

async fn fixture(path: &std::path::Path, suffix: &str, runtime_state: &str, lease_owner: &str) {
    let pool = connect(path).await.unwrap();
    migrate(&pool).await.unwrap();
    let endpoint = Uuid::now_v7().to_string();
    let conversation = Uuid::now_v7().to_string();
    let task = Uuid::now_v7().to_string();
    let delivery = Uuid::now_v7().to_string();
    let runtime = format!("runtime-{suffix}");
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?,'acp',1,'[]')").bind(&endpoint).bind(Uuid::now_v7().to_string()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
        .bind(&conversation)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'secret-sentinel',1,0,0,0)").bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(&pool).await.unwrap();
    let payload = serde_json::to_string(&TaskEventPayload::Dispatched {
        delivery_id: delivery.parse().unwrap(),
        attempt: 1,
    })
    .unwrap();
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'dispatched','t',?)").bind(Uuid::now_v7().to_string()).bind(&task).bind(payload).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES(?,'t','t',?,'secret-fingerprint')").bind(&runtime).bind(runtime_state).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',4,?,'t','t')").bind(&task).bind(&runtime).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'t')").bind(&delivery).bind(&task).bind(&endpoint).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'prepared','fixture')").bind(&delivery).bind(&task).execute(&pool).await.unwrap();
    let owner = if lease_owner == "runtime" {
        runtime
    } else {
        lease_owner.to_owned()
    };
    sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,7,'recovery_needed','t','t','later')").bind(format!("resource-{suffix}")).bind(&task).bind(owner).execute(&pool).await.unwrap();
    pool.close().await;
}

fn assert_process(path: &std::path::Path, category: &str, status: i32) {
    for _ in 0..2 {
        let before = std::fs::read(path).unwrap();
        let output = Command::new(binary())
            .args([
                "reconcile-lease-owner",
                "--database",
                path.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        let after = std::fs::read(path).unwrap();
        assert_eq!(output.status.code(), Some(status));
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            format!("reconcile-lease-owner result={category} status={status}\n")
        );
        assert!(output.stderr.is_empty());
        let rendered = String::from_utf8_lossy(&output.stdout);
        assert!(!rendered.contains("secret-") && !rendered.contains(path.to_str().unwrap()));
        assert_eq!(before, after);
    }
}

fn binary() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_guigu-agent-bridge")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/debug/guigu-agent-bridge"))
}

#[tokio::test]
async fn t042_process_fixed_output_redaction_and_repeat() {
    let path = std::env::temp_dir().join(format!("t042-process-{}.db", Uuid::now_v7()));
    let pool = connect(&path).await.unwrap();
    migrate(&pool).await.unwrap();
    pool.close().await;
    let path_text = path.to_str().unwrap();
    let run = || {
        Command::new(binary())
            .args(["reconcile-lease-owner", "--database", path_text])
            .output()
            .unwrap()
    };
    let before_empty = std::fs::read(&path).unwrap();
    let first = run();
    let after_first_empty = std::fs::read(&path).unwrap();
    let second = run();
    let after_second_empty = std::fs::read(&path).unwrap();
    assert_eq!(before_empty, after_first_empty);
    assert_eq!(after_first_empty, after_second_empty);
    for output in [first, second] {
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(
            output.stdout,
            b"reconcile-lease-owner result=empty status=3\n"
        );
        assert!(output.stderr.is_empty());
        assert!(!String::from_utf8_lossy(&output.stdout).contains(path_text));
    }
    for _ in 0..2 {
        let missing = Command::new(binary())
            .args([
                "reconcile-lease-owner",
                "--database",
                "/no/such/t042.sqlite",
            ])
            .output()
            .unwrap();
        assert_eq!(missing.status.code(), Some(10));
        assert_eq!(
            missing.stdout,
            b"reconcile-lease-owner result=database status=10\n"
        );
        assert!(missing.stderr.is_empty());
        let invalid = Command::new(binary())
            .args(["reconcile-lease-owner", "--database"])
            .output()
            .unwrap();
        assert_eq!(invalid.status.code(), Some(2));
        assert_eq!(
            invalid.stdout,
            b"reconcile-lease-owner result=invalid-arguments status=2\n"
        );
        assert!(invalid.stderr.is_empty());
    }
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn t042_process_business_category_matrix() {
    let root = std::env::temp_dir();
    let eligible = root.join(format!("t042-eligible-{}.db", Uuid::now_v7()));
    fixture(&eligible, "eligible", "stopped", "runtime").await;
    assert_process(&eligible, "eligible", 0);
    let orphan = root.join(format!("t042-orphan-{}.db", Uuid::now_v7()));
    fixture(&orphan, "orphan", "stopped", "orphan-owner").await;
    assert_process(&orphan, "non-actionable-orphan", 6);
    let active = root.join(format!("t042-active-{}.db", Uuid::now_v7()));
    fixture(&active, "active", "active", "runtime").await;
    assert_process(&active, "active-owner", 7);
    let fenced = root.join(format!("t042-fenced-{}.db", Uuid::now_v7()));
    fixture(&fenced, "fenced", "stopped", "runtime").await;
    let pool = connect(&fenced).await.unwrap();
    sqlx::query("UPDATE execution_leases SET state='active'")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert_process(&fenced, "fenced", 8);
    let multiple = root.join(format!("t042-multiple-{}.db", Uuid::now_v7()));
    fixture(&multiple, "one", "stopped", "runtime").await;
    fixture(&multiple, "two", "stopped", "runtime").await;
    assert_process(&multiple, "multiple", 4);
    let malformed = root.join(format!("t042-malformed-{}.db", Uuid::now_v7()));
    fixture(&malformed, "malformed", "stopped", "runtime").await;
    let pool = connect(&malformed).await.unwrap();
    sqlx::query("DROP TABLE execution_leases")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert_process(&malformed, "malformed", 5);
    for path in [eligible, orphan, active, fenced, multiple, malformed] {
        let _ = std::fs::remove_file(path);
    }
}
