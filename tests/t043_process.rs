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

#[tokio::test]
async fn fixed_rejection_process_matrix_is_repeatable_and_redacted() {
    let empty = std::env::temp_dir().join(format!("t043-empty-{}.db", Uuid::now_v7()));
    let pool = connect(&empty).await.unwrap();
    migrate(&pool).await.unwrap();
    pool.close().await;
    assert_result(&empty, "empty", 3);
    let active = std::env::temp_dir().join(format!("t043-active-{}.db", Uuid::now_v7()));
    fixture(&active).await;
    let pool = connect(&active).await.unwrap();
    sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('orphan-owner','t','t','active','secret')").execute(&pool).await.unwrap();
    pool.close().await;
    assert_result(&active, "active-owner", 6);
    let fenced = std::env::temp_dir().join(format!("t043-fenced-{}.db", Uuid::now_v7()));
    fixture(&fenced).await;
    let pool = connect(&fenced).await.unwrap();
    sqlx::query("UPDATE runtime_instances SET state='active'")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert_result(&fenced, "fenced", 7);
    let multiple = std::env::temp_dir().join(format!("t043-multiple-{}.db", Uuid::now_v7()));
    fixture(&multiple).await;
    fixture(&multiple).await;
    assert_result(&multiple, "multiple", 4);
    let malformed = std::env::temp_dir().join(format!("t043-malformed-{}.db", Uuid::now_v7()));
    fixture(&malformed).await;
    let pool = connect(&malformed).await.unwrap();
    sqlx::query("DROP TABLE execution_leases")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    assert_result(&malformed, "malformed", 5);
    let zero = std::env::temp_dir().join(format!("t043-zero-cas-{}.db", Uuid::now_v7()));
    fixture(&zero).await;
    let pool = connect(&zero).await.unwrap();
    sqlx::query("CREATE TRIGGER t043_zero_lease BEFORE UPDATE OF state ON execution_leases WHEN NEW.state='released' BEGIN SELECT RAISE(IGNORE); END").execute(&pool).await.unwrap();
    pool.close().await;
    assert_result(&zero, "fenced", 7);
    for _ in 0..2 {
        let output = Command::new(binary())
            .args(["reconcile-orphaned-preacceptance", "--database"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(
            output.stdout,
            b"reconcile-orphaned-preacceptance result=invalid-arguments status=2\n"
        );
        assert!(output.stderr.is_empty());
        let output = run(Path::new("/no/such/t043.db"));
        assert_eq!(output.status.code(), Some(9));
        assert_eq!(
            output.stdout,
            b"reconcile-orphaned-preacceptance result=database status=9\n"
        );
        assert!(output.stderr.is_empty());
    }
    for path in [empty, active, fenced, multiple, malformed, zero] {
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn eligible_orphan_closes_once_then_is_exactly_idempotent() {
    let path = std::env::temp_dir().join(format!("t043-{}.db", Uuid::now_v7()));
    fixture(&path).await;
    fixture(&path).await;
    let pool = connect(&path).await.unwrap();
    let unrelated: String =
        sqlx::query_scalar("SELECT task_id FROM tasks ORDER BY rowid DESC LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("UPDATE task_admissions SET state='ready' WHERE task_id=?")
        .bind(&unrelated)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE execution_leases SET state='released' WHERE task_id=?")
        .bind(&unrelated)
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let first = run(&path);
    assert_eq!(
        first.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(
        first.stdout,
        b"reconcile-orphaned-preacceptance result=reconciled status=0\n"
    );
    assert!(first.stderr.is_empty());
    let pool = connect(&path).await.unwrap();
    let state:(String,i64,Option<String>,String,String)=sqlx::query_as("SELECT a.state,a.revision,d.acknowledged_at,p.state,l.state FROM task_admissions a JOIN deliveries d ON d.task_id=a.task_id JOIN delivery_dispositions p ON p.delivery_id=d.delivery_id JOIN execution_leases l ON l.task_id=a.task_id WHERE a.state='terminal'").fetch_one(&pool).await.unwrap();
    assert_eq!(state.0, "terminal");
    assert_eq!(state.1, 5);
    assert!(state.2.is_some());
    assert_eq!(state.3, "terminal");
    assert_eq!(state.4, "released");
    pool.close().await;
    let before = std::fs::read(&path).unwrap();
    let second = run(&path);
    let after = std::fs::read(&path).unwrap();
    assert_eq!(second.status.code(), Some(0));
    assert_eq!(
        second.stdout,
        b"reconcile-orphaned-preacceptance result=already-reconciled status=0\n"
    );
    assert!(second.stderr.is_empty());
    assert_eq!(before, after);
    let rendered = String::from_utf8_lossy(&first.stdout);
    assert!(!rendered.contains("secret") && !rendered.contains(path.to_str().unwrap()));
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn post_commit_verification_failure_reports_database_without_retry() {
    let path = std::env::temp_dir().join(format!("t043-post-verify-{}.db", Uuid::now_v7()));
    fixture(&path).await;
    let pool = connect(&path).await.unwrap();
    sqlx::query("CREATE TRIGGER t043_post_verify AFTER UPDATE OF state ON execution_leases WHEN NEW.state='released' BEGIN UPDATE delivery_dispositions SET reason_code='verification-sentinel' WHERE task_id=NEW.task_id; END").execute(&pool).await.unwrap();
    pool.close().await;
    let output = run(&path);
    assert_eq!(output.status.code(), Some(9));
    assert_eq!(
        output.stdout,
        b"reconcile-orphaned-preacceptance result=database status=9\n"
    );
    assert!(output.stderr.is_empty());
    let pool = connect(&path).await.unwrap();
    let fields:(String,i64,String,String,i64)=sqlx::query_as("SELECT a.state,a.revision,p.state,l.state,(SELECT count(*) FROM task_events WHERE status='failed') FROM task_admissions a JOIN delivery_dispositions p ON p.task_id=a.task_id JOIN execution_leases l ON l.task_id=a.task_id").fetch_one(&pool).await.unwrap();
    assert_eq!(
        fields,
        (
            "terminal".into(),
            5,
            "terminal".into(),
            "released".into(),
            1
        )
    );
    pool.close().await;
    let before = std::fs::read(&path).unwrap();
    let second = run(&path);
    assert_ne!(second.status.code(), Some(0));
    assert_eq!(before, std::fs::read(&path).unwrap());
    let _ = std::fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread")]
async fn immediate_lock_contention_is_fixed_busy_and_read_only() {
    let path = std::env::temp_dir().join(format!("t043-busy-{}.db", Uuid::now_v7()));
    fixture(&path).await;
    let opts = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(false)
        .foreign_keys(true);
    let before = std::fs::read(&path).unwrap();
    for index in 0..2 {
        let mut lock = SqliteConnection::connect_with(&opts).await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut lock)
            .await
            .unwrap();
        let output = run(&path);
        assert_eq!(output.status.code(), Some(8), "iteration {index}");
        assert_eq!(
            output.stdout,
            b"reconcile-orphaned-preacceptance result=busy status=8\n"
        );
        assert!(output.stderr.is_empty());
        assert_eq!(before, std::fs::read(&path).unwrap());
        let _ = sqlx::query("ROLLBACK").execute(&mut lock).await;
        lock.close().await.unwrap();
    }
    let _ = std::fs::remove_file(path);
}
