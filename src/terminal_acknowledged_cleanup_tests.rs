use super::*;

use sqlx::SqlitePool;
#[test]
fn parser_has_no_caller_target_fields() {
    let args = [
        "historical-terminal-cleanup",
        "--mode",
        "dry-run",
        "--database",
        "db",
        "--config",
        "cfg",
        "--decision-id",
        "d",
        "--policy-ref",
        "p",
        "--authorization-ref",
        "a",
    ]
    .into_iter()
    .map(OsString::from)
    .collect::<Vec<_>>();
    let request = parse_args(&args).unwrap();
    assert_eq!(request.mode, Mode::DryRun);
    let mut invalid = args.clone();
    invalid.extend([OsString::from("--task-id"), OsString::from("guess")]);
    assert_eq!(parse_args(&invalid), Err(CleanupError::InvalidArguments));
}

async fn fixture() -> (tempfile::TempDir, Request) {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("state.db");
    let config = root.path().join("config.toml");
    std::fs::write(&config, "").unwrap();
    let pool: SqlitePool = crate::storage::connect(&database).await.unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    for query in [
        "INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('endpoint','worker','acp',1,'[]')",
        "INSERT INTO conversations(conversation_id,participants_json) VALUES('conversation','[]')",
        "INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('runtime','now','now','stopped','fixture')",
        "INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','endpoint','endpoint','conversation','fixture',1,0,0,1)",
        "INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES('event','task',1,'completed','now','{}')",
        "INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES('task','terminal',1,'runtime','now','now')",
        "INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at,acknowledged_at) VALUES('delivery','task',1,'endpoint','now','now')",
        "INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES('delivery','task',1,'terminal')",
        "INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource','task','owner',7,'recovery_needed','now','now','2099')",
        "INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes) VALUES('task','resource','delivery',7,1,'recovery_needed',1,0,0,'prompt','now','now','now',0)",
    ] {
        sqlx::query(query).execute(&pool).await.unwrap();
    }
    pool.close().await;
    (
        root,
        Request {
            database,
            config,
            mode: Mode::Apply,
            decision_id: "decision".into(),
            policy_ref: "R050".into(),
            authorization_ref: "fixture".into(),
        },
    )
}

#[tokio::test]
async fn apply_reopen_verify_and_repeat_are_atomic_and_no_replay() {
    let (_root, request) = fixture().await;
    assert_eq!(run(request.clone()).await, Ok("applied"));
    let mut verify = request.clone();
    verify.mode = Mode::Verify;
    assert_eq!(run(verify).await.unwrap(), "verified post-state");
    assert_eq!(run(request.clone()).await.unwrap(), "already applied");
    let pool = crate::storage::connect(&request.database).await.unwrap();
    let states: (String, i64, String) = sqlx::query_as("SELECT l.state,c.revision,c.state FROM execution_leases l JOIN task_continuations c ON c.resource_key=l.resource_key")
            .fetch_one(&pool).await.unwrap();
    assert_eq!(states, ("released".into(), 2, "terminal".into()));
    let audit_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments")
            .fetch_one(&pool)
            .await
            .unwrap();
    let event_count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    let receipt_count: i64 = sqlx::query_scalar("SELECT count(*) FROM transport_receipts")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(audit_count, 1);
    assert_eq!(event_count, 1);
    assert_eq!(receipt_count, 0);
    pool.close().await;
}

#[tokio::test]
async fn injected_failures_after_audit_and_cas_roll_back_after_reopen() {
    for failure in [FailurePoint::AfterAudit, FailurePoint::AfterCas] {
        let (_root, request) = fixture().await;
        assert_eq!(
            run_with_failure(request.clone(), Some(failure)).await,
            Err(CleanupError::Database)
        );
        let pool = crate::storage::connect(&request.database).await.unwrap();
        let audit_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments")
                .fetch_one(&pool)
                .await
                .unwrap();
        let lease_state: String = sqlx::query_scalar("SELECT state FROM execution_leases")
            .fetch_one(&pool)
            .await
            .unwrap();
        let continuation: (i64, String) =
            sqlx::query_as("SELECT revision,state FROM task_continuations")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(audit_count, 0);
        assert_eq!(lease_state, "recovery_needed");
        assert_eq!(continuation, (1, "recovery_needed".into()));
        pool.close().await;
    }
}
