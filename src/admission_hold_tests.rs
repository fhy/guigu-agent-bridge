use super::*;

#[test]
fn exact_command_refuses_duplicate_or_missing_scope() {
    let arguments = [
        "hold-unresolved-admission",
        "--database",
        "/tmp/database",
        "--config",
        "/tmp/config",
        "--task",
        "task",
        "--owner",
        "owner",
        "--revision",
        "1",
    ];
    let parsed = arguments.iter().map(OsString::from).collect::<Vec<_>>();
    assert!(parse(&parsed).is_ok());
    let mut duplicate = parsed.clone();
    duplicate[3] = OsString::from("--database");
    assert!(parse(&duplicate).is_err());
    let mut missing = parsed;
    missing.pop();
    assert!(parse(&missing).is_err());
}

#[tokio::test]
async fn exact_dispatching_admission_is_held_without_a_terminal_event() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("state.db");
    let pool = crate::storage::connect(&database).await.unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let context = bootstrap_nonadmin(&pool, false).await.unwrap();
    let scope = RecoveryScope::new(context, EffectDomain::Local, None);
    let mut transaction = writer::begin(&pool, Some(&scope)).await.unwrap();
    let statements = seed_statements();
    for (table, key, statement) in statements {
        sqlx::query(statement)
            .execute(&mut *transaction)
            .await
            .unwrap();
        writer::inserted(
            &mut transaction,
            Some(&scope),
            &writer::text_identity(table, key).unwrap(),
        )
        .await
        .unwrap();
    }
    transaction.commit().await.unwrap();
    let mut request = Request {
        database,
        config: root.path().join("config"),
        task: "task".into(),
        owner: "owner".into(),
        revision: 0,
    };
    assert!(hold(&pool, &scope, &request).await.is_err());
    request.revision = 1;
    assert!(hold(&pool, &scope, &request).await.is_ok());
    let events: Vec<(i64, String)> =
        sqlx::query_as("SELECT seq,status FROM task_events ORDER BY seq")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(events, vec![(1, "queued".into()), (2, "dispatched".into())]);
    let state: (String, i64) =
        sqlx::query_as("SELECT state,revision FROM task_admissions WHERE task_id='task'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, ("recovery_needed".into(), 2));
    assert!(hold(&pool, &scope, &request).await.is_err());
    let admitted = crate::storage::ReliabilityStore::new(pool.clone())
        .classify_admissions(4096)
        .await
        .unwrap();
    assert!(admitted.covered.contains(&"task".into()));
    assert!(admitted.eligible.is_empty());
    assert!(!admitted.blocked);
}

fn seed_statements() -> [(&'static str, &'static str, &'static str); 7] {
    [
        (
            "agents",
            "agent",
            "INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('agent','worker','acp',1,'[]')",
        ),
        (
            "conversations",
            "conversation",
            "INSERT INTO conversations(conversation_id,participants_json) VALUES('conversation','[]')",
        ),
        (
            "runtime_instances",
            "owner",
            "INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('owner','t','t','stopped','pid:99999999')",
        ),
        (
            "tasks",
            "task",
            "INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','agent','agent','conversation','private',5,0,0,1)",
        ),
        (
            "task_admissions",
            "task",
            "INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES('task','dispatching',1,'owner','t','t')",
        ),
        (
            "task_events",
            "event1",
            "INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES('event1','task',1,'queued','t','{}')",
        ),
        (
            "task_events",
            "event2",
            "INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES('event2','task',2,'dispatched','t','{}')",
        ),
    ]
}
