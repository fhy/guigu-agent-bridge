use chrono::Utc;
use guigu_agent_bridge::{
    bus::{AdmissionContext, derive_endpoint_id},
    models::{
        AgentTask, ConversationId, EventId, Priority, TaskEvent, TaskEventPayload, TaskId,
        TaskStatus,
    },
    storage::{CutoverStore, ReliabilityStore, connect, cutover_digest, migrate},
};

#[tokio::test]
async fn crash_after_real_admission_before_sync_receipt_recovers_without_another_task() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("state.db");
    let pool = connect(&database).await.unwrap();
    migrate(&pool).await.unwrap();
    let binding = cutover_digest(b"binding");
    let cutover = CutoverStore::prepare(
        pool.clone(),
        "operation",
        &binding,
        &cutover_digest(b"archive"),
    )
    .await
    .unwrap();
    cutover.activate("A").await.unwrap();
    let endpoint = derive_endpoint_id("fixture");
    let conversation = ConversationId::generate();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,'fixture','acp',1,'[]')")
        .bind(endpoint.to_string()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
        .bind(conversation.to_string())
        .execute(&pool)
        .await
        .unwrap();
    let task_id = TaskId::generate();
    let task = AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: None,
        from_agent: endpoint,
        to_agent: endpoint,
        conversation_id: conversation,
        reply_to: None,
        text: "fixture".into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    };
    let event = TaskEvent {
        id: EventId::generate(),
        task_id,
        seq: 1,
        status: TaskStatus::Queued,
        timestamp: Utc::now(),
        payload: TaskEventPayload::Queued,
    };
    let admission = ReliabilityStore::new(pool.clone());
    assert!(
        admission
            .begin_runtime("fixture-runtime", "fixture-owner", &Utc::now().to_rfc3339())
            .await
            .unwrap()
    );
    let context = AdmissionContext {
        transport: "matrix".into(),
        external_event_id: "$event".into(),
        room_id: "!room".into(),
        thread_root: None,
        reply_event_id: "$event".into(),
        monitor_room: None,
        monitor_generation: 0,
    };
    assert!(
        cutover
            .begin_event("!room", "$event", "digest")
            .await
            .unwrap()
    );
    admission
        .admit_task(&task, &event, "fixture-runtime", Some(&context))
        .await
        .unwrap();
    prove_reopen(pool, &database, &binding).await;
}

async fn prove_reopen(pool: sqlx::SqlitePool, database: &std::path::Path, binding: &str) {
    pool.close().await;
    let pool = connect(database).await.unwrap();
    let cutover = CutoverStore::open(pool.clone(), binding)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !cutover
            .begin_event("!room", "$event", "digest")
            .await
            .unwrap()
    );
    assert!(
        !cutover
            .begin_event("!room", "$event", "digest")
            .await
            .unwrap()
    );
    cutover
        .commit_batch(&cutover.state().await.unwrap(), "B")
        .await
        .unwrap();
    let snapshot: (i64,i64,i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM tasks),(SELECT count(*) FROM task_events),(SELECT count(*) FROM task_admissions),(SELECT count(*) FROM transport_receipts),(SELECT count(*) FROM deliveries)")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(snapshot, (1, 1, 1, 1, 0));
    assert_eq!(
        cutover.state().await.unwrap().baseline.as_deref(),
        Some("A")
    );
}

#[tokio::test]
async fn stale_two_connection_watermark_loser_has_no_partial_write() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("state.db");
    let pool = connect(&database).await.unwrap();
    migrate(&pool).await.unwrap();
    let binding = cutover_digest(b"binding");
    let first = CutoverStore::prepare(
        pool.clone(),
        "operation",
        &binding,
        &cutover_digest(b"archive"),
    )
    .await
    .unwrap();
    first.activate("A").await.unwrap();
    let second_pool = connect(&database).await.unwrap();
    let second = CutoverStore::open(second_pool, &binding)
        .await
        .unwrap()
        .unwrap();
    let state = first.state().await.unwrap();
    let (left, right) = tokio::join!(
        first.commit_batch(&state, "B"),
        second.commit_batch(&state, "C")
    );
    assert_ne!(left.is_ok(), right.is_ok());
    let after = first.state().await.unwrap();
    assert_eq!(after.revision, state.revision + 1);
    assert_eq!(after.baseline, state.baseline);
    assert!(matches!(after.watermark.as_deref(), Some("B" | "C")));
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_cutover")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn required_marker_cannot_fall_back_to_legacy_startup_when_missing() {
    let root = tempfile::tempdir().unwrap();
    let pool = connect(root.path().join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let binding = cutover_digest(b"binding");
    CutoverStore::prepare(
        pool.clone(),
        "operation",
        &binding,
        &cutover_digest(b"archive"),
    )
    .await
    .unwrap();
    sqlx::query("DELETE FROM matrix_cutover")
        .execute(&pool)
        .await
        .unwrap();
    assert!(CutoverStore::open(pool.clone(), &binding).await.is_err());
    assert!(
        CutoverStore::prepare(
            pool.clone(),
            "other-operation",
            &binding,
            &cutover_digest(b"archive")
        )
        .await
        .is_err()
    );
    let required: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_cutover_required")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(required, 1);
}
