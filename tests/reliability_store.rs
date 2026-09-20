use guigu_agent_bridge::storage::{
    ReceiptOutcome, ReliabilityError, ReliabilityStore, RetryTaskInput, connect, migrate,
};
use guigu_agent_bridge::{
    app::OutboxDrain,
    matrix::{MatrixOutboxSender, ReplyError, ReplyFuture},
    models::{AgentTask, ConversationId, EndpointId, Priority, TaskId},
};
use sqlx::{Row, SqlitePool};
use std::sync::Mutex;
use uuid::Uuid;

async fn database() -> SqlitePool {
    let path = std::env::temp_dir().join(format!("guigu-reliability-{}.db", Uuid::now_v7()));
    let pool = connect(path).await.expect("connect");
    migrate(&pool).await.expect("migrate");
    pool
}

async fn seed(pool: &SqlitePool, status: &str) -> (String, String, String, String) {
    let endpoint = Uuid::now_v7().to_string();
    let conversation = Uuid::now_v7().to_string();
    let task = Uuid::now_v7().to_string();
    let event = Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES (?,?,'acp',1,NULL,'[]')")
        .bind(&endpoint).bind(format!("agent-{endpoint}")).execute(pool).await.expect("agent");
    sqlx::query("INSERT INTO conversations(conversation_id,transport,external_id,thread_ref,participants_json) VALUES (?,NULL,NULL,NULL,'[]')")
        .bind(&conversation).execute(pool).await.expect("conversation");
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,parent_task_id,from_agent,to_agent,conversation_id,reply_to,text,priority,depth,hops,deadline,version) VALUES (?,?,NULL,?,?,?,NULL,'original prompt',7,0,0,NULL,0)")
        .bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(pool).await.expect("task");
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES (?,?,1,?,'2026-09-18T00:00:00.000000000Z','{}')")
        .bind(&event).bind(&task).bind(status).execute(pool).await.expect("event");
    (endpoint, conversation, task, event)
}

fn workflow_task(from: &str, to: &str, conversation: &str, task: &str) -> AgentTask {
    AgentTask {
        task_id: TaskId::from_uuid(Uuid::parse_str(task).unwrap()),
        root_task_id: TaskId::from_uuid(Uuid::parse_str(task).unwrap()),
        parent_task_id: None,
        from_agent: EndpointId::from_uuid(Uuid::parse_str(from).unwrap()),
        to_agent: EndpointId::from_uuid(Uuid::parse_str(to).unwrap()),
        conversation_id: ConversationId::from_uuid(Uuid::parse_str(conversation).unwrap()),
        reply_to: None,
        text: "workflow".into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 1,
        deadline: None,
        version: 0,
    }
}

#[tokio::test]
async fn workflow_replay_compares_identity_and_classifies_idempotency_conflict() {
    let pool = database().await;
    let (endpoint, conversation, _, _) = seed(&pool, "completed").await;
    let store = ReliabilityStore::new(pool.clone());
    let task_id = Uuid::now_v7().to_string();
    let task = workflow_task(&endpoint, &endpoint, &conversation, &task_id);
    let sender = endpoint.clone();
    let task_key = task.task_id.to_string();
    let delivery = Uuid::now_v7().to_string();
    let input = guigu_agent_bridge::storage::WorkflowAdmission {
        transport: "matrix-workflow",
        external_event_id: "event-1",
        sender_endpoint_id: &sender,
        target_endpoint_id: &sender,
        task_id: &task_key,
        correlation_id: "corr",
        idempotency_key: "idem-1",
        kind: "dispatch",
        body_hash: "hash-1",
        now: "2026-09-20T00:00:00Z",
        task: &task,
        delivery_id: &delivery,
    };
    assert_eq!(
        store.admit_workflow(input).await.unwrap(),
        ReceiptOutcome::Inserted
    );
    let changed_delivery = Uuid::now_v7().to_string();
    let changed = guigu_agent_bridge::storage::WorkflowAdmission {
        body_hash: "changed",
        delivery_id: &changed_delivery,
        ..input
    };
    assert!(matches!(
        store.admit_workflow(changed).await,
        Err(ReliabilityError::WorkflowConflict)
    ));
    let handoff_task_id = Uuid::now_v7().to_string();
    let handoff_task = workflow_task(&endpoint, &endpoint, &conversation, &handoff_task_id);
    let handoff_key = handoff_task.task_id.to_string();
    let handoff_delivery = Uuid::now_v7().to_string();
    let handoff = guigu_agent_bridge::storage::WorkflowAdmission {
        external_event_id: "event-3",
        idempotency_key: "idem-3",
        kind: "handoff",
        body_hash: "hash-3",
        task_id: &handoff_key,
        task: &handoff_task,
        delivery_id: &handoff_delivery,
        ..input
    };
    assert_eq!(
        store.admit_workflow(handoff).await.unwrap(),
        ReceiptOutcome::Inserted
    );
    let persisted_kind: String =
        sqlx::query_scalar("SELECT kind FROM workflow_envelopes WHERE external_event_id='event-3'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(persisted_kind, "handoff");
    let other_task_id = Uuid::now_v7().to_string();
    let other = workflow_task(&endpoint, &endpoint, &conversation, &other_task_id);
    let other_key = other.task_id.to_string();
    let other_delivery = Uuid::now_v7().to_string();
    let collision = guigu_agent_bridge::storage::WorkflowAdmission {
        external_event_id: "event-2",
        task_id: &other_key,
        task: &other,
        body_hash: "hash-2",
        delivery_id: &other_delivery,
        ..input
    };
    assert!(matches!(
        store.admit_workflow(collision).await,
        Err(ReliabilityError::WorkflowConflict)
    ));
}

fn retry<'a>(
    endpoint: &'a str,
    conversation: &'a str,
    source: &'a str,
    task: &'a str,
    external: &'a str,
    event: &'a str,
) -> RetryTaskInput<'a> {
    RetryTaskInput {
        transport: "matrix",
        external_event_id: external,
        room_id: "!admin:example",
        thread_root: Some("$thread"),
        reply_event_id: external,
        admin_actor: "@admin:example",
        source_task_id: source,
        task_id: task,
        from_agent: endpoint,
        to_agent: endpoint,
        conversation_id: conversation,
        text: "original prompt",
        priority: 7,
        deadline: Some("2026-09-18T01:00:00.000000000Z"),
        timestamp: "2026-09-18T00:30:00.000000000Z",
        event_id: event,
        body: "retry=admitted",
        stable_txn_id: "txn-retry-1",
    }
}

#[tokio::test]
async fn retry_is_atomic_field_exact_and_same_event_idempotent() {
    let pool = database().await;
    let (endpoint, conversation, source, _) = seed(&pool, "completed").await;
    let task = Uuid::now_v7().to_string();
    let event = Uuid::now_v7().to_string();
    let external = "$retry-event";
    let store = ReliabilityStore::new(pool.clone());
    assert_eq!(
        store
            .admit_retry(retry(
                &endpoint,
                &conversation,
                &source,
                &task,
                external,
                &event
            ))
            .await
            .unwrap(),
        ReceiptOutcome::Inserted
    );
    assert_eq!(
        store
            .admit_retry(retry(
                &endpoint,
                &conversation,
                &source,
                &task,
                external,
                &event
            ))
            .await
            .unwrap(),
        ReceiptOutcome::Replay {
            task_id: Some(task.clone()),
            result_code: "admitted".into()
        }
    );

    let row = sqlx::query("SELECT * FROM tasks WHERE task_id=?")
        .bind(&task)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("root_task_id"), task);
    assert_eq!(row.get::<Option<String>, _>("parent_task_id"), None);
    assert_eq!(row.get::<String, _>("from_agent"), endpoint);
    assert_eq!(row.get::<String, _>("to_agent"), endpoint);
    assert_eq!(row.get::<String, _>("conversation_id"), conversation);
    assert_eq!(row.get::<Option<String>, _>("reply_to"), None);
    assert_eq!(row.get::<String, _>("text"), "original prompt");
    assert_eq!(row.get::<i64, _>("priority"), 7);
    assert_eq!(row.get::<i64, _>("depth"), 0);
    assert_eq!(row.get::<i64, _>("hops"), 0);
    assert_eq!(row.get::<i64, _>("version"), 0);
    let retry_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_events WHERE task_id=?")
        .bind(&task)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(retry_events, 1);
    for table in ["task_admissions", "projection_outbox"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1, "{table}");
    }
    let tasks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(tasks, 2);
}

#[tokio::test]
async fn retry_rejects_a_nonterminal_source_without_partial_rows() {
    let pool = database().await;
    let (endpoint, conversation, source, _) = seed(&pool, "running").await;
    let task = Uuid::now_v7().to_string();
    let event = Uuid::now_v7().to_string();
    let store = ReliabilityStore::new(pool.clone());
    let error = store
        .admit_retry(retry(
            &endpoint,
            &conversation,
            &source,
            &task,
            "$retry-running",
            &event,
        ))
        .await
        .unwrap_err();
    assert!(matches!(error, ReliabilityError::NotTerminal));
    let receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transport_receipts")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(receipts, 0);
    let retries: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE task_id=?")
        .bind(task)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(retries, 0);
}

#[derive(Default)]
struct OutboxProbe(Mutex<Vec<(String, String, String)>>);
impl MatrixOutboxSender for OutboxProbe {
    fn send_stable<'a>(
        &'a self,
        room: &'a str,
        thread: Option<&'a str>,
        reply: Option<&'a str>,
        body: &'a str,
        txn: &'a str,
    ) -> ReplyFuture<'a> {
        Box::pin(async move {
            assert_eq!(thread, Some("$thread"));
            assert_eq!(reply, Some("$outbox"));
            self.0
                .lock()
                .unwrap()
                .push((room.into(), body.into(), txn.into()));
            Ok::<_, ReplyError>(())
        })
    }
}

#[tokio::test]
async fn outbox_is_the_single_stable_transaction_send_owner() {
    let pool = database().await;
    let (endpoint, conversation, source, _) = seed(&pool, "completed").await;
    let task = Uuid::now_v7().to_string();
    let event = Uuid::now_v7().to_string();
    let store = ReliabilityStore::new(pool.clone());
    store
        .admit_retry(retry(
            &endpoint,
            &conversation,
            &source,
            &task,
            "$outbox",
            &event,
        ))
        .await
        .unwrap();
    let probe = std::sync::Arc::new(OutboxProbe::default());
    let sender: std::sync::Arc<dyn MatrixOutboxSender> = probe.clone();
    let drain = OutboxDrain::new(store, sender, 8);
    assert_eq!(drain.drain_once().await.unwrap(), 1);
    assert_eq!(drain.drain_once().await.unwrap(), 0);
    let calls = probe.0.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "!admin:example");
    assert_eq!(calls[0].1, "retry=admitted");
    assert_eq!(calls[0].2, "txn-retry-1");
}

#[tokio::test]
async fn concurrent_drains_claim_once_and_stale_sending_is_recoverable() {
    let pool = database().await;
    let (endpoint, conversation, source, _) = seed(&pool, "completed").await;
    let task = Uuid::now_v7().to_string();
    let event = Uuid::now_v7().to_string();
    let store = ReliabilityStore::new(pool.clone());
    store
        .admit_retry(retry(
            &endpoint,
            &conversation,
            &source,
            &task,
            "$outbox",
            &event,
        ))
        .await
        .unwrap();
    let stale = store
        .claim_projections(
            "dead-owner",
            "2020-01-01T00:00:00Z",
            "2019-01-01T00:00:00Z",
            8,
        )
        .await
        .unwrap();
    assert_eq!(stale.len(), 1);
    let probe = std::sync::Arc::new(OutboxProbe::default());
    let a = OutboxDrain::new(store.clone(), probe.clone(), 8);
    let b = OutboxDrain::new(store.clone(), probe.clone(), 8);
    let (left, right) = tokio::join!(a.drain_once(), b.drain_once());
    assert_eq!(left.unwrap() + right.unwrap(), 1);
    assert_eq!(probe.0.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn admission_claim_is_cross_connection_cas() {
    let pool = database().await;
    let (endpoint, conversation, source, _) = seed(&pool, "completed").await;
    let task = Uuid::now_v7().to_string();
    let event = Uuid::now_v7().to_string();
    let store = ReliabilityStore::new(pool.clone());
    store
        .admit_retry(retry(
            &endpoint,
            &conversation,
            &source,
            &task,
            "$claim",
            &event,
        ))
        .await
        .unwrap();
    let other = ReliabilityStore::new(pool.clone());
    let (a, b) = tokio::join!(
        store.claim_admission(&task, 0, None, "2026-09-18T00:31:00Z"),
        other.claim_admission(&task, 0, None, "2026-09-18T00:31:00Z")
    );
    assert_ne!(a.unwrap(), b.unwrap());
}

#[tokio::test]
async fn delivery_preparation_and_acknowledgement_are_atomic_and_idempotent() {
    let pool = database().await;
    let (endpoint, _, task, _) = seed(&pool, "dispatched").await;
    let delivery = Uuid::now_v7().to_string();
    let store = ReliabilityStore::new(pool.clone());
    store
        .prepare_delivery(
            &delivery,
            &task,
            1,
            &endpoint,
            "2026-09-18T00:31:00.000000000Z",
        )
        .await
        .unwrap();
    let prepared: (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT state,session_id,acknowledged_at FROM delivery_dispositions JOIN deliveries USING(delivery_id) WHERE delivery_id=?",
    )
    .bind(&delivery)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(prepared, ("prepared".into(), None, None));
    assert!(
        store
            .acknowledge_delivery(&delivery, "session-1", "2026-09-18T00:32:00.000000000Z")
            .await
            .unwrap()
    );
    assert!(
        store
            .acknowledge_delivery(&delivery, "session-1", "2026-09-18T00:33:00.000000000Z")
            .await
            .unwrap()
    );
    let acknowledged: (String, String, String) = sqlx::query_as(
        "SELECT state,session_id,acknowledged_at FROM delivery_dispositions JOIN deliveries USING(delivery_id) WHERE delivery_id=?",
    )
    .bind(&delivery)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(acknowledged.0, "acknowledged");
    assert_eq!(acknowledged.1, "session-1");
    assert_eq!(acknowledged.2, "2026-09-18T00:32:00.000000000Z");
}

#[tokio::test]
async fn recovery_classifies_ready_and_only_cleanly_stopped_enqueued_work() {
    let pool = database().await;
    let (endpoint, conversation, source, _) = seed(&pool, "completed").await;
    let task = Uuid::now_v7().to_string();
    let event = Uuid::now_v7().to_string();
    let store = ReliabilityStore::new(pool.clone());
    store
        .admit_retry(retry(
            &endpoint,
            &conversation,
            &source,
            &task,
            "$recover",
            &event,
        ))
        .await
        .unwrap();
    let ready = store.classify_admissions(8).await.unwrap();
    assert_eq!(ready.eligible, vec![(task.clone(), 0)]);
    assert!(!ready.blocked);

    let old = Uuid::now_v7().to_string();
    assert!(
        store
            .begin_runtime(&old, "fingerprint", "2026-09-18T00:00:00.000000000Z")
            .await
            .unwrap()
    );
    assert!(
        store
            .claim_admission(&task, 0, Some(&old), "2026-09-18T00:01:00.000000000Z")
            .await
            .unwrap()
    );
    let active = store.classify_admissions(8).await.unwrap();
    assert!(active.eligible.is_empty());
    assert!(active.blocked);

    assert!(
        store
            .stopping_runtime(&old, "2026-09-18T00:01:30.000000000Z")
            .await
            .unwrap()
    );
    let stopping = store.classify_admissions(8).await.unwrap();
    assert!(stopping.eligible.is_empty());
    assert!(stopping.blocked);

    assert!(
        store
            .stop_runtime(&old, "2026-09-18T00:02:00.000000000Z")
            .await
            .unwrap()
    );
    let stopped = store.classify_admissions(8).await.unwrap();
    assert_eq!(stopped.eligible, vec![(task.clone(), 1)]);
    assert!(!stopped.blocked);
    let new = Uuid::now_v7().to_string();
    assert!(
        store
            .begin_runtime(&new, "fingerprint-2", "2026-09-18T00:03:00.000000000Z")
            .await
            .unwrap()
    );
    assert!(
        store
            .reclaim_stopped_admission(&task, 1, &new, "2026-09-18T00:03:00.000000000Z")
            .await
            .unwrap()
    );
    assert!(
        !store
            .reclaim_stopped_admission(&task, 1, &new, "2026-09-18T00:03:00.000000000Z")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn admission_recovery_does_not_truncate_past_the_old_page_boundary() {
    let pool = database().await;
    let (endpoint, conversation, _, _) = seed(&pool, "completed").await;
    let mut tx = pool.begin().await.unwrap();
    for index in 0..4097_u32 {
        let task = Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,parent_task_id,from_agent,to_agent,conversation_id,reply_to,text,priority,depth,hops,deadline,version) VALUES(?,?,NULL,?,?,?,NULL,'recovery',5,0,0,NULL,0)")
            .bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation)
            .execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,created_at,updated_at) VALUES(?,'ready',0,?,?)")
            .bind(&task).bind(format!("2026-09-18T00:00:{:02}.{:09}Z", index % 60, index))
            .bind("2026-09-18T00:00:00.000000000Z")
            .execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    let recovery = ReliabilityStore::new(pool.clone())
        .classify_admissions(4096)
        .await
        .unwrap();
    assert_eq!(recovery.covered.len(), 4097);
    assert_eq!(recovery.eligible.len(), 4097);
    assert!(!recovery.blocked);
    pool.close().await;
}
