//! Actual SQLite writer contention, not live A2A/Matrix evidence.
use chrono::Utc;
use guigu_agent_bridge::{
    bus::{AdmissionContext, LifecycleResult, TaskLifecycle, derive_endpoint_id},
    models::{
        AgentTask, ConversationId, EndpointId, EventId, Priority, TaskEvent, TaskEventPayload,
        TaskId, TaskStatus,
    },
    runtime::SqliteTaskLifecycle,
    storage::{ReliabilityStore, connect, migrate},
};
use std::time::Duration;

type ProjectionEnvelope = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

#[tokio::test]
async fn queued_cancel_waits_for_writer_and_preserves_terminal_cas() {
    let root = tempfile::tempdir().unwrap();
    let pool = connect(root.path().join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let task = TaskId::generate();
    let endpoint = derive_endpoint_id("fixture").to_string();
    let conversation = uuid::Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,'fixture','acp',1,'[]')")
        .bind(&endpoint).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
        .bind(&conversation)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'fixture',5,0,0,0)")
        .bind(task.to_string()).bind(task.to_string()).bind(&endpoint).bind(&endpoint).bind(conversation).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'queued',?,'{\"kind\":\"queued\"}')")
        .bind(EventId::generate().to_string()).bind(task.to_string()).bind(Utc::now().to_rfc3339()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_admissions(task_id,state,revision,render_version,created_at,updated_at) VALUES(?,'enqueued',0,'v1',?,?)")
        .bind(task.to_string()).bind(Utc::now().to_rfc3339()).bind(Utc::now().to_rfc3339()).execute(&pool).await.unwrap();
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: task,
        seq: 2,
        status: TaskStatus::Cancelled,
        timestamp: Utc::now(),
        payload: TaskEventPayload::Cancelled {
            reason: "fixture cancellation".into(),
        },
    };
    let mut writer = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE tasks SET version=version WHERE task_id=?")
        .bind(task.to_string())
        .execute(&mut *writer)
        .await
        .unwrap();
    let lifecycle = SqliteTaskLifecycle::new(pool.clone());
    let pending_event = event.clone();
    let pending = tokio::spawn(async move { lifecycle.transition(&pending_event).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let waited_for_writer = !pending.is_finished();
    writer.commit().await.unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(
        waited_for_writer,
        "read-to-write upgrade failed instead of waiting for writer: {outcome:?}"
    );
    assert_eq!(outcome.unwrap(), LifecycleResult::Committed);
    let lifecycle = SqliteTaskLifecycle::new(pool.clone());
    assert_eq!(
        lifecycle.transition(&event).await.unwrap(),
        LifecycleResult::Replay
    );
    let mut foreign = event.clone();
    foreign.id = EventId::generate();
    assert_eq!(
        lifecycle.transition(&foreign).await.unwrap(),
        LifecycleResult::Stale
    );
    let row: (i64, String, i64) = sqlx::query_as("SELECT t.version,a.state,(SELECT COUNT(*) FROM task_events e WHERE e.task_id=t.task_id AND e.status='cancelled') FROM tasks t JOIN task_admissions a USING(task_id)")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(row, (1, "terminal".into(), 1));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_leases")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    pool.close().await;
}

#[tokio::test]
async fn ordinary_admission_keeps_receipt_but_isolates_matrix_projection() {
    let root = tempfile::tempdir().unwrap();
    let pool = connect(root.path().join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let endpoint = derive_endpoint_id("fixture");
    let conversation = ConversationId::generate();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,'fixture','acp',1,'[]')")
        .bind(endpoint.to_string()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
        .bind(conversation.to_string())
        .execute(&pool)
        .await
        .unwrap();
    let store = ReliabilityStore::new(pool.clone());
    assert!(
        store
            .begin_runtime("fixture-runtime", "fixture-owner", &Utc::now().to_rfc3339())
            .await
            .unwrap()
    );
    for transport in ["matrix", "matrix-workflow", "a2a"] {
        let task = admitted_task(endpoint, conversation);
        let task_id = task.task_id;
        let event = TaskEvent {
            id: EventId::generate(),
            task_id,
            seq: 1,
            status: TaskStatus::Queued,
            timestamp: Utc::now(),
            payload: TaskEventPayload::Queued,
        };
        let context = AdmissionContext {
            transport: transport.into(),
            external_event_id: format!("event-{transport}"),
            room_id: "context-or-room".into(),
            thread_root: Some("thread".into()),
            reply_event_id: "reply".into(),
            monitor_room: Some("monitor".into()),
            monitor_generation: 7,
        };
        store
            .admit_task(&task, &event, "fixture-runtime", Some(&context))
            .await
            .unwrap();
        let envelope: ProjectionEnvelope = sqlx::query_as("SELECT reply_room,reply_thread_root,reply_event_id,monitor_room,monitor_generation FROM task_admissions WHERE task_id=?")
            .bind(task_id.to_string()).fetch_one(&pool).await.unwrap();
        if transport == "a2a" {
            assert_eq!(envelope, (None, None, None, None, None));
        } else {
            assert_eq!(
                envelope,
                (
                    Some("context-or-room".into()),
                    Some("thread".into()),
                    Some("reply".into()),
                    Some("monitor".into()),
                    Some(7)
                )
            );
        }
        let receipt: (String, String) =
            sqlx::query_as("SELECT transport,room_id FROM transport_receipts WHERE task_id=?")
                .bind(task_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(receipt, (transport.into(), "context-or-room".into()));
    }
    pool.close().await;
}

fn admitted_task(endpoint: EndpointId, conversation: ConversationId) -> AgentTask {
    let task_id = TaskId::generate();
    AgentTask {
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
    }
}
