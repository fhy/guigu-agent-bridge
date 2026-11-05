use crate::{
    bus::{LifecycleResult, TaskLifecycle},
    models::{DeliveryId, EventId, TaskEvent, TaskEventPayload, TaskId, TaskStatus},
    runtime::{
        AcquireOutcome, ExecutionResourceKey, SqliteRuntimeStore, SqliteTaskLifecycle, WorkspaceId,
    },
    storage::{
        ReliabilityStore,
        recovery_eligibility::{EffectDomain, RecoveryScope, bootstrap_nonadmin, writer},
    },
};
use chrono::Utc;
use std::time::Duration;

#[path = "workspace_deletion_tests.rs"]
mod workspace_deletion;

#[path = "dispatch_reuse_tests.rs"]
mod dispatch_reuse;

struct Seed {
    root: tempfile::TempDir,
    pool: sqlx::SqlitePool,
    scope: RecoveryScope,
    task: TaskId,
    endpoint: crate::models::EndpointId,
}

impl Seed {
    async fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("r092-runtime-audit-")
            .tempdir_in("/tmp")
            .unwrap();
        let pool = crate::storage::connect(root.path().join("state.db"))
            .await
            .unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        let context = bootstrap_nonadmin(&pool, false).await.unwrap();
        let scope = RecoveryScope::new(context, EffectDomain::Local, None);
        let endpoint = crate::models::EndpointId::generate();
        let task = TaskId::generate();
        let mut tx = writer::begin(&pool, Some(&scope)).await.unwrap();
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,'worker','acp',1,'[]')")
            .bind(endpoint.to_string()).execute(&mut *tx).await.unwrap();
        writer::inserted(
            &mut tx,
            Some(&scope),
            &writer::text_identity("agents", &endpoint.to_string()).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO conversations(conversation_id,participants_json) VALUES('local','[]')",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        writer::inserted(
            &mut tx,
            Some(&scope),
            &writer::text_identity("conversations", "local").unwrap(),
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,'local','synthetic',5,0,0,0)")
            .bind(task.to_string()).bind(task.to_string()).bind(endpoint.to_string()).bind(endpoint.to_string()).execute(&mut *tx).await.unwrap();
        writer::inserted(
            &mut tx,
            Some(&scope),
            &writer::text_identity("tasks", &task.to_string()).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,render_version,created_at,updated_at) VALUES(?,'enqueued',0,'v1','now','now')")
            .bind(task.to_string()).execute(&mut *tx).await.unwrap();
        writer::inserted(
            &mut tx,
            Some(&scope),
            &writer::text_identity("task_admissions", &task.to_string()).unwrap(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        Self {
            root,
            pool,
            scope,
            task,
            endpoint,
        }
    }

    fn dispatched(&self, delivery: DeliveryId) -> TaskEvent {
        TaskEvent {
            id: EventId::generate(),
            task_id: self.task,
            seq: 2,
            status: TaskStatus::Dispatched,
            timestamp: Utc::now(),
            payload: TaskEventPayload::Dispatched {
                delivery_id: delivery,
                attempt: 1,
            },
        }
    }
}

#[tokio::test]
async fn scoped_lifecycle_delivery_and_turn_preserve_parent_snapshots_and_refuse_drift() {
    let seed = Seed::new().await;
    let lifecycle = SqliteTaskLifecycle::new_scoped(seed.pool.clone(), Some(seed.scope.clone()));
    let delivery = DeliveryId::generate();
    let event = seed.dispatched(delivery);
    assert_eq!(
        lifecycle.transition(&event).await.unwrap(),
        LifecycleResult::Committed
    );
    assert_eq!(
        lifecycle.transition(&event).await.unwrap(),
        LifecycleResult::Replay
    );
    let reliability = ReliabilityStore::with_recovery(seed.pool.clone(), seed.scope.clone());
    let now = Utc::now();
    reliability
        .prepare_delivery(
            &delivery.to_string(),
            &seed.task.to_string(),
            1,
            &seed.endpoint.to_string(),
            &now.to_rfc3339(),
        )
        .await
        .unwrap();
    assert!(
        reliability
            .acknowledge_delivery(
                &delivery.to_string(),
                "synthetic-session",
                &now.to_rfc3339()
            )
            .await
            .unwrap()
    );
    let store = SqliteRuntimeStore::new_scoped(seed.pool.clone(), Some(seed.scope.clone()));
    let resource = ExecutionResourceKey::new(
        seed.endpoint,
        WorkspaceId::from_canonical_path(seed.root.path()).unwrap(),
    );
    let AcquireOutcome::Acquired(lease) = store
        .acquire(resource, seed.task, now, Duration::from_secs(30))
        .await
        .unwrap()
    else {
        panic!("lease not acquired")
    };
    store
        .claim_workspaces(&lease, &[seed.root.path().to_path_buf()])
        .await
        .unwrap();
    store
        .begin_continuation(&lease, delivery, "synthetic", now)
        .await
        .unwrap();
    let continuation = store.continuation(seed.task).await.unwrap().unwrap();
    let lease = store
        .renew(&lease, now, Duration::from_secs(30))
        .await
        .unwrap();
    let claimed = store
        .claim_turn_with_generation(
            &lease,
            continuation.revision,
            &continuation.runtime_generation,
            now,
        )
        .await
        .unwrap();
    store
        .record_activity_with_generation(
            &lease,
            claimed.revision,
            &claimed.runtime_generation,
            1,
            now,
        )
        .await
        .unwrap();
    let mut tx = writer::begin(&seed.pool, Some(&seed.scope)).await.unwrap();
    for (table, key) in [
        ("tasks", seed.task.to_string()),
        ("task_admissions", seed.task.to_string()),
        ("task_continuations", seed.task.to_string()),
        ("deliveries", delivery.to_string()),
    ] {
        writer::require(
            &mut tx,
            Some(&seed.scope),
            &writer::text_identity(table, &key).unwrap(),
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    sqlx::query("UPDATE task_continuations SET next_prompt='unclassified drift' WHERE task_id=?")
        .bind(seed.task.to_string())
        .execute(&seed.pool)
        .await
        .unwrap();
    assert!(
        store
            .record_activity_with_generation(
                &lease,
                claimed.revision,
                &claimed.runtime_generation,
                1,
                now
            )
            .await
            .is_err()
    );
    let bytes: i64 =
        sqlx::query_scalar("SELECT observed_output_bytes FROM task_continuations WHERE task_id=?")
            .bind(seed.task.to_string())
            .fetch_one(&seed.pool)
            .await
            .unwrap();
    assert_eq!(bytes, 1);
    seed.pool.close().await;
}

#[tokio::test]
async fn scoped_terminal_workspace_deletion_retains_audit_and_allows_restart() {
    let seed = Seed::new().await;
    let lifecycle = SqliteTaskLifecycle::new_scoped(seed.pool.clone(), Some(seed.scope.clone()));
    let delivery = DeliveryId::generate();
    lifecycle
        .transition(&seed.dispatched(delivery))
        .await
        .unwrap();
    let now = Utc::now();
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: seed.task,
        seq: 3,
        status: TaskStatus::Running,
        timestamp: now,
        payload: TaskEventPayload::Running { started_at: now },
    };
    lifecycle.transition(&event).await.unwrap();
    let reliability = ReliabilityStore::with_recovery(seed.pool.clone(), seed.scope.clone());
    reliability
        .prepare_delivery(
            &delivery.to_string(),
            &seed.task.to_string(),
            1,
            &seed.endpoint.to_string(),
            &now.to_rfc3339(),
        )
        .await
        .unwrap();
    reliability
        .acknowledge_delivery(&delivery.to_string(), "synthetic", &now.to_rfc3339())
        .await
        .unwrap();
    let store = SqliteRuntimeStore::new_scoped(seed.pool.clone(), Some(seed.scope.clone()));
    let resource = ExecutionResourceKey::new(
        seed.endpoint,
        WorkspaceId::from_canonical_path(seed.root.path()).unwrap(),
    );
    let AcquireOutcome::Acquired(lease) = store
        .acquire(resource, seed.task, now, Duration::from_secs(30))
        .await
        .unwrap()
    else {
        panic!("lease not acquired")
    };
    store
        .claim_workspaces(&lease, &[seed.root.path().to_path_buf()])
        .await
        .unwrap();
    store
        .begin_continuation(&lease, delivery, "synthetic", now)
        .await
        .unwrap();
    let continuation = store.continuation(seed.task).await.unwrap().unwrap();
    let claimed = store
        .claim_turn_with_generation(
            &lease,
            continuation.revision,
            &continuation.runtime_generation,
            now,
        )
        .await
        .unwrap();
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: seed.task,
        seq: 4,
        status: TaskStatus::Completed,
        timestamp: now,
        payload: TaskEventPayload::Completed {
            output: "synthetic result".into(),
        },
    };
    assert!(matches!(
        store
            .finalize_terminal(
                &lease,
                claimed.revision,
                &claimed.runtime_generation,
                super::super::ContinuationState::Terminal,
                0,
                &event,
                now
            )
            .await
            .unwrap(),
        crate::bus::FinalizeResult::Committed
    ));
    let mut tx = writer::begin(&seed.pool, Some(&seed.scope)).await.unwrap();
    for (table, key) in [
        ("tasks", seed.task.to_string()),
        ("task_admissions", seed.task.to_string()),
        ("task_continuations", seed.task.to_string()),
        ("deliveries", delivery.to_string()),
        ("task_events", event.id.to_string()),
    ] {
        writer::require(
            &mut tx,
            Some(&seed.scope),
            &writer::text_identity(table, &key).unwrap(),
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workspace_claims")
            .fetch_one(&seed.pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM offline_recovery_rows WHERE table_name='workspace_claims'"
        )
        .fetch_one(&seed.pool)
        .await
        .unwrap(),
        1
    );
    bootstrap_nonadmin(&seed.pool, false).await.unwrap();
    seed.pool.close().await;
}
