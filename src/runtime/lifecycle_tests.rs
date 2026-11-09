//! Scoped production consumers reused by the actual same-store runtime fixture.
use super::*;
use crate::models::{EventId, TaskEvent, TaskEventPayload, TaskStatus};
use crate::storage::{
    ReliabilityStore,
    recovery_eligibility::{EffectDomain, RecoveryScope, bootstrap_nonadmin, writer},
};

async fn insert_task(
    pool: &SqlitePool,
    scope: &RecoveryScope,
    endpoint: crate::models::EndpointId,
) -> TaskId {
    let task = TaskId::generate();
    let mut tx = writer::begin(pool, Some(scope)).await.unwrap();
    let inserted = sqlx::query("INSERT OR IGNORE INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,'lifecycle-worker','acp',1,'[]')")
        .bind(endpoint.to_string()).execute(&mut *tx).await.unwrap();
    if inserted.rows_affected() == 1 {
        writer::inserted(
            &mut tx,
            Some(scope),
            &writer::text_identity("agents", &endpoint.to_string()).unwrap(),
        )
        .await
        .unwrap();
    }
    let inserted = sqlx::query("INSERT OR IGNORE INTO conversations(conversation_id,participants_json) VALUES('lifecycle-local','[]')")
        .execute(&mut *tx).await.unwrap();
    if inserted.rows_affected() == 1 {
        writer::inserted(
            &mut tx,
            Some(scope),
            &writer::text_identity("conversations", "lifecycle-local").unwrap(),
        )
        .await
        .unwrap();
    }
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,'lifecycle-local','synthetic',5,0,0,0)")
        .bind(task.to_string()).bind(task.to_string()).bind(endpoint.to_string()).bind(endpoint.to_string()).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO task_admissions(task_id,state,revision,render_version,created_at,updated_at) VALUES(?,'enqueued',0,'v1','now','now')")
        .bind(task.to_string()).execute(&mut *tx).await.unwrap();
    for table in ["tasks", "task_admissions"] {
        writer::inserted(
            &mut tx,
            Some(scope),
            &writer::text_identity(table, &task.to_string()).unwrap(),
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    task
}

async fn prepare(
    pool: &SqlitePool,
    workspace: &Path,
    endpoint: crate::models::EndpointId,
) -> (SqliteRuntimeStore, SqliteTaskLifecycle, Lease, Continuation) {
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let task = insert_task(pool, &scope, endpoint).await;
    let lifecycle = SqliteTaskLifecycle::new_scoped(pool.clone(), Some(scope.clone()));
    let delivery = DeliveryId::generate();
    let now = Utc::now();
    let dispatched = TaskEvent {
        id: EventId::generate(),
        task_id: task,
        seq: 2,
        status: TaskStatus::Dispatched,
        timestamp: now,
        payload: TaskEventPayload::Dispatched {
            delivery_id: delivery,
            attempt: 1,
        },
    };
    assert_eq!(
        lifecycle.transition(&dispatched).await.unwrap(),
        LifecycleResult::Committed
    );
    let reliability = ReliabilityStore::with_recovery(pool.clone(), scope.clone());
    reliability
        .prepare_delivery(
            &delivery.to_string(),
            &task.to_string(),
            1,
            &endpoint.to_string(),
            &now.to_rfc3339(),
        )
        .await
        .unwrap();
    reliability
        .acknowledge_delivery(&delivery.to_string(), "synthetic", &now.to_rfc3339())
        .await
        .unwrap();
    let store = SqliteRuntimeStore::new_scoped(pool.clone(), Some(scope));
    let resource = ExecutionResourceKey::new(
        endpoint,
        WorkspaceId::from_canonical_path(workspace).unwrap(),
    );
    let AcquireOutcome::Acquired(lease) = store
        .acquire(resource, task, now, Duration::from_secs(120))
        .await
        .unwrap()
    else {
        panic!("new work refused")
    };
    store
        .claim_workspaces(&lease, &[workspace.to_path_buf()])
        .await
        .unwrap();
    store
        .begin_continuation(&lease, delivery, "synthetic", now)
        .await
        .unwrap();
    let ready = store.continuation(task).await.unwrap().unwrap();
    let claimed = store
        .claim_turn_with_generation(&lease, ready.revision, &ready.runtime_generation, now)
        .await
        .unwrap();
    (store, lifecycle, lease, claimed)
}

/// Same durable path as pre-prompt cleanup, without claiming an actual ACP execution.
pub(crate) async fn task_cycle(
    pool: &SqlitePool,
    workspace: &Path,
    endpoint: crate::models::EndpointId,
    failed: bool,
) -> TaskId {
    let (store, lifecycle, lease, claimed) = prepare(pool, workspace, endpoint).await;
    let task = lease.task_id;
    let now = Utc::now();
    if !failed {
        lifecycle
            .transition(&TaskEvent {
                id: EventId::generate(),
                task_id: task,
                seq: 3,
                status: TaskStatus::Running,
                timestamp: now,
                payload: TaskEventPayload::Running { started_at: now },
            })
            .await
            .unwrap();
    }
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: task,
        seq: if failed { 3 } else { 4 },
        status: if failed {
            TaskStatus::Failed
        } else {
            TaskStatus::Completed
        },
        timestamp: now,
        payload: if failed {
            TaskEventPayload::Failed {
                error: "failed_before_prompt".into(),
            }
        } else {
            TaskEventPayload::Completed {
                output: "synthetic result".into(),
            }
        },
    };
    assert!(matches!(
        store
            .finalize_terminal(
                &lease,
                claimed.revision,
                &claimed.runtime_generation,
                ContinuationState::Terminal,
                0,
                &event,
                now
            )
            .await
            .unwrap(),
        FinalizeResult::Committed
    ));
    assert!(!matches!(
        store
            .finalize_terminal(
                &lease,
                claimed.revision,
                &claimed.runtime_generation,
                ContinuationState::Terminal,
                0,
                &event,
                now
            )
            .await
            .unwrap(),
        FinalizeResult::Committed
    ));
    assert_terminal(pool, task).await;
    task
}

async fn assert_terminal(pool: &SqlitePool, task: TaskId) {
    let terminal: (i64, String, String) = sqlx::query_as("SELECT (SELECT count(*) FROM task_events WHERE task_id=? AND status IN ('failed','completed')),a.state,l.state FROM task_admissions a JOIN execution_leases l USING(task_id) WHERE a.task_id=?")
        .bind(task.to_string()).bind(task.to_string()).fetch_one(pool).await.unwrap();
    assert_eq!(terminal, (1, "terminal".into(), "released".into()));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workspace_claims WHERE task_id=?")
            .bind(task.to_string())
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
    bootstrap_nonadmin(pool, true).await.unwrap();
}
