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

// Opt-in baseline diagnostics: these assert the observed defect, not repair success.
async fn mechanism_diagnostic_fixture() -> (tempfile::TempDir, SqlitePool) {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    assert!(matches!(
        crate::storage::MatrixCompatibilityStore::new(pool.clone())
            .gate(true, None)
            .await
            .unwrap(),
        crate::storage::CompatibilityGate::Native(_)
    ));
    (root, pool)
}

#[tokio::test]
#[ignore = "R112 baseline defect reproduction, not a repaired-behavior gate"]
async fn mechanism_diagnostic_continue_commits_unclassified_receipt() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let (store, _, lease, claimed) =
        prepare(&pool, root.path(), crate::models::EndpointId::generate()).await;
    bootstrap_nonadmin(&pool, true).await.unwrap();
    let next = store
        .record_continue_with_receipt(
            &lease,
            claimed.revision,
            &claimed.runtime_generation,
            "next",
            1,
            "structured",
            "continue:next",
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(next.revision, claimed.revision + 1);
    assert_eq!(next.state, ContinuationState::Ready);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM continuation_response_receipts")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM offline_recovery_rows WHERE table_name='continuation_response_receipts'").fetch_one(&pool).await.unwrap(), 0);
    drop(store);
    pool.close().await;
    let reopened = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    let error = bootstrap_nonadmin(&reopened, true).await.unwrap_err();
    assert_eq!(mechanism_diagnostic_drift(&reopened).await, (1, 1));
    println!(
        "continue=committed receipt=1 classified_receipt=0 drift=1 missing=1 reopened_bootstrap={error}"
    );
    reopened.close().await;
}

#[tokio::test]
#[ignore = "R112 baseline defect reproduction, not a repaired-behavior gate"]
async fn mechanism_diagnostic_unknown_finish_commits_digest_drift() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let (store, _, lease, claimed) =
        prepare(&pool, root.path(), crate::models::EndpointId::generate()).await;
    bootstrap_nonadmin(&pool, true).await.unwrap();
    store
        .finish_with_generation(
            &lease,
            claimed.revision,
            &claimed.runtime_generation,
            ContinuationState::RecoveryNeeded,
            0,
            Utc::now(),
        )
        .await
        .unwrap();
    let states: (String, String, String) = sqlx::query_as("SELECT c.state,l.state,d.state FROM task_continuations c JOIN execution_leases l USING(task_id) JOIN delivery_dispositions d USING(task_id)").fetch_one(&pool).await.unwrap();
    assert_eq!(
        states,
        (
            "recovery_needed".into(),
            "recovery_needed".into(),
            "outcome_unknown".into()
        )
    );
    drop(store);
    pool.close().await;
    let reopened = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    let error = bootstrap_nonadmin(&reopened, true).await.unwrap_err();
    assert_eq!(mechanism_diagnostic_drift(&reopened).await, (3, 0));
    println!(
        "unknown_finish=committed unknown_effect=preserved drift=3 missing=0 reopened_bootstrap={error}"
    );
    reopened.close().await;
}

#[tokio::test]
#[ignore = "R112 scoped producer positive control"]
async fn mechanism_diagnostic_scoped_terminal_control() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let endpoint = crate::models::EndpointId::generate();
    for failed in [true, false] {
        task_cycle(&pool, root.path(), endpoint, failed).await;
    }
    pool.close().await;
    let reopened = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    bootstrap_nonadmin(&reopened, true).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_admissions WHERE state='terminal'")
            .fetch_one(&reopened)
            .await
            .unwrap(),
        2
    );
    println!("scoped_failed=1 scoped_completed=1 reopened_inventory=valid");
    reopened.close().await;
}

async fn mechanism_diagnostic_drift(pool: &SqlitePool) -> (usize, usize) {
    use crate::storage::recovery_eligibility::inventory;
    let mut c = pool.acquire().await.unwrap();
    let mut drift = 0;
    let mut missing = 0;
    for id in inventory::identities(&mut c).await.unwrap() {
        let actual = inventory::snapshot(&mut c, &id).await.unwrap();
        let stored: Option<String> = sqlx::query_scalar(
            "SELECT original_digest FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=?",
        ).bind(&id.table).bind(&id.key).fetch_optional(&mut *c).await.unwrap();
        match stored {
            None => missing += 1,
            Some(value) if value != actual.digest => drift += 1,
            _ => {}
        }
    }
    (drift, missing)
}
