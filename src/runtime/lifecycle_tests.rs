//! Scoped production consumers reused by the actual same-store runtime fixture.
use super::*;
use crate::acp::{AcpDispatcher, SqliteSessionStore};
use crate::bus::{
    DispatchRequest, EndpointRegistry, RegisteredEndpoint, TaskDispatcher, derive_endpoint_id,
};
use crate::models::{
    AgentTask, ConversationId, DeliveryId, EventId, Priority, TaskEvent, TaskEventPayload,
    TaskStatus,
};
use crate::storage::{
    ReliabilityStore, SqliteRepository,
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
    prepare_scoped(pool, workspace, endpoint, scope).await
}

async fn prepare_scoped(
    pool: &SqlitePool,
    workspace: &Path,
    endpoint: crate::models::EndpointId,
    scope: RecoveryScope,
) -> (SqliteRuntimeStore, SqliteTaskLifecycle, Lease, Continuation) {
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
    scope: RecoveryScope,
) -> TaskId {
    let (store, lifecycle, lease, claimed) = prepare_scoped(pool, workspace, endpoint, scope).await;
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

// Production scoped writer regressions (baseline failures retained in R112 evidence).
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
async fn scoped_continue_receipt_survives_reopen() {
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
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM offline_recovery_rows WHERE table_name='continuation_response_receipts'").fetch_one(&pool).await.unwrap(), 1);
    drop(store);
    pool.close().await;
    let reopened = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    bootstrap_nonadmin(&reopened, true).await.unwrap();
    assert_eq!(mechanism_diagnostic_drift(&reopened).await, (0, 0));
    reopened.close().await;
}

#[tokio::test]
async fn scoped_unknown_finish_preserves_inventory() {
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
    assert_eq!(mechanism_diagnostic_drift(&reopened).await, (0, 0));
    // Unknown work remains unknown; integrity does not grant execution.
    bootstrap_nonadmin(&reopened, true).await.unwrap();
    reopened.close().await;
}

#[tokio::test]
async fn scoped_terminal_control() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let endpoint = crate::models::EndpointId::generate();
    for failed in [true, false] {
        let scope = RecoveryScope::new(
            bootstrap_nonadmin(&pool, true).await.unwrap(),
            EffectDomain::Local,
            None,
        );
        task_cycle(&pool, root.path(), endpoint, failed, scope).await;
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

/// Fixture that stops before `claim_turn_with_generation`, leaving the
/// continuation in the exact unstarted state (ready / rev-1 / next_turn-1).
async fn prepare_unstarted(
    pool: &SqlitePool,
    workspace: &Path,
    endpoint: crate::models::EndpointId,
) -> (SqliteRuntimeStore, SqliteTaskLifecycle, Lease, DeliveryId) {
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
    (store, lifecycle, lease, delivery)
}

#[tokio::test]
async fn f1_ensure_unstarted_continuation_create_or_validate() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let endpoint = crate::models::EndpointId::generate();
    let (store, _lifecycle, lease, delivery) =
        prepare_unstarted(&pool, root.path(), endpoint).await;
    let now = Utc::now();

    // Existing unstarted row validates cleanly.
    store
        .ensure_unstarted_continuation(&lease, delivery, "synthetic", now)
        .await
        .unwrap();

    // Mismatched prompt is fenced.
    assert!(matches!(
        store
            .ensure_unstarted_continuation(&lease, delivery, "other", now)
            .await,
        Err(RuntimeError::Fenced)
    ));

    // A receipt row fences the continuation.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        "INSERT INTO continuation_response_receipts(idempotency_key,task_id,delivery_id,resource_key,lease_fence,continuation_revision,response_hash,outcome,output_bytes,runtime_generation,created_at) VALUES('f1-receipt',?,?,?,1,1,'hash','structured',0,'generation.v1:strict','now')",
    )
    .bind(lease.task_id.to_string())
    .bind(delivery.to_string())
    .bind(lease.resource.to_string())
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(
        store
            .ensure_unstarted_continuation(&lease, delivery, "synthetic", now)
            .await,
        Err(RuntimeError::Fenced)
    ));

    pool.close().await;
}

#[tokio::test]
async fn f2_finish_failed_setup_true_reap_terminal() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let endpoint = crate::models::EndpointId::generate();
    let (store, _lifecycle, lease, delivery) =
        prepare_unstarted(&pool, root.path(), endpoint).await;
    let now = Utc::now();

    // Validate the unstarted continuation.
    store
        .ensure_unstarted_continuation(&lease, delivery, "synthetic", now)
        .await
        .unwrap();

    // Claim turn 1 (advances revision to 2).
    let claimed = store
        .claim_turn_with_generation(&lease, 1, "generation.v1:strict", now)
        .await
        .unwrap();
    assert_eq!(claimed.revision, 2);

    // Finalize terminal — commits exactly once.
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: lease.task_id,
        seq: 3,
        status: TaskStatus::Failed,
        timestamp: now,
        payload: TaskEventPayload::Failed {
            error: "setup_failed".into(),
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

    // Second attempt is fenced.
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

    assert_terminal(&pool, lease.task_id).await;
    pool.close().await;
}

fn test_target(workspace: &Path) -> crate::bus::RegisteredEndpoint {
    let config = format!(
        "[agents.worker]\ntransport='acp'\ncommand='true'\nworkspace={}\n",
        serde_json::to_string(workspace.to_str().unwrap()).unwrap()
    );
    EndpointRegistry::from_config(&crate::config::load_from_str(&config).unwrap())
        .get(derive_endpoint_id("worker"))
        .unwrap()
        .clone()
}

fn test_request<'a>(
    task: &'a AgentTask,
    target: &'a crate::bus::RegisteredEndpoint,
    delivery_id: DeliveryId,
) -> DispatchRequest<'a> {
    DispatchRequest {
        task,
        target,
        delivery_id,
        attempt: 1,
        queue_lease: None,
    }
}

#[derive(Clone, Default)]
struct ControlledTimer {
    notify: Arc<tokio::sync::Notify>,
}

impl ControlledTimer {
    fn fire(&self) {
        self.notify.notify_one();
    }
}

impl RuntimeTimer for ControlledTimer {
    fn sleep<'a>(&'a self, _duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(self.notify.notified())
    }
}

fn production_target(workspace: &Path, command: &str, args: &[&str]) -> RegisteredEndpoint {
    let config = format!(
        "[agents.worker]\ntransport='acp'\ncommand={}\nargs={}\nworkspace={}\n",
        serde_json::to_string(command).unwrap(),
        serde_json::to_string(args).unwrap(),
        serde_json::to_string(workspace.to_str().unwrap()).unwrap(),
    );
    EndpointRegistry::from_config(&crate::config::load_from_str(&config).unwrap())
        .get(derive_endpoint_id("worker"))
        .unwrap()
        .clone()
}

fn mock_backend() -> std::path::PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    path.pop();
    path.push("examples/acp-mock-backend");
    path.set_extension(std::env::consts::EXE_EXTENSION);
    assert!(
        path.exists(),
        "build the local acp-mock-backend example first"
    );
    path
}

async fn insert_production_task(
    pool: &SqlitePool,
    endpoint: crate::models::EndpointId,
    text: &str,
    seed_delivery: bool,
) -> (AgentTask, DeliveryId) {
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let task_id = TaskId::generate();
    let conversation = ConversationId::generate();
    let delivery = DeliveryId::generate();
    let mut tx = writer::begin(pool, Some(&scope)).await.unwrap();
    for query in [
        "INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,'production-worker','acp',1,'[]')",
        "INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')",
        "INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,?,5,0,0,0)",
        "INSERT INTO task_admissions(task_id,state,revision,render_version,created_at,updated_at) VALUES(?,'enqueued',0,'v1','now','now')",
    ] {
        let statement = sqlx::query(query);
        match query {
            value if value.contains("agents") => statement
                .bind(endpoint.to_string())
                .execute(&mut *tx)
                .await
                .unwrap(),
            value if value.contains("conversations") => statement
                .bind(conversation.to_string())
                .execute(&mut *tx)
                .await
                .unwrap(),
            value if value.contains("tasks") => statement
                .bind(task_id.to_string())
                .bind(task_id.to_string())
                .bind(endpoint.to_string())
                .bind(endpoint.to_string())
                .bind(conversation.to_string())
                .bind(text)
                .execute(&mut *tx)
                .await
                .unwrap(),
            _ => statement
                .bind(task_id.to_string())
                .execute(&mut *tx)
                .await
                .unwrap(),
        };
    }
    for (table, key) in [
        ("agents", endpoint.to_string()),
        ("conversations", conversation.to_string()),
        ("tasks", task_id.to_string()),
        ("task_admissions", task_id.to_string()),
    ] {
        writer::inserted(
            &mut tx,
            Some(&scope),
            &writer::text_identity(table, &key).unwrap(),
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    let now = Utc::now();
    SqliteTaskLifecycle::new_scoped(pool.clone(), Some(scope.clone()))
        .transition(&TaskEvent {
            id: EventId::generate(),
            task_id,
            seq: 2,
            status: TaskStatus::Dispatched,
            timestamp: now,
            payload: TaskEventPayload::Dispatched {
                delivery_id: delivery,
                attempt: 1,
            },
        })
        .await
        .unwrap();
    if seed_delivery {
        let reliability = ReliabilityStore::with_recovery(pool.clone(), scope);
        reliability
            .prepare_delivery(
                &delivery.to_string(),
                &task_id.to_string(),
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
    }
    (
        AgentTask {
            task_id,
            root_task_id: task_id,
            parent_task_id: None,
            from_agent: endpoint,
            to_agent: endpoint,
            conversation_id: conversation,
            reply_to: None,
            text: text.into(),
            priority: Priority::DEFAULT,
            depth: 0,
            hops: 0,
            deadline: None,
            version: 1,
        },
        delivery,
    )
}

async fn build_production_leased(
    pool: &SqlitePool,
    workspace: &Path,
    timer: Arc<ControlledTimer>,
) -> (tempfile::TempDir, LeasedAcpDispatcher) {
    let root = tempfile::tempdir().unwrap();
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let dispatcher = AcpDispatcher::builder()
        .cwd(root.path())
        .sessions(SqliteSessionStore::new_scoped(
            pool.clone(),
            Some(scope.clone()),
        ))
        .repository(Arc::new(SqliteRepository::with_recovery(
            pool.clone(),
            scope.clone(),
        )))
        .reliability(ReliabilityStore::with_recovery(pool.clone(), scope.clone()))
        .build()
        .unwrap();
    let leased = LeasedAcpDispatcher::new(
        dispatcher,
        SqliteRuntimeStore::new_scoped(pool.clone(), Some(scope)),
        WorkspaceId::from_canonical_path(workspace).unwrap(),
        ContinuationPolicy {
            max_turns: 2,
            max_wall_time: Duration::from_secs(30),
            max_inactivity: Duration::from_secs(30),
            max_consecutive_no_progress: 2,
            max_observed_output_bytes: 4096,
            lease_ttl: Duration::from_secs(30),
        },
        Arc::new(SystemRuntimeClock),
        timer,
        Arc::new(RuntimeMetrics::default()),
    );
    (root, leased)
}

async fn production_snapshot(
    pool: &SqlitePool,
    task_id: TaskId,
    delivery_id: DeliveryId,
) -> (String, i64, i64, i64, String, String) {
    let lease: String = sqlx::query_scalar("SELECT state FROM execution_leases WHERE task_id=?")
        .bind(task_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
    let continuations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_continuations WHERE task_id=?")
            .bind(task_id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM continuation_response_receipts WHERE task_id=?")
            .bind(task_id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM task_events WHERE task_id=?")
        .bind(task_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
    let admission: String = sqlx::query_scalar("SELECT state FROM task_admissions WHERE task_id=?")
        .bind(task_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
    let disposition: String = sqlx::query_scalar(
        "SELECT state FROM delivery_dispositions WHERE delivery_id=? AND task_id=?",
    )
    .bind(delivery_id.to_string())
    .bind(task_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    (
        lease,
        continuations,
        receipts,
        events,
        admission,
        disposition,
    )
}

#[tokio::test]
async fn f3_production_finalize_delivery_failure_preserves_uncertain() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let endpoint = derive_endpoint_id("worker");
    let (task, delivery) = insert_production_task(&pool, endpoint, "f3 production", true).await;
    let target = production_target(root.path(), "__r132_missing_backend__", &[]);
    let request = test_request(&task, &target, delivery);
    let timer = Arc::new(ControlledTimer::default());
    let (_dispatcher_root, leased) = build_production_leased(&pool, root.path(), timer).await;
    leased.queue_test_reap_results([false]);
    assert!(leased.deliver(request.clone()).await.is_err());
    let before = production_snapshot(&pool, task.task_id, delivery).await;
    assert!(matches!(
        leased
            .finalize_delivery_failure(
                request.clone(),
                TaskEvent {
                    id: EventId::generate(),
                    task_id: task.task_id,
                    seq: 3,
                    status: TaskStatus::Failed,
                    timestamp: Utc::now(),
                    payload: TaskEventPayload::Failed {
                        error: "failed_before_prompt".into(),
                    },
                },
            )
            .await,
        Err(DispatchError::RecoveryNeeded)
    ));
    assert_eq!(
        production_snapshot(&pool, task.task_id, delivery).await,
        before
    );
    assert!(
        leased
            .pending
            .lock()
            .unwrap()
            .get(&task.task_id)
            .is_some_and(|entry| entry.preserved)
    );
    leased.shutdown().await;
    pool.close().await;
}

#[tokio::test]
async fn f4_production_finalize_delivery_failure_terminalizes_once() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let endpoint = derive_endpoint_id("worker");
    let (task, delivery) = insert_production_task(&pool, endpoint, "f4 production", true).await;
    let target = production_target(root.path(), "__r132_missing_backend__", &[]);
    let request = test_request(&task, &target, delivery);
    let timer = Arc::new(ControlledTimer::default());
    let (_dispatcher_root, leased) = build_production_leased(&pool, root.path(), timer).await;
    leased.queue_test_reap_results([false, true]);
    assert!(leased.deliver(request.clone()).await.is_err());
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: task.task_id,
        seq: 3,
        status: TaskStatus::Failed,
        timestamp: Utc::now(),
        payload: TaskEventPayload::Failed {
            error: "failed_before_prompt".into(),
        },
    };
    assert!(
        leased
            .finalize_delivery_failure(request.clone(), event.clone())
            .await
            .is_err()
    );
    assert!(
        leased
            .pending
            .lock()
            .unwrap()
            .get(&task.task_id)
            .is_some_and(|entry| entry.preserved)
    );

    assert!(
        leased
            .finalize_delivery_failure(request, event)
            .await
            .is_ok()
    );
    let terminal = production_snapshot(&pool, task.task_id, delivery).await;
    assert_eq!(terminal.0, "released");
    assert_eq!(terminal.1, 1);
    assert_eq!(terminal.2, 1);
    assert_eq!(terminal.3, 2);
    assert_eq!(terminal.4, "terminal");
    assert_eq!(terminal.5, "terminal");
    assert!(
        leased
            .finalize_delivery_failure(
                test_request(&task, &target, delivery,),
                TaskEvent {
                    id: EventId::generate(),
                    task_id: task.task_id,
                    seq: 3,
                    status: TaskStatus::Failed,
                    timestamp: Utc::now(),
                    payload: TaskEventPayload::Failed {
                        error: "failed_before_prompt".into(),
                    },
                },
            )
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM continuation_response_receipts WHERE task_id=?",
        )
        .bind(task.task_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    leased.shutdown().await;
    pool.close().await;
}

#[tokio::test]
async fn f6_p4_production_supervisor_cancel_reap_branches() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let endpoint = derive_endpoint_id("worker");
    let (task, delivery) = insert_production_task(&pool, endpoint, "f6 p4 uncertain", false).await;
    let target = production_target(root.path(), &mock_backend().to_string_lossy(), &["happy"]);
    let request = test_request(&task, &target, delivery);
    let timer = Arc::new(ControlledTimer::default());
    let (_dispatcher_root, leased) = build_production_leased(&pool, root.path(), timer).await;
    leased.queue_test_reap_results([false]);
    leased.deliver(request.clone()).await.unwrap();
    let before = production_snapshot(&pool, task.task_id, delivery).await;
    assert!(
        leased
            .cancel_and_reap_task(task.task_id, "uncertain cancel")
            .await
            .is_none()
    );
    assert_eq!(
        production_snapshot(&pool, task.task_id, delivery).await,
        before
    );
    assert!(
        leased
            .pending
            .lock()
            .unwrap()
            .get(&task.task_id)
            .is_some_and(|entry| entry.preserved)
    );
    leased.shutdown().await;
    pool.close().await;

    let (root, pool) = mechanism_diagnostic_fixture().await;
    let (task, delivery) = insert_production_task(&pool, endpoint, "f6 p4 positive", false).await;
    let target = production_target(root.path(), &mock_backend().to_string_lossy(), &["happy"]);
    let request = test_request(&task, &target, delivery);
    let timer = Arc::new(ControlledTimer::default());
    let (_dispatcher_root, leased) = build_production_leased(&pool, root.path(), timer).await;
    leased.queue_test_reap_results([true]);
    leased.deliver(request).await.unwrap();
    assert!(
        leased
            .cancel_and_reap_task(task.task_id, "positive cancel")
            .await
            .is_some()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM execution_leases WHERE task_id=?")
            .bind(task.task_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "released"
    );
    leased.shutdown().await;
    pool.close().await;
}

#[tokio::test]
async fn f6_p5_production_supervisor_timeout_reap_branches() {
    let endpoint = derive_endpoint_id("worker");
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let (task, delivery) = insert_production_task(&pool, endpoint, "f6 p5 uncertain", false).await;
    let target = production_target(root.path(), &mock_backend().to_string_lossy(), &["happy"]);
    let request = test_request(&task, &target, delivery);
    let timer = Arc::new(ControlledTimer::default());
    let (_dispatcher_root, leased) =
        build_production_leased(&pool, root.path(), timer.clone()).await;
    leased.queue_test_reap_results([false]);
    leased.deliver(request.clone()).await.unwrap();
    let before = production_snapshot(&pool, task.task_id, delivery).await;
    timer.fire();
    for _ in 0..1_000 {
        if leased
            .pending
            .lock()
            .unwrap()
            .get(&task.task_id)
            .is_some_and(|entry| entry.preserved)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(matches!(
        leased.execute_prepared(request).await,
        Err(DispatchError::RecoveryNeeded)
    ));
    assert_eq!(
        production_snapshot(&pool, task.task_id, delivery).await,
        before
    );
    leased.shutdown().await;
    pool.close().await;

    let (root, pool) = mechanism_diagnostic_fixture().await;
    let (task, delivery) = insert_production_task(&pool, endpoint, "f6 p5 positive", false).await;
    let target = production_target(root.path(), &mock_backend().to_string_lossy(), &["happy"]);
    let request = test_request(&task, &target, delivery);
    let timer = Arc::new(ControlledTimer::default());
    let (_dispatcher_root, leased) =
        build_production_leased(&pool, root.path(), timer.clone()).await;
    leased.queue_test_reap_results([true]);
    leased.deliver(request).await.unwrap();
    timer.fire();
    for _ in 0..1_000 {
        if matches!(
            production_snapshot(&pool, task.task_id, delivery).await,
            (ref lease, 1, 1, 2, ref admission, ref disposition)
                if lease == "released" && admission == "terminal" && disposition == "terminal"
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let terminal = production_snapshot(&pool, task.task_id, delivery).await;
    assert_eq!(terminal.0, "released");
    assert_eq!(terminal.1, 1);
    assert_eq!(terminal.2, 1);
    assert_eq!(terminal.3, 2);
    assert_eq!(terminal.4, "terminal");
    assert_eq!(terminal.5, "terminal");
    leased.shutdown().await;
    pool.close().await;
}

#[tokio::test]
async fn f4_storage_terminal_idempotency_contract() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let endpoint = crate::models::EndpointId::generate();
    let (_store, _lifecycle, lease, delivery) =
        prepare_unstarted(&pool, root.path(), endpoint).await;
    let (_dispatcher_root, leased) = build_leased(&pool, root.path()).await;
    let (claimed_tx, mut claimed_rx) = tokio::sync::oneshot::channel();
    let pending = Arc::clone(&leased.pending);
    let value = PendingLease {
        lease: lease.clone(),
        claimed: claimed_tx,
        preserved: false,
    };

    // The uncertain-reap arm preserves the exact active lease and does not touch
    // continuation, receipt or terminal rows.
    assert!(matches!(
        LeasedAcpDispatcher::preserve_uncertain_pending(&pending, lease.task_id, value).await,
        Err(DispatchError::RecoveryNeeded)
    ));
    assert!(
        pending
            .lock()
            .unwrap()
            .get(&lease.task_id)
            .is_some_and(|entry| entry.preserved)
    );
    assert!(claimed_rx.try_recv().is_err());
    let state: String = sqlx::query_scalar("SELECT state FROM execution_leases WHERE task_id=?")
        .bind(lease.task_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "active");

    // A later supported current-caller finalizer consumes the preserved capability
    // and uses the exact public true-reap terminal path.
    let entry = pending.lock().unwrap().remove(&lease.task_id).unwrap();
    let target = test_target(root.path());
    let task = AgentTask {
        task_id: lease.task_id,
        root_task_id: lease.task_id,
        parent_task_id: None,
        from_agent: target.id(),
        to_agent: target.id(),
        conversation_id: ConversationId::generate(),
        reply_to: None,
        text: "synthetic".into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    };
    let request = test_request(&task, &target, delivery);
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: lease.task_id,
        seq: 3,
        status: TaskStatus::Failed,
        timestamp: Utc::now(),
        payload: TaskEventPayload::Failed {
            error: "setup_failed".into(),
        },
    };
    leased
        .finish_failed_setup(&request, &entry.lease, &event)
        .await
        .unwrap();
    assert_terminal(&pool, lease.task_id).await;
    let terminal_events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_events WHERE task_id=? AND status='failed'")
            .bind(lease.task_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(terminal_events, 1);
    let receipts_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM continuation_response_receipts WHERE task_id=?")
            .bind(lease.task_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(receipts_before >= 1);

    // Repeating the disposition cannot create a second terminal effect.
    assert!(
        leased
            .finish_failed_setup(&request, &entry.lease, &event)
            .await
            .is_err()
    );
    let receipts_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM continuation_response_receipts WHERE task_id=?")
            .bind(lease.task_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(receipts_after, receipts_before);
    pool.close().await;
}

/// Build a minimal `LeasedAcpDispatcher` for supervisor-arm tests.
async fn build_leased(
    pool: &SqlitePool,
    workspace: &Path,
) -> (tempfile::TempDir, LeasedAcpDispatcher) {
    let root = tempfile::tempdir().unwrap();
    let dispatcher = crate::acp::AcpDispatcher::builder()
        .cwd(root.path())
        .sessions(SqliteSessionStore::new(pool.clone()))
        .repository(Arc::new(SqliteRepository::new(pool.clone())))
        .build()
        .unwrap();
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let store = SqliteRuntimeStore::new_scoped(pool.clone(), Some(scope));
    let leased = LeasedAcpDispatcher::new(
        dispatcher,
        store,
        WorkspaceId::from_canonical_path(workspace).unwrap(),
        ContinuationPolicy {
            max_turns: 2,
            max_wall_time: Duration::from_secs(30),
            max_inactivity: Duration::from_secs(30),
            max_consecutive_no_progress: 2,
            max_observed_output_bytes: 4096,
            lease_ttl: Duration::from_secs(30),
        },
        Arc::new(SystemRuntimeClock),
        Arc::new(TokioRuntimeTimer),
        Arc::new(RuntimeMetrics::default()),
    );
    (root, leased)
}

#[tokio::test]
async fn f6_preserved_shutdown_drain_contract() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let endpoint = crate::models::EndpointId::generate();
    let (store, _lifecycle, lease, _delivery) =
        prepare_unstarted(&pool, root.path(), endpoint).await;
    let (_root2, leased) = build_leased(&pool, root.path()).await;

    // Create a second lease for the "normal" path by directly inserting
    // into the execution_leases table (bypassing the full delivery pipeline).
    let endpoint2 = crate::models::EndpointId::generate();
    let scope2 = RecoveryScope::new(
        bootstrap_nonadmin(&pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let task2 = insert_task(&pool, &scope2, endpoint2).await;
    let now = Utc::now();
    let resource2 = ExecutionResourceKey::new(
        endpoint2,
        WorkspaceId::from_canonical_path(root.path()).unwrap(),
    );
    let AcquireOutcome::Acquired(lease2) = store
        .acquire(resource2, task2, now, Duration::from_secs(120))
        .await
        .unwrap()
    else {
        panic!("second lease refused")
    };

    // Insert a preserved PendingLease (task 1) and a normal PendingLease (task 2).
    let (tx1, _rx1) = tokio::sync::oneshot::channel();
    let (tx2, _rx2) = tokio::sync::oneshot::channel();
    let pending = leased.pending.clone();
    let mut map = pending.lock().unwrap_or_else(|p| p.into_inner());
    map.insert(
        lease.task_id,
        PendingLease {
            lease: lease.clone(),
            claimed: tx1,
            preserved: true,
        },
    );
    map.insert(
        task2,
        PendingLease {
            lease: lease2.clone(),
            claimed: tx2,
            preserved: false,
        },
    );
    drop(map);

    // Shutdown: preserved lease stays active, normal lease is released.
    leased.shutdown().await;

    let preserved_state: String =
        sqlx::query_scalar("SELECT state FROM execution_leases WHERE task_id=?")
            .bind(lease.task_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(preserved_state, "active");

    let normal_state: String =
        sqlx::query_scalar("SELECT state FROM execution_leases WHERE task_id=?")
            .bind(task2.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(normal_state, "recovery_needed");

    pool.close().await;
}

#[tokio::test]
async fn f6_preserved_execute_refusal_contract() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let endpoint = crate::models::EndpointId::generate();
    let (_store, _lifecycle, lease, delivery) =
        prepare_unstarted(&pool, root.path(), endpoint).await;
    let (_dispatcher_root, leased) = build_leased(&pool, root.path()).await;
    let (claimed_tx, mut claimed_rx) = tokio::sync::oneshot::channel();
    leased.pending.lock().unwrap().insert(
        lease.task_id,
        PendingLease {
            lease: lease.clone(),
            claimed: claimed_tx,
            preserved: true,
        },
    );
    let target = test_target(root.path());
    let task = AgentTask {
        task_id: lease.task_id,
        root_task_id: lease.task_id,
        parent_task_id: None,
        from_agent: target.id(),
        to_agent: target.id(),
        conversation_id: ConversationId::generate(),
        reply_to: None,
        text: "must not prompt".into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    };
    let request = test_request(&task, &target, delivery);
    assert!(matches!(
        leased.execute_prepared(request).await,
        Err(DispatchError::RecoveryNeeded)
    ));
    assert!(claimed_rx.try_recv().is_err());
    assert!(
        leased
            .pending
            .lock()
            .unwrap()
            .get(&lease.task_id)
            .is_some_and(|entry| entry.preserved)
    );
    let continuation_state: String =
        sqlx::query_scalar("SELECT state FROM task_continuations WHERE task_id=?")
            .bind(lease.task_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(continuation_state, "ready");
    pool.close().await;
}

#[path = "writer_rollback_tests.rs"]
mod writer_rollback_tests;

#[path = "scoped_dispatch_tests.rs"]
mod scoped_dispatch_tests;
