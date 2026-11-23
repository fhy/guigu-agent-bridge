use super::*;
use crate::{
    bus::{MemoryBus, derive_endpoint_id},
    matrix::route_workflow,
    models::{
        AuthenticatedWorkflowIngress, ConversationId, TaskId, WorkflowEnvelope, WorkflowKind,
        WorkflowRole,
    },
    storage::{
        CompatibilityGate, MatrixCompatibilityStore, MatrixRecoveryStore,
        recovery_eligibility::{
            EffectDomain, LiveFence, RecoveryScope, bootstrap_nonadmin, inventory, writer,
        },
    },
};

struct Fixture {
    root: tempfile::TempDir,
    pool: SqlitePool,
    store: ReliabilityStore,
    bus: MemoryBus,
    conversation: ConversationId,
    _receivers: crate::bus::BusReceivers,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap()
    else {
        panic!("native")
    };
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(&pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let recovery = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
    recovery
        .activate_cutover(&operation, "cutover", "now")
        .await
        .unwrap();
    let now = chrono::Utc::now();
    let epoch = recovery
        .acquire_component_lease(
            "MatrixSync",
            "owner",
            &now.to_rfc3339(),
            &(now + chrono::Duration::minutes(10)).to_rfc3339(),
        )
        .await
        .unwrap();
    let scope = scope.for_component(LiveFence {
        component: "MatrixSync".into(),
        owner: "owner".into(),
        epoch,
    });
    let config = crate::config::load_from_str("[agents.worker]\ntransport=\"acp\"\ncommand=\"unused\"\nworkspace=\"/tmp\"\nenabled=true\n[agents.coordinator]\ntransport=\"acp\"\ncommand=\"unused\"\nworkspace=\"/tmp\"\nenabled=true\n").unwrap();
    let (bus, receivers) = MemoryBus::from_config(&config, 16);
    let conversation = ConversationId::generate();
    seed_registry(&pool, &scope, conversation).await;
    Fixture {
        root,
        pool: pool.clone(),
        store: ReliabilityStore::with_recovery(pool, scope),
        bus,
        conversation,
        _receivers: receivers,
    }
}

async fn seed_registry(pool: &SqlitePool, scope: &RecoveryScope, conversation: ConversationId) {
    let endpoint = derive_endpoint_id("worker");
    let mut tx = writer::begin(pool, Some(scope)).await.unwrap();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,'worker','acp',1,'[]')").bind(endpoint.to_string()).execute(&mut *tx).await.unwrap();
    writer::inserted(
        &mut tx,
        Some(scope),
        &writer::text_identity("agents", &endpoint.to_string()).unwrap(),
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
        .bind(conversation.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    writer::inserted(
        &mut tx,
        Some(scope),
        &writer::text_identity("conversations", &conversation.to_string()).unwrap(),
    )
    .await
    .unwrap();
    let coordinator = derive_endpoint_id("coordinator");
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,'coordinator','acp',1,'[]')").bind(coordinator.to_string()).execute(&mut *tx).await.unwrap();
    writer::inserted(
        &mut tx,
        Some(scope),
        &writer::text_identity("agents", &coordinator.to_string()).unwrap(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

fn envelope() -> WorkflowEnvelope {
    WorkflowEnvelope {
        schema: "workflow.v1".into(),
        kind: WorkflowKind::Dispatch,
        message_id: Uuid::now_v7().to_string(),
        from: derive_endpoint_id("coordinator"),
        to: derive_endpoint_id("worker"),
        task_id: TaskId::generate(),
        correlation_id: "work".into(),
        idempotency_key: Uuid::now_v7().to_string(),
        state: None,
        body: "synthetic".into(),
        mention: None,
    }
}
async fn route(f: &Fixture, e: &WorkflowEnvelope) -> Result<AgentTask, crate::matrix::RouteError> {
    route_workflow(
        e,
        f.conversation,
        f.bus.registry(),
        &f.store,
        &f.bus,
        AuthenticatedWorkflowIngress {
            endpoint: e.from,
            role: WorkflowRole::Coordinator,
        },
    )
    .await
}
async fn assert_inventory(pool: &SqlitePool) {
    let mut c = pool.acquire().await.unwrap();
    for id in inventory::identities(&mut c).await.unwrap() {
        let actual = inventory::snapshot(&mut c, &id).await.unwrap();
        let stored: String = sqlx::query_scalar("SELECT original_digest FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=?")
            .bind(&id.table).bind(&id.key).fetch_one(&mut *c).await.unwrap();
        assert_eq!(stored, actual.digest, "{}", id.table);
    }
}
#[tokio::test]
async fn real_workflow_route_dispatch_handoff_duplicate_and_reopen() {
    let f = fixture().await;
    let dispatch = envelope();
    route(&f, &dispatch).await.unwrap();
    route(&f, &dispatch).await.unwrap();
    let mut handoff = dispatch.clone();
    handoff.kind = WorkflowKind::Handoff;
    handoff.message_id = "handoff".into();
    handoff.idempotency_key = "handoff".into();
    route(&f, &handoff).await.unwrap();
    assert_inventory(&f.pool).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM agent_work_queue")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        2
    );
    let reopened = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    bootstrap_nonadmin(&reopened, true).await.unwrap();
    assert_inventory(&reopened).await;
}
#[tokio::test]
async fn workflow_classification_failure_rolls_back_and_exact_retry_commits() {
    let f = fixture().await;
    let e = envelope();
    sqlx::query("CREATE TRIGGER reject_queue BEFORE INSERT ON offline_recovery_rows WHEN NEW.table_name='agent_work_queue' BEGIN SELECT RAISE(ABORT,'late queue classification'); END").execute(&f.pool).await.unwrap();
    assert!(route(&f, &e).await.is_err());
    for table in [
        "tasks",
        "workflow_envelopes",
        "deliveries",
        "task_events",
        "task_admissions",
        "agent_work_queue",
        "agent_queue_counters",
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM {table}"
            )))
            .fetch_one(&f.pool)
            .await
            .unwrap(),
            0,
            "{table}"
        );
    }
    sqlx::query("DROP TRIGGER reject_queue")
        .execute(&f.pool)
        .await
        .unwrap();
    route(&f, &e).await.unwrap();
    assert_inventory(&f.pool).await;
}
#[tokio::test]
async fn workflow_pause_resume_and_recovery_control_keep_classification() {
    let f = fixture().await;
    let e = envelope();
    route(&f, &e).await.unwrap();
    assert_eq!(
        f.store
            .pause_task_queued(&e.task_id.to_string(), "pause")
            .await
            .unwrap(),
        1
    );
    assert_inventory(&f.pool).await;
    assert_eq!(
        f.store
            .resume_task(&e.task_id.to_string(), "resume")
            .await
            .unwrap(),
        1
    );
    assert_inventory(&f.pool).await;
    assert_eq!(
        f.store
            .pause_task_after_reap(&e.task_id.to_string(), "unused", "unused", 1, "pause-again")
            .await
            .unwrap(),
        "paused"
    );
    assert_inventory(&f.pool).await;
    let (id, revision): (String, i64) =
        sqlx::query_as("SELECT queue_id,revision FROM agent_work_queue")
            .fetch_one(&f.pool)
            .await
            .unwrap();
    f.store
        .resume_queue(&id, revision, "unused", 1, "resume-again")
        .await
        .unwrap();
    assert_inventory(&f.pool).await;
    assert_eq!(
        f.store
            .mark_pause_recovery_needed(&e.task_id.to_string(), "unknown")
            .await
            .unwrap(),
        "absent"
    );
    bootstrap_nonadmin(&f.pool, true).await.unwrap();
}

#[path = "legacy_queue_guard_tests.rs"]
mod legacy_queue_guard_tests;
