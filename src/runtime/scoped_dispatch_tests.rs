use super::*;
use crate::{
    acp::SqliteSessionStore,
    bus::{Clock, EndpointRegistry, FinalizeCommand, RegisteredEndpoint},
    models::{Conversation, ConversationId, Priority},
    storage::{Repository, SqliteRepository, sync_agents},
};

struct Fixture {
    root: tempfile::TempDir,
    pool: SqlitePool,
    scope: RecoveryScope,
    repository: Arc<SqliteRepository>,
    target: RegisteredEndpoint,
    conversation: ConversationId,
    dispatcher: LeasedAcpDispatcher,
}
fn backend_config(root: &std::path::Path, scenario: &str) -> crate::config::Config {
    let backend = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/acp-mock-backend");
    assert!(
        backend.is_file(),
        "build acp-mock-backend before scoped tests"
    );
    crate::config::load_from_str(&format!(
        "[agents.worker]\ntransport='acp'\ncommand={}\nargs=[{}]\nworkspace={}\nenabled=true\n",
        serde_json::json!(backend),
        serde_json::json!(scenario),
        serde_json::json!(root)
    ))
    .unwrap()
}
async fn fixture(scenario: &str) -> Fixture {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(&pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let config = backend_config(root.path(), scenario);
    let registry = EndpointRegistry::from_config(&config);
    let repository = Arc::new(SqliteRepository::with_recovery(pool.clone(), scope.clone()));
    sync_agents(repository.as_ref(), &registry).await.unwrap();
    let conversation = ConversationId::generate();
    repository
        .insert_conversation(&Conversation {
            id: conversation,
            participants: vec![],
            external_ref: None,
        })
        .await
        .unwrap();
    let target = registry
        .get(registry.resolve_agent_id("worker").unwrap())
        .unwrap()
        .clone();
    let reliability = ReliabilityStore::with_recovery(pool.clone(), scope.clone());
    reliability
        .begin_runtime_with_evidence(
            "fixture",
            &crate::storage::RuntimeOwnerEvidence::fingerprint().unwrap(),
            &Utc::now().to_rfc3339(),
            &crate::storage::RuntimeOwnerEvidence::capture().unwrap(),
        )
        .await
        .unwrap();
    let store = SqliteSessionStore::new_scoped(pool.clone(), Some(scope.clone()));
    let acp = AcpDispatcher::builder()
        .cwd(root.path())
        .clock(Clock::system())
        .sessions(store)
        .repository(repository.clone())
        .reliability(ReliabilityStore::with_recovery(pool.clone(), scope.clone()))
        .build()
        .unwrap();
    let dispatcher = LeasedAcpDispatcher::new(
        acp,
        SqliteRuntimeStore::new_scoped(pool.clone(), Some(scope.clone())),
        WorkspaceId::from_canonical_path(root.path()).unwrap(),
        ContinuationPolicy {
            max_turns: 4,
            max_wall_time: Duration::from_secs(60),
            max_inactivity: Duration::from_secs(30),
            max_consecutive_no_progress: 3,
            max_observed_output_bytes: 4096,
            lease_ttl: Duration::from_secs(60),
        },
        Arc::new(SystemRuntimeClock),
        Arc::new(TokioRuntimeTimer),
        Arc::new(RuntimeMetrics::default()),
    );
    Fixture {
        root,
        pool,
        scope,
        repository,
        target,
        conversation,
        dispatcher,
    }
}
async fn begin(f: &Fixture) -> (AgentTask, DeliveryId) {
    let task_id = TaskId::generate();
    let task = AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: None,
        from_agent: f.target.id(),
        to_agent: f.target.id(),
        conversation_id: f.conversation,
        reply_to: None,
        text: "synthetic".into(),
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
    ReliabilityStore::with_recovery(f.pool.clone(), f.scope.clone())
        .admit_task(&task, &event, "fixture", None)
        .await
        .unwrap();
    let delivery = DeliveryId::generate();
    let lifecycle = SqliteTaskLifecycle::new_scoped(f.pool.clone(), Some(f.scope.clone()));
    lifecycle
        .transition(&TaskEvent {
            id: EventId::generate(),
            task_id,
            seq: 2,
            status: TaskStatus::Dispatched,
            timestamp: Utc::now(),
            payload: TaskEventPayload::Dispatched {
                delivery_id: delivery,
                attempt: 1,
            },
        })
        .await
        .unwrap();
    f.dispatcher
        .deliver(request(f, &task, delivery))
        .await
        .unwrap();
    lifecycle
        .transition(&TaskEvent {
            id: EventId::generate(),
            task_id,
            seq: 3,
            status: TaskStatus::Running,
            timestamp: Utc::now(),
            payload: TaskEventPayload::Running {
                started_at: Utc::now(),
            },
        })
        .await
        .unwrap();
    (task, delivery)
}
fn request<'a>(f: &'a Fixture, task: &'a AgentTask, delivery: DeliveryId) -> DispatchRequest<'a> {
    DispatchRequest {
        task,
        target: &f.target,
        delivery_id: delivery,
        attempt: 1,
        queue_lease: None,
    }
}
async fn reopen(f: &Fixture) {
    let pool = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    assert_eq!(mechanism_diagnostic_drift(&pool).await, (0, 0));
    bootstrap_nonadmin(&pool, true).await.unwrap();
    pool.close().await;
}
#[tokio::test]
async fn scoped_dispatch_continue_terminal_and_new_task_survive_reopen() {
    let f = fixture("continue-once").await;
    for _ in 0..2 {
        let (task, delivery) = begin(&f).await;
        let mut prepared = f
            .dispatcher
            .execute_prepared(request(&f, &task, delivery))
            .await
            .unwrap();
        let DispatchOutcome::Completed { output } = prepared.outcome else {
            panic!("expected completed")
        };
        let event = TaskEvent {
            id: EventId::generate(),
            task_id: task.task_id,
            seq: 4,
            status: TaskStatus::Completed,
            timestamp: Utc::now(),
            payload: TaskEventPayload::Completed { output },
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        prepared
            .capability
            .command
            .take()
            .unwrap()
            .send(FinalizeCommand {
                task_id: task.task_id,
                event,
                response: tx,
            })
            .unwrap();
        assert!(matches!(rx.await.unwrap(), FinalizeResult::Committed));
        assert_terminal(&f.pool, task.task_id).await;
        reopen(&f).await;
    }
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM continuation_response_receipts WHERE outcome='structured'"
        )
        .fetch_one(&f.pool)
        .await
        .unwrap()
            >= 1
    );
    f.dispatcher.shutdown().await;
    reopen(&f).await;
}
#[tokio::test]
async fn dropped_real_finalization_capability_preserves_unknown_without_inventory_drift() {
    let f = fixture("continue-once").await;
    let (task, delivery) = begin(&f).await;
    let prepared = f
        .dispatcher
        .execute_prepared(request(&f, &task, delivery))
        .await
        .unwrap();
    drop(prepared);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state: String =
                sqlx::query_scalar("SELECT state FROM task_continuations WHERE task_id=?")
                    .bind(task.task_id.to_string())
                    .fetch_one(&f.pool)
                    .await
                    .unwrap();
            if state == "recovery_needed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    f.dispatcher.shutdown().await;
    reopen(&f).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM task_events WHERE status IN ('completed','failed')"
        )
        .fetch_one(&f.pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        f.repository
            .get_task(task.task_id)
            .await
            .unwrap()
            .unwrap()
            .version,
        2
    );
}
