use super::*;
use crate::acp::{AcpDispatcher, AcpLimits, AcpSpawnGate, SqliteSessionStore};
use crate::bus::{Clock, EndpointRegistry, RegisteredEndpoint, derive_endpoint_id};
use crate::models::{
    AgentTask, Conversation, ConversationId, DeliveryId, EventId, Priority, TaskEventPayload,
    TaskStatus,
};
use crate::runtime::{
    ContinuationPolicy, RuntimeMetrics, SqliteRuntimeStore, SystemRuntimeClock, TokioRuntimeTimer,
    WorkspaceId,
};
use crate::storage::{
    ReliabilityStore, Repository, RuntimeOwnerEvidence, SqliteRepository, connect, migrate,
    sync_agents,
};
use chrono::Utc;

struct Fixture {
    root: tempfile::TempDir,
    pool: sqlx::SqlitePool,
    store: ReliabilityStore,
    token: String,
    task: AgentTask,
    target: RegisteredEndpoint,
    dispatcher: AcpDispatcher,
    router: AcpDispatcherRouter,
    gate: AcpSpawnGate,
}

fn registry(root: &std::path::Path) -> EndpointRegistry {
    let mut backend = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/acp-mock-backend");
    backend.set_extension(std::env::consts::EXE_EXTENSION);
    assert!(
        backend.exists(),
        "build the local acp-mock-backend example first"
    );
    let text = format!(
        "[agents.worker]\ntransport='acp'\ncommand={}\nargs=['happy']\nworkspace={}\n",
        serde_json::json!(backend),
        serde_json::json!(root)
    );
    EndpointRegistry::from_config(&crate::config::load_from_str(&text).unwrap())
}

async fn seed(
    pool: &sqlx::SqlitePool,
    registry: &EndpointRegistry,
    target: &RegisteredEndpoint,
) -> (Arc<dyn Repository>, AgentTask) {
    let implementation = SqliteRepository::new(pool.clone());
    let repository: Arc<dyn Repository> = Arc::new(implementation.clone());
    sync_agents(repository.as_ref(), registry).await.unwrap();
    let conversation = Conversation {
        id: ConversationId::generate(),
        participants: vec![target.id()],
        external_ref: None,
    };
    repository.insert_conversation(&conversation).await.unwrap();
    let task_id = TaskId::generate();
    let task = AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: None,
        from_agent: target.id(),
        to_agent: target.id(),
        conversation_id: conversation.id,
        reply_to: None,
        text: "local owner fixture".into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    };
    implementation
        .insert_task_and_event(
            &task,
            &TaskEvent {
                id: EventId::generate(),
                task_id,
                seq: 1,
                status: TaskStatus::Queued,
                timestamp: Utc::now(),
                payload: TaskEventPayload::Queued,
            },
        )
        .await
        .unwrap();
    (repository, task)
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let pool = connect(root.path().join("bridge.db")).await.unwrap();
        migrate(&pool).await.unwrap();
        let registry = registry(root.path());
        let target = registry.get(derive_endpoint_id("worker")).unwrap().clone();
        let (repository, task) = seed(&pool, &registry, &target).await;
        let store = ReliabilityStore::new(pool.clone());
        let token = uuid::Uuid::now_v7().to_string();
        let evidence = RuntimeOwnerEvidence::capture().unwrap();
        let fingerprint = RuntimeOwnerEvidence::fingerprint().unwrap();
        assert!(
            store
                .begin_runtime_with_evidence(
                    &token,
                    &fingerprint,
                    &Utc::now().to_rfc3339(),
                    &evidence
                )
                .await
                .unwrap()
        );
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,created_at,updated_at) VALUES(?,'ready',0,'t','t')")
            .bind(task.task_id.to_string()).execute(&pool).await.unwrap();
        assert!(
            store
                .claim_admission(
                    &task.task_id.to_string(),
                    0,
                    Some(&token),
                    &Utc::now().to_rfc3339()
                )
                .await
                .unwrap()
        );
        let gate = AcpSpawnGate::for_generation(&token);
        let dispatcher = AcpDispatcher::builder()
            .cwd(root.path())
            .clock(Clock::system())
            .limits(AcpLimits::default())
            .sessions(SqliteSessionStore::new(pool.clone()))
            .repository(repository)
            .reliability(store.clone())
            .spawn_gate(gate.clone())
            .build()
            .unwrap();
        let leased = Arc::new(LeasedAcpDispatcher::new(
            dispatcher.clone(),
            SqliteRuntimeStore::new(pool.clone()),
            WorkspaceId::from_canonical_path(root.path()).unwrap(),
            ContinuationPolicy {
                max_turns: 2,
                max_wall_time: std::time::Duration::from_secs(30),
                max_inactivity: std::time::Duration::from_secs(30),
                max_consecutive_no_progress: 2,
                max_observed_output_bytes: 4096,
                lease_ttl: std::time::Duration::from_secs(30),
            },
            Arc::new(SystemRuntimeClock),
            Arc::new(TokioRuntimeTimer),
            Arc::new(RuntimeMetrics::default()),
        ));
        let router =
            AcpDispatcherRouter::with_gate(HashMap::from([(target.id(), leased)]), gate.clone());
        Self {
            root,
            pool,
            store,
            token,
            task,
            target,
            dispatcher,
            router,
            gate,
        }
    }

    async fn assert_reopened_refusal(&self) {
        let reopened = connect(self.root.path().join("bridge.db")).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_instances r JOIN runtime_owner_evidence e USING(instance_token) WHERE r.state='stopped' AND e.state='reaped'").fetch_one(&reopened).await.unwrap();
        assert_eq!(count, 0);
        let audit: i64 = sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments")
            .fetch_one(&reopened)
            .await
            .unwrap();
        assert_eq!(audit, 0);
        reopened.close().await;
    }
}

#[tokio::test]
async fn owner_real_router_reaps_but_cannot_publish_without_containment() {
    let fixture = Fixture::new().await;
    let request = DispatchRequest {
        task: &fixture.task,
        target: &fixture.target,
        delivery_id: DeliveryId::generate(),
        attempt: 1,
    };
    fixture.router.deliver(request.clone()).await.unwrap();
    let outcome = fixture.router.execute(request.clone()).await.unwrap();
    assert!(matches!(outcome, DispatchOutcome::Completed { .. }));
    let children = fixture.gate.registered_pids();
    assert_eq!(children.len(), 1);
    assert!(
        fixture
            .store
            .stopping_runtime(&fixture.token, &Utc::now().to_rfc3339())
            .await
            .unwrap()
    );
    let observation = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        fixture.router.shutdown_observation(),
    )
    .await
    .unwrap();
    assert!(fixture.gate.is_closed_joined());
    for pid in children {
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "owned ACP child must be reaped"
        );
    }
    assert!(fixture.gate.reserve().is_none());
    assert!(!observation.qualifies(observation.epoch));
    assert!(fixture.dispatcher.deliver(request).await.is_err());
    assert!(
        !fixture
            .store
            .finalize_owner(
                &fixture.token,
                &Utc::now().to_rfc3339(),
                &RuntimeOwnerEvidence::capture().unwrap(),
                &observation
            )
            .await
            .unwrap()
    );
    assert!(
        !fixture
            .store
            .verify_final_owner(
                &fixture.token,
                &Utc::now().to_rfc3339(),
                &RuntimeOwnerEvidence::capture().unwrap(),
                &observation
            )
            .await
            .unwrap()
    );
    assert!(fixture.dispatcher.shutdown().await.is_none());
    fixture.pool.close().await;
    fixture.assert_reopened_refusal().await;
}

#[tokio::test]
async fn owner_router_reaps_cancelled_uncached_handshake() {
    let fixture = Fixture::new().await;
    let gate = fixture.gate.clone();
    let connecting = tokio::spawn(async move {
        let address = crate::models::EndpointAddress::Acp {
            command: "sh".into(),
            args: vec!["-c".into(), "while IFS= read -r line; do :; done".into()],
        };
        crate::acp::AcpClient::connect_with_gate(
            &address,
            "cancelled",
            None,
            AcpLimits::default(),
            Some(&gate),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while fixture.gate.registered_pids().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let children = fixture.gate.registered_pids();
    connecting.abort();
    assert!(connecting.await.unwrap_err().is_cancelled());
    assert!(
        fixture
            .store
            .stopping_runtime(&fixture.token, &Utc::now().to_rfc3339())
            .await
            .unwrap()
    );
    let observation = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        fixture.router.shutdown_observation(),
    )
    .await
    .unwrap();
    assert!(fixture.gate.is_closed_joined());
    assert!(!observation.qualifies(observation.epoch));
    for pid in children {
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
    fixture.pool.close().await;
    fixture.assert_reopened_refusal().await;
}
