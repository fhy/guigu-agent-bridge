use super::*;
use guigu_agent_bridge::runtime::RuntimeTimer;
use std::sync::atomic::{AtomicI64, Ordering};

#[derive(Default)]
struct ControlledClock(AtomicI64);

impl RuntimeClock for ControlledClock {
    fn now(&self) -> DateTime<Utc> {
        ts() + chrono::Duration::seconds(self.0.load(Ordering::SeqCst))
    }
}

struct HeartbeatTimer;

impl RuntimeTimer for HeartbeatTimer {
    fn sleep<'a>(
        &'a self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        let wait = if duration < Duration::from_secs(30) {
            Duration::from_millis(200)
        } else {
            Duration::from_secs(300)
        };
        Box::pin(tokio::time::sleep(wait))
    }
}

struct Setup {
    harness: Harness,
    root: tempfile::TempDir,
    task: AgentTask,
    clock: Arc<ControlledClock>,
    leased: Arc<LeasedAcpDispatcher>,
}

impl Setup {
    async fn new(tag: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let scenario = format!("gated-initialize:{}", root.path().join("gate").display());
        let mut limits = default_limits();
        limits.initialize_deadline = Duration::from_secs(300);
        let harness = Harness::new_reliable(tag, &scenario, limits, true).await;
        let task = harness.task("setup only; must not prompt");
        harness
            .repository_impl
            .insert_task_and_event(
                &task,
                &TaskEvent {
                    id: EventId::generate(),
                    task_id: task.task_id,
                    seq: 1,
                    status: TaskStatus::Queued,
                    timestamp: ts(),
                    payload: TaskEventPayload::Queued,
                },
            )
            .await
            .unwrap();
        let clock = Arc::new(ControlledClock::default());
        let leased = Arc::new(LeasedAcpDispatcher::new(
            (*harness.dispatcher).clone(),
            SqliteRuntimeStore::new(harness.pool.clone()),
            WorkspaceId::from_canonical_path(root.path()).unwrap(),
            default_policy(),
            clock.clone(),
            Arc::new(HeartbeatTimer),
            Arc::new(RuntimeMetrics::default()),
        ));
        Self {
            harness,
            root,
            task,
            clock,
            leased,
        }
    }

    fn deliver(
        &self,
    ) -> tokio::task::JoinHandle<Result<(), guigu_agent_bridge::bus::DispatchError>> {
        let target = self
            .harness
            .registry
            .get(self.task.to_agent)
            .unwrap()
            .clone();
        let task = self.task.clone();
        let leased = self.leased.clone();
        tokio::spawn(async move {
            leased
                .deliver(guigu_agent_bridge::bus::DispatchRequest {
                    task: &task,
                    target: &target,
                    delivery_id: DeliveryId::generate(),
                    attempt: 1,
                    queue_lease: None,
                })
                .await
        })
    }

    async fn wait_until_setup(&self) {
        tokio::time::timeout(Duration::from_secs(90), async {
            while !self.root.path().join("gate.ready").exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("backend must reach its gated initialization");
    }

    fn release_setup(&self) {
        std::fs::write(self.root.path().join("gate"), b"continue").unwrap();
    }

    async fn close(self) {
        self.leased.shutdown().await;
        self.harness.pool.close().await;
        remove_db_files(&self.harness.path);
    }
}

#[tokio::test]
async fn session_setup_longer_than_lease_ttl_retains_the_exact_live_fence() {
    let setup = Setup::new("r107-setup-heartbeat-controlled").await;
    let delivery = setup.deliver();
    setup.wait_until_setup().await;
    let mut observed = Vec::new();
    let mut heartbeat_ok = true;
    for seconds in [10, 20, 30, 40] {
        setup.clock.0.store(seconds, Ordering::SeqCst);
        let expected = setup.clock.now();
        let updated = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let heartbeat: String = sqlx::query_scalar(
                    "SELECT heartbeat_at FROM execution_leases WHERE task_id=? AND state='active'",
                )
                .bind(setup.task.task_id.to_string())
                .fetch_one(&setup.harness.pool)
                .await
                .unwrap();
                if DateTime::parse_from_rfc3339(&heartbeat)
                    .unwrap()
                    .with_timezone(&Utc)
                    >= expected
                {
                    return heartbeat;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        match updated {
            Ok(heartbeat) => observed.push(heartbeat),
            Err(_) => {
                heartbeat_ok = false;
                break;
            }
        }
    }
    setup.release_setup();
    let delivered = delivery.await.unwrap();
    let continuation = SqliteRuntimeStore::new(setup.harness.pool.clone())
        .continuation(setup.task.task_id)
        .await
        .unwrap();
    let lease: (i64, String, String, String) = sqlx::query_as(
        "SELECT fence,acquired_at,heartbeat_at,state FROM execution_leases WHERE task_id=?",
    )
    .bind(setup.task.task_id.to_string())
    .fetch_one(&setup.harness.pool)
    .await
    .unwrap();
    setup.close().await;
    assert!(
        heartbeat_ok,
        "setup must renew at each controlled clock step"
    );
    assert_eq!(observed.len(), 4);
    assert!(
        delivered.is_ok(),
        "long setup lost its lease: {delivered:?}"
    );
    assert!(continuation.is_some());
    assert_eq!(lease.0, 1);
    assert!(lease.2 > lease.1);
    assert_eq!(lease.3, "active");
}

#[tokio::test]
async fn setup_renewal_loss_reaps_backend_without_prompt_or_foreign_lease_mutation() {
    let setup = Setup::new("r107-setup-fenced-controlled").await;
    let delivery = setup.deliver();
    setup.wait_until_setup().await;
    sqlx::query("UPDATE execution_leases SET owner_token='foreign',fence=fence+1 WHERE task_id=?")
        .bind(setup.task.task_id.to_string())
        .execute(&setup.harness.pool)
        .await
        .unwrap();
    setup.release_setup();
    let result = delivery.await.unwrap();
    let lease: (String, i64, String) =
        sqlx::query_as("SELECT owner_token,fence,state FROM execution_leases WHERE task_id=?")
            .bind(setup.task.task_id.to_string())
            .fetch_one(&setup.harness.pool)
            .await
            .unwrap();
    let continuations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_continuations WHERE task_id=?")
            .bind(setup.task.task_id.to_string())
            .fetch_one(&setup.harness.pool)
            .await
            .unwrap();
    let backend_reaped = setup.harness.dispatcher.backend_id().is_none();
    setup.close().await;
    assert!(matches!(
        result,
        Err(guigu_agent_bridge::bus::DispatchError::RecoveryNeeded)
    ));
    assert_eq!(lease, ("foreign".into(), 2, "active".into()));
    assert_eq!(continuations, 0);
    assert!(backend_reaped);
}
