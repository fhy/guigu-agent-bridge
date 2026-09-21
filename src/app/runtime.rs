use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use chrono::Utc;
use sqlx::SqlitePool;

use crate::{
    a2a::{A2aServerHandle, A2aStore, CleanupHandle, CleanupPolicy, start_cleanup},
    acp::{AcpDispatcher, SqliteSessionStore},
    bus::{
        Cancellation, Clock, DispatcherRegistry, EndpointRegistry, EventBroadcaster, EventConsumer,
        MpscEventSink, TokioTimer, Worker, WorkerConfig,
    },
    config::{Config, ConfigError},
    gateway::{GatewayRawConsumer, GatewayStore, MatrixGateway, MatrixRoute},
    matrix::{MatrixClient, MatrixSync, MatrixSyncHandle, MemorySyncTokenStore, SdkMatrixSender},
    models::TransportType,
    runtime::{
        ContinuationPolicy, LeasedAcpDispatcher, RuntimeMetrics, SqliteRuntimeStore,
        SystemRuntimeClock, TokioRuntimeTimer, WorkspaceId,
    },
    storage::{
        Repository, RepositoryEventConsumer, SqliteRepository, StorageError, connect, migrate,
        plan_recovery, sync_agents,
    },
};

use super::{
    AcpDispatcherRouter, FileConfigSource, HealthServer, HealthState, MatrixIngress,
    MatrixIngressHandle, OwnerState, PersistenceFirstProjection, PersistingBus, ProjectionMetrics,
    ReloadController, ReloadError, ReplyRegistry,
};

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("application configuration failed")]
    Config(#[from] ConfigError),
    #[error("application storage failed")]
    Storage(#[from] StorageError),
    #[error("application runtime control failed")]
    Runtime,
    #[error("application Matrix adapter failed")]
    Matrix,
    #[error("application assembly failed: {0}")]
    Assembly(&'static str),
    #[error("application health listener failed")]
    Health(#[source] std::io::Error),
    #[error("application reload failed")]
    Reload(#[from] ReloadError),
    #[error("application shutdown signal failed")]
    Shutdown(#[source] std::io::Error),
}

pub struct AppRuntime {
    pool: Option<SqlitePool>,
    reload: Arc<ReloadController>,
    health_state: Arc<HealthState>,
    health: Option<HealthServer>,
    matrix_sync: Option<MatrixSyncHandle>,
    ingress: Option<MatrixIngressHandle>,
    bus: Option<Arc<dyn crate::bus::Bus>>,
    worker: Option<tokio::task::JoinHandle<Result<(), crate::bus::WorkerError>>>,
    broadcaster: Option<tokio::task::JoinHandle<()>>,
    outbox: Option<crate::app::OutboxDrainHandle>,
    acp: Option<Arc<AcpDispatcherRouter>>,
    a2a: Option<A2aServerHandle>,
    a2a_cleanup: Option<CleanupHandle>,
    gateway: Option<Arc<MatrixGateway>>,
    gateway_cleanup: Option<crate::gateway::GatewayCleanupHandle>,
    shutdown_timeout: Duration,
    reliability: crate::storage::ReliabilityStore,
    runtime_instance: Option<String>,
}

impl AppRuntime {
    pub async fn start(path: impl AsRef<Path>) -> Result<Self, AppError> {
        let path = path.as_ref().to_path_buf();
        let config = crate::config::load(&path)?;
        prepare_directories(&config).map_err(AppError::Health)?;
        let pool = connect(&config.bridge.database).await?;
        if let Err(error) = migrate(&pool).await {
            pool.close().await;
            return Err(error.into());
        }
        let reliability = crate::storage::ReliabilityStore::new(pool.clone());
        let runtime_instance = uuid::Uuid::now_v7().to_string();
        let startup_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let owns_runtime = reliability
            .begin_runtime(&runtime_instance, &runtime_instance, &startup_at)
            .await
            .map_err(|_| AppError::Runtime)?;
        match Self::build(
            path,
            config,
            pool.clone(),
            reliability.clone(),
            runtime_instance.clone(),
            owns_runtime,
        )
        .await
        {
            Ok(runtime) => Ok(runtime),
            Err(error) => {
                if owns_runtime {
                    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
                    if !reliability
                        .stop_runtime(&runtime_instance, &now)
                        .await
                        .unwrap_or(false)
                    {
                        tracing::warn!("runtime epoch rollback failed during startup cleanup");
                    }
                }
                pool.close().await;
                Err(error)
            }
        }
    }

    async fn build(
        path: PathBuf,
        config: Config,
        pool: SqlitePool,
        reliability: crate::storage::ReliabilityStore,
        runtime_instance: String,
        owns_runtime: bool,
    ) -> Result<Self, AppError> {
        let repository = Arc::new(SqliteRepository::new(pool.clone()));
        let repository_trait: Arc<dyn Repository> = repository.clone();
        let sessions = SqliteSessionStore::new(pool.clone());
        let runtime_store = SqliteRuntimeStore::new(pool.clone());
        let registry = Arc::new(EndpointRegistry::from_config(&config));
        sync_agents(repository_trait.as_ref(), &registry).await?;

        let mut recovery = plan_recovery(repository_trait.as_ref()).await?;
        let mut admission_recovery = reliability
            .classify_admissions(4096)
            .await
            .map_err(|_| AppError::Runtime)?;
        if admission_recovery.eligible.len() > config.bridge.queue_capacity {
            admission_recovery.blocked = true;
        }
        recovery.unfinished.retain(|id| {
            !admission_recovery
                .covered
                .iter()
                .any(|value| value == &id.to_string())
        });
        let runtime_recovery = runtime_store
            .recovery_snapshot(Utc::now(), 4096)
            .await
            .map_err(|_| AppError::Runtime)?;
        let counts = runtime_store
            .counts()
            .await
            .map_err(|_| AppError::Runtime)?;
        let recovery_blocked = !owns_runtime
            || admission_recovery.blocked
            || !recovery.is_empty()
            || !runtime_recovery.ready.is_empty()
            || !runtime_recovery.ambiguous.is_empty()
            || counts.active_leases > 0
            || counts.recovery_needed_leases > 0
            || counts.recovery_backlog > 0;

        let reload = Arc::new(ReloadController::new(
            config.clone(),
            Arc::new(FileConfigSource::new(path)),
        )?);
        let metrics = Arc::new(RuntimeMetrics::default());
        metrics.set_gauges(counts);
        let projection_metrics = Arc::new(ProjectionMetrics::default());
        let health_state = Arc::new(HealthState::new(
            runtime_store.clone(),
            Arc::clone(&metrics),
            Arc::clone(&projection_metrics),
        ));
        let insecure_a2a_peers = if config.transports.a2a.enabled {
            config
                .transports
                .a2a
                .peers
                .values()
                .filter(|peer| peer.danger_accept_invalid_certs)
                .count()
        } else {
            0
        };
        health_state.set_insecure_a2a_peers(insecure_a2a_peers);
        health_state.set_recovery_blocked(recovery_blocked);
        let mut runtime = Self {
            pool: Some(pool.clone()),
            reload,
            health_state,
            health: None,
            matrix_sync: None,
            ingress: None,
            bus: None,
            worker: None,
            broadcaster: None,
            outbox: None,
            acp: None,
            a2a: None,
            a2a_cleanup: None,
            gateway: None,
            gateway_cleanup: None,
            shutdown_timeout: Duration::from_secs(config.bridge.shutdown_timeout_seconds),
            reliability: reliability.clone(),
            runtime_instance: owns_runtime.then_some(runtime_instance.clone()),
        };
        if recovery_blocked {
            runtime.health = start_health(config.bridge.health_bind, &runtime.health_state).await?;
            runtime.health_state.set_owner(OwnerState::Running);
            return Ok(runtime);
        }

        let mut endpoint_dispatchers = HashMap::new();
        for (agent_id, declared) in &config.agents {
            if !declared.enabled || declared.transport != TransportType::Acp {
                continue;
            }
            let workspace = declared
                .workspace
                .as_ref()
                .ok_or(AppError::Assembly("enabled ACP workspace missing"))?;
            let workspace_path = workspace.clone();
            let endpoint = registry
                .get_by_agent_id(agent_id)
                .ok_or(AppError::Assembly("configured endpoint missing"))?;
            let dispatcher = AcpDispatcher::builder()
                .cwd(workspace)
                .additional_directories(
                    declared
                        .additional_workspaces
                        .iter()
                        .map(|path| path.to_string_lossy().into_owned())
                        .collect(),
                )
                .sessions(sessions.clone())
                .repository(repository_trait.clone())
                .reliability(reliability.clone())
                .allow_nonterminal_end_turn(config.runtime.allow_nonterminal_end_turn)
                .build()
                .map_err(AppError::Assembly)?;
            let workspace =
                WorkspaceId::from_canonical_path(workspace).map_err(|_| AppError::Runtime)?;
            endpoint_dispatchers.insert(
                endpoint.id(),
                Arc::new(LeasedAcpDispatcher::new_with_paths(
                    dispatcher,
                    runtime_store.clone(),
                    workspace,
                    std::iter::once(workspace_path.clone())
                        .chain(declared.additional_workspaces.iter().cloned())
                        .collect(),
                    continuation_policy(&config),
                    Arc::new(SystemRuntimeClock),
                    Arc::new(TokioRuntimeTimer),
                    Arc::clone(&metrics),
                )),
            );
        }
        let acp = Arc::new(AcpDispatcherRouter::new(endpoint_dispatchers));
        let mut dispatchers = DispatcherRegistry::new().with(TransportType::Acp, acp.clone());
        if config.transports.a2a.enabled {
            let a2a = crate::a2a::assembly::dispatchers(&config, pool.clone())
                .await
                .map_err(|_| AppError::Assembly("A2A peer startup failed"))?;
            dispatchers = dispatchers.with(TransportType::A2a, Arc::new(a2a));
        }

        let (event_sink, events) = MpscEventSink::new(config.bridge.event_capacity);
        let event_sink: Arc<dyn crate::bus::EventSink> = Arc::new(event_sink);
        let default_timeout =
            chrono::Duration::from_std(Duration::from_secs(config.bridge.default_timeout_seconds))
                .map_err(|_| AppError::Assembly("default timeout is out of range"))?;
        let (bus, tasks) = PersistingBus::new(
            Arc::clone(&registry),
            config.bridge.queue_capacity,
            Arc::clone(&event_sink),
            repository.as_ref().clone(),
            Clock::system(),
        );
        let bus = bus
            .with_default_timeout(default_timeout)
            .with_reliability(reliability.clone(), runtime_instance.clone());
        let durable_bus = Arc::new(bus);
        let gateway_handoff = crate::app::gateway_handoff::GatewayHandoff::new(
            crate::gateway::GatewayStore::new(pool.clone()),
            Arc::clone(&durable_bus),
            runtime_instance.clone(),
        );
        if config.transports.gateway.enabled
            && !crate::gateway::GatewayStore::new(pool.clone())
                .validate_retained_bytes()
                .await
                .map_err(|_| AppError::Assembly("gateway retained-byte validation failed"))?
        {
            return Err(AppError::Assembly(
                "gateway retained-byte validation failed",
            ));
        }
        for (task_id, revision) in admission_recovery.eligible {
            let parsed = task_id.parse().map_err(|_| AppError::Runtime)?;
            if let Some(task) = repository_trait.get_task(parsed).await? {
                let gateway_task: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM gateway_envelopes WHERE internal_task_id=?)",
                )
                .bind(&task_id)
                .fetch_one(&pool)
                .await
                .map_err(|_| AppError::Runtime)?;
                if gateway_task {
                    gateway_handoff
                        .handoff(task, revision)
                        .await
                        .map_err(|_| AppError::Runtime)?;
                    continue;
                }
                let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
                let state: Option<String> =
                    sqlx::query_scalar("SELECT state FROM task_admissions WHERE task_id=?")
                        .bind(&task_id)
                        .fetch_optional(&pool)
                        .await
                        .map_err(|_| AppError::Runtime)?;
                let claimed = if state.as_deref() == Some("ready") {
                    reliability
                        .claim_admission(&task_id, revision, Some(&runtime_instance), &now)
                        .await
                        .map_err(|_| AppError::Runtime)?
                } else {
                    reliability
                        .reclaim_stopped_admission(&task_id, revision, &runtime_instance, &now)
                        .await
                        .map_err(|_| AppError::Runtime)?
                };
                if claimed && durable_bus.enqueue_persisted(task).await.is_err() {
                    let _ = reliability
                        .release_admission(&task_id, revision + 1, &now)
                        .await;
                }
            }
        }
        let bus: Arc<dyn crate::bus::Bus> = durable_bus.clone();
        let cancellation = Cancellation::new();
        let worker = Worker::builder(
            Arc::clone(&registry),
            tasks,
            Arc::clone(&event_sink),
            Clock::system(),
            dispatchers,
        )
        .lifecycle(Arc::new(crate::runtime::SqliteTaskLifecycle::new(
            pool.clone(),
        )))
        .config(WorkerConfig {
            limits: crate::bus::LoopLimits::from_bridge(&config.bridge),
            retry: Default::default(),
            cancellation: Some(cancellation.clone()),
            timer: Some(Arc::new(TokioTimer)),
        })
        .build();

        let persistence: Arc<dyn EventConsumer> =
            Arc::new(RepositoryEventConsumer::new(repository_trait.clone()));
        let siblings: Vec<Arc<dyn EventConsumer>> = if config.transports.a2a.enabled {
            vec![Arc::new(crate::a2a::A2aTerminalProjection::new(
                A2aStore::new(pool.clone()),
            ))]
        } else {
            Vec::new()
        };

        let matrix = if config.transports.matrix.enabled {
            let client = MatrixClient::restore(&config.transports.matrix)
                .await
                .map_err(|_| AppError::Matrix)?;
            let sdk = Arc::new(SdkMatrixSender::new(client.clone()));
            let mut sync = MatrixSync::new(
                client,
                Arc::new(MemorySyncTokenStore::default()),
                config.transports.matrix.sync_capacity,
            )
            .map_err(|_| AppError::Matrix)?;
            let gateway = if config.transports.gateway.enabled {
                let gateway = Arc::new(MatrixGateway::new(
                    sdk.clone(),
                    MatrixRoute {
                        room_id: config.transports.gateway.room_id.clone(),
                        peer_id: config.transports.gateway.peer_id.clone(),
                        local_endpoint_id: config.transports.gateway.local_endpoint_id.clone(),
                        remote_endpoint_id: config.transports.gateway.remote_endpoint_id.clone(),
                        generation: config.transports.gateway.generation,
                        max_payload_bytes: config.transports.gateway.max_payload_bytes,
                        deadline_seconds: config.transports.gateway.deadline_seconds,
                        allowed_senders: config.transports.gateway.allowed_senders.clone(),
                        own_user: config.transports.matrix.user_id.clone(),
                        runtime_instance: runtime_instance.clone(),
                    },
                ));
                sync = sync.with_raw_consumer(Arc::new(GatewayRawConsumer::new(
                    Arc::clone(&gateway),
                    GatewayStore::new(pool.clone()),
                    repository.as_ref().clone(),
                    gateway_handoff.clone(),
                )));
                Some(gateway)
            } else {
                None
            };
            Some((sdk, sync, gateway))
        } else {
            None
        };

        runtime.health = start_health(config.bridge.health_bind, &runtime.health_state).await?;

        if let Some((sdk, sync, gateway)) = matrix {
            let matrix_sender: Arc<dyn crate::matrix::MatrixSender> = sdk.clone();
            runtime.gateway = gateway;
            if runtime.gateway.is_some() {
                runtime.gateway_cleanup = Some(crate::gateway::GatewayCleanupHandle::start(
                    GatewayStore::new(pool.clone()),
                    runtime_instance.clone(),
                ));
            }
            let outbox_sender: Arc<dyn crate::matrix::MatrixOutboxSender> = sdk.clone();
            let outbox = crate::app::OutboxDrain::new(reliability.clone(), outbox_sender, 64);
            let outbox_owner = outbox.clone().start(Duration::from_secs(1));
            let replies = Arc::new(ReplyRegistry::default());

            let (sync_handle, receiver) = sync.start();
            let retry: Arc<dyn crate::matrix::RetryAdmission> =
                Arc::new(crate::app::DurableRetryAdmission::new(
                    reliability.clone(),
                    Clock::system(),
                    default_timeout,
                    durable_bus.as_ref().clone(),
                    outbox,
                    runtime_instance.clone(),
                ));
            let ingress = MatrixIngress::new(
                receiver,
                repository_trait.clone(),
                Arc::clone(&registry),
                durable_bus.clone(),
                matrix_sender,
                Arc::clone(&runtime.reload),
                cancellation.clone(),
                Arc::new(crate::matrix::CommandLedger::new(4096)),
                replies,
                4096,
            )
            .with_retry_admission(retry)
            .start();
            runtime.health_state.register_required(sync_handle.alive());
            runtime.health_state.register_required(ingress.alive());
            runtime.matrix_sync = Some(sync_handle);
            runtime.ingress = Some(ingress);
            runtime.outbox = Some(outbox_owner);
        }

        let projection: Arc<dyn EventConsumer> = Arc::new(PersistenceFirstProjection::new(
            persistence,
            siblings,
            projection_metrics,
        ));
        let worker_alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
        runtime
            .health_state
            .register_required(Arc::clone(&worker_alive));
        runtime.worker = Some(tokio::spawn(async move {
            let result = worker.run().await;
            worker_alive.store(false, std::sync::atomic::Ordering::Release);
            result
        }));
        let broadcaster_alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
        runtime
            .health_state
            .register_required(Arc::clone(&broadcaster_alive));
        runtime.broadcaster = Some(tokio::spawn(async move {
            EventBroadcaster::new(events, vec![projection]).run().await;
            broadcaster_alive.store(false, std::sync::atomic::Ordering::Release);
        }));
        runtime.bus = Some(bus);
        runtime.acp = Some(acp);
        if config.transports.a2a.enabled {
            runtime.a2a_cleanup = Some(
                start_cleanup(
                    A2aStore::new(pool.clone()),
                    runtime_instance.clone(),
                    CleanupPolicy {
                        terminal_ttl: Duration::from_secs(
                            config.transports.a2a.terminal_content_ttl_seconds,
                        ),
                        byte_ceiling: config.transports.a2a.retained_bytes_ceiling as i64,
                        low_watermark: config.transports.a2a.retained_bytes_low_watermark as i64,
                        batch: config.transports.a2a.cleanup_batch,
                        interval: Duration::from_secs(60),
                    },
                )
                .await
                .map_err(|_| AppError::Assembly("A2A cleanup startup failed"))?,
            );
            runtime.a2a = Some(
                crate::a2a::assembly::server(
                    &config,
                    pool.clone(),
                    repository_trait.clone(),
                    repository.as_ref().clone(),
                    durable_bus,
                    cancellation,
                    Arc::clone(&registry),
                )
                .await
                .map_err(|_| AppError::Assembly("A2A listener startup failed"))?,
            );
        }
        runtime.health_state.set_adapter_ready(true);
        runtime.health_state.set_owner(OwnerState::Running);
        Ok(runtime)
    }

    pub fn reload_controller(&self) -> Arc<ReloadController> {
        Arc::clone(&self.reload)
    }

    pub fn health_state(&self) -> Arc<HealthState> {
        Arc::clone(&self.health_state)
    }

    pub fn health_addr(&self) -> Option<std::net::SocketAddr> {
        self.health.as_ref().map(HealthServer::local_addr)
    }

    pub async fn shutdown(mut self) -> Result<(), AppError> {
        let mut shutdown_error = None;
        self.health_state.set_owner(OwnerState::Stopping);
        if let Some(token) = self.runtime_instance.as_deref() {
            let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
            if !self
                .reliability
                .stopping_runtime(token, &now)
                .await
                .unwrap_or(false)
            {
                shutdown_error = Some(AppError::Runtime);
            }
        }
        self.reload.stop().await;
        if let Some(ingress) = self.ingress.take() {
            ingress.shutdown().await;
        }
        self.bus.take();
        if let Some(mut worker) = self.worker.take() {
            match tokio::time::timeout(self.shutdown_timeout, &mut worker).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(_))) => {
                    shutdown_error = Some(AppError::Assembly("worker failed"));
                }
                Ok(Err(_)) => {
                    shutdown_error = Some(AppError::Assembly("worker join failed"));
                }
                Err(_) => {
                    worker.abort();
                    let _ = worker.await;
                    shutdown_error = Some(AppError::Assembly("worker shutdown timed out"));
                }
            }
        }
        if let Some(mut broadcaster) = self.broadcaster.take() {
            match tokio::time::timeout(self.shutdown_timeout, &mut broadcaster).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    shutdown_error.get_or_insert(AppError::Assembly("event owner join failed"));
                }
                Err(_) => {
                    broadcaster.abort();
                    let _ = broadcaster.await;
                    shutdown_error = Some(AppError::Assembly("event drain timed out"));
                }
            }
        }
        if let Some(acp) = self.acp.take() {
            acp.shutdown().await;
        }
        if let Some(a2a) = self.a2a.take()
            && a2a.shutdown().await.is_err()
        {
            shutdown_error.get_or_insert(AppError::Assembly("A2A server join failed"));
        }
        if let Some(cleanup) = self.a2a_cleanup.take()
            && cleanup.shutdown().await.is_err()
        {
            shutdown_error.get_or_insert(AppError::Assembly("A2A cleanup join failed"));
        }
        if let Some(cleanup) = self.gateway_cleanup.take()
            && cleanup.shutdown().await.is_err()
        {
            shutdown_error.get_or_insert(AppError::Assembly("gateway cleanup join failed"));
        }
        if let Some(outbox) = self.outbox.take()
            && outbox.shutdown().await.is_err()
        {
            shutdown_error.get_or_insert(AppError::Assembly("outbox owner join failed"));
        }
        self.gateway.take();
        if let Some(sync) = self.matrix_sync.take() {
            let _ = sync.shutdown().await;
        }
        if let Some(health) = self.health.take()
            && let Err(error) = health.shutdown().await
        {
            shutdown_error.get_or_insert(AppError::Health(error));
        }
        self.health_state.set_owner(OwnerState::Stopped);
        if shutdown_error.is_none()
            && let Some(token) = self.runtime_instance.take()
        {
            let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
            match self.reliability.stop_runtime(&token, &now).await {
                Ok(true) => {}
                Ok(false) | Err(_) => shutdown_error = Some(AppError::Runtime),
            }
        }
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        match shutdown_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn continuation_policy(config: &Config) -> ContinuationPolicy {
    ContinuationPolicy {
        max_turns: config.runtime.max_turns,
        max_wall_time: Duration::from_secs(config.runtime.max_wall_seconds),
        max_inactivity: Duration::from_secs(config.runtime.max_inactivity_seconds),
        max_consecutive_no_progress: config.runtime.max_no_progress,
        max_observed_output_bytes: config.runtime.max_output_bytes,
        lease_ttl: Duration::from_secs(config.runtime.lease_ttl_seconds),
    }
}

fn prepare_directories(config: &Config) -> std::io::Result<()> {
    if let Some(parent) = config.bridge.database.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir_all(&config.bridge.session_root)
}

async fn start_health(
    address: Option<std::net::SocketAddr>,
    state: &Arc<HealthState>,
) -> Result<Option<HealthServer>, AppError> {
    match address {
        Some(address) => HealthServer::start(address, Arc::clone(state))
            .await
            .map(Some)
            .map_err(AppError::Health),
        None => Ok(None),
    }
}

pub async fn run_config_with_shutdown<F>(
    path: impl AsRef<Path>,
    shutdown: F,
) -> Result<(), AppError>
where
    F: Future<Output = std::io::Result<()>>,
{
    let runtime = AppRuntime::start(path).await?;
    let shutdown_result;
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut reload = match signal(SignalKind::hangup()) {
            Ok(reload) => reload,
            Err(error) => {
                let _ = runtime.shutdown().await;
                return Err(AppError::Shutdown(error));
            }
        };
        tokio::pin!(shutdown);
        shutdown_result = loop {
            tokio::select! {
                result = &mut shutdown => {
                    break result.map_err(AppError::Shutdown);
                }
                received = reload.recv() => {
                    if received.is_none() {
                        break Ok(());
                    }
                    match runtime.reload_controller().reload().await {
                        Ok(outcome) => tracing::info!(?outcome, "configuration reload finished"),
                        Err(error) => tracing::warn!(%error, "configuration reload rejected"),
                    }
                }
            }
        };
    }
    #[cfg(not(unix))]
    {
        shutdown_result = shutdown.await.map_err(AppError::Shutdown);
    }
    let cleanup_result = runtime.shutdown().await;
    shutdown_result?;
    cleanup_result
}
