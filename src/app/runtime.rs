use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerLiveness {
    Live,
    Stale,
    Unknown,
}

fn owner_liveness(pid: u32) -> OwnerLiveness {
    match std::fs::metadata(format!("/proc/{pid}")) {
        Ok(_) => OwnerLiveness::Live,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => OwnerLiveness::Stale,
        Err(_) => OwnerLiveness::Unknown,
    }
}

fn owner_fingerprint_liveness(fingerprint: &str) -> OwnerLiveness {
    owner_fingerprint_pid(fingerprint).map_or(OwnerLiveness::Unknown, owner_liveness)
}

fn owner_fingerprint_pid(fingerprint: &str) -> Option<u32> {
    let value = fingerprint.strip_prefix("pid:")?;
    let pid = if let Some((pid, fields)) = value.split_once(';') {
        let (executable, cgroup) = fields.strip_prefix("exe:")?.split_once(";cgroup:")?;
        let fields: Vec<_> = executable.split(':').collect();
        if fields.len() != 5
            || fields[..3]
                .iter()
                .any(|field| field.parse::<u64>().is_err())
            || fields[3].parse::<i64>().is_err()
            || !fields[4]
                .parse::<u32>()
                .ok()
                .is_some_and(|value| value < 1_000_000_000)
            || cgroup.trim().is_empty()
            || cgroup.contains('\0')
        {
            return None;
        }
        pid
    } else {
        value
    };
    if pid.is_empty() || !pid.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    pid.parse::<u32>().ok().filter(|pid| *pid > 0)
}

#[cfg(test)]
mod owner_liveness_tests {
    use super::{OwnerLiveness, owner_fingerprint_liveness, owner_liveness};

    #[test]
    fn current_pid_is_live_and_missing_pid_is_the_only_stale_proof() {
        assert_eq!(owner_liveness(std::process::id()), OwnerLiveness::Live);
        assert_eq!(owner_liveness(u32::MAX), OwnerLiveness::Stale);
    }

    #[test]
    fn unknown_fingerprint_cannot_enter_recovery() {
        assert_eq!(
            owner_fingerprint_liveness("legacy-owner"),
            OwnerLiveness::Unknown
        );
        assert_eq!(
            owner_fingerprint_liveness("pid:not-a-pid"),
            OwnerLiveness::Unknown
        );
    }

    #[test]
    fn stale_pid_is_the_only_recovery_candidate() {
        assert_eq!(
            owner_fingerprint_liveness("pid:4294967295"),
            OwnerLiveness::Stale
        );
        assert_eq!(
            owner_fingerprint_liveness(&format!("pid:{}", std::process::id())),
            OwnerLiveness::Live
        );
    }

    #[test]
    fn structured_fingerprint_retains_current_and_stale_pid_liveness() {
        let fingerprint = crate::storage::RuntimeOwnerEvidence::fingerprint().unwrap();
        assert_eq!(
            owner_fingerprint_liveness(&fingerprint),
            OwnerLiveness::Live
        );
        let (_, suffix) = fingerprint.split_once(';').unwrap();
        assert_eq!(
            owner_fingerprint_liveness(&format!("pid:{};{suffix}", u32::MAX)),
            OwnerLiveness::Stale
        );
    }

    #[test]
    fn malformed_structured_and_numeric_fingerprints_remain_unknown() {
        for fingerprint in [
            "pid:0",
            "pid:+1",
            "pid:4294967296",
            "pid:",
            "pid:1;",
            "pid:1;garbage",
            "pid:1;exe:1:2:3:4:5",
            "pid:1;exe:1:2:3:4:5;cgroup:",
            "pid:1;exe:1:2:3:4:1000000000;cgroup:0::/",
            "pid:x;exe:1:2:3:4:5;cgroup:0::/",
        ] {
            assert_eq!(
                owner_fingerprint_liveness(fingerprint),
                OwnerLiveness::Unknown,
                "{fingerprint}"
            );
        }
    }
}

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
        RecoveryDiagnostic, RecoveryFixture, Repository, RepositoryEventConsumer, SqliteRepository,
        StorageError, migrate, plan_recovery, sync_agents,
    },
};

use super::{
    AcpDispatcherRouter, FileConfigSource, HealthServer, HealthState, MatrixFailure, MatrixIngress,
    MatrixIngressHandle, MatrixPhase, OwnerState, PersistenceFirstProjection, PersistingBus,
    ProjectionMetrics, ReloadController, ReloadError, ReplyRegistry,
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
    #[error("required owner failed: {component}/{class}")]
    RequiredOwner {
        component: &'static str,
        class: &'static str,
    },
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
    heartbeat: Option<tokio::task::JoinHandle<()>>,
    outbox: Option<crate::app::OutboxDrainHandle>,
    output_drain: Option<crate::app::output_drain::OutputSetDrainHandle>,
    acp: Option<Arc<AcpDispatcherRouter>>,
    a2a: Option<A2aServerHandle>,
    a2a_cleanup: Option<CleanupHandle>,
    gateway: Option<Arc<MatrixGateway>>,
    gateway_cleanup: Option<crate::gateway::GatewayCleanupHandle>,
    shutdown_timeout: Duration,
    reliability: crate::storage::ReliabilityStore,
    runtime_instance: Option<String>,
    first_failure: Arc<Mutex<Option<RequiredOwnerFailure>>>,
    _maintenance_lock: Option<Arc<crate::offline_production_migration::RuntimeLock>>,
}

#[derive(Clone, Copy)]
struct RequiredOwnerFailure {
    component: &'static str,
    class: &'static str,
}

#[inline]
fn recovery_allows_service_assembly(recovery_blocked: bool) -> bool {
    !recovery_blocked
}

fn retain_failure(slot: &Arc<Mutex<Option<RequiredOwnerFailure>>>, failure: RequiredOwnerFailure) {
    let mut value = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    value.get_or_insert(failure);
}

fn record_task_worker_result(
    result: &Result<(), crate::bus::WorkerError>,
    slot: &Arc<Mutex<Option<RequiredOwnerFailure>>>,
) {
    if result.is_err() {
        retain_failure(
            slot,
            RequiredOwnerFailure {
                component: "task-worker",
                class: "failed",
            },
        );
    }
}

fn spawn_task_worker<F>(
    future: F,
    alive: Arc<std::sync::atomic::AtomicBool>,
    failure: Arc<Mutex<Option<RequiredOwnerFailure>>>,
) -> tokio::task::JoinHandle<Result<(), crate::bus::WorkerError>>
where
    F: Future<Output = Result<(), crate::bus::WorkerError>> + Send + 'static,
{
    tokio::spawn(async move {
        let result = future.await;
        record_task_worker_result(&result, &failure);
        alive.store(false, std::sync::atomic::Ordering::Release);
        result
    })
}

fn record_matrix_owner_failure(
    slot: &Arc<Mutex<Option<RequiredOwnerFailure>>>,
    health: &HealthState,
    generation: u64,
    error: &crate::matrix::MatrixError,
) {
    let reason = matrix_runtime_failure(error);
    retain_failure(
        slot,
        RequiredOwnerFailure {
            component: "matrix-sync",
            class: matrix_error_class(error),
        },
    );
    health.matrix().transition(
        generation,
        &[MatrixPhase::Registering, MatrixPhase::Ready],
        MatrixPhase::Failed(reason),
    );
}

fn matrix_failure_observer(
    slot: Arc<Mutex<Option<RequiredOwnerFailure>>>,
    health: Arc<HealthState>,
    generation: u64,
) -> crate::matrix::FailureObserver {
    Arc::new(move |error| {
        record_matrix_owner_failure(&slot, health.as_ref(), generation, error);
    })
}

impl AppRuntime {
    pub async fn start(path: impl AsRef<Path>) -> Result<Self, AppError> {
        Self::start_inner(path.as_ref().to_path_buf(), Arc::new(crate::a2a::NoFault)).await
    }

    #[cfg(feature = "test-support")]
    pub async fn start_with_a2a_faults(
        path: impl AsRef<Path>,
        faults: Arc<dyn crate::a2a::IngressFaultInjector>,
    ) -> Result<Self, AppError> {
        Self::start_inner(path.as_ref().to_path_buf(), faults).await
    }

    #[cfg(feature = "test-support")]
    pub async fn start_with_config_and_a2a_faults(
        path: impl AsRef<Path>,
        config: Config,
        faults: Arc<dyn crate::a2a::IngressFaultInjector>,
    ) -> Result<Self, AppError> {
        Self::start_config_inner(path.as_ref().to_path_buf(), config, faults).await
    }

    async fn start_inner(
        path: PathBuf,
        a2a_faults: Arc<dyn crate::a2a::IngressFaultInjector>,
    ) -> Result<Self, AppError> {
        let config = crate::config::load(&path)?;
        Self::start_config_inner(path, config, a2a_faults).await
    }

    async fn start_config_inner(
        path: PathBuf,
        config: Config,
        a2a_faults: Arc<dyn crate::a2a::IngressFaultInjector>,
    ) -> Result<Self, AppError> {
        prepare_directories(&config).map_err(AppError::Health)?;
        let maintenance_lock = Arc::new(
            crate::offline_production_migration::acquire_runtime_lock(&config.bridge.database)
                .map_err(|_| AppError::Runtime)?,
        );
        let pool =
            crate::storage::connect_for_runtime(&config.bridge.database, maintenance_lock.clone())
                .await?;
        if let Err(error) = migrate(&pool).await {
            pool.close().await;
            return Err(error.into());
        }
        let reliability = crate::storage::ReliabilityStore::with_owner_evidence_window(
            pool.clone(),
            config.bridge.owner_evidence_max_age_seconds as i64,
        )
        .map_err(|_| AppError::Runtime)?;
        let admission_guard =
            crate::runtime_admission::Guard::acquire(&config.bridge.database, &path, "startup")
                .map_err(|_| AppError::Runtime)?;
        let runtime_instance = uuid::Uuid::now_v7().to_string();
        let process_fingerprint =
            crate::storage::RuntimeOwnerEvidence::fingerprint().ok_or(AppError::Runtime)?;
        let startup_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let evidence = crate::storage::RuntimeOwnerEvidence::capture().ok_or(AppError::Runtime)?;
        let mut owns_runtime = reliability
            .begin_runtime_with_evidence(
                &runtime_instance,
                &process_fingerprint,
                &startup_at,
                &evidence,
            )
            .await
            .map_err(|_| AppError::Runtime)?;
        if !owns_runtime {
            // A prior isolated generation may have stopped before publishing its
            // terminal runtime row. Reuse the reviewed CAS recovery boundary for
            // each durable owner identity, then retry the fresh generation.
            let stale = sqlx::query(
                "SELECT instance_token,process_fingerprint FROM runtime_instances WHERE state IN ('active','stopping')",
            )
            .fetch_all(&pool)
            .await
            .map_err(|_| AppError::Runtime)?;
            let mut proven_stale = false;
            let mut blocked = false;
            let mut live_count = 0usize;
            let mut unknown_count = 0usize;
            let mut stale_count = 0usize;
            for row in stale {
                let token: String =
                    sqlx::Row::try_get(&row, "instance_token").map_err(|_| AppError::Runtime)?;
                let fingerprint: String = sqlx::Row::try_get(&row, "process_fingerprint")
                    .map_err(|_| AppError::Runtime)?;
                match owner_fingerprint_liveness(&fingerprint) {
                    OwnerLiveness::Live => {
                        live_count += 1;
                        blocked = true;
                    }
                    OwnerLiveness::Unknown => {
                        unknown_count += 1;
                        blocked = true;
                    }
                    OwnerLiveness::Stale => {
                        stale_count += 1;
                        proven_stale = true;
                        if crate::offline_recovery::recover_proven_stale_runtime(
                            &config.bridge.database,
                            &token,
                            &fingerprint,
                            &startup_at,
                        )
                        .await
                        .is_err()
                        {
                            blocked = true;
                        }
                    }
                }
            }
            tracing::info!(
                category = "t050-recovery",
                owner_live = live_count,
                owner_unknown = unknown_count,
                owner_stale = stale_count,
                "owner liveness classification"
            );
            if proven_stale && !blocked {
                owns_runtime = reliability
                    .begin_runtime_with_evidence(
                        &runtime_instance,
                        &process_fingerprint,
                        &startup_at,
                        &evidence,
                    )
                    .await
                    .map_err(|_| AppError::Runtime)?;
            }
        }
        admission_guard
            .revalidate()
            .map_err(|_| AppError::Runtime)?;
        drop(admission_guard);
        match Self::build(
            path,
            config,
            pool.clone(),
            reliability.clone(),
            runtime_instance.clone(),
            owns_runtime,
            maintenance_lock.clone(),
            a2a_faults,
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

    #[allow(clippy::too_many_arguments)]
    async fn build(
        path: PathBuf,
        config: Config,
        pool: SqlitePool,
        reliability: crate::storage::ReliabilityStore,
        runtime_instance: String,
        owns_runtime: bool,
        maintenance_lock: Arc<crate::offline_production_migration::RuntimeLock>,
        a2a_faults: Arc<dyn crate::a2a::IngressFaultInjector>,
    ) -> Result<Self, AppError> {
        let repository = Arc::new(SqliteRepository::new(pool.clone()));
        let repository_trait: Arc<dyn Repository> = repository.clone();
        let sessions = SqliteSessionStore::new(pool.clone());
        let runtime_store = SqliteRuntimeStore::new(pool.clone());
        let metrics = Arc::new(RuntimeMetrics::default());
        let projection_metrics = Arc::new(ProjectionMetrics::default());
        let health_state = Arc::new(HealthState::new(
            runtime_store.clone(),
            Arc::clone(&metrics),
            Arc::clone(&projection_metrics),
        ));
        tracing::info!(
            category = "startup-recovery",
            phase = "begin",
            emission = "startup-recovery-begin"
        );
        let output_policy = crate::acp::output_policy::DurableOutputPolicy::derive(
            &config.bridge.database,
            config.runtime.max_output_bytes,
        )
        .map_err(|_| AppError::Assembly("durable output policy failed"))?;
        let output_store =
            crate::storage::OutputSpoolStore::new(pool.clone(), output_policy.process_quota_bytes)
                .map_err(|_| AppError::Assembly("durable output store failed"))?;
        let output_spool =
            crate::acp::output::DurableOutputSpool::new(output_policy, output_store.clone());
        output_spool
            .prepare_root()
            .map_err(|_| AppError::Assembly("durable output root failed"))?;
        output_spool
            .recover_assembling(&runtime_instance)
            .await
            .map_err(|_| AppError::Assembly("durable output recovery failed"))?;
        tracing::info!(
            category = "startup-recovery",
            phase = "output-assembling",
            emission = "startup-recovery-output-assembling"
        );
        let recovery_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        output_spool
            .recover_cleanup(&recovery_at)
            .await
            .map_err(|_| AppError::Assembly("durable output cleanup recovery failed"))?;
        output_store
            .verify_quota()
            .await
            .map_err(|_| AppError::Assembly("durable output quota mismatch"))?;
        tracing::info!(
            category = "startup-recovery",
            phase = "output-complete",
            emission = "startup-recovery-output-complete"
        );
        let registry = Arc::new(EndpointRegistry::from_config(&config));
        sync_agents(repository_trait.as_ref(), &registry).await?;
        tracing::info!(
            category = "startup-recovery",
            phase = "agents-synced",
            emission = "startup-recovery-agents-synced"
        );

        // T022 durable queue recovery is part of startup, before any worker is
        // assembled. Ambiguous send-start rows become recovery_needed; only
        // pre-send rows are returned to queued by the storage CAS.
        reliability
            .recover_orphaned_queues(&recovery_at)
            .await
            .map_err(|_| AppError::Runtime)?;
        tracing::info!(
            category = "startup-recovery",
            phase = "queues-recovered",
            emission = "startup-recovery-queues-recovered"
        );

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
        let recovery_diagnostic =
            RecoveryDiagnostic::classify(RecoveryFixture::from_runtime_facts(
                owns_runtime,
                admission_recovery.blocked,
                &recovery,
                &runtime_recovery,
                &counts,
            ));
        tracing::info!(category = "t050-recovery", emission = %recovery_diagnostic.line());
        eprintln!("{}", recovery_diagnostic.line());
        let recovery_blocked = recovery_diagnostic.blocker.is_some();
        tracing::info!(
            category = "startup-recovery",
            phase = "classified",
            blocked = recovery_blocked,
            emission = "startup-recovery-classified"
        );

        let reload = Arc::new(ReloadController::new(
            config.clone(),
            Arc::new(FileConfigSource::new(path)),
        )?);
        metrics.set_gauges(counts);
        let matrix_generation = config
            .transports
            .matrix
            .enabled
            .then(|| health_state.matrix().begin());
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
        health_state.set_recovery_diagnostic(recovery_diagnostic.line());
        let first_failure = Arc::new(Mutex::new(None));
        let mut runtime = Self {
            _maintenance_lock: Some(maintenance_lock),
            pool: Some(pool.clone()),
            reload,
            health_state,
            health: None,
            matrix_sync: None,
            ingress: None,
            bus: None,
            worker: None,
            broadcaster: None,
            heartbeat: None,
            outbox: None,
            output_drain: None,
            acp: None,
            a2a: None,
            a2a_cleanup: None,
            gateway: None,
            gateway_cleanup: None,
            shutdown_timeout: Duration::from_secs(config.bridge.shutdown_timeout_seconds),
            reliability: reliability.clone(),
            runtime_instance: owns_runtime.then_some(runtime_instance.clone()),
            first_failure: Arc::clone(&first_failure),
        };
        if owns_runtime {
            let heartbeat_store = reliability.clone();
            let heartbeat_token = runtime_instance.clone();
            let heartbeat_interval =
                Duration::from_millis(config.bridge.owner_evidence_max_age_seconds * 1_000 / 3);
            runtime.heartbeat = Some(tokio::spawn(async move {
                let mut tick = tokio::time::interval(heartbeat_interval);
                loop {
                    tick.tick().await;
                    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
                    let evidence = crate::storage::RuntimeOwnerEvidence::capture().unwrap_or(
                        crate::storage::RuntimeOwnerEvidence {
                            pid: 0,
                            proc_starttime: 0,
                            boot_id: String::new(),
                            boottime_ns: 0,
                        },
                    );
                    let heartbeat_fingerprint =
                        crate::storage::RuntimeOwnerEvidence::fingerprint().unwrap_or_default();
                    if !heartbeat_store
                        .publish_owner_heartbeat(
                            &heartbeat_token,
                            &heartbeat_fingerprint,
                            &now,
                            &evidence,
                        )
                        .await
                        .unwrap_or(false)
                    {
                        break;
                    }
                }
            }));
        }
        // Bind the loopback health listener before Matrix restore/proof.  Health
        // remains false until the existing state predicates transition, but the
        // endpoint is observable while startup is blocked on external/runtime
        // prerequisites.
        if !recovery_allows_service_assembly(recovery_blocked) {
            if let Some(generation) = matrix_generation {
                runtime.health_state.matrix().transition(
                    generation,
                    &[MatrixPhase::Preflight],
                    MatrixPhase::RecoveryBlocked,
                );
            }
            tracing::warn!(
                category = "recovery-blocked",
                action = "startup-terminated",
                emission = "recovery-blocked-startup-terminated"
            );
            return Err(AppError::Runtime);
        }

        // Only a recovery-safe runtime may expose the health listener.  The
        // recovery-blocked path above terminates before any listener, Matrix
        // ingress, ACP dispatcher, or worker can be assembled.
        runtime.health = start_health(config.bridge.health_bind, &runtime.health_state).await?;
        tracing::info!(
            category = "health-listener",
            phase = "bound",
            emission = "health-listener-bound"
        );

        // Matrix authentication, store binding and fresh device-key proof are the
        // assembly gate. No ACP, bus, gateway, outbox or task owner exists before it.
        let matrix_client = if let Some(generation) = matrix_generation {
            let identity_health = Arc::clone(&runtime.health_state);
            let client =
                match MatrixClient::restore_with_identity(&config.transports.matrix, || {
                    identity_health.matrix().transition(
                        generation,
                        &[MatrixPhase::Preflight],
                        MatrixPhase::IdentityMatched,
                    );
                })
                .await
                {
                    Ok(client) => client,
                    Err(error) => {
                        runtime.health_state.matrix().transition(
                            generation,
                            &[MatrixPhase::Preflight, MatrixPhase::IdentityMatched],
                            MatrixPhase::Failed(matrix_failure(&error)),
                        );
                        tracing::warn!(
                            category = matrix_failure(&error).as_log_str(),
                            "Matrix startup is health-only"
                        );
                        runtime.health_state.set_owner(OwnerState::Running);
                        return Ok(runtime);
                    }
                };
            runtime.health_state.matrix().transition(
                generation,
                &[MatrixPhase::IdentityMatched],
                MatrixPhase::StoreBound,
            );
            let proof =
                tokio::time::timeout(Duration::from_secs(30), client.initialize_and_prove()).await;
            let (initial_response, proof_error) = match proof {
                Ok(Ok(response)) => (Some(response), None),
                Ok(Err(error)) => (None, Some(matrix_failure(&error))),
                Err(_) => (None, Some(MatrixFailure::Timeout)),
            };
            if let Some(reason) = proof_error {
                runtime.health_state.matrix().transition(
                    generation,
                    &[MatrixPhase::StoreBound],
                    MatrixPhase::Failed(reason),
                );
                tracing::warn!(
                    category = reason.as_log_str(),
                    "Matrix startup is health-only"
                );
                runtime.health_state.set_owner(OwnerState::Running);
                return Ok(runtime);
            }
            runtime.health_state.matrix().transition(
                generation,
                &[MatrixPhase::StoreBound],
                MatrixPhase::KeyProved,
            );
            Some((
                client,
                initial_response.expect("successful Matrix proof has initial response"),
            ))
        } else {
            None
        };

        let mut endpoint_dispatchers = HashMap::new();
        let spawn_gate = crate::acp::AcpSpawnGate::for_generation(&runtime_instance);
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
                .legacy_results(config.runtime.legacy_acp_results)
                .durable_output(output_spool.clone(), runtime_instance.clone())
                .allow_nonterminal_end_turn(config.runtime.allow_nonterminal_end_turn)
                .spawn_gate(spawn_gate.clone())
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
        let acp = Arc::new(AcpDispatcherRouter::with_gate(
            endpoint_dispatchers,
            spawn_gate,
        ));
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
        let mut siblings: Vec<Arc<dyn EventConsumer>> = if config.transports.a2a.enabled {
            vec![Arc::new(crate::a2a::A2aTerminalProjection::new(
                A2aStore::new(pool.clone()),
            ))]
        } else {
            Vec::new()
        };
        if config.transports.matrix.enabled {
            siblings.push(Arc::new(
                crate::app::output_projection::DurableOutputProjection::new(
                    output_store.clone(),
                    output_spool.clone(),
                    runtime_instance.clone(),
                ),
            ));
        }

        let matrix = if let Some((client, initial_response)) = matrix_client {
            let sdk = Arc::new(SdkMatrixSender::new(client.clone()));
            let failure_slot = Arc::clone(&first_failure);
            let health_for_failure = Arc::clone(&runtime.health_state);
            let matrix_failure_generation =
                matrix_generation.expect("enabled Matrix has a generation");
            let mut sync = MatrixSync::new(
                client,
                Arc::new(MemorySyncTokenStore::default()),
                config.transports.matrix.sync_capacity,
            )
            .map(|sync| sync.with_initial_response(initial_response))
            .map_err(|_| AppError::Matrix)?
            .with_failure_observer(matrix_failure_observer(
                failure_slot,
                health_for_failure,
                matrix_failure_generation,
            ))
            .with_missing_key_observer(runtime.health_state.clone());
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

        if let Some((sdk, sync, gateway)) = matrix {
            let generation = matrix_generation.expect("enabled Matrix has a generation");
            let matrix_sender: Arc<dyn crate::matrix::MatrixSender> = sdk.clone();
            runtime.gateway = gateway;
            if runtime.gateway.is_some() {
                runtime.gateway_cleanup = Some(crate::gateway::GatewayCleanupHandle::start(
                    GatewayStore::new(pool.clone()),
                    runtime_instance.clone(),
                ));
            }
            let outbox_sender: Arc<dyn crate::matrix::MatrixOutboxSender> = sdk.clone();
            let durable_sender: Arc<dyn crate::matrix::DurableMatrixSender> = sdk.clone();
            let outbox = crate::app::OutboxDrain::new(reliability.clone(), outbox_sender, 64);
            let outbox_owner = outbox.clone().start(Duration::from_secs(1));
            let output_owner = crate::app::output_drain::OutputSetDrain::new(
                output_store.clone(),
                output_spool.clone(),
                durable_sender,
            )
            .start(Duration::from_secs(1));
            let replies = Arc::new(ReplyRegistry::default());

            runtime.health_state.matrix().transition(
                generation,
                &[MatrixPhase::KeyProved],
                MatrixPhase::Registering,
            );
            let (sync_handle, receiver, dispositions) = sync.start_with_dispositions();
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
            .with_dispositions(dispositions)
            .with_queue_control_store(Arc::new(reliability.clone()))
            .with_reap_control(acp.clone())
            .with_retry_admission(retry)
            .start();
            runtime
                .health_state
                .register_matrix_required(sync_handle.alive());
            runtime
                .health_state
                .register_matrix_required(ingress.alive());
            runtime.health_state.matrix().transition(
                generation,
                &[MatrixPhase::Registering],
                MatrixPhase::Ready,
            );
            runtime.matrix_sync = Some(sync_handle);
            runtime.ingress = Some(ingress);
            runtime.outbox = Some(outbox_owner);
            runtime.output_drain = Some(output_owner);
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
        let worker_failure = Arc::clone(&first_failure);
        runtime.worker = Some(spawn_task_worker(
            async move { worker.run().await },
            worker_alive,
            worker_failure,
        ));
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
                    crate::a2a::assembly::ServerDependencies {
                        pool: pool.clone(),
                        repository: repository_trait.clone(),
                        concrete: repository.as_ref().clone(),
                        bus: durable_bus,
                        cancellation,
                        registry: Arc::clone(&registry),
                        faults: a2a_faults,
                    },
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

    pub fn a2a_addr(&self) -> Option<std::net::SocketAddr> {
        self.a2a.as_ref().and_then(A2aServerHandle::local_addr)
    }

    pub fn a2a_listener_inode(&self) -> Option<u64> {
        self.a2a.as_ref().and_then(A2aServerHandle::listener_inode)
    }

    pub async fn shutdown(mut self) -> Result<(), AppError> {
        self.health_state.matrix().stop();
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
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.abort();
            let _ = heartbeat.await;
        }
        if let Some(sync) = self.matrix_sync.take()
            && let Err(error) = sync.shutdown_bounded(self.shutdown_timeout).await
        {
            retain_failure(
                &self.first_failure,
                RequiredOwnerFailure {
                    component: "matrix-sync",
                    class: matrix_error_class(&error),
                },
            );
        }
        if let Some(ingress) = self.ingress.take() {
            ingress.shutdown().await;
        }
        // Close the external listener before draining internal event ownership so
        // no new ingress can keep the broadcaster alive during bounded shutdown.
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
        self.bus.take();
        if let Some(mut worker) = self.worker.take() {
            match tokio::time::timeout(self.shutdown_timeout, &mut worker).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(_))) => {
                    shutdown_error = Some(AppError::Assembly("task-worker/failed"));
                }
                Ok(Err(_)) => {
                    shutdown_error = Some(AppError::Assembly("task-worker/join"));
                }
                Err(_) => {
                    worker.abort();
                    let _ = worker.await;
                    shutdown_error = Some(AppError::Assembly("task-worker/timeout"));
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
        let mut acp_shutdown = None;
        if let Some(acp) = self.acp.take() {
            acp_shutdown = Some(acp.shutdown_observation().await);
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
        if let Some(output) = self.output_drain.take()
            && output.shutdown().await.is_err()
        {
            shutdown_error.get_or_insert(AppError::Assembly("durable output owner join failed"));
        }
        self.gateway.take();
        if let Some(health) = self.health.take()
            && let Err(error) = health.shutdown().await
        {
            shutdown_error.get_or_insert(AppError::Health(error));
        }
        self.health_state.set_owner(OwnerState::Stopped);
        if let Some(token) = self.runtime_instance.take() {
            let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
            let evidence = crate::storage::RuntimeOwnerEvidence::capture();
            let final_owner = evidence.as_ref().zip(acp_shutdown.as_ref());
            let result = self
                .reliability
                .finish_runtime_stop(&token, &now, final_owner)
                .await;
            match result {
                Ok(true) => {}
                Ok(false) if shutdown_error.is_none() => shutdown_error = Some(AppError::Runtime),
                Err(_) if shutdown_error.is_none() => shutdown_error = Some(AppError::Runtime),
                Ok(false) | Err(_) => {}
            }
        }
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        self._maintenance_lock.take();
        self.health_state.matrix().stopped();
        if let Some(failure) = *self
            .first_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            return Err(AppError::RequiredOwner {
                component: failure.component,
                class: failure.class,
            });
        }
        match shutdown_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn matrix_error_class(error: &crate::matrix::MatrixError) -> &'static str {
    use crate::matrix::MatrixError;
    match error {
        MatrixError::Storage => "storage",
        MatrixError::Backpressure => "backpressure",
        MatrixError::ConsumerClosed => "consumer-closed",
        MatrixError::Protocol { .. } => "protocol",
        MatrixError::Transport => "transport",
        MatrixError::DeviceKicked | MatrixError::Authentication => "authentication",
        MatrixError::CryptoInitialization => "decryption",
        MatrixError::Join => "join",
        _ => "configuration",
    }
}

fn matrix_runtime_failure(error: &crate::matrix::MatrixError) -> MatrixFailure {
    use crate::matrix::MatrixError;
    match error {
        MatrixError::Storage => MatrixFailure::SyncStorage,
        MatrixError::Backpressure => MatrixFailure::SyncBackpressure,
        MatrixError::ConsumerClosed => MatrixFailure::SyncConsumerClosed,
        MatrixError::Protocol { .. } => MatrixFailure::SyncProtocol,
        MatrixError::CryptoInitialization => MatrixFailure::SyncDecryption,
        MatrixError::Transport => MatrixFailure::SyncTransport,
        _ => MatrixFailure::RequiredTaskExited,
    }
}

fn matrix_failure(error: &crate::matrix::MatrixError) -> MatrixFailure {
    use crate::matrix::MatrixError;
    match error {
        MatrixError::Authentication | MatrixError::DeviceKicked => MatrixFailure::Authentication,
        MatrixError::UserMismatch => MatrixFailure::UserMismatch,
        MatrixError::DeviceMissing => MatrixFailure::DeviceMissing,
        MatrixError::DeviceMismatch => MatrixFailure::DeviceMismatch,
        MatrixError::UnsafeStorePath | MatrixError::Configuration => MatrixFailure::UnsafeStorePath,
        MatrixError::UnsafeStoreAncestor => MatrixFailure::UnsafeStoreAncestor,
        MatrixError::StoreCorrupt => MatrixFailure::StoreCorrupt,
        MatrixError::StoreBindingMismatch => MatrixFailure::StoreBindingMismatch,
        MatrixError::DeviceKeyUpload => MatrixFailure::DeviceKeyUpload,
        _ => MatrixFailure::CryptoInitialization,
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
    let started_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let timer = Instant::now();
    tracing::info!(category = "health-listener", phase = "start", %started_at, emission = "health-listener-start");
    match address {
        Some(address) => match HealthServer::start(address, Arc::clone(state)).await {
            Ok(server) => {
                state.set_listener_available(true);
                tracing::info!(category = "health-listener", phase = "bind", %started_at, elapsed_ms = timer.elapsed().as_millis() as u64, emission = "health-listener-bind-ok");
                Ok(Some(server))
            }
            Err(error) => {
                tracing::warn!(category = "health-listener", phase = "bind", %started_at, elapsed_ms = timer.elapsed().as_millis() as u64, emission = "health-listener-bind-failed");
                Err(AppError::Health(error))
            }
        },
        None => {
            tracing::info!(category = "health-listener", phase = "disabled", %started_at, emission = "health-listener-disabled");
            Ok(None)
        }
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

#[cfg(test)]
mod assembly_tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path_regex},
    };

    use super::{AppError, AppRuntime, MatrixPhase, matrix_failure_observer, spawn_task_worker};
    use crate::{
        bus::WorkerError,
        config::load_from_str_with_env,
        matrix::{MatrixClient, MatrixError, MatrixSync, MemorySyncTokenStore},
        models::TaskId,
        storage::{ReliabilityStore, connect, migrate},
    };

    #[derive(Clone, Copy)]
    enum FailureCase {
        Authentication,
        StoreBinding,
        KeyProof,
    }

    async fn assert_matrix_failure_is_health_only(case: FailureCase) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(r"/_matrix/client/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "versions": ["v1.1"], "unstable_features": {}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex(r"/_matrix/client/.*/account/whoami"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(5))
                    .set_body_json(json!({
                        "user_id": if matches!(case, FailureCase::Authentication) {
                            "@other:example.test"
                        } else {
                            "@bridge:example.test"
                        },
                        "device_id": "BRIDGE",
                        "is_guest": false
                    })),
            )
            .mount(&server)
            .await;
        if matches!(case, FailureCase::KeyProof) {
            Mock::given(method("GET"))
                .and(path_regex(r"/_matrix/client/.*/sync"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "next_batch": "proof",
                    "rooms": {"join": {}, "invite": {}, "leave": {}, "knock": {}},
                    "presence": {"events": []}, "account_data": {"events": []},
                    "to_device": {"events": []},
                    "device_lists": {"changed": [], "left": []},
                    "device_one_time_keys_count": {}
                })))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path_regex(r"/_matrix/client/.*/keys/upload"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(json!({"one_time_key_counts": {}})),
                )
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path_regex(r"/_matrix/client/.*/keys/query"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(json!({"failures": {}, "device_keys": {}})),
                )
                .mount(&server)
                .await;
        }

        let private_root = std::path::PathBuf::from(std::env::var_os("HOME").unwrap())
            .join(".guigu-agent-bridge-tests");
        std::fs::create_dir_all(&private_root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&private_root, std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        let root = private_root.join(format!("t029-{}", uuid::Uuid::now_v7()));
        let sessions = root.join("sessions");
        let store = root.join("matrix");
        std::fs::create_dir_all(&sessions).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::set_permissions(&sessions, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        if matches!(case, FailureCase::StoreBinding) {
            std::fs::create_dir(&store).unwrap();
            std::fs::write(store.join("unbound-store"), b"preserve").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&store, std::fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
        let database = root.join("state.db");
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let health_port = probe.local_addr().unwrap().port();
        drop(probe);
        let config_path = root.join("bridge.toml");
        let text = format!(
            r#"
[bridge]
database = {database:?}
session_root = {sessions:?}
health_bind = "127.0.0.1:{health_port}"

[transports.matrix]
enabled = true
homeserver = {homeserver:?}
user_id = "@bridge:example.test"
device_id = "BRIDGE"
access_token = "{{env:MATRIX_TOKEN}}"
crypto_store_path = {store:?}
device_trusted = true

[agents.worker]
transport = "acp"
command = "must-not-start"
workspace = {root:?}
enabled = true
"#,
            database = database.to_string_lossy(),
            sessions = sessions.to_string_lossy(),
            homeserver = server.uri(),
            store = store.to_string_lossy(),
            root = root.to_string_lossy(),
            health_port = health_port,
        );
        std::fs::write(&config_path, &text).unwrap();
        let config = load_from_str_with_env(
            &text,
            &BTreeMap::from([
                ("HOME".to_owned(), root.to_string_lossy().into_owned()),
                ("MATRIX_TOKEN".to_owned(), "planted-token".to_owned()),
            ]),
        )
        .unwrap();
        let pool = connect(&database).await.unwrap();
        migrate(&pool).await.unwrap();
        let reliability = ReliabilityStore::new(pool.clone());
        let instance = uuid::Uuid::now_v7().to_string();
        assert!(
            reliability
                .begin_runtime(&instance, &instance, "2026-09-23T00:00:00Z")
                .await
                .unwrap()
        );

        let build = AppRuntime::build(
            config_path,
            config,
            pool,
            reliability,
            instance,
            true,
            Arc::new(crate::offline_production_migration::acquire_runtime_lock(&database).unwrap()),
            Arc::new(crate::a2a::NoFault),
        );
        tokio::pin!(build);
        let address = format!("127.0.0.1:{health_port}");
        let mut listener_observed = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        let runtime = loop {
            tokio::select! {
                result = &mut build => break result.unwrap(),
                _ = tokio::time::sleep(Duration::from_millis(25)) => {
                    if tokio::net::TcpStream::connect(&address).await.is_ok() {
                        listener_observed = true;
                        break build.await.unwrap();
                    }
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "health listener did not start before Matrix proof"
            );
        };
        assert!(
            listener_observed,
            "Matrix proof completed before listener probe"
        );
        assert!(runtime.bus.is_none());
        assert!(runtime.worker.is_none());
        assert!(runtime.broadcaster.is_none());
        assert!(runtime.outbox.is_none());
        assert!(runtime.acp.is_none());
        assert!(runtime.a2a.is_none());
        assert!(runtime.a2a_cleanup.is_none());
        assert!(runtime.gateway.is_none());
        assert!(runtime.gateway_cleanup.is_none());
        assert!(runtime.matrix_sync.is_none());
        assert!(runtime.ingress.is_none());
        let snapshot = runtime.health_state.snapshot().await;
        let expected = match case {
            FailureCase::Authentication => "matrix-user-mismatch",
            FailureCase::StoreBinding => "matrix-store-binding-mismatch",
            FailureCase::KeyProof => "matrix-device-key-upload-failed",
        };
        assert_eq!(snapshot.diagnostic, Some(expected));
        let predicate = runtime
            .health_state
            .redacted_predicate_snapshot()
            .expect("matrix predicate snapshot");
        assert!(predicate.listener_available);
        assert!(!predicate.recovery_blocked);
        assert_eq!(predicate.matrix_category, "failed");
        assert!(!predicate.ready);
        let debug = format!("{predicate:?}");
        for forbidden in ["fixture", "token", "room", "device", "message", "task"] {
            assert!(!debug.to_ascii_lowercase().contains(forbidden));
        }

        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");

        runtime.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn matrix_failures_are_health_only_before_work_owner_assembly() {
        for case in [
            FailureCase::Authentication,
            FailureCase::StoreBinding,
            FailureCase::KeyProof,
        ] {
            assert_matrix_failure_is_health_only(case).await;
        }
    }

    async fn runtime_fixture() -> (AppRuntime, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("t044-runtime-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let config = root.join("bridge.toml");
        std::fs::write(
            &config,
            format!(
                "[bridge]\ndatabase = {:?}\nsession_root = {:?}\nhealth_bind = \"127.0.0.1:0\"\nshutdown_timeout_seconds = 1\n",
                root.join("state.db").to_string_lossy(),
                root.join("sessions").to_string_lossy(),
            ),
        )
        .unwrap();
        (AppRuntime::start(config).await.unwrap(), root)
    }

    async fn replace_worker_with_failure(runtime: &mut AppRuntime) {
        if let Some(worker) = runtime.worker.take() {
            worker.abort();
            let _ = worker.await;
        }
        let slot = std::sync::Arc::clone(&runtime.first_failure);
        let alive = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let observed_alive = std::sync::Arc::clone(&alive);
        runtime.worker = Some(spawn_task_worker(
            async move {
                Err(WorkerError::EventStreamClosed {
                    task_id: TaskId::generate(),
                    seq: 1,
                })
            },
            alive,
            slot,
        ));
        while observed_alive.load(std::sync::atomic::Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    }

    async fn install_failing_matrix_owner(runtime: &mut AppRuntime, generation: u64) {
        let sync = MatrixSync::new(
            MatrixClient::for_test().await,
            std::sync::Arc::new(MemorySyncTokenStore::default()),
            1,
        )
        .unwrap()
        .with_failure_observer(matrix_failure_observer(
            std::sync::Arc::clone(&runtime.first_failure),
            std::sync::Arc::clone(&runtime.health_state),
            generation,
        ))
        .with_test_failure(MatrixError::Storage);
        let (handle, _events) = sync.start();
        let alive = handle.alive();
        runtime.matrix_sync = Some(handle);
        while alive.load(std::sync::atomic::Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    }

    fn mark_matrix_ready(runtime: &AppRuntime) -> u64 {
        let generation = runtime.health_state.matrix().begin();
        for (from, to) in [
            (MatrixPhase::Preflight, MatrixPhase::IdentityMatched),
            (MatrixPhase::IdentityMatched, MatrixPhase::StoreBound),
            (MatrixPhase::StoreBound, MatrixPhase::KeyProved),
            (MatrixPhase::KeyProved, MatrixPhase::Registering),
            (MatrixPhase::Registering, MatrixPhase::Ready),
        ] {
            assert!(
                runtime
                    .health_state
                    .matrix()
                    .transition(generation, &[from], to)
            );
        }
        generation
    }

    #[tokio::test]
    async fn runtime_shutdown_retains_matrix_failure_before_worker_failure() {
        let (mut runtime, root) = runtime_fixture().await;
        let generation = mark_matrix_ready(&runtime);
        install_failing_matrix_owner(&mut runtime, generation).await;
        assert_eq!(
            runtime.health_state.snapshot().await.diagnostic,
            Some("matrix-sync-storage")
        );
        replace_worker_with_failure(&mut runtime).await;
        assert!(matches!(
            runtime.shutdown().await,
            Err(AppError::RequiredOwner {
                component: "matrix-sync",
                class: "storage"
            })
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn healthy_readiness_snapshot_is_redacted_and_ready() {
        let (runtime, root) = runtime_fixture().await;
        let snapshot = runtime.health_state.snapshot().await;
        assert_eq!(snapshot.readiness, crate::runtime::Readiness::Ready);
        let predicate = runtime
            .health_state
            .redacted_predicate_snapshot()
            .expect("healthy predicate snapshot");
        assert!(predicate.ready);
        assert!(predicate.worker_ready);
        assert_eq!(predicate.matrix_category, "disabled");
        let debug = format!("{predicate:?}");
        assert!(!debug.contains("token"));
        runtime.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recovery_blocked_snapshot_is_redacted_and_not_ready() {
        let (runtime, root) = runtime_fixture().await;
        runtime.health_state.set_recovery_blocked(true);
        let snapshot = runtime.health_state.snapshot().await;
        assert_eq!(
            snapshot.readiness,
            crate::runtime::Readiness::RecoveryBlocked
        );
        let predicate = runtime
            .health_state
            .redacted_predicate_snapshot()
            .expect("recovery predicate snapshot");
        assert!(predicate.listener_available);
        assert!(predicate.recovery_blocked);
        assert!(!predicate.ready);
        assert!(predicate.adapter_ready);
        assert!(predicate.worker_ready);
        let debug = format!("{predicate:?}").to_ascii_lowercase();
        for forbidden in ["token", "room", "device", "message", "task", "fixture"] {
            assert!(!debug.contains(forbidden), "forbidden field: {forbidden}");
        }
        runtime.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recovery_blocked_startup_gate_terminates_before_listener_ingress_or_acp() {
        assert!(!super::recovery_allows_service_assembly(true));
        assert!(super::recovery_allows_service_assembly(false));
    }

    #[tokio::test]
    async fn runtime_shutdown_retains_task_worker_failure_before_matrix_failure() {
        let (mut runtime, root) = runtime_fixture().await;
        replace_worker_with_failure(&mut runtime).await;
        let generation = mark_matrix_ready(&runtime);
        install_failing_matrix_owner(&mut runtime, generation).await;
        assert!(matches!(
            runtime.shutdown().await,
            Err(AppError::RequiredOwner {
                component: "task-worker",
                class: "failed"
            })
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recovery_blocked_startup_fixture_terminates_before_any_service_creation() {
        let root = std::env::temp_dir().join(format!("t083-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let database = root.join("state.db");
        let sessions = root.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let health_port = probe.local_addr().unwrap().port();
        drop(probe);
        let config_text = format!(
            "[bridge]\ndatabase = {:?}\nsession_root = {:?}\nhealth_bind = \"127.0.0.1:{}\"\n\n[transports.matrix]\nenabled = false\n\n[agents.worker]\ntransport = \"acp\"\ncommand = \"must-not-start\"\nworkspace = {:?}\nenabled = true\n",
            database, sessions, health_port, root
        );
        let config = load_from_str_with_env(&config_text, &BTreeMap::new()).unwrap();
        let pool = connect(&database).await.unwrap();
        migrate(&pool).await.unwrap();
        let conversation = "00000000-0000-7000-8000-000000000001";
        let task = "00000000-0000-7000-8000-000000000002";
        let worker_endpoint = crate::bus::derive_endpoint_id("worker");
        let delivery = "00000000-0000-7000-8000-000000000003";
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
            .bind(conversation)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES(?,'worker','acp',1,NULL,'[]')")
            .bind(worker_endpoint.to_string()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,deadline,version) VALUES(?,?,?, ?,?,'fixture',1,0,0,NULL,0)")
            .bind(task).bind(task).bind("sender").bind("worker").bind(conversation)
            .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2026-10-18T00:00:00Z')")
            .bind(delivery).bind(task).bind(worker_endpoint.to_string()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,1,'active','2026-10-18T00:00:00Z','2026-10-18T00:00:00Z','2999-01-01T00:00:00Z')")
            .bind("fixture-resource").bind(task).bind("fixture-owner")
            .execute(&pool).await.unwrap();
        let reliability = ReliabilityStore::new(pool.clone());
        let runtime_instance = uuid::Uuid::now_v7().to_string();
        reliability
            .begin_runtime(&runtime_instance, "pid:fixture", "2026-10-18T00:00:00Z")
            .await
            .unwrap();
        let result = AppRuntime::build(
            root.join("bridge.toml"),
            config,
            pool.clone(),
            reliability,
            runtime_instance,
            true,
            Arc::new(crate::offline_production_migration::acquire_runtime_lock(&database).unwrap()),
            Arc::new(crate::a2a::NoFault),
        )
        .await;
        assert!(matches!(result, Err(AppError::Runtime)));
        assert!(
            tokio::net::TcpStream::connect(("127.0.0.1", health_port))
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), tokio::fs::read_dir(&sessions))
                .await
                .is_ok()
        );
        pool.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }
}
