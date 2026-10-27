use super::*;

impl AppRuntime {
    pub(super) async fn compatibility_held(
        path: PathBuf,
        config: Config,
        pool: SqlitePool,
        reliability: crate::storage::ReliabilityStore,
        maintenance_lock: Arc<crate::offline_production_migration::RuntimeLock>,
        reason: &'static str,
    ) -> Result<Self, AppError> {
        let runtime_store = SqliteRuntimeStore::new(pool.clone());
        let health_state = Arc::new(HealthState::new(
            runtime_store,
            Arc::new(RuntimeMetrics::default()),
            Arc::new(ProjectionMetrics::default()),
        ));
        health_state.set_recovery_blocked(true);
        health_state.set_recovery_diagnostic(reason.to_owned());
        if config.transports.matrix.enabled {
            let generation = health_state.matrix().begin();
            health_state.matrix().mark_recovery_blocked();
            health_state.matrix().transition(
                generation,
                &[MatrixPhase::Preflight],
                MatrixPhase::RecoveryBlocked,
            );
        }
        let health = start_health(config.bridge.health_bind, &health_state).await?;
        health_state.set_owner(OwnerState::Running);
        let shutdown_timeout = Duration::from_secs(config.bridge.shutdown_timeout_seconds);
        Ok(Self {
            pool: Some(pool),
            reload: Arc::new(ReloadController::new(
                config,
                Arc::new(FileConfigSource::new(path)),
            )?),
            health_state,
            health,
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
            shutdown_timeout,
            reliability,
            runtime_instance: None,
            first_failure: Arc::new(Mutex::new(None)),
            _maintenance_lock: Some(maintenance_lock),
        })
    }
}
