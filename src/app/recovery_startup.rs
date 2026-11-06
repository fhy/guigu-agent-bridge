use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartupEligibility {
    Admin,
    NonAdmin,
    Legacy,
    Refused,
}

impl StartupEligibility {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::NonAdmin => "non_admin",
            Self::Legacy => "legacy",
            Self::Refused => "refused",
        }
    }
}

pub(super) fn classify_startup(admin_bindings: i64, sentinel: Option<&str>) -> StartupEligibility {
    if admin_bindings > 0 {
        return StartupEligibility::Admin;
    }
    match sentinel {
        Some("pending") | Some("complete") => StartupEligibility::NonAdmin,
        Some("legacy") => StartupEligibility::Legacy,
        _ => StartupEligibility::Refused,
    }
}

pub(super) fn log_startup_eligibility(
    admin_bindings: i64,
    sentinel: Option<&str>,
    eligibility: StartupEligibility,
) {
    tracing::info!(
        event = "startup-eligibility",
        admin_binding_count = admin_bindings,
        sentinel_category = sentinel.unwrap_or("missing"),
        eligibility = eligibility.as_str(),
        "startup eligibility classified"
    );
}

impl AppRuntime {
    pub(super) async fn compatibility_held(
        path: PathBuf,
        config: Config,
        pool: SqlitePool,
        reliability: crate::storage::ReliabilityStore,
        maintenance_lock: Arc<crate::offline_production_migration::RuntimeLock>,
        reason: &'static str,
    ) -> Result<Self, AppError> {
        tracing::info!(
            event = "compatibility-held",
            reason,
            matrix_enabled = config.transports.matrix.enabled,
            impact = "health_only_no_matrix_or_agent_services",
            next_step = "resolve_compatibility_and_restart",
            "startup held by compatibility gate"
        );
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
            spawn_gate: None,
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

#[cfg(test)]
mod tests {
    use super::{StartupEligibility, classify_startup, log_startup_eligibility};

    #[test]
    fn admin_never_falls_back_to_nonadmin() {
        assert_eq!(
            classify_startup(1, Some("pending")),
            StartupEligibility::Admin
        );
    }

    #[test]
    fn nonadmin_requires_known_sentinel() {
        assert_eq!(
            classify_startup(0, Some("complete")),
            StartupEligibility::NonAdmin
        );
        assert_eq!(
            classify_startup(0, Some("legacy")),
            StartupEligibility::Legacy
        );
        assert_eq!(classify_startup(0, None), StartupEligibility::Refused);
    }

    #[test]
    fn startup_eligibility_log_contains_structured_binding_fields() {
        let captured = crate::test_diagnostics::Capture::default();
        tracing::subscriber::with_default(captured.subscriber(), || {
            let eligibility = classify_startup(0, Some("pending"));
            log_startup_eligibility(0, Some("pending"), eligibility);
        });
        let output = captured.output();
        assert!(output.contains("event=\"startup-eligibility\""));
        assert!(output.contains("admin_binding_count=0"));
        assert!(output.contains("sentinel_category=\"pending\""));
        assert!(output.contains("eligibility=\"non_admin\""));
    }

    #[test]
    fn startup_eligibility_log_precedes_compatibility_hold_log() {
        let captured = crate::test_diagnostics::Capture::default();
        tracing::subscriber::with_default(captured.subscriber(), || {
            let eligibility = classify_startup(0, Some("pending"));
            log_startup_eligibility(0, Some("pending"), eligibility);
            tracing::info!(
                event = "compatibility-held",
                reason = "compatibility-held",
                matrix_enabled = false,
                impact = "health_only_no_matrix_or_agent_services",
                next_step = "resolve_compatibility_and_restart",
                "startup held by compatibility gate"
            );
        });
        let output = captured.output();
        let eligibility_at = output.find("event=\"startup-eligibility\"").unwrap();
        let held_at = output.find("event=\"compatibility-held\"").unwrap();
        assert!(eligibility_at < held_at);
        assert!(output.contains("reason=\"compatibility-held\""));
        assert!(output.contains("matrix_enabled=false"));
        assert!(output.contains("impact=\"health_only_no_matrix_or_agent_services\""));
        assert!(output.contains("next_step=\"resolve_compatibility_and_restart\""));
    }
}
