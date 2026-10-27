use super::*;

impl HealthState {
    pub(super) async fn metrics_body(&self) -> String {
        let readiness = self.snapshot().await;
        let snapshot = self.metrics.snapshot();
        let mut body = String::new();
        for (metric, value) in snapshot.values {
            body.push_str(&format!(
                "guigu_runtime_total{{kind=\"{}\"}} {}\n",
                metric_name(metric),
                value
            ));
        }
        let gauges = snapshot.gauges;
        for (name, value) in [
            ("active_leases", gauges.active_leases),
            ("expired_leases", gauges.expired_leases),
            ("recovery_needed_leases", gauges.recovery_needed_leases),
            ("ready_turns", gauges.ready_turns),
            ("in_flight_turns", gauges.in_flight_turns),
            ("recovery_backlog", gauges.recovery_backlog),
            ("projection_failures", self.projections.failures()),
            ("a2a_insecure_peers", self.insecure_a2a_peers()),
            ("matrix_missing_room_keys", self.matrix.missing_room_keys()),
        ] {
            body.push_str(&format!("guigu_runtime_{name} {value}\n"));
        }
        self.append_readiness_metrics(&mut body, &readiness).await;
        body
    }

    async fn append_readiness_metrics(&self, body: &mut String, readiness: &HealthSnapshot) {
        let (required_total, required_alive) = self.liveness_counts(&self.required);
        let (matrix_required_total, matrix_required_alive) =
            self.liveness_counts(&self.matrix_required);
        body.push_str(&format!(
            "guigu_readiness_evaluation{{readiness=\"{}\"}} 1\n",
            readiness_name(readiness.readiness)
        ));
        let (storage_available, current_recovery) = self.append_current_metrics(body).await;
        body.push_str(&format!(
            "guigu_readiness_owner_state{{state=\"{}\"}} 1\n",
            owner_state_name(self.owner())
        ));
        body.push_str(&format!(
            "guigu_readiness_matrix_phase{{phase=\"{}\"}} 1\n",
            matrix_phase_name(self.matrix.phase())
        ));
        if let Some(reason) = matrix_phase_reason(self.matrix.phase()) {
            body.push_str(&format!(
                "guigu_readiness_matrix_phase_failure{{reason=\"{reason}\"}} 1\n"
            ));
        }
        body.push_str(&format!(
            "guigu_readiness_adapter_ready {}\n",
            u8::from(self.adapter_ready.load(Ordering::Acquire))
        ));
        body.push_str(&format!(
            "guigu_readiness_recovery_blocked {}\n",
            u8::from(self.recovery_blocked.load(Ordering::Acquire))
        ));
        body.push_str(&format!(
            "guigu_readiness_required_total {required_total}\n"
        ));
        body.push_str(&format!(
            "guigu_readiness_required_alive {required_alive}\n"
        ));
        body.push_str(&format!(
            "guigu_readiness_matrix_required_total {matrix_required_total}\n"
        ));
        body.push_str(&format!(
            "guigu_readiness_matrix_required_alive {matrix_required_alive}\n"
        ));
        self.append_startup_metrics(body);
        let blockers = readiness_blockers(ReadinessInputs {
            owner: self.owner(),
            adapter_ready: self.adapter_ready.load(Ordering::Acquire),
            recovery_blocked: self.recovery_blocked.load(Ordering::Acquire),
            required_total,
            required_alive,
            matrix_phase: self.matrix.phase(),
            storage_available,
            current_recovery,
        });
        for blocker in blockers {
            body.push_str(&format!(
                "guigu_readiness_blocker{{predicate=\"{blocker}\"}} 1\n"
            ));
        }
    }
    async fn append_current_metrics(&self, body: &mut String) -> (bool, bool) {
        let current_counts = self.runtime.counts().await;
        let (storage_available, current_recovery) = match &current_counts {
            Ok(counts) => {
                body.push_str(&format!(
                    "guigu_readiness_current_recovery_leases {}\n",
                    counts.recovery_needed_leases
                ));
                body.push_str(&format!(
                    "guigu_readiness_current_recovery_backlog {}\n",
                    counts.recovery_backlog
                ));
                (
                    true,
                    counts.recovery_needed_leases > 0 || counts.recovery_backlog > 0,
                )
            }
            Err(_) => {
                body.push_str("guigu_readiness_storage_available 0\n");
                (false, false)
            }
        };
        if storage_available {
            body.push_str("guigu_readiness_storage_available 1\n");
        }
        (storage_available, current_recovery)
    }

    fn append_startup_metrics(&self, body: &mut String) {
        let diagnostics = self.matrix.diagnostics();
        body.push_str(&format!(
            "guigu_readiness_matrix_startup{{phase=\"{}\",authentication=\"{}\",store_binding=\"{}\",session_restore=\"{}\",e2ee_initialization=\"{}\",server_device_key_proof=\"{}\"}} 1\n",
            matrix_phase_name(self.matrix.phase()),
            startup_attempt_name(diagnostics.authentication),
            startup_attempt_name(diagnostics.store_binding),
            startup_attempt_name(diagnostics.session_restore),
            startup_attempt_name(diagnostics.e2ee_initialization),
            startup_attempt_name(diagnostics.server_device_key_proof),
        ));
        for (stage, attempt) in [
            ("authentication", diagnostics.authentication),
            ("store-binding", diagnostics.store_binding),
            ("session-restore", diagnostics.session_restore),
            ("e2ee-initialization", diagnostics.e2ee_initialization),
            (
                "server-device-key-proof",
                diagnostics.server_device_key_proof,
            ),
        ] {
            if let Some(reason) = startup_attempt_reason(attempt) {
                body.push_str(&format!(
                    "guigu_readiness_matrix_failure{{stage=\"{stage}\",reason=\"{reason}\"}} 1\n"
                ));
            }
        }
    }
}
