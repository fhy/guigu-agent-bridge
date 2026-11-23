use super::*;

/// Redacted status for each independent Matrix startup prerequisite.
///
/// `NotAttemptedRecoveryBlocked` is intentionally distinct from an attempted
/// failure: the recovery gate returns before any Matrix client or network work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatrixStartupAttempt {
    NotAttempted,
    NotAttemptedRecoveryBlocked,
    Succeeded,
    Failed(MatrixFailure),
    Unknown(MatrixFailure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatrixStartupDiagnostics {
    pub authentication: MatrixStartupAttempt,
    pub store_binding: MatrixStartupAttempt,
    pub session_restore: MatrixStartupAttempt,
    pub e2ee_initialization: MatrixStartupAttempt,
    pub server_device_key_proof: MatrixStartupAttempt,
}

impl Default for MatrixStartupDiagnostics {
    fn default() -> Self {
        Self {
            authentication: MatrixStartupAttempt::NotAttempted,
            store_binding: MatrixStartupAttempt::NotAttempted,
            session_restore: MatrixStartupAttempt::NotAttempted,
            e2ee_initialization: MatrixStartupAttempt::NotAttempted,
            server_device_key_proof: MatrixStartupAttempt::NotAttempted,
        }
    }
}

impl MatrixReadinessOwner {
    pub fn startup_diagnostics(&self) -> MatrixStartupDiagnostics {
        *self.diagnostics.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn mark_recovery_blocked(&self) {
        *self.diagnostics.lock().unwrap_or_else(|p| p.into_inner()) = MatrixStartupDiagnostics {
            authentication: MatrixStartupAttempt::NotAttemptedRecoveryBlocked,
            store_binding: MatrixStartupAttempt::NotAttemptedRecoveryBlocked,
            session_restore: MatrixStartupAttempt::NotAttemptedRecoveryBlocked,
            e2ee_initialization: MatrixStartupAttempt::NotAttemptedRecoveryBlocked,
            server_device_key_proof: MatrixStartupAttempt::NotAttemptedRecoveryBlocked,
        };
    }

    pub fn mark_authentication_succeeded(&self) {
        self.diagnostics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .authentication = MatrixStartupAttempt::Succeeded;
    }

    pub fn mark_store_binding_succeeded(&self) {
        self.diagnostics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .store_binding = MatrixStartupAttempt::Succeeded;
    }

    pub fn mark_session_restore_succeeded(&self) {
        self.diagnostics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .session_restore = MatrixStartupAttempt::Succeeded;
    }

    pub fn mark_key_proof_succeeded(&self) {
        let mut diagnostics = self.diagnostics.lock().unwrap_or_else(|p| p.into_inner());
        diagnostics.server_device_key_proof = MatrixStartupAttempt::Succeeded;
    }

    pub fn mark_e2ee_succeeded(&self) {
        self.diagnostics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .e2ee_initialization = MatrixStartupAttempt::Succeeded;
    }

    pub fn mark_restore_failure(
        &self,
        identity_matched: bool,
        store_bound: bool,
        reason: MatrixFailure,
    ) {
        let mut diagnostics = self.diagnostics.lock().unwrap_or_else(|p| p.into_inner());
        if store_bound {
            diagnostics.session_restore = MatrixStartupAttempt::Failed(reason);
        } else if identity_matched {
            diagnostics.store_binding = MatrixStartupAttempt::Failed(reason);
        } else {
            diagnostics.authentication = MatrixStartupAttempt::Failed(reason);
        }
    }

    pub fn mark_e2ee_failure(&self, reason: MatrixFailure) {
        self.diagnostics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .e2ee_initialization = MatrixStartupAttempt::Failed(reason);
    }

    pub fn mark_e2ee_timeout(&self) {
        self.diagnostics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .e2ee_initialization = MatrixStartupAttempt::Unknown(MatrixFailure::Timeout);
    }

    pub fn mark_key_proof_failure(&self, reason: MatrixFailure) {
        self.diagnostics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .server_device_key_proof = MatrixStartupAttempt::Failed(reason);
    }

    pub fn mark_key_proof_timeout(&self) {
        self.diagnostics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .server_device_key_proof = MatrixStartupAttempt::Unknown(MatrixFailure::Timeout);
    }
}
