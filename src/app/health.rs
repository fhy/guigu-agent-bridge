use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
};

use crate::runtime::{
    HealthSnapshot, Readiness, RuntimeMetrics, SqliteRuntimeStore, health_snapshot,
};

use super::ProjectionMetrics;

const MAX_REQUEST_BYTES: usize = 4096;
const MAX_CONNECTIONS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OwnerState {
    Starting = 0,
    Running = 1,
    Stopping = 2,
    Stopped = 3,
    Failed = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatrixFailure {
    Authentication,
    UserMismatch,
    DeviceMissing,
    DeviceMismatch,
    UnsafeStorePath,
    UnsafeStoreAncestor,
    StoreCorrupt,
    StoreBindingMismatch,
    CryptoInitialization,
    DeviceKeyUpload,
    RequiredTaskExited,
    Timeout,
}

impl MatrixFailure {
    pub(crate) fn as_log_str(self) -> &'static str {
        match self {
            Self::Authentication => "matrix-auth-failed",
            Self::UserMismatch => "matrix-user-mismatch",
            Self::DeviceMissing => "matrix-device-missing",
            Self::DeviceMismatch => "matrix-device-mismatch",
            Self::UnsafeStorePath => "matrix-unsafe-store-path",
            Self::UnsafeStoreAncestor => "matrix-unsafe-store-ancestor",
            Self::StoreCorrupt => "matrix-store-corrupt",
            Self::StoreBindingMismatch => "matrix-store-binding-mismatch",
            Self::CryptoInitialization => "matrix-crypto-initialization-failed",
            Self::DeviceKeyUpload => "matrix-device-key-upload-failed",
            Self::RequiredTaskExited => "matrix-required-task-exited",
            Self::Timeout => "matrix-startup-timeout",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatrixPhase {
    Disabled,
    Preflight,
    IdentityMatched,
    StoreBound,
    KeyProved,
    Registering,
    Ready,
    Failed(MatrixFailure),
    RecoveryBlocked,
    ShuttingDown,
    Stopped,
}

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

#[derive(Debug, Clone, Copy)]
struct MatrixReadiness {
    generation: u64,
    phase: MatrixPhase,
}

#[derive(Debug)]
pub struct MatrixReadinessOwner {
    state: Mutex<MatrixReadiness>,
    diagnostics: Mutex<MatrixStartupDiagnostics>,
    missing_room_keys: AtomicU64,
}

impl MatrixReadinessOwner {
    fn new() -> Self {
        Self {
            state: Mutex::new(MatrixReadiness {
                generation: 0,
                phase: MatrixPhase::Disabled,
            }),
            diagnostics: Mutex::new(MatrixStartupDiagnostics::default()),
            missing_room_keys: AtomicU64::new(0),
        }
    }

    pub fn begin(&self) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.generation = state
            .generation
            .checked_add(1)
            .expect("Matrix readiness generation exhausted");
        state.phase = MatrixPhase::Preflight;
        *self.diagnostics.lock().unwrap_or_else(|p| p.into_inner()) =
            MatrixStartupDiagnostics::default();
        state.generation
    }

    pub fn transition(&self, generation: u64, allowed: &[MatrixPhase], next: MatrixPhase) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.generation != generation
            || !allowed.contains(&state.phase)
            || !legal_matrix_transition(state.phase, next)
        {
            return false;
        }
        state.phase = next;
        true
    }

    pub fn phase(&self) -> MatrixPhase {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).phase
    }

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

    pub fn fail_required_task(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if matches!(state.phase, MatrixPhase::Registering | MatrixPhase::Ready) {
            state.phase = MatrixPhase::Failed(MatrixFailure::RequiredTaskExited);
        }
    }

    pub fn stop(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.generation = state
            .generation
            .checked_add(1)
            .expect("Matrix readiness generation exhausted");
        state.phase = MatrixPhase::ShuttingDown;
    }

    pub fn stopped(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.phase == MatrixPhase::ShuttingDown {
            state.phase = MatrixPhase::Stopped;
        }
    }

    pub fn record_missing_room_key(&self) {
        self.missing_room_keys.fetch_add(1, Ordering::Relaxed);
    }

    pub fn missing_room_keys(&self) -> u64 {
        self.missing_room_keys.load(Ordering::Relaxed)
    }

    fn diagnostics(&self) -> MatrixStartupDiagnostics {
        self.startup_diagnostics()
    }
}

fn legal_matrix_transition(from: MatrixPhase, to: MatrixPhase) -> bool {
    use MatrixPhase::{
        Failed, IdentityMatched, KeyProved, Preflight, Ready, RecoveryBlocked, Registering,
        StoreBound,
    };
    matches!(
        (from, to),
        (Preflight, IdentityMatched | Failed(_) | RecoveryBlocked)
            | (IdentityMatched, StoreBound | Failed(_))
            | (StoreBound, KeyProved | Failed(_))
            | (KeyProved, Registering | Failed(_))
            | (Registering, Ready | Failed(_))
            | (Ready, Failed(MatrixFailure::RequiredTaskExited))
    )
}

impl crate::matrix::MissingRoomKeyObserver for HealthState {
    fn record_missing_room_key(&self) {
        self.matrix.record_missing_room_key();
    }
}

pub struct HealthState {
    runtime: SqliteRuntimeStore,
    metrics: Arc<RuntimeMetrics>,
    projections: Arc<ProjectionMetrics>,
    owner: AtomicU8,
    adapter_ready: AtomicBool,
    recovery_blocked: AtomicBool,
    insecure_a2a_peers: AtomicU64,
    required: Mutex<Vec<Arc<AtomicBool>>>,
    matrix_required: Mutex<Vec<Arc<AtomicBool>>>,
    matrix: MatrixReadinessOwner,
}

impl HealthState {
    pub fn new(
        runtime: SqliteRuntimeStore,
        metrics: Arc<RuntimeMetrics>,
        projections: Arc<ProjectionMetrics>,
    ) -> Self {
        Self {
            runtime,
            metrics,
            projections,
            owner: AtomicU8::new(OwnerState::Starting as u8),
            adapter_ready: AtomicBool::new(false),
            recovery_blocked: AtomicBool::new(false),
            insecure_a2a_peers: AtomicU64::new(0),
            required: Mutex::new(Vec::new()),
            matrix_required: Mutex::new(Vec::new()),
            matrix: MatrixReadinessOwner::new(),
        }
    }

    pub fn set_owner(&self, state: OwnerState) {
        self.owner.store(state as u8, Ordering::Release);
    }

    pub fn set_adapter_ready(&self, ready: bool) {
        self.adapter_ready.store(ready, Ordering::Release);
    }

    pub fn set_recovery_blocked(&self, blocked: bool) {
        self.recovery_blocked.store(blocked, Ordering::Release);
    }

    pub fn set_insecure_a2a_peers(&self, count: usize) {
        self.insecure_a2a_peers
            .store(count.try_into().unwrap_or(u64::MAX), Ordering::Release);
    }

    pub fn insecure_a2a_peers(&self) -> u64 {
        self.insecure_a2a_peers.load(Ordering::Acquire)
    }

    pub fn register_required(&self, alive: Arc<AtomicBool>) {
        self.required
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(alive);
    }

    pub fn matrix(&self) -> &MatrixReadinessOwner {
        &self.matrix
    }

    pub fn register_matrix_required(&self, alive: Arc<AtomicBool>) {
        self.matrix_required
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(alive);
    }

    pub fn owner(&self) -> OwnerState {
        match self.owner.load(Ordering::Acquire) {
            0 => OwnerState::Starting,
            1 => OwnerState::Running,
            2 => OwnerState::Stopping,
            3 => OwnerState::Stopped,
            _ => OwnerState::Failed,
        }
    }

    fn liveness_counts(&self, required: &Mutex<Vec<Arc<AtomicBool>>>) -> (usize, usize) {
        let values = required.lock().unwrap_or_else(|p| p.into_inner());
        let total = values.len();
        let alive = values
            .iter()
            .filter(|value| value.load(Ordering::Acquire))
            .count();
        (total, alive)
    }

    pub async fn snapshot(&self) -> HealthSnapshot {
        let owner = self.owner();
        let owners_running = self
            .required
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .all(|alive| alive.load(Ordering::Acquire));
        let matrix_running = self
            .matrix_required
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .all(|alive| alive.load(Ordering::Acquire));
        let matrix_phase = self.matrix.phase();
        if !matrix_running && matches!(matrix_phase, MatrixPhase::Registering | MatrixPhase::Ready)
        {
            self.matrix.fail_required_task();
        }
        let mut snapshot = health_snapshot(
            &self.runtime,
            Utc::now(),
            owner == OwnerState::Running && owners_running,
            self.adapter_ready.load(Ordering::Acquire),
        )
        .await;
        if self.recovery_blocked.load(Ordering::Acquire) {
            snapshot.readiness = Readiness::RecoveryBlocked;
            snapshot.diagnostic = Some("recovery-blocked");
        } else {
            match self.matrix.phase() {
                MatrixPhase::Disabled | MatrixPhase::Ready => {}
                MatrixPhase::Failed(reason) => {
                    snapshot.readiness = Readiness::AdapterUnavailable;
                    snapshot.diagnostic = Some(reason.as_log_str());
                }
                MatrixPhase::RecoveryBlocked => {
                    snapshot.readiness = Readiness::RecoveryBlocked;
                    snapshot.diagnostic = Some("recovery-blocked");
                }
                _ => {
                    snapshot.readiness = Readiness::AdapterUnavailable;
                    snapshot.diagnostic = Some("matrix-not-ready");
                }
            }
        }
        snapshot.live = !matches!(owner, OwnerState::Stopped | OwnerState::Failed);
        snapshot
    }

    async fn metrics_body(&self) -> String {
        let readiness = self.snapshot().await;
        let current_counts = self.runtime.counts().await;
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
        let (required_total, required_alive) = self.liveness_counts(&self.required);
        let (matrix_required_total, matrix_required_alive) =
            self.liveness_counts(&self.matrix_required);
        body.push_str(&format!(
            "guigu_readiness_evaluation{{readiness=\"{}\"}} 1\n",
            readiness_name(readiness.readiness)
        ));
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
        body
    }
}

struct ReadinessInputs {
    owner: OwnerState,
    adapter_ready: bool,
    recovery_blocked: bool,
    required_total: usize,
    required_alive: usize,
    matrix_phase: MatrixPhase,
    storage_available: bool,
    current_recovery: bool,
}

fn readiness_blockers(inputs: ReadinessInputs) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if !inputs.storage_available {
        blockers.push("storage-unavailable");
    }
    if inputs.current_recovery {
        blockers.push("recovery-backlog");
    }
    if inputs.recovery_blocked {
        blockers.push("recovery-blocked");
    }
    if inputs.owner != OwnerState::Running {
        blockers.push("owner-not-running");
    }
    if !inputs.adapter_ready {
        blockers.push("adapter-not-ready");
    }
    if inputs.required_alive != inputs.required_total {
        blockers.push("required-child-not-alive");
    }
    if !matches!(
        inputs.matrix_phase,
        MatrixPhase::Disabled | MatrixPhase::Ready
    ) {
        blockers.push("matrix-phase-not-ready");
    }
    if blockers.is_empty() {
        blockers.push("none");
    }
    blockers
}

fn readiness_name(readiness: Readiness) -> &'static str {
    match readiness {
        Readiness::Ready => "ready",
        Readiness::StorageUnavailable => "storage-unavailable",
        Readiness::OwnerStopped => "owner-stopped",
        Readiness::RecoveryBlocked => "recovery-blocked",
        Readiness::AdapterUnavailable => "adapter-unavailable",
    }
}

fn owner_state_name(state: OwnerState) -> &'static str {
    match state {
        OwnerState::Starting => "starting",
        OwnerState::Running => "running",
        OwnerState::Stopping => "stopping",
        OwnerState::Stopped => "stopped",
        OwnerState::Failed => "failed",
    }
}

fn matrix_phase_name(phase: MatrixPhase) -> &'static str {
    match phase {
        MatrixPhase::Disabled => "disabled",
        MatrixPhase::Preflight => "preflight",
        MatrixPhase::IdentityMatched => "identity-matched",
        MatrixPhase::StoreBound => "store-bound",
        MatrixPhase::KeyProved => "key-proved",
        MatrixPhase::Registering => "registering",
        MatrixPhase::Ready => "ready",
        MatrixPhase::Failed(_) => "failed",
        MatrixPhase::RecoveryBlocked => "recovery-blocked",
        MatrixPhase::ShuttingDown => "shutting-down",
        MatrixPhase::Stopped => "stopped",
    }
}

fn matrix_phase_reason(phase: MatrixPhase) -> Option<&'static str> {
    match phase {
        MatrixPhase::Failed(reason) => Some(reason.as_log_str()),
        MatrixPhase::Disabled
        | MatrixPhase::Preflight
        | MatrixPhase::IdentityMatched
        | MatrixPhase::StoreBound
        | MatrixPhase::KeyProved
        | MatrixPhase::Registering
        | MatrixPhase::Ready
        | MatrixPhase::RecoveryBlocked
        | MatrixPhase::ShuttingDown
        | MatrixPhase::Stopped => None,
    }
}

fn startup_attempt_name(attempt: MatrixStartupAttempt) -> &'static str {
    match attempt {
        MatrixStartupAttempt::NotAttempted => "not-attempted",
        MatrixStartupAttempt::NotAttemptedRecoveryBlocked => "not-attempted-recovery-blocked",
        MatrixStartupAttempt::Succeeded => "succeeded",
        MatrixStartupAttempt::Failed(_) => "failed",
        MatrixStartupAttempt::Unknown(_) => "unknown",
    }
}

fn startup_attempt_reason(attempt: MatrixStartupAttempt) -> Option<&'static str> {
    match attempt {
        MatrixStartupAttempt::Failed(reason) | MatrixStartupAttempt::Unknown(reason) => {
            Some(reason.as_log_str())
        }
        MatrixStartupAttempt::NotAttempted
        | MatrixStartupAttempt::NotAttemptedRecoveryBlocked
        | MatrixStartupAttempt::Succeeded => None,
    }
}

fn metric_name(metric: crate::runtime::Metric) -> &'static str {
    use crate::runtime::{Metric, PolicyLimit};
    match metric {
        Metric::LeaseAcquired => "lease_acquired",
        Metric::LeaseBusy => "lease_busy",
        Metric::LeaseRecoveryNeeded => "lease_recovery_needed",
        Metric::TurnStarted => "turn_started",
        Metric::TurnContinued => "turn_continued",
        Metric::TurnCompleted => "turn_completed",
        Metric::TurnBlocked => "turn_blocked",
        Metric::TurnFailed => "turn_failed",
        Metric::PolicyExhausted(PolicyLimit::Turns) => "policy_turns",
        Metric::PolicyExhausted(PolicyLimit::Deadline) => "policy_deadline",
        Metric::PolicyExhausted(PolicyLimit::Inactivity) => "policy_inactivity",
        Metric::PolicyExhausted(PolicyLimit::NoProgress) => "policy_no_progress",
        Metric::PolicyExhausted(PolicyLimit::OutputBytes) => "policy_output_bytes",
    }
}

pub struct HealthServer {
    address: SocketAddr,
    shutdown: watch::Sender<bool>,
    join: JoinHandle<std::io::Result<()>>,
}

impl HealthServer {
    pub async fn start(address: SocketAddr, state: Arc<HealthState>) -> std::io::Result<Self> {
        if !address.ip().is_loopback() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "health listener must be loopback",
            ));
        }
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let (shutdown, receiver) = watch::channel(false);
        let join = tokio::spawn(run(listener, state, receiver));
        Ok(Self {
            address,
            shutdown,
            join,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    pub async fn shutdown(self) -> std::io::Result<()> {
        let _ = self.shutdown.send(true);
        self.join
            .await
            .map_err(|_| std::io::Error::other("health owner join failed"))?
    }
}

async fn run(
    listener: TcpListener,
    state: Arc<HealthState>,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                break;
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    continue;
                };
                let state = Arc::clone(&state);
                connections.spawn(async move {
                    let _permit = permit;
                    let _ = serve(stream, state).await;
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn serve(mut stream: TcpStream, state: Arc<HealthState>) -> std::io::Result<()> {
    let mut input = [0_u8; MAX_REQUEST_BYTES + 1];
    let count = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut input))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "health read timeout"))??;
    if count == 0 || count > MAX_REQUEST_BYTES {
        return write_response(&mut stream, 400, "bad_request\n").await;
    }
    let request = std::str::from_utf8(&input[..count]).unwrap_or("");
    let first = request.lines().next().unwrap_or("");
    let (status, body) = match first {
        "GET /live HTTP/1.1" | "GET /live HTTP/1.0" => {
            let live = state.snapshot().await.live;
            if live {
                (200, "live=true\n".into())
            } else {
                (503, "live=false\n".into())
            }
        }
        "GET /ready HTTP/1.1" | "GET /ready HTTP/1.0" => {
            let ready = state.snapshot().await.readiness == Readiness::Ready;
            if ready {
                (200, "ready=true\n".into())
            } else {
                (503, "ready=false\n".into())
            }
        }
        "GET /metrics HTTP/1.1" | "GET /metrics HTTP/1.0" => (200, state.metrics_body().await),
        value if value.starts_with("GET ") => (404, "not_found\n".into()),
        _ => (405, "method_not_allowed\n".into()),
    };
    write_response(&mut stream, status, &body).await
}

async fn write_response(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Service Unavailable",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod matrix_readiness_tests {
    use std::sync::Arc;

    use crate::{
        app::ProjectionMetrics,
        runtime::{RuntimeMetrics, SqliteRuntimeStore},
        storage::{connect, migrate},
    };

    use super::{
        MatrixFailure, MatrixPhase, MatrixReadinessOwner, MatrixStartupAttempt, OwnerState,
        ReadinessInputs,
    };

    fn readiness_blockers(
        owner: OwnerState,
        adapter_ready: bool,
        recovery_blocked: bool,
        required_total: usize,
        required_alive: usize,
        matrix_phase: MatrixPhase,
    ) -> Vec<&'static str> {
        super::readiness_blockers(ReadinessInputs {
            owner,
            adapter_ready,
            recovery_blocked,
            required_total,
            required_alive,
            matrix_phase,
            storage_available: true,
            current_recovery: false,
        })
    }

    async fn fixture() -> (super::HealthState, sqlx::SqlitePool, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("guigu-health-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let pool = connect(root.join("state.db")).await.unwrap();
        migrate(&pool).await.unwrap();
        let state = super::HealthState::new(
            SqliteRuntimeStore::new(pool.clone()),
            Arc::new(RuntimeMetrics::default()),
            Arc::new(ProjectionMetrics::default()),
        );
        (state, pool, root)
    }

    async fn assert_ready_metrics(state: &super::HealthState) {
        assert_eq!(
            state.snapshot().await.readiness,
            crate::runtime::Readiness::Ready
        );
        let metrics = state.metrics_body().await;
        assert!(metrics.contains("guigu_readiness_evaluation{readiness=\"ready\"} 1"));
        assert!(metrics.contains("predicate=\"none\""));
    }

    #[tokio::test]
    async fn metrics_matrix_matches_readiness_matrix() {
        let (state, _pool, root) = fixture().await;
        state.set_owner(OwnerState::Running);
        state.set_adapter_ready(true);
        assert_ready_metrics(&state).await;
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn metrics_matrix_failed_exposes_matrix_blocker() {
        let (state, _pool, root) = fixture().await;
        state.set_owner(OwnerState::Running);
        state.set_adapter_ready(true);
        let generation = state.matrix().begin();
        assert!(state.matrix().transition(
            generation,
            &[MatrixPhase::Preflight],
            MatrixPhase::Failed(MatrixFailure::Authentication)
        ));
        assert_eq!(
            state.snapshot().await.readiness,
            crate::runtime::Readiness::AdapterUnavailable
        );
        let metrics = state.metrics_body().await;
        assert!(metrics.contains("predicate=\"matrix-phase-not-ready\""));
        assert!(!metrics.contains("predicate=\"none\""));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn metrics_matrix_generic_child_dead_exposes_child_blocker() {
        let (state, _pool, root) = fixture().await;
        state.set_owner(OwnerState::Running);
        state.set_adapter_ready(true);
        state.register_required(Arc::new(std::sync::atomic::AtomicBool::new(false)));
        assert_eq!(
            state.snapshot().await.readiness,
            crate::runtime::Readiness::OwnerStopped
        );
        let metrics = state.metrics_body().await;
        assert!(metrics.contains("predicate=\"required-child-not-alive\""));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn metrics_first_matrix_child_dead_updates_phase_before_classifying() {
        let (state, _pool, root) = fixture().await;
        state.set_owner(OwnerState::Running);
        state.set_adapter_ready(true);
        let generation = state.matrix().begin();
        for (from, to) in [
            (MatrixPhase::Preflight, MatrixPhase::IdentityMatched),
            (MatrixPhase::IdentityMatched, MatrixPhase::StoreBound),
            (MatrixPhase::StoreBound, MatrixPhase::KeyProved),
            (MatrixPhase::KeyProved, MatrixPhase::Registering),
            (MatrixPhase::Registering, MatrixPhase::Ready),
        ] {
            assert!(state.matrix().transition(generation, &[from], to));
        }
        state.register_matrix_required(Arc::new(std::sync::atomic::AtomicBool::new(false)));
        let metrics = state.metrics_body().await;
        assert!(metrics.contains("predicate=\"matrix-phase-not-ready\""));
        assert!(!metrics.contains("predicate=\"none\""));
        assert_eq!(
            state.snapshot().await.readiness,
            crate::runtime::Readiness::AdapterUnavailable
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn metrics_startup_recovery_block_matches_readiness() {
        let (state, _pool, root) = fixture().await;
        state.set_owner(OwnerState::Running);
        state.set_adapter_ready(true);
        state.set_recovery_blocked(true);
        assert_eq!(
            state.snapshot().await.readiness,
            crate::runtime::Readiness::RecoveryBlocked
        );
        let metrics = state.metrics_body().await;
        assert!(metrics.contains("predicate=\"recovery-blocked\""));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn metrics_current_recovery_lease_blocks_without_startup_flag() {
        let (state, pool, root) = fixture().await;
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query("PRAGMA foreign_keys=OFF")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES ('health-resource','health-task','health-owner',1,'recovery_needed','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
            .execute(&mut *connection)
            .await
            .unwrap();
        drop(connection);
        state.set_owner(OwnerState::Running);
        state.set_adapter_ready(true);
        assert_eq!(
            state.snapshot().await.readiness,
            crate::runtime::Readiness::RecoveryBlocked
        );
        let metrics = state.metrics_body().await;
        assert!(metrics.contains("current_recovery_leases 1"));
        assert!(metrics.contains("predicate=\"recovery-backlog\""));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn metrics_storage_failure_is_not_reported_as_none() {
        let (state, pool, root) = fixture().await;
        pool.close().await;
        let metrics = state.metrics_body().await;
        assert!(metrics.contains("readiness_storage_available 0"));
        assert!(metrics.contains("predicate=\"storage-unavailable\""));
        assert!(!metrics.contains("predicate=\"none\""));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn metrics_missing_room_key_counter_is_not_causal() {
        let (state, _pool, root) = fixture().await;
        state.set_owner(OwnerState::Running);
        state.set_adapter_ready(true);
        let generation = state.matrix().begin();
        for (from, to) in [
            (MatrixPhase::Preflight, MatrixPhase::IdentityMatched),
            (MatrixPhase::IdentityMatched, MatrixPhase::StoreBound),
            (MatrixPhase::StoreBound, MatrixPhase::KeyProved),
            (MatrixPhase::KeyProved, MatrixPhase::Registering),
            (MatrixPhase::Registering, MatrixPhase::Ready),
        ] {
            assert!(state.matrix().transition(generation, &[from], to));
        }
        state.matrix().record_missing_room_key();
        assert_ready_metrics(&state).await;
        let metrics = state.metrics_body().await;
        assert!(metrics.contains("matrix_missing_room_keys 1"));
        assert!(!metrics.contains("predicate=\"matrix-phase-not-ready\""));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn readiness_blockers_are_empty_for_healthy_ready_state() {
        assert_eq!(
            readiness_blockers(OwnerState::Running, true, false, 2, 2, MatrixPhase::Ready,),
            vec!["none"]
        );
    }

    #[test]
    fn readiness_blockers_identify_matrix_phase_without_changing_semantics() {
        assert_eq!(
            readiness_blockers(
                OwnerState::Running,
                true,
                false,
                0,
                0,
                MatrixPhase::Failed(MatrixFailure::DeviceKeyUpload),
            ),
            vec!["matrix-phase-not-ready"]
        );
    }

    #[test]
    fn readiness_blockers_identify_dead_required_child() {
        assert_eq!(
            readiness_blockers(OwnerState::Running, true, false, 2, 1, MatrixPhase::Ready,),
            vec!["required-child-not-alive"]
        );
    }

    #[test]
    fn readiness_blockers_identify_recovery_block() {
        assert_eq!(
            readiness_blockers(OwnerState::Running, false, true, 0, 0, MatrixPhase::Ready,),
            vec!["recovery-blocked", "adapter-not-ready"]
        );
    }

    #[test]
    fn missing_room_key_telemetry_is_not_a_readiness_blocker() {
        let owner = MatrixReadinessOwner::new();
        let generation = owner.begin();
        for (from, to) in [
            (MatrixPhase::Preflight, MatrixPhase::IdentityMatched),
            (MatrixPhase::IdentityMatched, MatrixPhase::StoreBound),
            (MatrixPhase::StoreBound, MatrixPhase::KeyProved),
            (MatrixPhase::KeyProved, MatrixPhase::Registering),
            (MatrixPhase::Registering, MatrixPhase::Ready),
        ] {
            assert!(owner.transition(generation, &[from], to));
        }
        owner.record_missing_room_key();
        assert_eq!(owner.missing_room_keys(), 1);
        assert_eq!(
            readiness_blockers(OwnerState::Running, true, false, 0, 0, owner.phase(),),
            vec!["none"]
        );
    }

    #[test]
    fn recovery_blocked_marks_each_matrix_gate_not_attempted() {
        let owner = MatrixReadinessOwner::new();
        let generation = owner.begin();

        owner.mark_recovery_blocked();
        assert!(owner.transition(
            generation,
            &[MatrixPhase::Preflight],
            MatrixPhase::RecoveryBlocked
        ));

        let diagnostics = owner.startup_diagnostics();
        let blocked = MatrixStartupAttempt::NotAttemptedRecoveryBlocked;
        assert_eq!(diagnostics.authentication, blocked);
        assert_eq!(diagnostics.store_binding, blocked);
        assert_eq!(diagnostics.e2ee_initialization, blocked);
        assert_eq!(diagnostics.server_device_key_proof, blocked);
    }

    #[test]
    fn attempted_matrix_failure_is_distinct_from_recovery_block() {
        let owner = MatrixReadinessOwner::new();
        let generation = owner.begin();
        owner.mark_restore_failure(false, false, MatrixFailure::Authentication);
        assert!(owner.transition(
            generation,
            &[MatrixPhase::Preflight],
            MatrixPhase::Failed(MatrixFailure::Authentication)
        ));

        let diagnostics = owner.startup_diagnostics();
        assert_eq!(
            diagnostics.authentication,
            MatrixStartupAttempt::Failed(MatrixFailure::Authentication)
        );
        assert_eq!(
            diagnostics.store_binding,
            MatrixStartupAttempt::NotAttempted
        );
        assert_ne!(
            diagnostics.authentication,
            MatrixStartupAttempt::NotAttemptedRecoveryBlocked
        );
    }

    #[test]
    fn startup_failure_attribution_keeps_later_gates_unattempted() {
        let owner = MatrixReadinessOwner::new();
        owner.begin();
        owner.mark_e2ee_failure(MatrixFailure::CryptoInitialization);
        let diagnostics = owner.startup_diagnostics();
        assert_eq!(
            diagnostics.e2ee_initialization,
            MatrixStartupAttempt::Failed(MatrixFailure::CryptoInitialization)
        );
        assert_eq!(
            diagnostics.server_device_key_proof,
            MatrixStartupAttempt::NotAttempted
        );

        owner.mark_e2ee_succeeded();
        owner.mark_key_proof_failure(MatrixFailure::DeviceKeyUpload);
        let diagnostics = owner.startup_diagnostics();
        assert_eq!(
            diagnostics.e2ee_initialization,
            MatrixStartupAttempt::Succeeded
        );
        assert_eq!(
            diagnostics.server_device_key_proof,
            MatrixStartupAttempt::Failed(MatrixFailure::DeviceKeyUpload)
        );
    }

    #[test]
    fn timeout_and_session_restore_have_separate_boundaries() {
        let owner = MatrixReadinessOwner::new();
        owner.begin();
        owner.mark_e2ee_timeout();
        let diagnostics = owner.startup_diagnostics();
        assert_eq!(
            diagnostics.e2ee_initialization,
            MatrixStartupAttempt::Unknown(MatrixFailure::Timeout)
        );
        assert_eq!(
            diagnostics.server_device_key_proof,
            MatrixStartupAttempt::NotAttempted
        );

        owner.mark_store_binding_succeeded();
        owner.mark_restore_failure(true, true, MatrixFailure::Authentication);
        assert_eq!(
            owner.startup_diagnostics().session_restore,
            MatrixStartupAttempt::Failed(MatrixFailure::Authentication)
        );
        assert_eq!(
            owner.startup_diagnostics().store_binding,
            MatrixStartupAttempt::Succeeded
        );
    }

    #[test]
    fn old_generation_cannot_publish_ready_after_supersession() {
        let owner = MatrixReadinessOwner::new();
        let old = owner.begin();
        assert!(owner.transition(old, &[MatrixPhase::Preflight], MatrixPhase::IdentityMatched));
        owner.stop();
        let current = owner.begin();

        assert!(!owner.transition(old, &[MatrixPhase::IdentityMatched], MatrixPhase::Ready));
        assert_eq!(owner.phase(), MatrixPhase::Preflight);
        assert!(owner.transition(
            current,
            &[MatrixPhase::Preflight],
            MatrixPhase::Failed(MatrixFailure::Timeout)
        ));
        assert!(!owner.transition(
            current,
            &[MatrixPhase::Failed(MatrixFailure::Timeout)],
            MatrixPhase::Ready
        ));
    }

    #[test]
    fn required_task_exit_and_shutdown_are_terminal_for_the_generation() {
        let owner = MatrixReadinessOwner::new();
        let generation = owner.begin();
        assert!(owner.transition(
            generation,
            &[MatrixPhase::Preflight],
            MatrixPhase::IdentityMatched
        ));
        assert!(owner.transition(
            generation,
            &[MatrixPhase::IdentityMatched],
            MatrixPhase::StoreBound
        ));
        assert!(owner.transition(
            generation,
            &[MatrixPhase::StoreBound],
            MatrixPhase::KeyProved
        ));
        assert!(owner.transition(
            generation,
            &[MatrixPhase::KeyProved],
            MatrixPhase::Registering
        ));
        owner.fail_required_task();
        assert_eq!(
            owner.phase(),
            MatrixPhase::Failed(MatrixFailure::RequiredTaskExited)
        );
        assert!(!owner.transition(generation, &[MatrixPhase::Registering], MatrixPhase::Ready));

        owner.stop();
        assert_eq!(owner.phase(), MatrixPhase::ShuttingDown);
        owner.stopped();
        assert_eq!(owner.phase(), MatrixPhase::Stopped);
    }

    #[test]
    fn missing_room_key_does_not_change_transport_phase() {
        let owner = MatrixReadinessOwner::new();
        let generation = owner.begin();
        for (from, to) in [
            (MatrixPhase::Preflight, MatrixPhase::IdentityMatched),
            (MatrixPhase::IdentityMatched, MatrixPhase::StoreBound),
            (MatrixPhase::StoreBound, MatrixPhase::KeyProved),
            (MatrixPhase::KeyProved, MatrixPhase::Registering),
            (MatrixPhase::Registering, MatrixPhase::Ready),
        ] {
            assert!(owner.transition(generation, &[from], to));
        }
        owner.record_missing_room_key();
        assert_eq!(owner.missing_room_keys(), 1);
        assert_eq!(owner.phase(), MatrixPhase::Ready);
    }
}
