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

#[derive(Debug, Clone, Copy)]
struct MatrixReadiness {
    generation: u64,
    phase: MatrixPhase,
}

#[derive(Debug)]
pub struct MatrixReadinessOwner {
    state: Mutex<MatrixReadiness>,
    missing_room_keys: AtomicU64,
}

impl MatrixReadinessOwner {
    fn new() -> Self {
        Self {
            state: Mutex::new(MatrixReadiness {
                generation: 0,
                phase: MatrixPhase::Disabled,
            }),
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

    fn metrics_body(&self) -> String {
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
        body
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
        "GET /metrics HTTP/1.1" | "GET /metrics HTTP/1.0" => (200, state.metrics_body()),
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
    use super::{MatrixFailure, MatrixPhase, MatrixReadinessOwner};

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
