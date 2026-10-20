use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::transport::AcpTransport;
use crate::storage::owner_observation::{DomainBinding, FinalObservation};

#[derive(Clone, Debug)]
pub(crate) struct AcpSpawnGate {
    state: Arc<(Mutex<SpawnState>, Notify)>,
    domain: Arc<DomainBinding>,
}

struct SpawnState {
    phase: SpawnPhase,
    in_flight: usize,
    epoch: i64,
    children: Vec<String>,
    registration_failed: bool,
    transports: Vec<Arc<AcpTransport>>,
}

impl std::fmt::Debug for SpawnState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SpawnState")
            .field("phase", &self.phase)
            .field("in_flight", &self.in_flight)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpawnPhase {
    Open,
    Closing,
    ClosedJoined,
}

pub(crate) struct SpawnReservation {
    gate: AcpSpawnGate,
    pub(crate) identity: Option<String>,
}

impl AcpSpawnGate {
    pub(crate) fn new() -> Self {
        Self::for_generation(&uuid::Uuid::now_v7().to_string())
    }

    pub(crate) fn for_generation(generation: &str) -> Self {
        Self {
            state: Arc::new((
                Mutex::new(SpawnState {
                    phase: SpawnPhase::Open,
                    in_flight: 0,
                    epoch: 0,
                    children: Vec::new(),
                    registration_failed: false,
                    transports: Vec::new(),
                }),
                Notify::new(),
            )),
            domain: Arc::new(DomainBinding::for_generation(generation)),
        }
    }

    pub(crate) fn reserve(&self) -> Option<SpawnReservation> {
        let mut state = self.state.0.lock().ok()?;
        if state.phase != SpawnPhase::Open {
            return None;
        }
        state.in_flight = state.in_flight.checked_add(1)?;
        Some(SpawnReservation {
            gate: self.clone(),
            identity: None,
        })
    }

    pub(crate) fn register_child(reservation: &mut SpawnReservation, pid: u32) -> bool {
        let identity = (|| {
            if pid == 0 {
                return None;
            }
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
            let starttime = stat.rsplit_once(") ")?.1.split_whitespace().nth(19)?;
            let executable = std::fs::read_link(format!("/proc/{pid}/exe")).ok()?;
            let cgroup = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
            Some(format!(
                "pid={pid};starttime={starttime};exe={};cgroup={cgroup}",
                executable.display()
            ))
        })();
        let Ok(mut state) = reservation.gate.state.0.lock() else {
            return false;
        };
        if state.children.len() >= 256 || identity.is_none() {
            state.registration_failed = true;
            return false;
        }
        let identity = identity.expect("checked identity");
        state.children.push(identity.clone());
        reservation.identity = Some(identity);
        true
    }

    pub(crate) fn retain_transport(
        reservation: &SpawnReservation,
        transport: Arc<AcpTransport>,
    ) -> bool {
        let Ok(mut state) = reservation.gate.state.0.lock() else {
            return false;
        };
        if state.transports.len() >= 256 {
            state.registration_failed = true;
            return false;
        }
        state.transports.push(transport);
        true
    }

    pub(crate) async fn reap_transports(&self) {
        let transports = self
            .state
            .0
            .lock()
            .expect("spawn gate poisoned")
            .transports
            .clone();
        for transport in transports {
            let _ = transport.shutdown().await;
        }
    }

    pub(crate) async fn close_and_join(&self) {
        loop {
            let notified = self.state.1.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.0.lock().expect("spawn gate poisoned");
                if state.phase == SpawnPhase::ClosedJoined {
                    return;
                }
                state.phase = SpawnPhase::Closing;
                if state.in_flight == 0 {
                    state.phase = SpawnPhase::ClosedJoined;
                    state.epoch = 1;
                    self.state.1.notify_waiters();
                    return;
                }
            }
            notified.await;
        }
    }

    pub(crate) fn is_closed_joined(&self) -> bool {
        self.state
            .0
            .lock()
            .is_ok_and(|state| state.phase == SpawnPhase::ClosedJoined && state.in_flight == 0)
    }

    #[cfg(test)]
    pub(crate) fn epoch(&self) -> i64 {
        self.state.0.lock().map(|state| state.epoch).unwrap_or(-1)
    }

    pub(crate) fn final_observation(&self) -> FinalObservation {
        let epoch = self
            .state
            .0
            .lock()
            .ok()
            .and_then(|state| {
                (state.phase == SpawnPhase::ClosedJoined
                    && state.in_flight == 0
                    && !state.registration_failed)
                    .then_some(state.epoch)
            })
            .unwrap_or(-1);
        self.domain.scan(epoch)
    }

    #[cfg(test)]
    pub(crate) fn rescan_children(&self) -> Result<String, &'static str> {
        let observation = self.final_observation();
        if !observation.qualifies(self.epoch()) {
            return Err("unavailable_containment");
        }
        serde_json::to_string(&observation).map_err(|_| "malformed")
    }

    #[cfg(test)]
    pub(crate) fn registered_pids(&self) -> Vec<u32> {
        self.state
            .0
            .lock()
            .unwrap()
            .children
            .iter()
            .map(|identity| {
                identity
                    .strip_prefix("pid=")
                    .unwrap()
                    .split(';')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap()
            })
            .collect()
    }
}

impl Drop for SpawnReservation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.0.lock() {
            state.in_flight -= 1;
        }
        self.gate.state.1.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn owner_gate_concurrent_close_drains_once_and_is_cancel_safe() {
        let gate = AcpSpawnGate::new();
        let reservation = gate.reserve().unwrap();
        let closing = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.close_and_join().await })
        };
        let second = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.close_and_join().await })
        };
        tokio::task::yield_now().await;
        assert!(!gate.is_closed_joined());
        drop(reservation);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            closing.await.unwrap();
            second.await.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(gate.epoch(), 1);
        gate.close_and_join().await;
        assert_eq!(gate.epoch(), 1);
        assert!(gate.reserve().is_none());
        assert!(gate.rescan_children().is_err());
    }

    #[tokio::test]
    async fn owner_gate_cancelled_reservation_and_registration_failure_refuse_proof() {
        let gate = AcpSpawnGate::new();
        let reservation = gate.reserve().unwrap();
        let pending = tokio::spawn(async move {
            let _reservation = reservation;
            std::future::pending::<()>().await;
        });
        pending.abort();
        let _ = pending.await;
        let mut invalid = gate.reserve().unwrap();
        assert!(!AcpSpawnGate::register_child(&mut invalid, 0));
        drop(invalid);
        tokio::time::timeout(std::time::Duration::from_secs(2), gate.close_and_join())
            .await
            .unwrap();
        assert!(gate.is_closed_joined());
        assert!(!gate.final_observation().qualifies(1));
    }
}
