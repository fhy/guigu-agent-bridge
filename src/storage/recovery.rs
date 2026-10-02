//! Restart-recovery primitives (T009).
//!
//! Restart is where the durable store stops being a cache of what the process
//! already knows and becomes the only source of truth. Two questions must be
//! answered before a restarted bridge consumes anything, and both are answered
//! from the three frozen `Repository` queries — never from an in-memory
//! projection, and never by rewriting history:
//!
//! 1. **Which tasks were unfinished?** [`RecoveryPlan::unfinished`] — tasks whose
//!    latest event is not terminal, **including task rows with no events at all**
//!    (the crash window between "task row written" and "first event written").
//! 2. **Which deliveries are unresolved?** [`RecoveryPlan::unacknowledged`] (never
//!    acknowledged — the retry input) and [`RecoveryPlan::awaiting_outcome`]
//!    (acknowledged but the task never reached a terminal state).
//!
//! # Dispositions (frozen here; executed by T017/T018, not by T009)
//!
//! | set | disposition | why |
//! |-----|-------------|-----|
//! | `unfinished` | take the task over again — re-submit the derived work, or record an explicit terminal event | T009 never invents a terminal state: writing `Failed` for an unknown outcome would turn "we do not know" into a business verdict |
//! | `unacknowledged` | re-dispatch using a **new attempt** and a new `delivery_id` | `UNIQUE (task_id, attempt)` forbids rewriting an attempt, and T006's retry semantics require a fresh delivery identity; the old row stays as an audit fact |
//! | `awaiting_outcome` | obtain the outcome, or re-dispatch as a new attempt | the delivery was acknowledged, so the work may already have run; dropping the row would lose that fact |
//!
//! **Nothing in this module writes.** [`plan_recovery`] is a read, so it is safe
//! to call repeatedly, to call before deciding anything, and to call from a test
//! that asserts the store did not change.
//!
//! # Startup `agents` synchronisation
//!
//! [`sync_agents`] is the other half of "the database is the source of truth":
//! `deliveries.target_endpoint_id` is a foreign key into `agents`, so the
//! configuration snapshot must be written **before** any delivery or `Dispatched`
//! event is persisted. See [`crate::storage`]'s write-order contract.

use crate::bus::EndpointRegistry;
use crate::models::TaskId;
use crate::storage::{Delivery, Repository, StorageError};

/// Everything a restarting bridge needs to know, read in one call.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoveryPlan {
    /// Tasks whose latest event is not terminal, including tasks with no events.
    pub unfinished: Vec<TaskId>,
    /// Deliveries that were never acknowledged — re-dispatch these.
    pub unacknowledged: Vec<Delivery>,
    /// Acknowledged deliveries whose task never reached a terminal state.
    pub awaiting_outcome: Vec<Delivery>,
}

/// Stable, redacted startup diagnosis used by isolated recovery probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryBlocker {
    Owner,
    Admission,
    Unfinished,
    Unacknowledged,
    Outcome,
    RuntimeReady,
    RuntimeAmbiguous,
    ActiveLease,
    RecoveryLease,
    RecoveryContinuation,
    Capacity,
}

impl RecoveryBlocker {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Admission => "admission",
            Self::Unfinished => "unfinished",
            Self::Unacknowledged => "unacknowledged",
            Self::Outcome => "outcome",
            Self::RuntimeReady => "runtime-ready",
            Self::RuntimeAmbiguous => "runtime-ambiguous",
            Self::ActiveLease => "active-lease",
            Self::RecoveryLease => "recovery-lease",
            Self::RecoveryContinuation => "recovery-continuation",
            Self::Capacity => "capacity",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoverySignals {
    pub owns_runtime: bool,
    pub admission_blocked: bool,
    pub unfinished: usize,
    pub unacknowledged: usize,
    pub awaiting_outcome: usize,
    pub runtime_ready: usize,
    pub runtime_ambiguous: usize,
    pub active_leases: usize,
    pub recovery_leases: usize,
    pub recovery_continuations: usize,
    pub capacity_overflow: bool,
}

/// Deterministic signal fixtures for isolated startup tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryFixture;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessProbe {
    pub pid: u32,
    pub port: u16,
    pub card_status: u16,
    pub content_type: &'static str,
    pub listener_identity: u64,
}

#[allow(dead_code)]
impl ReadinessProbe {
    pub fn owned(pid: u32, identity: u64) -> Self {
        Self {
            pid,
            port: 9292,
            card_status: 200,
            content_type: "application/json",
            listener_identity: identity,
        }
    }

    pub fn is_ready_for(&self, expected_pid: u32, expected_identity: u64) -> bool {
        self.pid == expected_pid
            && self.port == 9292
            && self.card_status == 200
            && self.content_type == "application/json"
            && self.listener_identity == expected_identity
    }
}

impl RecoveryFixture {
    pub fn from_facts(
        owns_runtime: bool,
        admission_blocked: bool,
        plan: &RecoveryPlan,
        runtime_ready: usize,
        runtime_ambiguous: usize,
        active_leases: usize,
        recovery_leases: usize,
        recovery_continuations: usize,
    ) -> RecoverySignals {
        RecoverySignals {
            owns_runtime,
            admission_blocked,
            unfinished: plan.unfinished.len(),
            unacknowledged: plan.unacknowledged.len(),
            awaiting_outcome: plan.awaiting_outcome.len(),
            runtime_ready,
            runtime_ambiguous,
            active_leases,
            recovery_leases,
            recovery_continuations,
            capacity_overflow: false,
        }
    }

    pub const fn clean() -> RecoverySignals {
        RecoverySignals {
            owns_runtime: true,
            ..RecoverySignals {
                owns_runtime: false,
                admission_blocked: false,
                unfinished: 0,
                unacknowledged: 0,
                awaiting_outcome: 0,
                runtime_ready: 0,
                runtime_ambiguous: 0,
                active_leases: 0,
                recovery_leases: 0,
                recovery_continuations: 0,
                capacity_overflow: false,
            }
        }
    }

    pub const fn eligible_stale_owner() -> RecoverySignals {
        RecoverySignals {
            owns_runtime: false,
            ..Self::clean()
        }
    }

    pub const fn ambiguous_active_owner() -> RecoverySignals {
        RecoverySignals {
            owns_runtime: true,
            runtime_ambiguous: 1,
            ..Self::clean()
        }
    }

    pub const fn recovery_lease() -> RecoverySignals {
        RecoverySignals {
            owns_runtime: true,
            recovery_leases: 1,
            ..Self::clean()
        }
    }

    pub const fn recovery_continuation() -> RecoverySignals {
        RecoverySignals {
            owns_runtime: true,
            recovery_continuations: 1,
            ..Self::clean()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryDiagnostic {
    pub status: u8,
    pub blocker: Option<RecoveryBlocker>,
    pub counts: RecoverySignals,
}

impl RecoveryDiagnostic {
    pub fn classify(signals: RecoverySignals) -> Self {
        let blocker = if !signals.owns_runtime {
            Some(RecoveryBlocker::Owner)
        } else if signals.admission_blocked {
            Some(RecoveryBlocker::Admission)
        } else if signals.unfinished != 0 {
            Some(RecoveryBlocker::Unfinished)
        } else if signals.unacknowledged != 0 {
            Some(RecoveryBlocker::Unacknowledged)
        } else if signals.awaiting_outcome != 0 {
            Some(RecoveryBlocker::Outcome)
        } else if signals.runtime_ready != 0 {
            Some(RecoveryBlocker::RuntimeReady)
        } else if signals.runtime_ambiguous != 0 {
            Some(RecoveryBlocker::RuntimeAmbiguous)
        } else if signals.active_leases != 0 {
            Some(RecoveryBlocker::ActiveLease)
        } else if signals.recovery_leases != 0 {
            Some(RecoveryBlocker::RecoveryLease)
        } else if signals.recovery_continuations != 0 {
            Some(RecoveryBlocker::RecoveryContinuation)
        } else if signals.capacity_overflow {
            Some(RecoveryBlocker::Capacity)
        } else {
            None
        };
        Self {
            status: u8::from(blocker.is_some()) * 3,
            blocker,
            counts: signals,
        }
    }

    pub fn line(&self) -> String {
        let blocking = self.blocker.map_or("none", RecoveryBlocker::name);
        let c = self.counts;
        format!(
            "t050-recovery status={} blocking={} counts=unfinished:{},unacknowledged:{},outcome:{},runtime-ready:{},runtime-ambiguous:{},active-lease:{},recovery-lease:{},recovery-continuation:{},capacity:{}",
            self.status,
            blocking,
            c.unfinished,
            c.unacknowledged,
            c.awaiting_outcome,
            c.runtime_ready,
            c.runtime_ambiguous,
            c.active_leases,
            c.recovery_leases,
            c.recovery_continuations,
            u8::from(c.capacity_overflow)
        )
    }
}

impl RecoveryPlan {
    /// Whether the store has nothing to recover.
    ///
    /// A clean shutdown that drained every task and reached every terminal state
    /// produces an empty plan; so does a fresh database.
    pub fn is_empty(&self) -> bool {
        self.unfinished.is_empty()
            && self.unacknowledged.is_empty()
            && self.awaiting_outcome.is_empty()
    }
}

/// Read the three recovery inputs.
///
/// Read-only and idempotent: calling it twice returns the same plan and leaves
/// the store unchanged. Each set comes from exactly one frozen query, so the
/// semantics (terminal statuses, the "no events" case, the acknowledgement
/// condition) live in one place — the SQL behind `SqliteRepository`.
///
/// # Errors
///
/// Propagates [`StorageError`] from the three reads.
pub async fn plan_recovery(repository: &dyn Repository) -> Result<RecoveryPlan, StorageError> {
    Ok(RecoveryPlan {
        unfinished: repository.unfinished_tasks().await?,
        unacknowledged: repository.unacknowledged_deliveries().await?,
        awaiting_outcome: repository.deliveries_awaiting_outcome().await?,
    })
}

/// Write the configuration's endpoint snapshot into `agents`, returning how many
/// endpoints were written.
///
/// Only **addressable** endpoints are written: `AgentEndpoint` has a mandatory
/// address, so a declared-but-unaddressable endpoint (v1 `matrix`/`http`) has no
/// model form to store. Those rows are never created by this function, and
/// `agents`/`get_agent` do not surface them (see
/// [`crate::storage::SqliteRepository`]).
///
/// Idempotent: [`Repository::upsert_agent`] updates an existing `agent_id` in
/// place, so calling this on every start is safe and leaves no duplicates.
///
/// # Errors
///
/// Propagates [`StorageError`] from the upserts. [`StorageError::IntegrityViolation`]
/// means an `agent_id` is already mapped to a different endpoint identity — which
/// ADR-003 makes impossible for a correct caller, so it is reported rather than
/// papered over.
pub async fn sync_agents(
    repository: &dyn Repository,
    registry: &EndpointRegistry,
) -> Result<usize, StorageError> {
    let mut written = 0;
    for declared in registry.iter() {
        let Some(endpoint) = declared.agent_endpoint() else {
            continue;
        };
        repository
            .upsert_agent(declared.agent_id(), &endpoint)
            .await?;
        written += 1;
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_plan_is_reported_as_empty() {
        assert!(RecoveryPlan::default().is_empty());

        let plan = RecoveryPlan {
            unfinished: vec![TaskId::generate()],
            ..RecoveryPlan::default()
        };
        assert!(!plan.is_empty());
    }

    #[test]
    fn diagnostic_uses_fixed_precedence_and_redacted_line() {
        let diagnostic = RecoveryDiagnostic::classify(RecoverySignals {
            owns_runtime: true,
            unfinished: 2,
            active_leases: 1,
            ..RecoverySignals::default()
        });
        assert_eq!(diagnostic.status, 3);
        assert_eq!(diagnostic.blocker, Some(RecoveryBlocker::Unfinished));
        assert_eq!(
            diagnostic.line(),
            "t050-recovery status=3 blocking=unfinished counts=unfinished:2,unacknowledged:0,outcome:0,runtime-ready:0,runtime-ambiguous:0,active-lease:1,recovery-lease:0,recovery-continuation:0,capacity:0"
        );
    }

    #[test]
    fn clean_and_ambiguous_fixtures_are_distinct_without_mutation() {
        let clean = RecoveryDiagnostic::classify(RecoverySignals {
            owns_runtime: true,
            ..Default::default()
        });
        assert_eq!(clean.status, 0);
        assert_eq!(clean.blocker, None);
        let ambiguous = RecoveryDiagnostic::classify(RecoverySignals {
            owns_runtime: true,
            runtime_ambiguous: 1,
            ..Default::default()
        });
        assert_eq!(ambiguous.blocker, Some(RecoveryBlocker::RuntimeAmbiguous));
        assert_ne!(clean.line(), ambiguous.line());
    }

    #[test]
    fn fixture_matrix_has_stable_status_and_owner_precedence() {
        let cases = [
            (RecoveryFixture::clean(), 0, "none"),
            (RecoveryFixture::eligible_stale_owner(), 3, "owner"),
            (
                RecoveryFixture::ambiguous_active_owner(),
                3,
                "runtime-ambiguous",
            ),
            (RecoveryFixture::recovery_lease(), 3, "recovery-lease"),
            (
                RecoveryFixture::recovery_continuation(),
                3,
                "recovery-continuation",
            ),
        ];
        for (signals, status, blocker) in cases {
            let diagnostic = RecoveryDiagnostic::classify(signals);
            assert_eq!(diagnostic.status, status);
            assert!(diagnostic.line().contains(&format!("blocking={blocker}")));
            assert!(!diagnostic.line().contains("token"));
        }
    }

    #[test]
    fn readiness_harness_rejects_stale_identity_and_models_shutdown_repeat() {
        let ready = ReadinessProbe::owned(1712267, 44);
        assert!(ready.is_ready_for(1712267, 44));
        assert!(!ready.is_ready_for(1712266, 44));
        assert!(!ready.is_ready_for(1712267, 43));
        let shutdown = ReadinessProbe {
            port: 0,
            card_status: 0,
            ..ready.clone()
        };
        assert!(!shutdown.is_ready_for(1712267, 44));
        let repeat = ReadinessProbe::owned(1712268, 45);
        assert!(repeat.is_ready_for(1712268, 45));
    }
}
