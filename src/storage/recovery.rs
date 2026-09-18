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
}
