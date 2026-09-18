//! The persisted task-tree cycle gate (T009).
//!
//! [`detect`] walks the durable `parent_task_id` chain of a task that is about to
//! be submitted and reports the first structural loop it finds:
//!
//! ```text
//! VisitedAgent    a `to_agent` repeats along the ancestor chain  (A → B → A)
//! ChainTooLong    the observed chain is deeper than the ceiling
//! TooManySubtasks the task's parent already has the maximum number of children
//! ```
//!
//! # Why this lives here and not in the worker
//!
//! T006's worker owns the *declared* limits: it blocks a task whose own `depth`
//! or `hops` field exceeds `Config.bridge`. Those checks need no I/O and run on
//! every dispatch. They cannot see, however, **who** was visited or **how many**
//! children a parent has — that knowledge only exists in the persisted task tree.
//!
//! T009 therefore adds the durable half, and the two layers do not overlap:
//!
//! | dimension | T006 (`Worker`, in-memory) | T009 (this module) |
//! |-----------|---------------------------|--------------------|
//! | `depth` ceiling | the task's **declared** `depth` field | the **observed** chain length, same configured ceiling |
//! | `hops` ceiling | the task's `hops` field | not checked: the tree carries no hop evidence |
//! | visited agent | not implementable (no field, no task store) | the ancestor `to_agent` set |
//! | subtask count | not implementable (no counter) | `child_tasks(parent).len()` |
//! | when | before every dispatch, no I/O | submission/recovery gate, bounded I/O |
//!
//! Both layers reuse the `Failed { error }` terminal shape, so a blocked task
//! looks the same downstream wherever it was caught.
//!
//! # Wiring (T009 delivers the primitive; the caller wires it)
//!
//! ```text
//! let hit = detect(&*repository, &task, limits).await?;
//! match hit {
//!     None => { /* submit normally */ }
//!     Some(hit) => {
//!         // Accept the task first so the block is durable and visible to T012's
//!         // call-chain projection ...
//!         repository.insert_task_and_event(&task, &queued_seq1).await?;
//!         // ... then record the terminal event at seq = 2.
//!         repository.append_event(&hit.failed_event(now)).await?;
//!         // ... and tell the submitter it was blocked.
//!     }
//! }
//! ```
//!
//! Nothing in this crate wires that in: submission policy belongs to T011, and
//! assembly to T017. [`detect`] is read-only, so a caller that wants to reject
//! before writing anything can do that too — the task tree simply has no record
//! of the attempt.
//!
//! # Bounded and deterministic
//!
//! The walk reads at most `max_chain + 1` ancestors — the ceiling plus one, which
//! is exactly the read that decides between `VisitedAgent` and `ChainTooLong` when
//! the collision lands on the boundary — so it terminates even against a corrupted
//! table with a cyclic `parent_task_id` chain, and it keeps no visited set of its
//! own. The reported kind is stable, and independent of where a collision happens
//! to land: a revisit outranks a too-long chain (nearest ancestor first), and the
//! subtask count is checked last.

use chrono::{DateTime, Utc};

use crate::config::BridgeConfig;
use crate::models::{
    AgentTask, EndpointId, EventId, TaskEvent, TaskEventPayload, TaskId, TaskStatus,
};
use crate::storage::{Repository, StorageError};

/// The `seq` a cycle-block terminal event is written at.
///
/// The task has already been accepted (`Queued` took `seq = 1`), so the block is
/// the first event the gate owns — the same landing point T006's cycle limit uses.
const CYCLE_FAILED_SEQ: u64 = 2;

/// The default ceiling on a parent's direct children.
///
/// `Config.bridge` has no field for this (adding one is a public-contract change
/// owned by T016/T017), so it is a constant here and [`CycleLimits::from_bridge`]
/// is the single place that would read a future field. The value is deliberately
/// far above a realistic fan-out (a handful of subtasks per parent) and far below
/// a runaway loop.
pub const DEFAULT_MAX_SUBTASKS: u32 = 32;

/// The default chain ceiling, matching `Config.bridge.max_task_depth`'s default.
const DEFAULT_MAX_CHAIN: u32 = 8;

/// The ceilings [`detect`] enforces.
///
/// Both are ceilings, not budgets: a chain of exactly `max_chain` ancestors is
/// allowed, and a parent with exactly `max_subtasks` children blocks the next
/// one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CycleLimits {
    /// Maximum number of ancestors a task may have in the persisted tree.
    pub max_chain: u32,
    /// Maximum number of direct children a task's parent may already have.
    pub max_subtasks: u32,
}

impl CycleLimits {
    /// Take the ceilings from a validated bridge configuration.
    ///
    /// `max_chain` is `Config.bridge.max_task_depth` — the same configured value
    /// T006 applies to the *declared* `depth`, applied here to the *observed*
    /// chain. `max_subtasks` has no configuration field yet (see
    /// [`DEFAULT_MAX_SUBTASKS`]).
    pub fn from_bridge(bridge: &BridgeConfig) -> Self {
        Self {
            max_chain: bridge.max_task_depth,
            max_subtasks: DEFAULT_MAX_SUBTASKS,
        }
    }
}

impl Default for CycleLimits {
    /// The configuration defaults: depth 8 and [`DEFAULT_MAX_SUBTASKS`].
    ///
    /// Pinned by a unit test against the real configuration defaults, so the two
    /// definitions of the same policy cannot drift apart.
    fn default() -> Self {
        Self {
            max_chain: DEFAULT_MAX_CHAIN,
            max_subtasks: DEFAULT_MAX_SUBTASKS,
        }
    }
}

/// Which structural loop [`detect`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleKind {
    /// The task's `to_agent` already appears as a `to_agent` further up the chain.
    VisitedAgent,
    /// The observed ancestor chain is longer than the ceiling.
    ChainTooLong,
    /// The task's parent already has the maximum number of direct children.
    TooManySubtasks,
}

/// A detected structural loop, with the bounded identity set that describes it.
///
/// The fields are exactly the identifiers that are safe to render: task identity,
/// the two endpoints and the observed values. The task's `text` is never read
/// here, and no address or credential is involved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleHit {
    kind: CycleKind,
    task_id: TaskId,
    root_task_id: TaskId,
    parent_task_id: Option<TaskId>,
    from_agent: EndpointId,
    to_agent: EndpointId,
    observed: u32,
    limit: u32,
    revisited: Option<EndpointId>,
}

impl CycleHit {
    /// Which loop was found.
    pub fn kind(&self) -> CycleKind {
        self.kind
    }

    /// The observed value: the chain length (including the offending ancestor) or
    /// the parent's current child count.
    pub fn observed(&self) -> u32 {
        self.observed
    }

    /// The ceiling the observed value crossed.
    pub fn limit(&self) -> u32 {
        self.limit
    }

    /// The endpoint that was revisited, for [`CycleKind::VisitedAgent`].
    pub fn revisited(&self) -> Option<EndpointId> {
        self.revisited
    }

    /// The bounded reason recorded in the task's `Failed` event.
    ///
    /// Contains only the task's identity, the two endpoint ids, the observed
    /// values and their ceilings. It never renders `AgentTask::text`, an
    /// `EndpointAddress` (command, args, user id, url) or a credential.
    pub fn reason(&self) -> String {
        let hit = match self.kind {
            CycleKind::VisitedAgent => "visited-agent",
            CycleKind::ChainTooLong => "chain-too-long",
            CycleKind::TooManySubtasks => "too-many-subtasks",
        };
        let parent = self
            .parent_task_id
            .map_or_else(|| "none".to_owned(), |parent| parent.to_string());
        let mut reason = format!(
            "visit cycle detected (hit: {hit}): task_id={} root_task_id={} parent_task_id={parent} \
             from_agent={} to_agent={}",
            self.task_id, self.root_task_id, self.from_agent, self.to_agent,
        );
        match self.kind {
            CycleKind::VisitedAgent => {
                let revisited = self
                    .revisited
                    .map_or_else(|| "unknown".to_owned(), |agent| agent.to_string());
                reason.push_str(&format!(
                    " chain={} (max {}) revisited={revisited}",
                    self.observed, self.limit
                ));
            }
            CycleKind::ChainTooLong => {
                reason.push_str(&format!(" chain={} (max {})", self.observed, self.limit))
            }
            CycleKind::TooManySubtasks => reason.push_str(&format!(
                " parent_children={} (max {})",
                self.observed, self.limit
            )),
        }
        reason
    }

    /// The terminal event that blocks the detected task: `Failed { error }` at
    /// `seq = 2`, mirroring T006's cycle-limit landing point.
    ///
    /// The event belongs to the task [`detect`] was given (captured in the hit),
    /// so it cannot be written against a different task by mistake. The caller
    /// must have accepted the task first (`Queued` at `seq = 1`) — the foreign key
    /// requires the row, and `seq` must stay contiguous.
    pub fn failed_event(&self, at: DateTime<Utc>) -> TaskEvent {
        TaskEvent {
            id: EventId::generate(),
            task_id: self.task_id,
            seq: CYCLE_FAILED_SEQ,
            status: TaskStatus::Failed,
            timestamp: at,
            payload: TaskEventPayload::Failed {
                error: self.reason(),
            },
        }
    }
}

/// Look for a structural loop in the persisted task tree.
///
/// Read-only: nothing is written, so a caller may run it as often as it likes
/// (including on every submission) and decide separately what to do with a hit.
///
/// Reads at most `max_chain + 1` ancestors, then one `child_tasks` query when the
/// task has a parent.
///
/// Returns `Ok(None)` for a task that is within its limits, including a task with
/// no parent and a task whose parent is not stored: `parent_task_id` is a foreign
/// key, so an unknown parent is the database's error to report, not this gate's.
///
/// # Errors
///
/// Propagates [`StorageError`] from the tree reads; no other failure mode exists.
pub async fn detect(
    repository: &dyn Repository,
    task: &AgentTask,
    limits: CycleLimits,
) -> Result<Option<CycleHit>, StorageError> {
    let Some(parent) = task.parent_task_id else {
        // A root task has no ancestors and no parent to count children for.
        return Ok(None);
    };

    let mut cursor = Some(parent);
    let mut steps: u32 = 0;
    while let Some(ancestor_id) = cursor {
        steps += 1;
        // The ceiling bounds the walk, but it is not *decided* until the ancestor
        // has been read: a revisit outranks a too-long chain (D11), including a
        // revisit that sits one step past the ceiling. Reading first is what makes
        // the reported kind independent of where the collision happens to land.
        let over_chain_limit = steps > limits.max_chain;
        let ancestor = repository.get_task(ancestor_id).await?;

        if let Some(ancestor) = &ancestor {
            if ancestor.to_agent == task.to_agent {
                return Ok(Some(hit(
                    task,
                    parent,
                    CycleKind::VisitedAgent,
                    steps,
                    limits.max_chain,
                    Some(ancestor.to_agent),
                )));
            }
        }
        if over_chain_limit {
            return Ok(Some(hit(
                task,
                parent,
                CycleKind::ChainTooLong,
                steps,
                limits.max_chain,
                None,
            )));
        }
        let Some(ancestor) = ancestor else {
            // An unknown ancestor ends the walk: whether `parent_task_id` names a
            // stored row is the foreign key's business, not this gate's.
            break;
        };
        cursor = ancestor.parent_task_id;
    }

    let children = repository.child_tasks(parent).await?;
    let count = u32::try_from(children.len()).unwrap_or(u32::MAX);
    if count >= limits.max_subtasks {
        return Ok(Some(hit(
            task,
            parent,
            CycleKind::TooManySubtasks,
            count,
            limits.max_subtasks,
            None,
        )));
    }

    Ok(None)
}

/// Build a hit for `task` under `parent`.
///
/// Every hit carries the same identity set and the values of its own dimension, so
/// the three branches above cannot drift apart field by field.
fn hit(
    task: &AgentTask,
    parent: TaskId,
    kind: CycleKind,
    observed: u32,
    limit: u32,
    revisited: Option<EndpointId>,
) -> CycleHit {
    CycleHit {
        kind,
        task_id: task.task_id,
        root_task_id: task.root_task_id,
        parent_task_id: Some(parent),
        from_agent: task.from_agent,
        to_agent: task.to_agent,
        observed,
        limit,
        revisited,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::config::{Config, load_from_str_with_env};

    fn load(toml: &str) -> Config {
        let mut env = BTreeMap::new();
        env.insert("HOME".to_string(), "/home/tester".to_string());
        load_from_str_with_env(toml, &env).expect("test config must be valid")
    }

    #[test]
    fn default_limits_match_the_config_defaults() {
        let config = load("");
        assert_eq!(
            CycleLimits::default(),
            CycleLimits::from_bridge(&config.bridge),
            "the fallback and the configuration must state one policy"
        );
        assert_eq!(CycleLimits::default().max_chain, 8);
        assert_eq!(CycleLimits::default().max_subtasks, DEFAULT_MAX_SUBTASKS);
    }

    #[test]
    fn limits_take_the_configured_depth_and_keep_the_subtask_default() {
        let config = load(
            r#"
[bridge]
max_task_depth = 3
max_task_hops = 7
"#,
        );
        assert_eq!(
            CycleLimits::from_bridge(&config.bridge),
            CycleLimits {
                max_chain: 3,
                max_subtasks: DEFAULT_MAX_SUBTASKS,
            }
        );
    }
}
