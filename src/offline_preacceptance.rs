//! Explicit offline closure for a proven pre-acceptance dispatch refusal.
use crate::models::{EventId, TaskEvent, TaskEventPayload, TaskId, TaskStatus};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PreAcceptanceError {
    #[error("pre-acceptance recovery rejected: invalid arguments")]
    InvalidArguments,
    #[error("pre-acceptance recovery rejected: database error")]
    Database,
    #[error("pre-acceptance recovery rejected: not eligible")]
    NotEligible,
    #[error("pre-acceptance recovery rejected: busy")]
    Busy,
    #[error("pre-acceptance recovery rejected: fencing conflict")]
    Fenced,
    #[error("pre-acceptance recovery already reconciled")]
    AlreadyReconciled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreAcceptanceTuple {
    pub delivery_id: String,
    pub task_id: String,
    pub attempt: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreAcceptanceOutcome {
    Recovered { count: usize },
    AlreadyReconciled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PlannerSets {
    unfinished: BTreeSet<String>,
    unacknowledged: BTreeSet<String>,
    awaiting_outcome: BTreeSet<String>,
}

impl PlannerSets {
    async fn read(c: &mut SqliteConnection) -> Result<Self, PreAcceptanceError> {
        let unfinished = sqlx::query_scalar::<_, String>("SELECT t.task_id FROM tasks t LEFT JOIN task_events e ON e.task_id=t.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=t.task_id) WHERE e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled') ORDER BY t.task_id")
            .fetch_all(&mut *c).await.map_err(|_| PreAcceptanceError::Database)?.into_iter().collect();
        let unacknowledged = sqlx::query("SELECT delivery_id,task_id,attempt FROM deliveries WHERE acknowledged_at IS NULL ORDER BY delivery_id")
            .fetch_all(&mut *c).await.map_err(|_| PreAcceptanceError::Database)?.iter().map(|row| {
                Ok(format!("{}:{}:{}", row.try_get::<String, _>("delivery_id").map_err(|_| PreAcceptanceError::Database)?, row.try_get::<String, _>("task_id").map_err(|_| PreAcceptanceError::Database)?, row.try_get::<i64, _>("attempt").map_err(|_| PreAcceptanceError::Database)?))
            }).collect::<Result<_, PreAcceptanceError>>()?;
        let awaiting_outcome = sqlx::query("SELECT d.delivery_id,d.task_id,d.attempt FROM deliveries d LEFT JOIN task_events e ON e.task_id=d.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=d.task_id) WHERE d.acknowledged_at IS NOT NULL AND (e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled')) ORDER BY d.delivery_id")
            .fetch_all(&mut *c).await.map_err(|_| PreAcceptanceError::Database)?.iter().map(|row| {
                Ok(format!("{}:{}:{}", row.try_get::<String, _>("delivery_id").map_err(|_| PreAcceptanceError::Database)?, row.try_get::<String, _>("task_id").map_err(|_| PreAcceptanceError::Database)?, row.try_get::<i64, _>("attempt").map_err(|_| PreAcceptanceError::Database)?))
            }).collect::<Result<_, PreAcceptanceError>>()?;
        Ok(Self {
            unfinished,
            unacknowledged,
            awaiting_outcome,
        })
    }

    fn remove_targets(&mut self, tuples: &[PreAcceptanceTuple]) -> Result<(), PreAcceptanceError> {
        for tuple in tuples {
            let identity = format!("{}:{}:{}", tuple.delivery_id, tuple.task_id, tuple.attempt);
            if !self.unfinished.remove(&tuple.task_id)
                || !self.unacknowledged.remove(&identity)
                || self.awaiting_outcome.contains(&identity)
            {
                return Err(PreAcceptanceError::NotEligible);
            }
        }
        Ok(())
    }

    fn matches_plan(&self, plan: &crate::storage::RecoveryPlan) -> bool {
        let unfinished: BTreeSet<_> = plan.unfinished.iter().map(ToString::to_string).collect();
        let encode = |items: &[crate::storage::Delivery]| -> BTreeSet<String> {
            items
                .iter()
                .map(|d| format!("{}:{}:{}", d.delivery_id(), d.task_id(), d.attempt()))
                .collect()
        };
        self.unfinished == unfinished
            && self.unacknowledged == encode(&plan.unacknowledged)
            && self.awaiting_outcome == encode(&plan.awaiting_outcome)
    }
}

pub fn parse_args(
    args: &[std::ffi::OsString],
) -> Result<(PathBuf, Vec<PreAcceptanceTuple>), PreAcceptanceError> {
    if args.len() < 7 || args[0] != "recover-preacceptance" {
        return Err(PreAcceptanceError::InvalidArguments);
    }
    let mut db = None;
    let mut tuples = Vec::new();
    let mut i = 1;
    while i < args.len() {
        match args[i]
            .to_str()
            .ok_or(PreAcceptanceError::InvalidArguments)?
        {
            "--database" => {
                if db.is_some() || i + 1 >= args.len() {
                    return Err(PreAcceptanceError::InvalidArguments);
                }
                db = Some(
                    args[i + 1]
                        .to_str()
                        .ok_or(PreAcceptanceError::InvalidArguments)?
                        .to_owned(),
                );
                i += 2;
            }
            "--delivery" => {
                if i + 3 >= args.len() {
                    return Err(PreAcceptanceError::InvalidArguments);
                }
                let d = args[i + 1]
                    .to_str()
                    .ok_or(PreAcceptanceError::InvalidArguments)?
                    .to_owned();
                let t = args[i + 2]
                    .to_str()
                    .ok_or(PreAcceptanceError::InvalidArguments)?
                    .to_owned();
                let a = args[i + 3]
                    .to_str()
                    .ok_or(PreAcceptanceError::InvalidArguments)?
                    .parse()
                    .map_err(|_| PreAcceptanceError::InvalidArguments)?;
                if d.is_empty() || t.is_empty() || a < 1 {
                    return Err(PreAcceptanceError::InvalidArguments);
                }
                tuples.push(PreAcceptanceTuple {
                    delivery_id: d,
                    task_id: t,
                    attempt: a,
                });
                i += 4;
            }
            _ => return Err(PreAcceptanceError::InvalidArguments),
        }
    }
    if db.as_deref().unwrap_or("").is_empty() || tuples.is_empty() {
        return Err(PreAcceptanceError::InvalidArguments);
    }
    let mut seen = std::collections::HashSet::new();
    if tuples
        .iter()
        .any(|t| !seen.insert((&t.delivery_id, &t.task_id, t.attempt)))
    {
        return Err(PreAcceptanceError::InvalidArguments);
    }
    Ok((PathBuf::from(db.unwrap()), tuples))
}

pub async fn recover(
    database: impl AsRef<Path>,
    tuples: &[PreAcceptanceTuple],
    now: &str,
) -> Result<PreAcceptanceOutcome, PreAcceptanceError> {
    if tuples.is_empty() || now.is_empty() {
        return Err(PreAcceptanceError::InvalidArguments);
    }
    let mut seen = std::collections::HashSet::new();
    if tuples
        .iter()
        .any(|t| !seen.insert((&t.delivery_id, &t.task_id, t.attempt)))
    {
        return Err(PreAcceptanceError::InvalidArguments);
    }
    let opts = SqliteConnectOptions::new()
        .filename(database.as_ref())
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(5));
    let mut c = SqliteConnection::connect_with(&opts)
        .await
        .map_err(|_| PreAcceptanceError::Database)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut c)
        .await
        .map_err(|e| {
            if is_busy(&e) {
                PreAcceptanceError::Busy
            } else {
                PreAcceptanceError::Database
            }
        })?;
    let result = recover_tx(&mut c, tuples, now).await;
    match result {
        Ok((n, expected_after)) => {
            sqlx::query("COMMIT")
                .execute(&mut c)
                .await
                .map_err(|_| PreAcceptanceError::Database)?;
            c.close().await.map_err(|_| PreAcceptanceError::Database)?;
            let pool = crate::storage::connect(database.as_ref())
                .await
                .map_err(|_| PreAcceptanceError::Database)?;
            let repository = crate::storage::SqliteRepository::new(pool.clone());
            let plan = crate::storage::plan_recovery(&repository)
                .await
                .map_err(|_| PreAcceptanceError::Database)?;
            pool.close().await;
            if !expected_after.matches_plan(&plan) {
                return Err(PreAcceptanceError::Fenced);
            }
            if n == 0 {
                Ok(PreAcceptanceOutcome::AlreadyReconciled)
            } else {
                Ok(PreAcceptanceOutcome::Recovered { count: n })
            }
        }
        Err(e) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut c).await;
            Err(e)
        }
    }
}

async fn recover_tx(
    c: &mut SqliteConnection,
    tuples: &[PreAcceptanceTuple],
    now: &str,
) -> Result<(usize, PlannerSets), PreAcceptanceError> {
    recover_tx_inner(c, tuples, now, None).await
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
enum TestFence {
    AdmissionRevision,
    LeaseOwner,
}

#[cfg(test)]
#[allow(dead_code)]
async fn recover_tx_with_test_fence(
    c: &mut SqliteConnection,
    tuples: &[PreAcceptanceTuple],
    now: &str,
    index: usize,
    fence: TestFence,
) -> Result<(usize, PlannerSets), PreAcceptanceError> {
    recover_tx_inner(c, tuples, now, Some((index, fence))).await
}

async fn recover_tx_inner(
    c: &mut SqliteConnection,
    tuples: &[PreAcceptanceTuple],
    now: &str,
    test_fence: Option<(usize, TestFence)>,
) -> Result<(usize, PlannerSets), PreAcceptanceError> {
    let mut pending = Vec::with_capacity(tuples.len());
    let mut already_closed = 0;
    for t in tuples {
        if validate(c, t).await? {
            pending.push(t);
        } else {
            already_closed += 1;
        }
    }
    let before = PlannerSets::read(c).await?;
    if already_closed > 0 {
        if !pending.is_empty() {
            return Err(PreAcceptanceError::NotEligible);
        }
        return Ok((0, before));
    }
    let mut expected_after = before.clone();
    expected_after.remove_targets(tuples)?;
    for (index, t) in pending.iter().enumerate() {
        #[cfg(test)]
        if let Some((fence_index, fence)) = test_fence
            && index == fence_index
        {
            close_one_inner(c, t, now, Some(fence)).await?;
            continue;
        }
        #[cfg(not(test))]
        let _ = (test_fence, index);
        close_one(c, t, now).await?;
    }
    let after = PlannerSets::read(c).await?;
    if after != expected_after {
        return Err(PreAcceptanceError::Fenced);
    }
    Ok((pending.len(), after))
}

#[cfg(test)]
async fn inject_test_fence(
    c: &mut SqliteConnection,
    t: &PreAcceptanceTuple,
    fence: TestFence,
) -> Result<(), PreAcceptanceError> {
    match fence {
        TestFence::AdmissionRevision => {
            sqlx::query("UPDATE task_admissions SET revision=revision+1 WHERE task_id=?")
                .bind(&t.task_id)
                .execute(&mut *c)
                .await
                .map_err(|_| PreAcceptanceError::Database)?;
        }
        TestFence::LeaseOwner => {
            sqlx::query(
                "UPDATE execution_leases SET owner_token='injected-stale-owner' WHERE task_id=?",
            )
            .bind(&t.task_id)
            .execute(&mut *c)
            .await
            .map_err(|_| PreAcceptanceError::Database)?;
        }
    }
    Ok(())
}

async fn validate(
    c: &mut SqliteConnection,
    t: &PreAcceptanceTuple,
) -> Result<bool, PreAcceptanceError> {
    let d = sqlx::query(
        "SELECT acknowledged_at FROM deliveries WHERE delivery_id=? AND task_id=? AND attempt=?",
    )
    .bind(&t.delivery_id)
    .bind(&t.task_id)
    .bind(t.attempt)
    .fetch_optional(&mut *c)
    .await
    .map_err(|_| PreAcceptanceError::Database)?
    .ok_or(PreAcceptanceError::NotEligible)?;
    let acknowledged = d
        .try_get::<Option<String>, _>("acknowledged_at")
        .map_err(|_| PreAcceptanceError::Database)?;
    let disposition = sqlx::query(
        "SELECT state,reason_code,session_id,child_fingerprint FROM delivery_dispositions WHERE delivery_id=? AND task_id=? AND attempt=?",
    )
    .bind(&t.delivery_id)
    .bind(&t.task_id)
    .bind(t.attempt)
    .fetch_optional(&mut *c).await.map_err(|_|PreAcceptanceError::Database)?.ok_or(PreAcceptanceError::NotEligible)?;
    let disposition_state: String = disposition
        .try_get("state")
        .map_err(|_| PreAcceptanceError::Database)?;
    if acknowledged.is_some() || disposition_state == "terminal" {
        return if validate_closed(c, t, &disposition).await? {
            Ok(false)
        } else {
            Err(PreAcceptanceError::NotEligible)
        };
    }
    if disposition_state != "prepared"
        || disposition
            .try_get::<Option<String>, _>("session_id")
            .map_err(|_| PreAcceptanceError::Database)?
            .is_some()
        || disposition
            .try_get::<Option<String>, _>("child_fingerprint")
            .map_err(|_| PreAcceptanceError::Database)?
            .is_some()
    {
        return Err(PreAcceptanceError::NotEligible);
    }
    let row = sqlx::query(
        "SELECT seq,status,payload FROM task_events WHERE task_id=? ORDER BY seq DESC LIMIT 1",
    )
    .bind(&t.task_id)
    .fetch_optional(&mut *c)
    .await
    .map_err(|_| PreAcceptanceError::Database)?
    .ok_or(PreAcceptanceError::NotEligible)?;
    if row
        .try_get::<String, _>("status")
        .map_err(|_| PreAcceptanceError::Database)?
        != "dispatched"
    {
        return Err(PreAcceptanceError::NotEligible);
    }
    let payload: String = row
        .try_get("payload")
        .map_err(|_| PreAcceptanceError::Database)?;
    let decoded: TaskEventPayload =
        serde_json::from_str(&payload).map_err(|_| PreAcceptanceError::NotEligible)?;
    match decoded {
        TaskEventPayload::Dispatched {
            delivery_id,
            attempt,
        } if delivery_id.to_string() == t.delivery_id && i64::from(attempt) == t.attempt => {}
        _ => return Err(PreAcceptanceError::NotEligible),
    }
    let event_seq: i64 = row
        .try_get("seq")
        .map_err(|_| PreAcceptanceError::Database)?;
    let task_version: i64 = sqlx::query_scalar("SELECT version FROM tasks WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_one(&mut *c)
        .await
        .map_err(|_| PreAcceptanceError::NotEligible)?;
    if event_seq != task_version + 1 {
        return Err(PreAcceptanceError::NotEligible);
    }
    let a = sqlx::query("SELECT state FROM task_admissions WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_optional(&mut *c)
        .await
        .map_err(|_| PreAcceptanceError::Database)?
        .ok_or(PreAcceptanceError::NotEligible)?;
    if a.try_get::<String, _>("state")
        .map_err(|_| PreAcceptanceError::Database)?
        != "dispatching"
    {
        return Err(PreAcceptanceError::NotEligible);
    }
    let l = sqlx::query("SELECT state FROM execution_leases WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_optional(&mut *c)
        .await
        .map_err(|_| PreAcceptanceError::Database)?
        .ok_or(PreAcceptanceError::NotEligible)?;
    if l.try_get::<String, _>("state")
        .map_err(|_| PreAcceptanceError::Database)?
        != "recovery_needed"
    {
        return Err(PreAcceptanceError::NotEligible);
    }
    let lease_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_leases WHERE task_id=?")
            .bind(&t.task_id)
            .fetch_one(&mut *c)
            .await
            .map_err(|_| PreAcceptanceError::Database)?;
    if lease_count != 1 {
        return Err(PreAcceptanceError::NotEligible);
    }
    let q: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_work_queue WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_one(&mut *c)
        .await
        .map_err(|_| PreAcceptanceError::Database)?;
    let cont: i64 = sqlx::query_scalar("SELECT count(*) FROM task_continuations WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_one(&mut *c)
        .await
        .map_err(|_| PreAcceptanceError::Database)?;
    if q != 0 || cont != 0 {
        return Err(PreAcceptanceError::NotEligible);
    }
    Ok(true)
}

async fn validate_closed(
    c: &mut SqliteConnection,
    t: &PreAcceptanceTuple,
    disposition: &sqlx::sqlite::SqliteRow,
) -> Result<bool, PreAcceptanceError> {
    let reason: Option<String> = disposition
        .try_get("reason_code")
        .map_err(|_| PreAcceptanceError::Database)?;
    if reason.as_deref() != Some("pre_acceptance_auth_failure")
        || disposition
            .try_get::<Option<String>, _>("session_id")
            .map_err(|_| PreAcceptanceError::Database)?
            .is_some()
        || disposition
            .try_get::<Option<String>, _>("child_fingerprint")
            .map_err(|_| PreAcceptanceError::Database)?
            .is_some()
    {
        return Ok(false);
    }
    let task = sqlx::query("SELECT version FROM tasks WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_optional(&mut *c)
        .await
        .map_err(|_| PreAcceptanceError::Database)?;
    let Some(task) = task else { return Ok(false) };
    let version: i64 = task
        .try_get("version")
        .map_err(|_| PreAcceptanceError::Database)?;
    let events = sqlx::query(
        "SELECT seq,status,payload FROM task_events WHERE task_id=? ORDER BY seq DESC LIMIT 2",
    )
    .bind(&t.task_id)
    .fetch_all(&mut *c)
    .await
    .map_err(|_| PreAcceptanceError::Database)?;
    if events.len() != 2 {
        return Ok(false);
    }
    let latest_seq: i64 = events[0]
        .try_get("seq")
        .map_err(|_| PreAcceptanceError::Database)?;
    if latest_seq != version + 1
        || events[0]
            .try_get::<String, _>("status")
            .map_err(|_| PreAcceptanceError::Database)?
            != "failed"
    {
        return Ok(false);
    }
    let latest_payload: String = events[0]
        .try_get("payload")
        .map_err(|_| PreAcceptanceError::Database)?;
    if !matches!(serde_json::from_str::<TaskEventPayload>(&latest_payload), Ok(TaskEventPayload::Failed { error }) if error == "pre_acceptance_auth_failure")
    {
        return Ok(false);
    }
    let dispatched_status: String = events[1]
        .try_get("status")
        .map_err(|_| PreAcceptanceError::Database)?;
    let dispatched_payload: String = events[1]
        .try_get("payload")
        .map_err(|_| PreAcceptanceError::Database)?;
    if dispatched_status != "dispatched"
        || !matches!(serde_json::from_str::<TaskEventPayload>(&dispatched_payload), Ok(TaskEventPayload::Dispatched { delivery_id, attempt }) if delivery_id.to_string() == t.delivery_id && i64::from(attempt) == t.attempt)
    {
        return Ok(false);
    }
    let admission: Option<String> =
        sqlx::query_scalar("SELECT state FROM task_admissions WHERE task_id=?")
            .bind(&t.task_id)
            .fetch_optional(&mut *c)
            .await
            .map_err(|_| PreAcceptanceError::Database)?;
    let ack: Option<String> = sqlx::query_scalar(
        "SELECT acknowledged_at FROM deliveries WHERE delivery_id=? AND task_id=? AND attempt=?",
    )
    .bind(&t.delivery_id)
    .bind(&t.task_id)
    .bind(t.attempt)
    .fetch_optional(&mut *c)
    .await
    .map_err(|_| PreAcceptanceError::Database)?
    .flatten();
    let event_timestamp: Option<String> =
        sqlx::query_scalar("SELECT timestamp FROM task_events WHERE task_id=? AND seq=?")
            .bind(&t.task_id)
            .bind(latest_seq)
            .fetch_optional(&mut *c)
            .await
            .map_err(|_| PreAcceptanceError::Database)?;
    let ack_matches_event = match (ack.as_deref(), event_timestamp.as_deref()) {
        (Some(ack), Some(event)) => {
            chrono::DateTime::parse_from_rfc3339(ack).ok()
                == chrono::DateTime::parse_from_rfc3339(event).ok()
        }
        _ => false,
    };
    let lease: Option<String> =
        sqlx::query_scalar("SELECT state FROM execution_leases WHERE task_id=?")
            .bind(&t.task_id)
            .fetch_optional(&mut *c)
            .await
            .map_err(|_| PreAcceptanceError::Database)?;
    let lease_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_leases WHERE task_id=?")
            .bind(&t.task_id)
            .fetch_one(&mut *c)
            .await
            .map_err(|_| PreAcceptanceError::Database)?;
    let queue: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_work_queue WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_one(&mut *c)
        .await
        .map_err(|_| PreAcceptanceError::Database)?;
    let continuation: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_continuations WHERE task_id=?")
            .bind(&t.task_id)
            .fetch_one(&mut *c)
            .await
            .map_err(|_| PreAcceptanceError::Database)?;
    Ok(admission.as_deref() == Some("terminal")
        && ack_matches_event
        && lease.as_deref() == Some("released")
        && lease_count == 1
        && queue == 0
        && continuation == 0)
}

async fn close_one(
    c: &mut SqliteConnection,
    t: &PreAcceptanceTuple,
    now: &str,
) -> Result<(), PreAcceptanceError> {
    close_one_inner(c, t, now, None).await
}

async fn close_one_inner(
    c: &mut SqliteConnection,
    t: &PreAcceptanceTuple,
    now: &str,
    test_fence: Option<TestFence>,
) -> Result<(), PreAcceptanceError> {
    let a=sqlx::query("SELECT revision,runtime_instance FROM task_admissions WHERE task_id=? AND state='dispatching'").bind(&t.task_id).fetch_one(&mut *c).await.map_err(|_|PreAcceptanceError::Fenced)?;
    let rev: i64 = a
        .try_get("revision")
        .map_err(|_| PreAcceptanceError::Database)?;
    let owner: Option<String> = a
        .try_get("runtime_instance")
        .map_err(|_| PreAcceptanceError::Database)?;
    let l=sqlx::query("SELECT resource_key,owner_token,fence,expires_at FROM execution_leases WHERE task_id=? AND state='recovery_needed'").bind(&t.task_id).fetch_one(&mut *c).await.map_err(|_|PreAcceptanceError::Fenced)?;
    let resource: String = l
        .try_get("resource_key")
        .map_err(|_| PreAcceptanceError::Database)?;
    let token: String = l
        .try_get("owner_token")
        .map_err(|_| PreAcceptanceError::Database)?;
    let fence: i64 = l
        .try_get("fence")
        .map_err(|_| PreAcceptanceError::Database)?;
    let expires_at: String = l
        .try_get("expires_at")
        .map_err(|_| PreAcceptanceError::Database)?;
    let task_version: i64 = sqlx::query_scalar("SELECT version FROM tasks WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_one(&mut *c)
        .await
        .map_err(|_| PreAcceptanceError::Fenced)?;
    let seq: i64 =
        sqlx::query_scalar("SELECT coalesce(max(seq),0)+1 FROM task_events WHERE task_id=?")
            .bind(&t.task_id)
            .fetch_one(&mut *c)
            .await
            .map_err(|_| PreAcceptanceError::Database)?;
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: t
            .task_id
            .parse::<TaskId>()
            .map_err(|_| PreAcceptanceError::NotEligible)?,
        seq: u64::try_from(seq).map_err(|_| PreAcceptanceError::NotEligible)?,
        status: TaskStatus::Failed,
        timestamp: chrono::DateTime::parse_from_rfc3339(now)
            .map_err(|_| PreAcceptanceError::InvalidArguments)?
            .with_timezone(&chrono::Utc),
        payload: TaskEventPayload::Failed {
            error: "pre_acceptance_auth_failure".into(),
        },
    };
    #[cfg(test)]
    if let Some(fence) = test_fence {
        inject_test_fence(c, t, fence).await?;
    }
    #[cfg(not(test))]
    let _ = test_fence;
    crate::terminal_closure::apply(
        c,
        crate::terminal_closure::ClosureContext {
            task_id: &t.task_id,
            resource_key: &resource,
            event: &event,
            now,
            delivery: crate::terminal_closure::DeliveryBinding::OfflineExact {
                delivery_id: &t.delivery_id,
                attempt: t.attempt,
            },
            admission: crate::terminal_closure::AdmissionMode::Offline {
                runtime_owner: owner.as_deref(),
                revision: rev,
            },
            task_version: crate::terminal_closure::TaskVersionMode::OfflineExpected(task_version),
            disposition_reason: Some("pre_acceptance_auth_failure"),
        },
        crate::terminal_closure::LeaseMode::OfflineRecoveryNeeded {
            owner: &token,
            fence,
            expires_at: &expires_at,
        },
    )
    .await
    .map_err(|e| match e {
        crate::terminal_closure::ClosureError::Fenced => PreAcceptanceError::Fenced,
        crate::terminal_closure::ClosureError::AdmissionStale => PreAcceptanceError::Fenced,
        crate::terminal_closure::ClosureError::Malformed => PreAcceptanceError::NotEligible,
        crate::terminal_closure::ClosureError::Database(_) => PreAcceptanceError::Database,
    })
}
fn is_busy(e: &sqlx::Error) -> bool {
    let s = e.to_string().to_ascii_lowercase();
    s.contains("busy") || s.contains("locked")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SqliteRepository;
    use std::os::unix::ffi::OsStringExt;

    async fn fixture(path: &Path, suffix: &str) -> PreAcceptanceTuple {
        let pool = crate::storage::connect(path).await.unwrap();
        let endpoint = uuid::Uuid::now_v7().to_string();
        let conversation = uuid::Uuid::now_v7().to_string();
        let task = uuid::Uuid::now_v7().to_string();
        let delivery = uuid::Uuid::now_v7().to_string();
        let resource = format!("resource-{suffix}");
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?, 'acp',1,'[]')").bind(&endpoint).bind(uuid::Uuid::now_v7().to_string()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
            .bind(&conversation)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'fixture sentinel',1,0,0,0)").bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(&pool).await.unwrap();
        let dispatched = serde_json::to_string(&TaskEventPayload::Dispatched {
            delivery_id: delivery.parse().unwrap(),
            attempt: 1,
        })
        .unwrap();
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'dispatched','2026-01-01T00:00:00Z',?)").bind(uuid::Uuid::now_v7().to_string()).bind(&task).bind(dispatched).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',4,NULL,'t','t')").bind(&task).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2026-01-01T00:00:00Z')").bind(&delivery).bind(&task).bind(&endpoint).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'prepared','before')").bind(&delivery).bind(&task).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,7,'recovery_needed','t','t','2099-01-01T00:00:00Z')").bind(resource).bind(&task).bind(format!("owner-{suffix}")).execute(&pool).await.unwrap();
        pool.close().await;
        PreAcceptanceTuple {
            delivery_id: delivery,
            task_id: task,
            attempt: 1,
        }
    }

    async fn full_snapshot(path: &Path) -> Vec<(String, String)> {
        let pool = crate::storage::connect(path).await.unwrap();
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let mut snapshot = Vec::new();
        for table in tables {
            let escaped = table.replace('"', "\"\"");
            let columns: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT name FROM pragma_table_info('{}') ORDER BY cid",
                table.replace('\'', "''")
            )))
            .fetch_all(&pool)
            .await
            .unwrap();
            let args = columns
                .iter()
                .map(|column| {
                    let quoted = column.replace('"', "\"\"");
                    format!(
                        "'{}',CASE WHEN typeof(\"{quoted}\")='blob' THEN hex(\"{quoted}\") ELSE \"{quoted}\" END",
                        column.replace('\'', "''")
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let json = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(format!(
                "SELECT coalesce(json_group_array(json_object({args})), '[]') FROM \"{escaped}\""
            )))
            .fetch_one(&pool)
            .await
            .unwrap();
            snapshot.push((table, json));
        }
        pool.close().await;
        snapshot
    }

    async fn initialized_path(prefix: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("t037-{prefix}-{}.db", uuid::Uuid::now_v7()));
        let pool = crate::storage::connect(&path).await.unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        pool.close().await;
        path
    }

    async fn assert_rejected_fixture(case: &str) {
        let path = initialized_path(case).await;
        let tuple = fixture(&path, case).await;
        let pool = crate::storage::connect(&path).await.unwrap();
        match case {
            "event:missing" => {
                sqlx::query("DELETE FROM task_events WHERE task_id=?")
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "event:malformed" => {
                let mut conn = pool.acquire().await.unwrap();
                sqlx::query("PRAGMA ignore_check_constraints=ON")
                    .execute(&mut *conn)
                    .await
                    .unwrap();
                sqlx::query("UPDATE task_events SET payload='not-json' WHERE task_id=?")
                    .bind(&tuple.task_id)
                    .execute(&mut *conn)
                    .await
                    .unwrap();
            }
            "event:wrong-variant" => {
                let payload = serde_json::to_string(&TaskEventPayload::Queued).unwrap();
                sqlx::query("UPDATE task_events SET payload=? WHERE task_id=?")
                    .bind(payload)
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "event:wrong-tuple" => {
                let payload = serde_json::to_string(&TaskEventPayload::Dispatched {
                    delivery_id: uuid::Uuid::now_v7().to_string().parse().unwrap(),
                    attempt: 2,
                })
                .unwrap();
                sqlx::query("UPDATE task_events SET payload=? WHERE task_id=?")
                    .bind(payload)
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "event:queued" | "event:running" | "event:completed" | "event:failed"
            | "event:timed_out" | "event:cancelled" => {
                sqlx::query("UPDATE task_events SET status=? WHERE task_id=?")
                    .bind(case.strip_prefix("event:").unwrap())
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "event:unknown" | "admission:unknown" | "lease:unknown" | "disposition:unknown" => {
                let mut conn = pool.acquire().await.unwrap();
                sqlx::query("PRAGMA ignore_check_constraints=ON")
                    .execute(&mut *conn)
                    .await
                    .unwrap();
                let (sql, value) = match case {
                    "event:unknown" => (
                        "UPDATE task_events SET status=? WHERE task_id=?",
                        "future_state",
                    ),
                    "admission:unknown" => (
                        "UPDATE task_admissions SET state=? WHERE task_id=?",
                        "future_state",
                    ),
                    "lease:unknown" => (
                        "UPDATE execution_leases SET state=? WHERE task_id=?",
                        "future_state",
                    ),
                    _ => (
                        "UPDATE delivery_dispositions SET state=? WHERE task_id=?",
                        "future_state",
                    ),
                };
                sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
                    .bind(value)
                    .bind(&tuple.task_id)
                    .execute(&mut *conn)
                    .await
                    .unwrap();
            }
            "admission:missing" => {
                sqlx::query("DELETE FROM task_admissions WHERE task_id=?")
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "admission:ready"
            | "admission:enqueued"
            | "admission:running"
            | "admission:terminal"
            | "admission:recovery_needed" => {
                sqlx::query("UPDATE task_admissions SET state=? WHERE task_id=?")
                    .bind(case.strip_prefix("admission:").unwrap())
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "lease:missing" => {
                sqlx::query("DELETE FROM execution_leases WHERE task_id=?")
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "lease:active" | "lease:released" => {
                sqlx::query("UPDATE execution_leases SET state=? WHERE task_id=?")
                    .bind(case.strip_prefix("lease:").unwrap())
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "lease:duplicate" => {
                let resource = format!("second-resource-{}", uuid::Uuid::now_v7());
                sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,8,'recovery_needed','t','t','2099-01-01T00:00:00Z')")
                    .bind(resource).bind(&tuple.task_id).bind("second-owner").execute(&pool).await.unwrap();
            }
            "delivery:acknowledged" => {
                sqlx::query("UPDATE deliveries SET acknowledged_at='2026-01-01T00:00:01Z' WHERE delivery_id=?")
                    .bind(&tuple.delivery_id).execute(&pool).await.unwrap();
            }
            "disposition:session-bound" => {
                sqlx::query("UPDATE delivery_dispositions SET session_id='session-sentinel' WHERE delivery_id=?")
                    .bind(&tuple.delivery_id).execute(&pool).await.unwrap();
            }
            "disposition:child-bound" => {
                sqlx::query("UPDATE delivery_dispositions SET child_fingerprint='child-sentinel' WHERE delivery_id=?")
                    .bind(&tuple.delivery_id).execute(&pool).await.unwrap();
            }
            "queue:queued"
            | "queue:claimed"
            | "queue:running"
            | "queue:paused"
            | "queue:completed"
            | "queue:expired"
            | "queue:superseded"
            | "queue:recovery_needed" => {
                let endpoint: String = sqlx::query_scalar(
                    "SELECT target_endpoint_id FROM deliveries WHERE delivery_id=?",
                )
                .bind(&tuple.delivery_id)
                .fetch_one(&pool)
                .await
                .unwrap();
                sqlx::query("INSERT INTO agent_work_queue(queue_id,task_id,delivery_id,target_endpoint_id,room_id,sender_endpoint_id,idempotency_key,body_hash,lane,state,sequence,revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'hash','ordinary',?,1,0,'t','t')")
                    .bind(uuid::Uuid::now_v7().to_string()).bind(&tuple.task_id).bind(&tuple.delivery_id).bind(endpoint).bind("room").bind("sender").bind(uuid::Uuid::now_v7().to_string()).bind(case.strip_prefix("queue:").unwrap()).execute(&pool).await.unwrap();
            }
            "continuation:ready"
            | "continuation:in_flight"
            | "continuation:recovery_needed"
            | "continuation:blocked"
            | "continuation:terminal" => {
                let resource: String =
                    sqlx::query_scalar("SELECT resource_key FROM execution_leases WHERE task_id=?")
                        .bind(&tuple.task_id)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                sqlx::query("INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes) VALUES(?,?,?,7,1,?,1,0,0,'sentinel','t','t','t',0)")
                    .bind(&tuple.task_id).bind(resource).bind(&tuple.delivery_id).bind(case.strip_prefix("continuation:").unwrap()).execute(&pool).await.unwrap();
            }
            "disposition:missing" => {
                sqlx::query("DELETE FROM delivery_dispositions WHERE delivery_id=?")
                    .bind(&tuple.delivery_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "disposition:conflicting" => {
                let endpoint: String =
                    sqlx::query_scalar("SELECT from_agent FROM tasks WHERE task_id=?")
                        .bind(&tuple.task_id)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                let conversation: String =
                    sqlx::query_scalar("SELECT conversation_id FROM tasks WHERE task_id=?")
                        .bind(&tuple.task_id)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                let other_task = uuid::Uuid::now_v7().to_string();
                sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'other',1,0,0,0)")
                    .bind(&other_task).bind(&other_task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(&pool).await.unwrap();
                sqlx::query("UPDATE delivery_dispositions SET task_id=? WHERE delivery_id=?")
                    .bind(other_task)
                    .bind(&tuple.delivery_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "disposition:acknowledged" | "disposition:outcome_unknown" | "disposition:terminal" => {
                sqlx::query("UPDATE delivery_dispositions SET state=? WHERE delivery_id=?")
                    .bind(case.strip_prefix("disposition:").unwrap())
                    .bind(&tuple.delivery_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            _ => panic!("unhandled T037 rejection case: {case}"),
        }
        pool.close().await;
        let before = full_snapshot(&path).await;
        let result = recover(&path, std::slice::from_ref(&tuple), "2026-01-02T00:00:00Z").await;
        assert_eq!(result, Err(PreAcceptanceError::NotEligible), "case={case}");
        assert_eq!(full_snapshot(&path).await, before, "case={case}");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn parser_is_strict_and_deduplicates_tuples() {
        let args = [
            "recover-preacceptance",
            "--database",
            "db",
            "--delivery",
            "d",
            "t",
            "1",
        ]
        .map(std::ffi::OsString::from);
        assert_eq!(parse_args(&args).unwrap().1.len(), 1);
        let duplicate = [
            "recover-preacceptance",
            "--database",
            "db",
            "--delivery",
            "d",
            "t",
            "1",
            "--delivery",
            "d",
            "t",
            "1",
        ]
        .map(std::ffi::OsString::from);
        assert_eq!(
            parse_args(&duplicate),
            Err(PreAcceptanceError::InvalidArguments)
        );
        let unknown = [
            "recover-preacceptance",
            "--database",
            "db",
            "--unknown",
            "x",
            "--delivery",
            "d",
            "t",
            "1",
        ]
        .map(std::ffi::OsString::from);
        assert_eq!(
            parse_args(&unknown),
            Err(PreAcceptanceError::InvalidArguments)
        );
        let non_utf8 = vec![
            std::ffi::OsString::from("recover-preacceptance"),
            std::ffi::OsString::from("--database"),
            std::ffi::OsString::from_vec(vec![0xff]),
        ];
        assert_eq!(
            parse_args(&non_utf8),
            Err(PreAcceptanceError::InvalidArguments)
        );
        let non_utf8_tuple = vec![
            std::ffi::OsString::from("recover-preacceptance"),
            std::ffi::OsString::from("--database"),
            std::ffi::OsString::from("db"),
            std::ffi::OsString::from("--delivery"),
            std::ffi::OsString::from_vec(vec![0xff]),
            std::ffi::OsString::from("task"),
            std::ffi::OsString::from("1"),
        ];
        assert_eq!(
            parse_args(&non_utf8_tuple),
            Err(PreAcceptanceError::InvalidArguments)
        );
        let empty = [
            "recover-preacceptance",
            "--database",
            "db",
            "--delivery",
            "",
            "task",
            "1",
        ]
        .map(std::ffi::OsString::from);
        assert_eq!(
            parse_args(&empty),
            Err(PreAcceptanceError::InvalidArguments)
        );
        let empty_database = [
            "recover-preacceptance",
            "--database",
            "",
            "--delivery",
            "d",
            "task",
            "1",
        ]
        .map(std::ffi::OsString::from);
        assert_eq!(
            parse_args(&empty_database),
            Err(PreAcceptanceError::InvalidArguments)
        );
    }

    #[tokio::test]
    async fn real_sqlite_success_uses_shared_core_and_preserves_unrelated_fields() {
        let path = std::env::temp_dir().join(format!("t037-{}.db", uuid::Uuid::now_v7()));
        let pool = crate::storage::connect(&path).await.unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        let endpoint = uuid::Uuid::now_v7().to_string();
        let conversation = uuid::Uuid::now_v7().to_string();
        let task = uuid::Uuid::now_v7().to_string();
        let delivery = uuid::Uuid::now_v7().to_string();
        let resource = "resource-t037";
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?, 'acp',1,'[]')").bind(&endpoint).bind(uuid::Uuid::now_v7().to_string()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
            .bind(&conversation)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'secret sentinel',1,0,0,0)").bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(&pool).await.unwrap();
        let dispatched = serde_json::to_string(&TaskEventPayload::Dispatched {
            delivery_id: delivery.parse().unwrap(),
            attempt: 1,
        })
        .unwrap();
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'dispatched',? ,?)").bind(uuid::Uuid::now_v7().to_string()).bind(&task).bind("2026-01-01T00:00:00Z").bind(dispatched).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',4,NULL,'t','t')").bind(&task).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2026-01-01T00:00:00Z')").bind(&delivery).bind(&task).bind(&endpoint).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'prepared','before')").bind(&delivery).bind(&task).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,7,'recovery_needed','t','t','2099-01-01T00:00:00Z')").bind(resource).bind(&task).bind("owner").execute(&pool).await.unwrap();
        pool.close().await;
        let tuple = PreAcceptanceTuple {
            delivery_id: delivery.clone(),
            task_id: task.clone(),
            attempt: 1,
        };
        assert_eq!(
            recover(&path, &[tuple], "2026-01-02T00:00:00Z").await,
            Ok(PreAcceptanceOutcome::Recovered { count: 1 })
        );
        let after = crate::storage::connect(&path).await.unwrap();
        let snapshot: (String, i64, String, String, String, String) = sqlx::query_as("SELECT t.text,t.version,a.state,d.acknowledged_at,p.state,l.state FROM tasks t JOIN task_admissions a ON a.task_id=t.task_id JOIN deliveries d ON d.task_id=t.task_id JOIN delivery_dispositions p ON p.task_id=t.task_id JOIN execution_leases l ON l.task_id=t.task_id WHERE t.task_id=?").bind(&task).fetch_one(&after).await.unwrap();
        assert_eq!(snapshot.0, "secret sentinel");
        assert_eq!(snapshot.1, 1);
        assert_eq!(snapshot.2, "terminal");
        assert!(!snapshot.3.is_empty());
        assert_eq!(snapshot.4, "terminal");
        assert_eq!(snapshot.5, "released");
        let terminal: (i64, Option<String>, Option<String>, Option<String>, i64, String, String) = sqlx::query_as(
            "SELECT a.revision,p.reason_code,p.session_id,p.child_fingerprint,t.version,l.state,l.expires_at FROM task_admissions a JOIN delivery_dispositions p ON p.task_id=a.task_id JOIN tasks t ON t.task_id=a.task_id JOIN execution_leases l ON l.task_id=t.task_id WHERE a.task_id=?",
        ).bind(&task).fetch_one(&after).await.unwrap();
        assert_eq!(
            terminal.0, 5,
            "captured admission revision increments exactly once"
        );
        assert_eq!(terminal.1.as_deref(), Some("pre_acceptance_auth_failure"));
        assert_eq!(terminal.2, None);
        assert_eq!(terminal.3, None);
        assert_eq!(terminal.4, 1);
        assert_eq!(terminal.5, "released");
        assert_eq!(
            terminal.6, "2099-01-01T00:00:00Z",
            "lease expiry is preserved while the fenced row is released"
        );
        let (ack_time, terminal_event_time): (String, String) = sqlx::query_as(
            "SELECT d.acknowledged_at,e.timestamp FROM deliveries d JOIN task_events e ON e.task_id=d.task_id AND e.status='failed' WHERE d.task_id=?",
        ).bind(&task).fetch_one(&after).await.unwrap();
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(&ack_time).unwrap(),
            chrono::DateTime::parse_from_rfc3339(&terminal_event_time).unwrap()
        );
        after.close().await;
        let closed_snapshot = full_snapshot(&path).await;
        assert_eq!(
            recover(
                &path,
                &[PreAcceptanceTuple {
                    delivery_id: delivery,
                    task_id: task,
                    attempt: 1,
                }],
                "2026-01-03T00:00:00Z"
            )
            .await,
            Ok(PreAcceptanceOutcome::AlreadyReconciled)
        );
        assert_eq!(full_snapshot(&path).await, closed_snapshot);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn same_database_two_tuple_success_matches_fresh_production_planner() {
        let path = std::env::temp_dir().join(format!("t037-multi-{}.db", uuid::Uuid::now_v7()));
        let pool = crate::storage::connect(&path).await.unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        pool.close().await;
        let first = fixture(&path, "first").await;
        let second = fixture(&path, "second").await;
        let unrelated = fixture(&path, "unrelated").await;
        let before = crate::storage::connect(&path).await.unwrap();
        let repository = SqliteRepository::new(before.clone());
        let plan = crate::storage::plan_recovery(&repository).await.unwrap();
        assert_eq!(plan.unfinished.len(), 3);
        assert_eq!(plan.unacknowledged.len(), 3);
        before.close().await;
        assert_eq!(
            recover(
                &path,
                &[first.clone(), second.clone()],
                "2026-01-02T00:00:00Z"
            )
            .await,
            Ok(PreAcceptanceOutcome::Recovered { count: 2 })
        );
        let after = crate::storage::connect(&path).await.unwrap();
        let repository = SqliteRepository::new(after.clone());
        let plan = crate::storage::plan_recovery(&repository).await.unwrap();
        assert_eq!(
            plan.unfinished
                .iter()
                .map(ToString::to_string)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([unrelated.task_id.clone()])
        );
        assert_eq!(plan.unacknowledged.len(), 1);
        assert_eq!(
            plan.unacknowledged[0].delivery_id().to_string(),
            unrelated.delivery_id
        );
        after.close().await;
        assert_eq!(
            recover(
                &path,
                std::slice::from_ref(&unrelated),
                "2026-01-02T00:00:00Z"
            )
            .await,
            Ok(PreAcceptanceOutcome::Recovered { count: 1 })
        );
        let after = crate::storage::connect(&path).await.unwrap();
        let repository = SqliteRepository::new(after.clone());
        let plan = crate::storage::plan_recovery(&repository).await.unwrap();
        assert!(plan.is_empty());
        let unfinished: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks t LEFT JOIN task_events e ON e.task_id=t.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=t.task_id) WHERE e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled')").fetch_one(&after).await.unwrap();
        let unacknowledged: i64 =
            sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE acknowledged_at IS NULL")
                .fetch_one(&after)
                .await
                .unwrap();
        let awaiting: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries d LEFT JOIN task_events e ON e.task_id=d.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=d.task_id) WHERE d.acknowledged_at IS NOT NULL AND (e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled'))").fetch_one(&after).await.unwrap();
        assert_eq!((unfinished, unacknowledged, awaiting), (0, 0, 0));
        after.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn actual_t033_then_t032_apis_accept_database_after_t037_closure() {
        let path = initialized_path("composition").await;
        let pool = crate::storage::connect(&path).await.unwrap();
        let owner = uuid::Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES(?,'t','t','active','composition-fp')")
            .bind(&owner).execute(&pool).await.unwrap();
        pool.close().await;
        let tuple = fixture(&path, "composition").await;
        let pool = crate::storage::connect(&path).await.unwrap();
        sqlx::query("UPDATE task_admissions SET runtime_instance=? WHERE task_id=?")
            .bind(&owner)
            .bind(&tuple.task_id)
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        assert_eq!(
            recover(&path, std::slice::from_ref(&tuple), "2026-01-02T00:00:00Z").await,
            Ok(PreAcceptanceOutcome::Recovered { count: 1 })
        );
        assert_eq!(
            crate::offline_delivery::reconcile(
                &path,
                &[crate::offline_delivery::DeliveryTuple {
                    delivery_id: tuple.delivery_id.clone(),
                    task_id: tuple.task_id.clone(),
                    attempt: tuple.attempt,
                }],
                "2026-01-02T00:30:00Z",
            )
            .await,
            Ok(1)
        );
        assert_eq!(
            crate::offline_recovery::recover_stale_runtime(
                &path,
                &owner,
                "composition-fp",
                "2026-01-03T00:00:00Z"
            )
            .await,
            Ok(crate::offline_recovery::RecoveryOutcome::Recovered)
        );
        let after = crate::storage::connect(&path).await.unwrap();
        let state: String =
            sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token=?")
                .bind(&owner)
                .fetch_one(&after)
                .await
                .unwrap();
        assert_eq!(state, "stopped");
        after.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn sqlite_busy_is_classified_and_leaves_target_fields_unchanged() {
        let path = std::env::temp_dir().join(format!("t037-busy-{}.db", uuid::Uuid::now_v7()));
        let pool = crate::storage::connect(&path).await.unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        pool.close().await;
        let tuple = fixture(&path, "busy").await;
        let before = full_snapshot(&path).await;
        let mut lock = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&path)
                .busy_timeout(std::time::Duration::from_millis(1)),
        )
        .await
        .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut lock)
            .await
            .unwrap();
        let result = recover(&path, std::slice::from_ref(&tuple), "2026-01-02T00:00:00Z").await;
        assert_eq!(result, Err(PreAcceptanceError::Busy));
        sqlx::query("ROLLBACK").execute(&mut lock).await.unwrap();
        let after = crate::storage::connect(&path).await.unwrap();
        let fields: (Option<String>, String, i64, String) = sqlx::query_as("SELECT d.acknowledged_at,p.state,a.revision,l.state FROM deliveries d JOIN delivery_dispositions p ON p.delivery_id=d.delivery_id JOIN task_admissions a ON a.task_id=d.task_id JOIN execution_leases l ON l.task_id=d.task_id WHERE d.delivery_id=?").bind(&tuple.delivery_id).fetch_one(&after).await.unwrap();
        assert_eq!(
            fields,
            (None, "prepared".into(), 4, "recovery_needed".into())
        );
        after.close().await;
        assert_eq!(full_snapshot(&path).await, before);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn late_lease_fence_rolls_back_prior_tuple_writes_and_preserves_snapshot() {
        let path = initialized_path("late-fence").await;
        let first = fixture(&path, "late-first").await;
        let second = fixture(&path, "late-second").await;
        let before = full_snapshot(&path).await;
        let mut conn = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&path))
            .await
            .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn)
            .await
            .unwrap();
        let result = recover_tx_with_test_fence(
            &mut conn,
            &[first.clone(), second.clone()],
            "2026-01-02T00:00:00Z",
            1,
            TestFence::LeaseOwner,
        )
        .await;
        assert_eq!(result, Err(PreAcceptanceError::Fenced));
        sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
        conn.close().await.unwrap();
        assert_eq!(full_snapshot(&path).await, before);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn admission_revision_fence_rolls_back_without_mutation() {
        let path = initialized_path("admission-fence").await;
        let tuple = fixture(&path, "admission-fence").await;
        let before = full_snapshot(&path).await;
        let mut conn = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&path))
            .await
            .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            recover_tx_with_test_fence(
                &mut conn,
                std::slice::from_ref(&tuple),
                "2026-01-02T00:00:00Z",
                0,
                TestFence::AdmissionRevision
            )
            .await,
            Err(PreAcceptanceError::Fenced)
        );
        sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
        conn.close().await.unwrap();
        assert_eq!(full_snapshot(&path).await, before);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn process_cli_success_repeat_rejection_and_late_multituple_rollback_are_redacted() {
        let executable = std::env::var_os("CARGO_BIN_EXE_guigu-agent-bridge")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/debug/guigu-agent-bridge"));
        let path = initialized_path("cli").await;
        let tuple = fixture(&path, "cli").await;
        let database = path.to_str().unwrap().to_owned();
        let success = tokio::process::Command::new(&executable)
            .args([
                "recover-preacceptance",
                "--database",
                database.as_str(),
                "--delivery",
                tuple.delivery_id.as_str(),
                tuple.task_id.as_str(),
                "1",
            ])
            .output()
            .await
            .unwrap();
        assert!(success.status.success());
        assert_eq!(
            String::from_utf8_lossy(&success.stdout),
            "recover-preacceptance result=recovered count=1\n"
        );
        assert!(success.stderr.is_empty());
        let repeated = tokio::process::Command::new(&executable)
            .args([
                "recover-preacceptance",
                "--database",
                database.as_str(),
                "--delivery",
                tuple.delivery_id.as_str(),
                tuple.task_id.as_str(),
                "1",
            ])
            .output()
            .await
            .unwrap();
        assert!(repeated.status.success());
        assert_eq!(
            String::from_utf8_lossy(&repeated.stdout),
            "recover-preacceptance result=already-reconciled\n"
        );
        assert!(repeated.stderr.is_empty());
        let rejected = tokio::process::Command::new(&executable)
            .args([
                "recover-preacceptance",
                "--database",
                database.as_str(),
                "--delivery",
                "untrusted-delivery",
                tuple.task_id.as_str(),
                "1",
            ])
            .output()
            .await
            .unwrap();
        assert!(!rejected.status.success());
        assert!(rejected.stdout.is_empty());
        assert_eq!(
            String::from_utf8_lossy(&rejected.stderr),
            "Error: Application(Runtime)\n"
        );
        let rejected_text = format!(
            "{}{}",
            String::from_utf8_lossy(&rejected.stdout),
            String::from_utf8_lossy(&rejected.stderr)
        );
        assert!(
            !rejected_text.contains("untrusted-delivery")
                && !rejected_text.contains(tuple.task_id.as_str())
                && !rejected_text.contains(database.as_str())
                && !rejected_text.contains("SELECT")
                && !rejected_text.contains("secret sentinel")
        );

        let multi_path = initialized_path("cli-multi").await;
        let first = fixture(&multi_path, "cli-first").await;
        let second = fixture(&multi_path, "cli-second").await;
        let trigger = format!(
            "CREATE TRIGGER fail_second_lease_release BEFORE UPDATE OF state ON execution_leases WHEN OLD.task_id='{}' AND NEW.state='released' BEGIN SELECT RAISE(IGNORE); END",
            second.task_id
        );
        let pool = crate::storage::connect(&multi_path).await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(trigger))
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let before = full_snapshot(&multi_path).await;
        let multi_database = multi_path.to_str().unwrap().to_owned();
        let failed_multi = tokio::process::Command::new(&executable)
            .args([
                "recover-preacceptance",
                "--database",
                multi_database.as_str(),
                "--delivery",
                first.delivery_id.as_str(),
                first.task_id.as_str(),
                "1",
                "--delivery",
                second.delivery_id.as_str(),
                second.task_id.as_str(),
                "1",
            ])
            .output()
            .await
            .unwrap();
        assert!(!failed_multi.status.success());
        assert!(failed_multi.stdout.is_empty());
        assert_eq!(
            String::from_utf8_lossy(&failed_multi.stderr),
            "Error: Application(Runtime)\n"
        );
        let failure_text = format!(
            "{}{}",
            String::from_utf8_lossy(&failed_multi.stdout),
            String::from_utf8_lossy(&failed_multi.stderr)
        );
        assert!(
            !failure_text.contains(first.delivery_id.as_str())
                && !failure_text.contains(first.task_id.as_str())
                && !failure_text.contains(second.delivery_id.as_str())
                && !failure_text.contains(second.task_id.as_str())
                && !failure_text.contains(multi_database.as_str())
                && !failure_text.contains("SELECT")
                && !failure_text.contains("secret sentinel")
        );
        assert_eq!(full_snapshot(&multi_path).await, before);
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(multi_path);
    }

    #[tokio::test]
    async fn typed_event_admission_lease_disposition_and_closed_work_rejections_preserve_snapshot()
    {
        for (name, mutation) in [
            ("missing-event", "DELETE FROM task_events WHERE task_id=?"),
            (
                "wrong-event",
                "UPDATE task_events SET status='running' WHERE task_id=?",
            ),
            (
                "bad-admission",
                "UPDATE task_admissions SET state='running' WHERE task_id=?",
            ),
            (
                "bad-lease",
                "UPDATE execution_leases SET state='active' WHERE task_id=?",
            ),
            (
                "bad-disposition",
                "UPDATE delivery_dispositions SET state='acknowledged' WHERE task_id=?",
            ),
        ] {
            let path = initialized_path(name).await;
            let tuple = fixture(&path, name).await;
            let pool = crate::storage::connect(&path).await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(mutation.to_owned()))
                .bind(&tuple.task_id)
                .execute(&pool)
                .await
                .unwrap();
            pool.close().await;
            let before = full_snapshot(&path).await;
            assert_eq!(
                recover(&path, std::slice::from_ref(&tuple), "2026-01-02T00:00:00Z").await,
                Err(PreAcceptanceError::NotEligible)
            );
            assert_eq!(full_snapshot(&path).await, before);
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn frozen_rejection_matrix_is_fail_closed_with_field_snapshot() {
        for case in [
            "event:missing",
            "event:malformed",
            "event:wrong-variant",
            "event:wrong-tuple",
            "event:queued",
            "event:running",
            "event:completed",
            "event:failed",
            "event:timed_out",
            "event:cancelled",
            "admission:missing",
            "admission:ready",
            "admission:enqueued",
            "admission:running",
            "admission:terminal",
            "admission:recovery_needed",
            "lease:missing",
            "lease:active",
            "lease:released",
            "lease:duplicate",
            "delivery:acknowledged",
            "disposition:missing",
            "disposition:session-bound",
            "disposition:child-bound",
            "disposition:acknowledged",
            "disposition:outcome_unknown",
            "disposition:terminal",
            "disposition:conflicting",
            "queue:queued",
            "queue:claimed",
            "queue:running",
            "queue:paused",
            "queue:completed",
            "queue:expired",
            "queue:superseded",
            "queue:recovery_needed",
            "continuation:ready",
            "continuation:in_flight",
            "continuation:recovery_needed",
            "continuation:blocked",
            "continuation:terminal",
            "event:unknown",
            "admission:unknown",
            "lease:unknown",
            "disposition:unknown",
        ] {
            assert_rejected_fixture(case).await;
        }
    }

    #[tokio::test]
    async fn wrong_delivery_task_attempt_and_malformed_schema_fail_closed_unchanged() {
        let path = initialized_path("wrong-tuple").await;
        let tuple = fixture(&path, "wrong-tuple").await;
        for wrong in [
            PreAcceptanceTuple {
                delivery_id: uuid::Uuid::now_v7().to_string(),
                ..tuple.clone()
            },
            PreAcceptanceTuple {
                task_id: uuid::Uuid::now_v7().to_string(),
                ..tuple.clone()
            },
            PreAcceptanceTuple {
                attempt: 2,
                ..tuple.clone()
            },
        ] {
            let before = full_snapshot(&path).await;
            assert_eq!(
                recover(&path, std::slice::from_ref(&wrong), "2026-01-02T00:00:00Z").await,
                Err(PreAcceptanceError::NotEligible)
            );
            assert_eq!(full_snapshot(&path).await, before);
        }
        let before = full_snapshot(&path).await;
        assert_eq!(
            recover(
                &path,
                &[tuple.clone(), tuple.clone()],
                "2026-01-02T00:00:00Z"
            )
            .await,
            Err(PreAcceptanceError::InvalidArguments)
        );
        assert_eq!(full_snapshot(&path).await, before);
        let _ = std::fs::remove_file(path);

        let malformed =
            std::env::temp_dir().join(format!("t037-malformed-schema-{}.db", uuid::Uuid::now_v7()));
        std::fs::File::create(&malformed).unwrap();
        let before = full_snapshot(&malformed).await;
        assert_eq!(
            recover(
                &malformed,
                &[PreAcceptanceTuple {
                    delivery_id: "d".into(),
                    task_id: "t".into(),
                    attempt: 1
                }],
                "2026-01-02T00:00:00Z"
            )
            .await,
            Err(PreAcceptanceError::Database)
        );
        assert_eq!(full_snapshot(&malformed).await, before);
        let _ = std::fs::remove_file(malformed);
    }

    #[tokio::test]
    async fn malformed_schema_is_database_rejection_and_empty_schema_is_unchanged() {
        let path =
            std::env::temp_dir().join(format!("t037-empty-schema-{}.db", uuid::Uuid::now_v7()));
        std::fs::File::create(&path).unwrap();
        let before = full_snapshot(&path).await;
        assert_eq!(
            recover(
                &path,
                &[PreAcceptanceTuple {
                    delivery_id: "d".into(),
                    task_id: "t".into(),
                    attempt: 1
                }],
                "2026-01-02T00:00:00Z"
            )
            .await,
            Err(PreAcceptanceError::Database)
        );
        assert_eq!(full_snapshot(&path).await, before);
        let _ = std::fs::remove_file(path);
    }
}
