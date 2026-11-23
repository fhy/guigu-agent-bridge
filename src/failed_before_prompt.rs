//! Fenced closure for an acknowledged dispatch whose ACP prompt never started.
use crate::models::{EventId, TaskEvent, TaskEventPayload, TaskId, TaskStatus};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq, Clone, Copy)]
pub enum Error {
    #[error("invalid arguments")]
    InvalidArguments,
    #[error("database error")]
    Database,
    #[error("proof is not eligible")]
    NotEligible,
    #[error("fencing conflict")]
    Fenced,
    #[error("database busy")]
    Busy,
    #[error("already reconciled")]
    AlreadyReconciled,
}

impl Error {
    pub const fn slug(self) -> &'static str {
        match self {
            Self::InvalidArguments => "invalid-arguments",
            Self::Database => "database",
            Self::NotEligible => "not-eligible",
            Self::Fenced => "fenced",
            Self::Busy => "busy",
            Self::AlreadyReconciled => "already-reconciled",
        }
    }
    pub const fn status(self) -> u8 {
        match self {
            Self::InvalidArguments => 2,
            Self::Database => 9,
            Self::NotEligible => 5,
            Self::Fenced => 7,
            Self::Busy => 8,
            Self::AlreadyReconciled => 0,
        }
    }
}

#[derive(Debug)]
struct Proof {
    task: String,
    delivery: String,
    attempt: i64,
    runtime_owner: String,
    lease_owner: String,
    resource: String,
    fence: i64,
    expiry: String,
    revision: i64,
    task_version: i64,
}

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, Error> {
    if args.len() != 3 || args[0] != "reconcile-failed-before-prompt" || args[1] != "--database" {
        return Err(Error::InvalidArguments);
    }
    let path = args[2].to_str().ok_or(Error::InvalidArguments)?;
    if path.is_empty() {
        return Err(Error::InvalidArguments);
    }
    Ok(PathBuf::from(path))
}

pub async fn reconcile(database: impl AsRef<Path>, now: &str) -> Result<(), Error> {
    if now.is_empty() {
        return Err(Error::InvalidArguments);
    }
    let options = SqliteConnectOptions::new()
        .filename(database.as_ref())
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(5));
    let mut c = SqliteConnection::connect_with(&options)
        .await
        .map_err(|_| Error::Database)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut c)
        .await
        .map_err(|e| {
            if is_busy(&e) {
                Error::Busy
            } else {
                Error::Database
            }
        })?;
    let result = reconcile_tx(&mut c, now).await;
    match result {
        Ok(()) => {
            sqlx::query("COMMIT")
                .execute(&mut c)
                .await
                .map_err(|_| Error::Database)?;
            c.close().await.map_err(|_| Error::Database)?;
            Ok(())
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut c).await;
            Err(error)
        }
    }
}

async fn reconcile_tx(c: &mut SqliteConnection, now: &str) -> Result<(), Error> {
    let rows = sqlx::query("SELECT d.delivery_id,d.task_id,d.attempt FROM deliveries d JOIN task_admissions a ON a.task_id=d.task_id WHERE a.state='dispatching' AND d.acknowledged_at IS NOT NULL ORDER BY d.delivery_id")
        .fetch_all(&mut *c).await.map_err(classify)?;
    if rows.len() != 1 {
        return Err(if rows.is_empty() {
            Error::NotEligible
        } else {
            Error::Fenced
        });
    }
    let delivery: String = rows[0]
        .try_get("delivery_id")
        .map_err(|_| Error::Database)?;
    let task: String = rows[0].try_get("task_id").map_err(|_| Error::Database)?;
    let attempt: i64 = rows[0].try_get("attempt").map_err(|_| Error::Database)?;
    let proof = read_proof(c, &task, &delivery, attempt).await?;
    let id: TaskId = task.parse().map_err(|_| Error::NotEligible)?;
    let seq: i64 =
        sqlx::query_scalar("SELECT coalesce(max(seq),0)+1 FROM task_events WHERE task_id=?")
            .bind(&task)
            .fetch_one(&mut *c)
            .await
            .map_err(|_| Error::Database)?;
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: id,
        seq: u64::try_from(seq).map_err(|_| Error::NotEligible)?,
        status: TaskStatus::Failed,
        timestamp: now.parse().map_err(|_| Error::InvalidArguments)?,
        payload: TaskEventPayload::Failed {
            error: "failed_before_prompt".into(),
        },
    };
    crate::terminal_closure::apply(
        c,
        crate::terminal_closure::ClosureContext {
            task_id: &proof.task,
            resource_key: &proof.resource,
            event: &event,
            now,
            delivery: crate::terminal_closure::DeliveryBinding::OfflinePrePrompt {
                delivery_id: &proof.delivery,
                attempt: proof.attempt,
            },
            admission: crate::terminal_closure::AdmissionMode::Offline {
                runtime_owner: Some(&proof.runtime_owner),
                revision: proof.revision,
            },
            task_version: crate::terminal_closure::TaskVersionMode::OfflineExpected(
                proof.task_version,
            ),
            disposition_reason: Some("failed_before_prompt"),
        },
        crate::terminal_closure::LeaseMode::OfflineRecoveryNeeded {
            owner: &proof.lease_owner,
            fence: proof.fence,
            expires_at: &proof.expiry,
        },
    )
    .await
    .map_err(|e| match e {
        crate::terminal_closure::ClosureError::Fenced
        | crate::terminal_closure::ClosureError::AdmissionStale => Error::Fenced,
        crate::terminal_closure::ClosureError::Malformed => Error::NotEligible,
        crate::terminal_closure::ClosureError::Database(_) => Error::Database,
    })
}

async fn read_proof(
    c: &mut SqliteConnection,
    task: &str,
    delivery: &str,
    attempt: i64,
) -> Result<Proof, Error> {
    let admission = sqlx::query("SELECT revision,runtime_instance FROM task_admissions WHERE task_id=? AND state='dispatching'").bind(task).fetch_one(&mut *c).await.map_err(|_| Error::NotEligible)?;
    let revision: i64 = admission.try_get("revision").map_err(|_| Error::Database)?;
    let owner: String = admission
        .try_get("runtime_instance")
        .map_err(|_| Error::NotEligible)?;
    let owner_state: Option<String> =
        sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token=?")
            .bind(&owner)
            .fetch_optional(&mut *c)
            .await
            .map_err(|_| Error::Database)?;
    if owner_state.as_deref() != Some("stopped") {
        return Err(Error::Fenced);
    }
    let lease = sqlx::query("SELECT resource_key,owner_token,fence,expires_at FROM execution_leases WHERE task_id=? AND state='recovery_needed'").bind(task).fetch_all(&mut *c).await.map_err(|_| Error::Database)?;
    if lease.len() != 1 {
        return Err(Error::Fenced);
    }
    let lease = &lease[0];
    let lease_owner: String = lease.try_get("owner_token").map_err(|_| Error::Database)?;
    if lease_owner.is_empty() {
        return Err(Error::Fenced);
    }
    let disposition = sqlx::query("SELECT state,session_id,child_fingerprint FROM delivery_dispositions WHERE delivery_id=? AND task_id=? AND attempt=?").bind(delivery).bind(task).bind(attempt).fetch_one(&mut *c).await.map_err(|_| Error::NotEligible)?;
    let state: String = disposition.try_get("state").map_err(|_| Error::Database)?;
    let child: Option<String> = disposition
        .try_get("child_fingerprint")
        .map_err(|_| Error::Database)?;
    if !matches!(
        state.as_str(),
        "prepared" | "acknowledged" | "outcome_unknown"
    ) || child.is_some()
    {
        return Err(Error::NotEligible);
    }
    let events =
        sqlx::query("SELECT seq,status,payload FROM task_events WHERE task_id=? ORDER BY seq")
            .bind(task)
            .fetch_all(&mut *c)
            .await
            .map_err(|_| Error::Database)?;
    if events.len() != 2 {
        return Err(Error::NotEligible);
    }
    let first: String = events[0].try_get("status").map_err(|_| Error::Database)?;
    let second: String = events[1].try_get("status").map_err(|_| Error::Database)?;
    if first != "queued" || second != "dispatched" {
        return Err(Error::NotEligible);
    }
    let payload: String = events[1].try_get("payload").map_err(|_| Error::Database)?;
    if !matches!(serde_json::from_str::<TaskEventPayload>(&payload), Ok(TaskEventPayload::Dispatched { delivery_id, attempt: a }) if delivery_id.to_string() == delivery && i64::from(a) == attempt)
    {
        return Err(Error::NotEligible);
    }
    let continuation: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_continuations WHERE task_id=?")
            .bind(task)
            .fetch_one(&mut *c)
            .await
            .map_err(|_| Error::Database)?;
    let queue: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_work_queue WHERE task_id=? AND state NOT IN ('completed','expired','superseded')").bind(task).fetch_one(&mut *c).await.map_err(|_| Error::Database)?;
    if continuation != 0 || queue != 0 {
        return Err(Error::NotEligible);
    }
    let task_version: i64 = sqlx::query_scalar("SELECT version FROM tasks WHERE task_id=?")
        .bind(task)
        .fetch_one(&mut *c)
        .await
        .map_err(|_| Error::Database)?;
    Ok(Proof {
        task: task.into(),
        delivery: delivery.into(),
        attempt,
        runtime_owner: owner,
        lease_owner,
        resource: lease.try_get("resource_key").map_err(|_| Error::Database)?,
        fence: lease.try_get("fence").map_err(|_| Error::Database)?,
        expiry: lease.try_get("expires_at").map_err(|_| Error::Database)?,
        revision,
        task_version,
    })
}

fn is_busy(error: &sqlx::Error) -> bool {
    error.to_string().to_ascii_lowercase().contains("busy")
        || error.to_string().to_ascii_lowercase().contains("locked")
}
fn classify(error: sqlx::Error) -> Error {
    if is_busy(&error) {
        Error::Busy
    } else {
        Error::Database
    }
}
