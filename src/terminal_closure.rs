//! Shared durable terminal writes for the live T034 worker and offline T037 path.
//!
//! Callers own the transaction and all work which is specific to their lifecycle.
//! This module borrows the caller's connection and never begins or commits a
//! transaction.

use crate::models::{TaskEvent, TaskStatus};
use sqlx::SqliteConnection;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClosureError {
    #[error("fenced")]
    Fenced,
    #[error("admission stale")]
    AdmissionStale,
    #[error("malformed terminal event")]
    Malformed,
    #[error("database")]
    Database(#[from] sqlx::Error),
}

/// Closed, internal lease contracts. Neither caller can weaken its predicates.
pub(crate) enum LeaseMode<'a> {
    LiveActive {
        owner: &'a str,
        fence: i64,
        now: &'a str,
    },
    OfflineRecoveryNeeded {
        owner: &'a str,
        fence: i64,
        expires_at: &'a str,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum AdmissionMode<'a> {
    /// Preserve T034's existing eligible admission states and error mapping.
    Live,
    /// T037's captured state/runtime/revision CAS.
    Offline {
        runtime_owner: Option<&'a str>,
        revision: i64,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum DeliveryBinding<'a> {
    LiveContinuation,
    OfflineExact { delivery_id: &'a str, attempt: i64 },
}

#[derive(Clone, Copy)]
pub(crate) enum TaskVersionMode {
    LiveAlreadyAdvanced,
    OfflineExpected(i64),
}

pub(crate) struct ClosureContext<'a> {
    pub task_id: &'a str,
    pub resource_key: &'a str,
    pub event: &'a TaskEvent,
    pub now: &'a str,
    pub delivery: DeliveryBinding<'a>,
    pub admission: AdmissionMode<'a>,
    pub task_version: TaskVersionMode,
    pub disposition_reason: Option<&'a str>,
}

/// Applies the common event/task/admission/delivery/disposition/lease writes.
/// It assumes caller-specific proof/continuation/receipt checks have succeeded.
/// Every zero-row CAS is returned to the transaction owner, which must roll back.
pub(crate) async fn apply(
    c: &mut SqliteConnection,
    context: ClosureContext<'_>,
    lease_mode: LeaseMode<'_>,
) -> Result<(), ClosureError> {
    if context.event.task_id.to_string() != context.task_id
        || !matches!(
            context.event.status,
            TaskStatus::Completed
                | TaskStatus::Failed
                | TaskStatus::TimedOut
                | TaskStatus::Cancelled
        )
    {
        return Err(ClosureError::Malformed);
    }
    let status = match context.event.status {
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::TimedOut => "timed_out",
        TaskStatus::Cancelled => "cancelled",
        _ => return Err(ClosureError::Malformed),
    };
    let payload =
        serde_json::to_string(&context.event.payload).map_err(|_| ClosureError::Malformed)?;
    let timestamp = context
        .event
        .timestamp
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);

    match context.task_version {
        TaskVersionMode::LiveAlreadyAdvanced => {}
        TaskVersionMode::OfflineExpected(expected) => {
            let update =
                sqlx::query("UPDATE tasks SET version=version+1 WHERE task_id=? AND version=?")
                    .bind(context.task_id)
                    .bind(expected)
                    .execute(&mut *c)
                    .await?;
            if update.rows_affected() != 1 {
                return Err(ClosureError::Fenced);
            }
        }
    }

    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,?,?,?,?)")
        .bind(context.event.id.to_string())
        .bind(context.task_id)
        .bind(i64::try_from(context.event.seq).map_err(|_| ClosureError::Malformed)?)
        .bind(status)
        .bind(timestamp)
        .bind(payload)
        .execute(&mut *c)
        .await?;

    let admission = match context.admission {
        AdmissionMode::Live => sqlx::query("UPDATE task_admissions SET state='terminal',revision=revision+1,updated_at=? WHERE task_id=? AND state IN ('enqueued','dispatching','running')")
            .bind(context.now).bind(context.task_id).execute(&mut *c).await?,
        AdmissionMode::Offline { runtime_owner, revision } => sqlx::query("UPDATE task_admissions SET state='terminal',revision=revision+1,updated_at=? WHERE task_id=? AND state='dispatching' AND revision=? AND (runtime_instance IS ? OR runtime_instance=?)")
            .bind(context.now).bind(context.task_id).bind(revision).bind(runtime_owner).bind(runtime_owner).execute(&mut *c).await?,
    };
    if admission.rows_affected() != 1 {
        return Err(match context.admission {
            AdmissionMode::Live => ClosureError::AdmissionStale,
            AdmissionMode::Offline { .. } => ClosureError::Fenced,
        });
    }

    let (ack, disposition) = match context.delivery {
        DeliveryBinding::LiveContinuation => {
            let ack = sqlx::query("UPDATE deliveries SET acknowledged_at=? WHERE delivery_id=(SELECT delivery_id FROM task_continuations WHERE task_id=?) AND task_id=? AND acknowledged_at IS NULL")
                .bind(context.now).bind(context.task_id).bind(context.task_id).execute(&mut *c).await?;
            let disposition = sqlx::query("UPDATE delivery_dispositions SET state='terminal',reason_code=NULL WHERE delivery_id=(SELECT delivery_id FROM task_continuations WHERE task_id=?) AND task_id=? AND state IN ('prepared','acknowledged','outcome_unknown')")
                .bind(context.task_id).bind(context.task_id).execute(&mut *c).await?;
            (ack, disposition)
        }
        DeliveryBinding::OfflineExact {
            delivery_id,
            attempt,
        } => {
            let ack = sqlx::query("UPDATE deliveries SET acknowledged_at=? WHERE delivery_id=? AND task_id=? AND attempt=? AND acknowledged_at IS NULL")
                .bind(context.now).bind(delivery_id).bind(context.task_id).bind(attempt).execute(&mut *c).await?;
            let disposition = sqlx::query("UPDATE delivery_dispositions SET state='terminal',reason_code=? WHERE delivery_id=? AND task_id=? AND attempt=? AND state='prepared' AND session_id IS NULL AND child_fingerprint IS NULL")
                .bind(context.disposition_reason.unwrap_or("pre_acceptance_auth_failure")).bind(delivery_id).bind(context.task_id).bind(attempt).execute(&mut *c).await?;
            (ack, disposition)
        }
    };
    if matches!(context.delivery, DeliveryBinding::OfflineExact { .. })
        && (ack.rows_affected() != 1 || disposition.rows_affected() != 1)
    {
        return Err(ClosureError::Fenced);
    }

    let lease = match lease_mode {
        LeaseMode::LiveActive { owner, fence, now } => sqlx::query("UPDATE execution_leases SET state='released',heartbeat_at=?,expires_at=? WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND state='active' AND expires_at>?")
            .bind(now).bind(now).bind(context.resource_key).bind(context.task_id).bind(owner).bind(fence).bind(now).execute(&mut *c).await?,
        LeaseMode::OfflineRecoveryNeeded { owner, fence, expires_at } => sqlx::query("UPDATE execution_leases SET state='released',heartbeat_at=? WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND state='recovery_needed' AND expires_at=?")
            .bind(context.now).bind(context.resource_key).bind(context.task_id).bind(owner).bind(fence).bind(expires_at).execute(&mut *c).await?,
    };
    if lease.rows_affected() != 1 {
        return Err(ClosureError::Fenced);
    }
    Ok(())
}
