use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum CleanupError {
    #[error("invalid arguments")]
    InvalidArguments,
    #[error("schema unavailable")]
    Schema,
    #[error("no unique eligible target")]
    NoUniqueTarget,
    #[error("conflicting target")]
    Conflict,
    #[error("database failure")]
    Database,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    DryRun,
    Apply,
    Verify,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Request {
    pub database: PathBuf,
    pub config: PathBuf,
    pub mode: Mode,
    pub decision_id: String,
    pub policy_ref: String,
    pub authorization_ref: String,
}

#[derive(Clone, Debug)]
struct Candidate {
    task_id: String,
    delivery_id: String,
    resource_key: String,
    owner_token: String,
    fence: i64,
    task_version: i64,
    admission_revision: i64,
    continuation_revision: i64,
    generation: String,
}

pub(crate) fn parse_args(args: &[OsString]) -> Result<Request, CleanupError> {
    if args.first().and_then(|v| v.to_str()) != Some("historical-terminal-cleanup") {
        return Err(CleanupError::InvalidArguments);
    }
    let mut mode = None;
    let mut values = std::collections::HashMap::new();
    let mut it = args.iter().skip(1);
    while let Some(key) = it.next() {
        let key = key.to_str().ok_or(CleanupError::InvalidArguments)?;
        let value = it
            .next()
            .and_then(|v| v.to_str())
            .ok_or(CleanupError::InvalidArguments)?;
        if key == "--mode" {
            if mode.replace(value).is_some() {
                return Err(CleanupError::InvalidArguments);
            }
        } else if matches!(
            key,
            "--database" | "--config" | "--decision-id" | "--policy-ref" | "--authorization-ref"
        ) {
            if values
                .insert(key.trim_start_matches('-'), value.to_owned())
                .is_some()
            {
                return Err(CleanupError::InvalidArguments);
            }
        } else {
            return Err(CleanupError::InvalidArguments);
        }
    }
    let mode = match mode {
        Some("dry-run") => Mode::DryRun,
        Some("apply") => Mode::Apply,
        Some("verify") => Mode::Verify,
        _ => return Err(CleanupError::InvalidArguments),
    };
    macro_rules! take {
        ($key:literal) => {
            values.remove($key).ok_or(CleanupError::InvalidArguments)?
        };
    }
    Ok(Request {
        database: PathBuf::from(take!("database")),
        config: PathBuf::from(take!("config")),
        mode,
        decision_id: take!("decision-id"),
        policy_ref: take!("policy-ref"),
        authorization_ref: take!("authorization-ref"),
    })
}

async fn connect(path: &Path) -> Result<SqliteConnection, CleanupError> {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .foreign_keys(true)
            .busy_timeout(std::time::Duration::from_secs(10)),
    )
    .await
    .map_err(|_| CleanupError::Database)
}

async fn candidate(c: &mut SqliteConnection) -> Result<Candidate, CleanupError> {
    let rows = sqlx::query(
        "SELECT t.task_id,v.delivery_id,l.resource_key,l.owner_token,l.fence,t.version,a.revision,c.revision,c.runtime_generation
         FROM tasks t
         JOIN task_events e ON e.task_id=t.task_id AND e.seq=(SELECT max(seq) FROM task_events WHERE task_id=t.task_id)
         JOIN task_admissions a ON a.task_id=t.task_id
         JOIN runtime_instances r ON r.instance_token=a.runtime_instance
         JOIN deliveries v ON v.task_id=t.task_id AND v.acknowledged_at IS NOT NULL
         JOIN delivery_dispositions d ON d.delivery_id=v.delivery_id AND d.attempt=v.attempt
         JOIN execution_leases l ON l.task_id=t.task_id
         JOIN task_continuations c ON c.task_id=t.task_id AND c.resource_key=l.resource_key AND c.delivery_id=v.delivery_id
         WHERE e.status IN ('completed','failed','timed_out','cancelled')
           AND a.state='terminal' AND a.revision>=0 AND r.state='stopped'
           AND d.state='terminal' AND d.session_id IS NULL AND d.child_fingerprint IS NULL
           AND d.reap_status IS NULL AND d.reap_at IS NULL
           AND l.state='recovery_needed' AND l.fence>=1 AND length(l.owner_token)>0
           AND c.state='recovery_needed' AND c.revision>0 AND c.lease_fence=l.fence
           AND NOT EXISTS (SELECT 1 FROM agent_work_queue q WHERE q.task_id=t.task_id)
           AND NOT EXISTS (SELECT 1 FROM historical_unknown_abandonments h WHERE h.task_id=t.task_id AND h.delivery_id=v.delivery_id AND h.resource_key=l.resource_key)
           AND NOT EXISTS (SELECT 1 FROM runtime_instances ar WHERE ar.state IN ('active','stopping'))")
        .fetch_all(&mut *c).await.map_err(|_| CleanupError::Database)?;
    if rows.len() != 1 {
        return Err(CleanupError::NoUniqueTarget);
    }
    let row = &rows[0];
    Ok(Candidate {
        task_id: row.get(0),
        delivery_id: row.get(1),
        resource_key: row.get(2),
        owner_token: row.get(3),
        fence: row.get(4),
        task_version: row.get(5),
        admission_revision: row.get(6),
        continuation_revision: row.get(7),
        generation: row.get(8),
    })
}

async fn audit(
    c: &mut SqliteConnection,
    candidate: &Candidate,
) -> Result<Option<(String, i64)>, CleanupError> {
    sqlx::query("SELECT decision_id,post_continuation_revision FROM historical_unknown_abandonments WHERE task_id=? AND delivery_id=? AND resource_key=?")
        .bind(&candidate.task_id).bind(&candidate.delivery_id).bind(&candidate.resource_key)
        .fetch_optional(&mut *c).await.map_err(|_| CleanupError::Database)
        .map(|row| row.map(|r| (r.get(0), r.get(1))))
}

async fn committed_repeat(
    c: &mut SqliteConnection,
    req: &Request,
) -> Result<Option<&'static str>, CleanupError> {
    let row = sqlx::query(
        "SELECT h.post_continuation_revision, c.revision, l.state, c.state, d.state
         FROM historical_unknown_abandonments h
         JOIN execution_leases l ON l.task_id=h.task_id AND l.resource_key=h.resource_key
         JOIN task_continuations c ON c.task_id=h.task_id AND c.resource_key=h.resource_key
         JOIN delivery_dispositions d ON d.delivery_id=h.delivery_id
         WHERE h.decision_id=? AND h.policy_ref=? AND h.authorization_ref=?",
    );
    let row = row
        .bind(&req.decision_id)
        .bind(&req.policy_ref)
        .bind(&req.authorization_ref)
        .fetch_optional(&mut *c)
        .await
        .map_err(|_| CleanupError::Database)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.get::<i64, _>(0) != row.get::<i64, _>(1)
        || row.get::<String, _>(2) != "released"
        || row.get::<String, _>(3) != "terminal"
        || row.get::<String, _>(4) != "terminal"
    {
        return Err(CleanupError::Conflict);
    }
    Ok(Some(match req.mode {
        Mode::DryRun => "dry-run already applied",
        Mode::Apply => "already applied",
        Mode::Verify => "verified post-state",
    }))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailurePoint {
    AfterAudit,
    AfterCas,
}

pub(crate) async fn run(req: Request) -> Result<&'static str, CleanupError> {
    run_with_failure(req, None).await
}

async fn run_with_failure(
    req: Request,
    failure: Option<FailurePoint>,
) -> Result<&'static str, CleanupError> {
    let guard = if req.mode == Mode::Apply {
        Some(
            crate::runtime_admission::Guard::acquire(
                &req.database,
                &req.config,
                "terminal-cleanup",
            )
            .map_err(|_| CleanupError::Conflict)?,
        )
    } else {
        None
    };
    let mut c = connect(&req.database).await?;
    if req.mode != Mode::Apply {
        sqlx::query("PRAGMA query_only=ON")
            .execute(&mut c)
            .await
            .map_err(|_| CleanupError::Database)?;
    }
    let table: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='historical_unknown_abandonments'").fetch_one(&mut c).await.map_err(|_| CleanupError::Schema)?;
    if table != 1 {
        return Err(CleanupError::Schema);
    }
    if let Some(result) = committed_repeat(&mut c, &req).await? {
        return Ok(result);
    }
    let selected = candidate(&mut c).await?;
    if let Some((decision, post_revision)) = audit(&mut c, &selected).await? {
        if decision != req.decision_id || post_revision != selected.continuation_revision + 1 {
            return Err(CleanupError::Conflict);
        }
        return Ok(match req.mode {
            Mode::DryRun => "dry-run already applied",
            Mode::Apply => "already applied",
            Mode::Verify => "verified post-state",
        });
    }
    match req.mode {
        Mode::DryRun => Ok("dry-run eligible"),
        Mode::Verify => Ok("verify eligible pre-state"),
        Mode::Apply => {
            if guard.as_ref().and_then(|g| g.revalidate().ok()).is_none() {
                return Err(CleanupError::Conflict);
            }
            let mut tx = c
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|_| CleanupError::Database)?;
            let fresh = candidate(&mut tx).await?;
            if fresh.task_id != selected.task_id
                || fresh.fence != selected.fence
                || fresh.continuation_revision != selected.continuation_revision
            {
                return Err(CleanupError::Conflict);
            }
            sqlx::query("INSERT INTO historical_unknown_abandonments (decision_id,task_id,delivery_id,resource_key,owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,disposition,reason,policy_ref,authorization_ref,not_replayed,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,'manual_abandonment',?,?,?,1,datetime('now'))")
                .bind(&req.decision_id).bind(&fresh.task_id).bind(&fresh.delivery_id).bind(&fresh.resource_key).bind(&fresh.owner_token).bind(fresh.fence).bind(fresh.task_version).bind(fresh.admission_revision).bind(fresh.continuation_revision).bind(&fresh.generation).bind(fresh.continuation_revision + 1).bind("R050 terminal acknowledged cleanup").bind(&req.policy_ref).bind(&req.authorization_ref).execute(&mut *tx).await.map_err(|_| CleanupError::Database)?;
            if failure == Some(FailurePoint::AfterAudit) {
                return Err(CleanupError::Database);
            }
            let lease = sqlx::query("UPDATE execution_leases SET state='released' WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND state='recovery_needed'").bind(&fresh.resource_key).bind(&fresh.task_id).bind(&fresh.owner_token).bind(fresh.fence).execute(&mut *tx).await.map_err(|_| CleanupError::Database)?;
            let continuation = sqlx::query("UPDATE task_continuations SET state='terminal', revision=revision+1 WHERE task_id=? AND resource_key=? AND delivery_id=? AND lease_fence=? AND revision=? AND state='recovery_needed'").bind(&fresh.task_id).bind(&fresh.resource_key).bind(&fresh.delivery_id).bind(fresh.fence).bind(fresh.continuation_revision).execute(&mut *tx).await.map_err(|_| CleanupError::Database)?;
            if lease.rows_affected() != 1 || continuation.rows_affected() != 1 {
                return Err(CleanupError::Conflict);
            }
            if failure == Some(FailurePoint::AfterCas) {
                return Err(CleanupError::Database);
            }
            tx.commit().await.map_err(|_| CleanupError::Database)?;
            if guard.as_ref().and_then(|g| g.revalidate().ok()).is_none() {
                return Err(CleanupError::Conflict);
            }
            Ok("applied")
        }
    }
}

#[cfg(test)]
#[path = "terminal_acknowledged_cleanup_tests.rs"]
mod tests;
