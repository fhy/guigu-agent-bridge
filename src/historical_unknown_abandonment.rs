use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum AbandonmentError {
    #[error("invalid arguments")]
    InvalidArguments,
    #[error("schema unavailable")]
    Schema,
    #[error("target unavailable or conflicting")]
    Conflict,
    #[error("database failure")]
    Database,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Request {
    pub database: PathBuf,
    pub mode: Mode,
    pub decision_id: String,
    pub task_id: String,
    pub delivery_id: String,
    pub resource_key: String,
    pub owner_token: String,
    pub fence: i64,
    pub revision: i64,
    pub generation: String,
    pub reason: String,
    pub policy_ref: String,
    pub authorization_ref: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    DryRun,
    Apply,
    Verify,
}

pub(crate) fn parse_args(args: &[std::ffi::OsString]) -> Result<Request, AbandonmentError> {
    let mut it = args.iter();
    if it.next().and_then(|x| x.to_str()) != Some("historical-unknown-abandon") {
        return Err(AbandonmentError::InvalidArguments);
    }
    let mut mode = None;
    let mut vals = std::collections::HashMap::new();
    while let Some(k) = it.next() {
        let k = k.to_str().ok_or(AbandonmentError::InvalidArguments)?;
        let v = it
            .next()
            .and_then(|x| x.to_str())
            .ok_or(AbandonmentError::InvalidArguments)?;
        match k {
            "--mode" => mode = Some(v),
            _ if k.starts_with("--") => {
                vals.insert(k.trim_start_matches('-'), v.to_owned());
            }
            _ => return Err(AbandonmentError::InvalidArguments),
        }
    }
    let mode = match mode {
        Some("dry-run") => Mode::DryRun,
        Some("apply") => Mode::Apply,
        Some("verify") => Mode::Verify,
        _ => return Err(AbandonmentError::InvalidArguments),
    };
    macro_rules! req {
        ($n:literal) => {
            vals.remove($n).ok_or(AbandonmentError::InvalidArguments)?
        };
    }
    Ok(Request {
        database: PathBuf::from(req!("database")),
        mode,
        decision_id: req!("decision-id"),
        task_id: req!("task-id"),
        delivery_id: req!("delivery-id"),
        resource_key: req!("resource-key"),
        owner_token: req!("owner-token"),
        fence: req!("fence")
            .parse()
            .map_err(|_| AbandonmentError::InvalidArguments)?,
        revision: req!("revision")
            .parse()
            .map_err(|_| AbandonmentError::InvalidArguments)?,
        generation: req!("generation"),
        reason: req!("reason"),
        policy_ref: req!("policy-ref"),
        authorization_ref: req!("authorization-ref"),
    })
}

async fn connect(path: &Path) -> Result<SqliteConnection, AbandonmentError> {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .foreign_keys(true)
            .busy_timeout(std::time::Duration::from_secs(10)),
    )
    .await
    .map_err(|_| AbandonmentError::Database)
}

pub(crate) async fn run(req: Request) -> Result<&'static str, AbandonmentError> {
    let mut c = connect(&req.database).await?;
    if !matches!(req.mode, Mode::Apply) {
        sqlx::query("PRAGMA query_only=ON")
            .execute(&mut c)
            .await
            .map_err(|_| AbandonmentError::Database)?;
    }
    let exists:i64=sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='historical_unknown_abandonments'").fetch_one(&mut c).await.map_err(|_|AbandonmentError::Schema)?;
    if exists != 1 {
        return Err(AbandonmentError::Schema);
    }
    let eligible = "SELECT t.version,a.revision,a.state,d.state,d.reason_code,v.attempt,l.state,l.owner_token,l.fence,c.revision,c.state,c.runtime_generation FROM tasks t JOIN task_admissions a ON a.task_id=t.task_id JOIN deliveries v ON v.task_id=t.task_id AND v.delivery_id=? JOIN delivery_dispositions d ON d.delivery_id=v.delivery_id JOIN execution_leases l ON l.task_id=t.task_id AND l.resource_key=? JOIN task_continuations c ON c.task_id=t.task_id WHERE t.task_id=? AND v.attempt=d.attempt AND t.version=? AND c.revision=? AND c.runtime_generation=? AND l.owner_token=? AND l.fence=? AND l.state='recovery_needed' AND c.state='recovery_needed' AND d.state='outcome_unknown'";
    let row = sqlx::query(eligible)
        .bind(&req.delivery_id)
        .bind(&req.resource_key)
        .bind(&req.task_id)
        .bind(req.revision)
        .bind(req.revision)
        .bind(&req.generation)
        .bind(&req.owner_token)
        .bind(req.fence)
        .fetch_optional(&mut c)
        .await
        .map_err(|_| AbandonmentError::Database)?
        .ok_or(AbandonmentError::Conflict)?;
    let before_task = row.get::<i64, _>(0);
    let before_adm = row.get::<i64, _>(1);
    let before_cont = row.get::<i64, _>(8);
    match req.mode {
        Mode::DryRun => Ok("dry-run eligible"),
        Mode::Verify => Ok("verify read-only"),
        Mode::Apply => {
            let mut tx = c
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|_| AbandonmentError::Database)?;
            let audit=sqlx::query("INSERT INTO historical_unknown_abandonments(decision_id,task_id,delivery_id,resource_key,owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,disposition,reason,policy_ref,authorization_ref,not_replayed,created_at) SELECT ?,?,?,?,?,?,?,?,?,?,?, 'manual_abandonment',?,?,?,1,datetime('now') WHERE NOT EXISTS (SELECT 1 FROM historical_unknown_abandonments WHERE task_id=? AND delivery_id=? AND resource_key=?)")
   .bind(&req.decision_id).bind(&req.task_id).bind(&req.delivery_id).bind(&req.resource_key).bind(&req.owner_token).bind(req.fence).bind(before_task).bind(before_adm).bind(before_cont).bind(&req.generation).bind(before_cont+1).bind(&req.reason).bind(&req.policy_ref).bind(&req.authorization_ref).bind(&req.task_id).bind(&req.delivery_id).bind(&req.resource_key).execute(&mut *tx).await.map_err(|_|AbandonmentError::Database)?;
            if audit.rows_affected() != 1 {
                return Err(AbandonmentError::Conflict);
            }
            let l=sqlx::query("UPDATE execution_leases SET state='released',heartbeat_at=datetime('now') WHERE task_id=? AND resource_key=? AND owner_token=? AND fence=? AND state='recovery_needed'").bind(&req.task_id).bind(&req.resource_key).bind(&req.owner_token).bind(req.fence).execute(&mut *tx).await.map_err(|_|AbandonmentError::Database)?;
            let k=sqlx::query("UPDATE task_continuations SET state='terminal',revision=revision+1,heartbeat_at=datetime('now') WHERE task_id=? AND resource_key=? AND lease_fence=? AND revision=? AND runtime_generation=? AND state='recovery_needed'").bind(&req.task_id).bind(&req.resource_key).bind(req.fence).bind(req.revision).bind(&req.generation).execute(&mut *tx).await.map_err(|_|AbandonmentError::Database)?;
            let d=sqlx::query("UPDATE delivery_dispositions SET state='terminal',reason_code='manual_abandonment' WHERE task_id=? AND delivery_id=? AND attempt=? AND state='outcome_unknown'").bind(&req.task_id).bind(&req.delivery_id).bind(row.get::<i64,_>(5)).execute(&mut *tx).await.map_err(|_|AbandonmentError::Database)?;
            if l.rows_affected() != 1 || k.rows_affected() != 1 || d.rows_affected() != 1 {
                return Err(AbandonmentError::Conflict);
            }
            tx.commit().await.map_err(|_| AbandonmentError::Database)?;
            Ok("applied")
        }
    }
}
