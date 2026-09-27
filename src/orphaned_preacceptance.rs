use crate::models::TaskEventPayload;
use crate::offline_preacceptance::{PreAcceptanceTuple, recover_tx};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Reconciled,
    AlreadyReconciled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidArguments,
    Empty,
    Multiple,
    Malformed,
    ActiveOwner,
    Fenced,
    Busy,
    Database,
}

#[derive(Clone)]
struct Proof {
    tuple: PreAcceptanceTuple,
    resource: String,
    owner: String,
    fence: i64,
    expiry: String,
}

impl Error {
    pub const fn slug(self) -> &'static str {
        match self {
            Self::InvalidArguments => "invalid-arguments",
            Self::Empty => "empty",
            Self::Multiple => "multiple",
            Self::Malformed => "malformed",
            Self::ActiveOwner => "active-owner",
            Self::Fenced => "fenced",
            Self::Busy => "busy",
            Self::Database => "database",
        }
    }
    pub const fn status(self) -> u8 {
        match self {
            Self::InvalidArguments => 2,
            Self::Empty => 3,
            Self::Multiple => 4,
            Self::Malformed => 5,
            Self::ActiveOwner => 6,
            Self::Fenced => 7,
            Self::Busy => 8,
            Self::Database => 9,
        }
    }
}

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, Error> {
    if args.len() != 3 || args[0] != "reconcile-orphaned-preacceptance" || args[1] != "--database" {
        return Err(Error::InvalidArguments);
    }
    let value = args[2].to_str().ok_or(Error::InvalidArguments)?;
    if value.is_empty() {
        return Err(Error::InvalidArguments);
    }
    Ok(PathBuf::from(value))
}

pub async fn reconcile(database: impl AsRef<Path>, now: &str) -> Result<Outcome, Error> {
    if now.is_empty() {
        return Err(Error::InvalidArguments);
    }
    let opts = SqliteConnectOptions::new()
        .filename(database.as_ref())
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(5));
    let mut c = SqliteConnection::connect_with(&opts)
        .await
        .map_err(|_| Error::Database)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut c)
        .await
        .map_err(|e| {
            if busy(&e) {
                Error::Busy
            } else {
                Error::Database
            }
        })?;
    let result = reconcile_tx(&mut c, now).await;
    match result {
        Ok((outcome, proof, expected)) => {
            sqlx::query("COMMIT")
                .execute(&mut c)
                .await
                .map_err(|_| Error::Database)?;
            c.close().await.map_err(|_| Error::Database)?;
            let pool = crate::storage::connect(database.as_ref())
                .await
                .map_err(|_| Error::Database)?;
            let closed:i64=sqlx::query_scalar("SELECT count(*) FROM deliveries d JOIN task_admissions a ON a.task_id=d.task_id JOIN delivery_dispositions p ON p.delivery_id=d.delivery_id AND p.task_id=d.task_id AND p.attempt=d.attempt JOIN execution_leases l ON l.task_id=d.task_id WHERE d.delivery_id=? AND d.task_id=? AND d.attempt=? AND l.resource_key=? AND l.owner_token=? AND l.fence=? AND l.expires_at=? AND a.state='terminal' AND d.acknowledged_at IS NOT NULL AND p.state='terminal' AND p.reason_code='pre_acceptance_auth_failure' AND l.state='released'").bind(&proof.tuple.delivery_id).bind(&proof.tuple.task_id).bind(proof.tuple.attempt).bind(&proof.resource).bind(&proof.owner).bind(proof.fence).bind(&proof.expiry).fetch_one(&pool).await.map_err(classify)?;
            let repo = crate::storage::SqliteRepository::new(pool.clone());
            let plan = crate::storage::plan_recovery(&repo)
                .await
                .map_err(|_| Error::Database)?;
            pool.close().await;
            if closed != 1 || !expected.matches_plan(&plan) {
                return Err(Error::Database);
            }
            Ok(outcome)
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut c).await;
            Err(error)
        }
    }
}

async fn reconcile_tx(
    c: &mut SqliteConnection,
    now: &str,
) -> Result<(Outcome, Proof, crate::offline_preacceptance::PlannerSets), Error> {
    let rows=sqlx::query("SELECT d.delivery_id,d.task_id,d.attempt FROM deliveries d JOIN task_admissions a ON a.task_id=d.task_id WHERE a.state='dispatching' AND d.acknowledged_at IS NULL ORDER BY d.delivery_id").fetch_all(&mut *c).await.map_err(classify)?;
    if rows.is_empty() {
        return reconcile_closed(c, now).await;
    }
    let mut tuples = Vec::new();
    let mut active = false;
    for row in rows {
        let tuple = PreAcceptanceTuple {
            delivery_id: row.try_get("delivery_id").map_err(|_| Error::Malformed)?,
            task_id: row.try_get("task_id").map_err(|_| Error::Malformed)?,
            attempt: row.try_get("attempt").map_err(|_| Error::Malformed)?,
        };
        match read_open_proof(c, tuple).await {
            Ok(proof) => tuples.push(proof),
            Err(Error::ActiveOwner) => active = true,
            Err(error) => return Err(error),
        }
    }
    if active {
        return Err(Error::ActiveOwner);
    }
    if tuples.is_empty() {
        return Err(Error::Empty);
    }
    if tuples.len() != 1 {
        return Err(Error::Multiple);
    }
    let proof = tuples.pop().ok_or(Error::Empty)?;
    let (count, expected) = recover_tx(c, std::slice::from_ref(&proof.tuple), now)
        .await
        .map_err(|e| match e {
            crate::offline_preacceptance::PreAcceptanceError::Database => Error::Database,
            crate::offline_preacceptance::PreAcceptanceError::Busy => Error::Busy,
            crate::offline_preacceptance::PreAcceptanceError::InvalidArguments => Error::Malformed,
            _ => Error::Fenced,
        })?;
    Ok((
        if count == 0 {
            Outcome::AlreadyReconciled
        } else {
            Outcome::Reconciled
        },
        proof,
        expected,
    ))
}

async fn read_open_proof(
    c: &mut SqliteConnection,
    tuple: PreAcceptanceTuple,
) -> Result<Proof, Error> {
    let admission_runtime: Option<String> = sqlx::query_scalar("SELECT r.state FROM task_admissions a LEFT JOIN runtime_instances r ON r.instance_token=a.runtime_instance WHERE a.task_id=? AND a.runtime_instance IS NOT NULL")
        .bind(&tuple.task_id).fetch_optional(&mut *c).await.map_err(classify)?;
    if admission_runtime.as_deref() != Some("stopped") {
        return Err(Error::Fenced);
    }
    let leases = sqlx::query(
        "SELECT owner_token FROM execution_leases WHERE task_id=? AND state='recovery_needed'",
    )
    .bind(&tuple.task_id)
    .fetch_all(&mut *c)
    .await
    .map_err(classify)?;
    if leases.len() > 1 {
        return Err(Error::Multiple);
    }
    let lease = leases.first().ok_or(Error::Fenced)?;
    let owner: String = lease.try_get("owner_token").map_err(|_| Error::Malformed)?;
    if owner.is_empty() {
        return Err(Error::Malformed);
    }
    let owner_state: Option<String> =
        sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token=?")
            .bind(&owner)
            .fetch_optional(&mut *c)
            .await
            .map_err(classify)?;
    match owner_state.as_deref() {
        Some("active" | "stopping") => return Err(Error::ActiveOwner),
        Some(_) => return Err(Error::Fenced),
        None => {}
    }
    validate_open_event(c, &tuple).await?;
    read_proof(c, tuple).await
}

async fn reconcile_closed(
    c: &mut SqliteConnection,
    now: &str,
) -> Result<(Outcome, Proof, crate::offline_preacceptance::PlannerSets), Error> {
    let rows = sqlx::query("SELECT d.delivery_id,d.task_id,d.attempt FROM deliveries d JOIN task_admissions a ON a.task_id=d.task_id JOIN delivery_dispositions p ON p.delivery_id=d.delivery_id AND p.task_id=d.task_id AND p.attempt=d.attempt JOIN execution_leases l ON l.task_id=d.task_id WHERE a.state='terminal' AND d.acknowledged_at IS NOT NULL AND p.state='terminal' AND p.reason_code='pre_acceptance_auth_failure' AND l.state='released' ORDER BY d.delivery_id")
            .fetch_all(&mut *c)
            .await
            .map_err(classify)?;
    let mut matches = Vec::new();
    for row in rows {
        let tuple = PreAcceptanceTuple {
            delivery_id: row.try_get("delivery_id").map_err(|_| Error::Malformed)?,
            task_id: row.try_get("task_id").map_err(|_| Error::Malformed)?,
            attempt: row.try_get("attempt").map_err(|_| Error::Malformed)?,
        };
        validate_closed_events(c, &tuple).await?;
        let proof = read_proof(c, tuple).await?;
        match recover_tx(c, std::slice::from_ref(&proof.tuple), now).await {
            Ok((0, expected)) => matches.push((proof, expected)),
            Ok(_) => return Err(Error::Fenced),
            Err(crate::offline_preacceptance::PreAcceptanceError::Database) => {
                return Err(Error::Database);
            }
            Err(_) => return Err(Error::Fenced),
        }
    }
    match matches.len() {
        0 => Err(Error::Empty),
        1 => {
            let (proof, expected) = matches.pop().unwrap();
            Ok((Outcome::AlreadyReconciled, proof, expected))
        }
        _ => Err(Error::Multiple),
    }
}

async fn validate_open_event(
    c: &mut SqliteConnection,
    t: &PreAcceptanceTuple,
) -> Result<(), Error> {
    let row = sqlx::query(
        "SELECT status,payload FROM task_events WHERE task_id=? ORDER BY seq DESC LIMIT 1",
    )
    .bind(&t.task_id)
    .fetch_optional(&mut *c)
    .await
    .map_err(classify)?
    .ok_or(Error::Fenced)?;
    let status: String = row.try_get("status").map_err(|_| Error::Malformed)?;
    let payload: String = row.try_get("payload").map_err(|_| Error::Malformed)?;
    if status != "dispatched" {
        return Err(Error::Malformed);
    }
    match serde_json::from_str::<TaskEventPayload>(&payload) {
        Ok(TaskEventPayload::Dispatched {
            delivery_id,
            attempt,
        }) if delivery_id.to_string() == t.delivery_id && i64::from(attempt) == t.attempt => Ok(()),
        _ => Err(Error::Malformed),
    }
}

async fn validate_closed_events(
    c: &mut SqliteConnection,
    t: &PreAcceptanceTuple,
) -> Result<(), Error> {
    let rows = sqlx::query(
        "SELECT status,payload FROM task_events WHERE task_id=? ORDER BY seq DESC LIMIT 2",
    )
    .bind(&t.task_id)
    .fetch_all(&mut *c)
    .await
    .map_err(classify)?;
    if rows.len() != 2 {
        return Err(Error::Fenced);
    }
    let latest_status: String = rows[0].try_get("status").map_err(|_| Error::Malformed)?;
    let latest_payload: String = rows[0].try_get("payload").map_err(|_| Error::Malformed)?;
    if latest_status != "failed"
        || !matches!(serde_json::from_str::<TaskEventPayload>(&latest_payload),Ok(TaskEventPayload::Failed{error}) if error=="pre_acceptance_auth_failure")
    {
        return Err(Error::Malformed);
    }
    let dispatched_status: String = rows[1].try_get("status").map_err(|_| Error::Malformed)?;
    let dispatched_payload: String = rows[1].try_get("payload").map_err(|_| Error::Malformed)?;
    if dispatched_status != "dispatched" {
        return Err(Error::Malformed);
    }
    match serde_json::from_str::<TaskEventPayload>(&dispatched_payload) {
        Ok(TaskEventPayload::Dispatched {
            delivery_id,
            attempt,
        }) if delivery_id.to_string() == t.delivery_id && i64::from(attempt) == t.attempt => Ok(()),
        _ => Err(Error::Malformed),
    }
}

async fn read_proof(c: &mut SqliteConnection, tuple: PreAcceptanceTuple) -> Result<Proof, Error> {
    let row = sqlx::query(
        "SELECT resource_key,owner_token,fence,expires_at FROM execution_leases WHERE task_id=?",
    )
    .bind(&tuple.task_id)
    .fetch_one(&mut *c)
    .await
    .map_err(classify)?;
    Ok(Proof {
        tuple,
        resource: row.try_get("resource_key").map_err(|_| Error::Malformed)?,
        owner: row.try_get("owner_token").map_err(|_| Error::Malformed)?,
        fence: row.try_get("fence").map_err(|_| Error::Malformed)?,
        expiry: row.try_get("expires_at").map_err(|_| Error::Malformed)?,
    })
}

fn busy(e: &sqlx::Error) -> bool {
    let s = e.to_string().to_ascii_lowercase();
    s.contains("busy") || s.contains("locked")
}
fn classify(e: sqlx::Error) -> Error {
    let s = e.to_string().to_ascii_lowercase();
    if s.contains("no such") || s.contains("datatype") {
        Error::Malformed
    } else if busy(&e) {
        Error::Busy
    } else {
        Error::Database
    }
}
