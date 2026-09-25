//! Explicit offline closure for a proven pre-acceptance dispatch refusal.
use crate::models::event::TaskEventPayload;
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreAcceptanceTuple {
    pub delivery_id: String,
    pub task_id: String,
    pub attempt: i64,
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
) -> Result<usize, PreAcceptanceError> {
    if tuples.is_empty() || now.is_empty() {
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
        Ok(n) => {
            sqlx::query("COMMIT")
                .execute(&mut c)
                .await
                .map_err(|_| PreAcceptanceError::Database)?;
            Ok(n)
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
) -> Result<usize, PreAcceptanceError> {
    for t in tuples {
        validate(c, t).await?;
    }
    for t in tuples {
        close_one(c, t, now).await?;
    }
    Ok(tuples.len())
}

async fn validate(
    c: &mut SqliteConnection,
    t: &PreAcceptanceTuple,
) -> Result<(), PreAcceptanceError> {
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
    if d.try_get::<Option<String>, _>("acknowledged_at")
        .map_err(|_| PreAcceptanceError::Database)?
        .is_some()
    {
        return Err(PreAcceptanceError::NotEligible);
    }
    let state: String = sqlx::query_scalar(
        "SELECT state FROM delivery_dispositions WHERE delivery_id=? AND task_id=? AND attempt=?",
    )
    .bind(&t.delivery_id)
    .bind(&t.task_id)
    .bind(t.attempt)
    .fetch_optional(&mut *c)
    .await
    .map_err(|_| PreAcceptanceError::Database)?
    .ok_or(PreAcceptanceError::NotEligible)?;
    if state != "prepared" {
        return Err(PreAcceptanceError::NotEligible);
    }
    let row = sqlx::query(
        "SELECT status,payload FROM task_events WHERE task_id=? ORDER BY seq DESC LIMIT 1",
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
    Ok(())
}

async fn close_one(
    c: &mut SqliteConnection,
    t: &PreAcceptanceTuple,
    now: &str,
) -> Result<(), PreAcceptanceError> {
    let a=sqlx::query("SELECT revision,runtime_instance FROM task_admissions WHERE task_id=? AND state='dispatching'").bind(&t.task_id).fetch_one(&mut *c).await.map_err(|_|PreAcceptanceError::Fenced)?;
    let rev: i64 = a
        .try_get("revision")
        .map_err(|_| PreAcceptanceError::Database)?;
    let owner: Option<String> = a
        .try_get("runtime_instance")
        .map_err(|_| PreAcceptanceError::Database)?;
    let l=sqlx::query("SELECT resource_key,owner_token,fence FROM execution_leases WHERE task_id=? AND state='recovery_needed'").bind(&t.task_id).fetch_one(&mut *c).await.map_err(|_|PreAcceptanceError::Fenced)?;
    let resource: String = l
        .try_get("resource_key")
        .map_err(|_| PreAcceptanceError::Database)?;
    let token: String = l
        .try_get("owner_token")
        .map_err(|_| PreAcceptanceError::Database)?;
    let fence: i64 = l
        .try_get("fence")
        .map_err(|_| PreAcceptanceError::Database)?;
    let seq: i64 =
        sqlx::query_scalar("SELECT coalesce(max(seq),0)+1 FROM task_events WHERE task_id=?")
            .bind(&t.task_id)
            .fetch_one(&mut *c)
            .await
            .map_err(|_| PreAcceptanceError::Database)?;
    let payload = serde_json::json!({"failed":{"error":"pre_acceptance_auth_failure"}}).to_string();
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,?,'failed',?,?)").bind(uuid::Uuid::now_v7().to_string()).bind(&t.task_id).bind(seq).bind(now).bind(payload).execute(&mut *c).await.map_err(|_|PreAcceptanceError::Fenced)?;
    let changed=sqlx::query("UPDATE task_admissions SET state='terminal',revision=revision+1,updated_at=? WHERE task_id=? AND state='dispatching' AND revision=? AND (runtime_instance IS ? OR runtime_instance=?)").bind(now).bind(&t.task_id).bind(rev).bind(&owner).bind(&owner).execute(&mut *c).await.map_err(|_|PreAcceptanceError::Fenced)?.rows_affected();
    if changed != 1 {
        return Err(PreAcceptanceError::Fenced);
    }
    if sqlx::query("UPDATE deliveries SET acknowledged_at=? WHERE delivery_id=? AND task_id=? AND attempt=? AND acknowledged_at IS NULL").bind(now).bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).execute(&mut *c).await.map_err(|_|PreAcceptanceError::Fenced)?.rows_affected()!=1{return Err(PreAcceptanceError::Fenced)}
    if sqlx::query("UPDATE delivery_dispositions SET state='terminal',reason_code='pre_acceptance_auth_failure' WHERE delivery_id=? AND task_id=? AND attempt=? AND state='prepared'").bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).execute(&mut *c).await.map_err(|_|PreAcceptanceError::Fenced)?.rows_affected()!=1{return Err(PreAcceptanceError::Fenced)}
    if sqlx::query("UPDATE execution_leases SET state='released',heartbeat_at=? WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND state='recovery_needed'").bind(now).bind(resource).bind(&t.task_id).bind(token).bind(fence).execute(&mut *c).await.map_err(|_|PreAcceptanceError::Fenced)?.rows_affected()!=1{return Err(PreAcceptanceError::Fenced)}
    Ok(())
}
fn is_busy(e: &sqlx::Error) -> bool {
    let s = e.to_string().to_ascii_lowercase();
    s.contains("busy") || s.contains("locked")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::ffi::OsStringExt;

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
    }
}
