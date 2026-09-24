//! Explicit offline reconciliation for already-terminal deliveries.
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReconcileError {
    #[error("reconciliation rejected: invalid arguments")]
    InvalidArguments,
    #[error("reconciliation rejected: database error")]
    Database,
    #[error("reconciliation rejected: work is present")]
    WorkPresent,
    #[error("reconciliation rejected: busy")]
    Busy,
    #[error("reconciliation rejected: fencing conflict")]
    Fenced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryTuple {
    pub delivery_id: String,
    pub task_id: String,
    pub attempt: i64,
}

pub fn parse_args(
    args: &[std::ffi::OsString],
) -> Result<(PathBuf, Vec<DeliveryTuple>), ReconcileError> {
    if args.len() < 7 || args[0] != "reconcile-delivery" {
        return Err(ReconcileError::InvalidArguments);
    }
    let mut db = None;
    let mut tuples = Vec::new();
    let mut i = 1;
    while i < args.len() {
        let flag = args[i].to_str().ok_or(ReconcileError::InvalidArguments)?;
        if flag == "--database" {
            if db.is_some() || i + 1 >= args.len() {
                return Err(ReconcileError::InvalidArguments);
            }
            db = Some(
                args[i + 1]
                    .to_str()
                    .ok_or(ReconcileError::InvalidArguments)?
                    .to_owned(),
            );
            i += 2;
        } else if flag == "--delivery" {
            if i + 3 >= args.len() {
                return Err(ReconcileError::InvalidArguments);
            }
            let delivery_id = args[i + 1]
                .to_str()
                .ok_or(ReconcileError::InvalidArguments)?
                .to_owned();
            let task_id = args[i + 2]
                .to_str()
                .ok_or(ReconcileError::InvalidArguments)?
                .to_owned();
            let attempt = args[i + 3]
                .to_str()
                .ok_or(ReconcileError::InvalidArguments)?
                .parse()
                .map_err(|_| ReconcileError::InvalidArguments)?;
            if delivery_id.is_empty() || task_id.is_empty() || attempt < 1 {
                return Err(ReconcileError::InvalidArguments);
            }
            tuples.push(DeliveryTuple {
                delivery_id,
                task_id,
                attempt,
            });
            i += 4;
        } else {
            return Err(ReconcileError::InvalidArguments);
        }
    }
    if db.as_deref().unwrap_or("").is_empty() || tuples.is_empty() {
        return Err(ReconcileError::InvalidArguments);
    }
    let mut seen = HashSet::new();
    if tuples
        .iter()
        .any(|t| !seen.insert((t.delivery_id.clone(), t.task_id.clone(), t.attempt)))
    {
        return Err(ReconcileError::InvalidArguments);
    }
    Ok((PathBuf::from(db.unwrap()), tuples))
}

pub async fn reconcile(
    database: impl AsRef<Path>,
    tuples: &[DeliveryTuple],
    now: &str,
) -> Result<usize, ReconcileError> {
    if tuples.is_empty() || now.is_empty() {
        return Err(ReconcileError::InvalidArguments);
    }
    let opts = SqliteConnectOptions::new()
        .filename(database.as_ref())
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(1));
    let mut c = SqliteConnection::connect_with(&opts)
        .await
        .map_err(|_| ReconcileError::Database)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut c)
        .await
        .map_err(|e| {
            if e.to_string().to_ascii_lowercase().contains("busy")
                || e.to_string().to_ascii_lowercase().contains("locked")
            {
                ReconcileError::Busy
            } else {
                ReconcileError::Database
            }
        })?;
    let result = reconcile_tx(&mut c, tuples, now).await;
    match result {
        Ok(n) => {
            sqlx::query("COMMIT")
                .execute(&mut c)
                .await
                .map_err(|_| ReconcileError::Database)?;
            Ok(n)
        }
        Err(e) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut c).await;
            Err(e)
        }
    }
}

async fn reconcile_tx(
    c: &mut SqliteConnection,
    tuples: &[DeliveryTuple],
    now: &str,
) -> Result<usize, ReconcileError> {
    for t in tuples {
        let row = sqlx::query(
            "SELECT task_id,attempt,acknowledged_at FROM deliveries WHERE delivery_id=?",
        )
        .bind(&t.delivery_id)
        .fetch_optional(&mut *c)
        .await
        .map_err(|_| ReconcileError::Database)?
        .ok_or(ReconcileError::WorkPresent)?;
        let task: String = row
            .try_get("task_id")
            .map_err(|_| ReconcileError::Database)?;
        let attempt: i64 = row
            .try_get("attempt")
            .map_err(|_| ReconcileError::Database)?;
        if task != t.task_id || attempt != t.attempt {
            return Err(ReconcileError::WorkPresent);
        }
        let status: Option<String> = sqlx::query_scalar("SELECT e.status FROM task_events e WHERE e.task_id=? AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=? )").bind(&t.task_id).bind(&t.task_id).fetch_optional(&mut *c).await.map_err(|_| ReconcileError::Database)?;
        if !matches!(
            status.as_deref(),
            Some("completed" | "failed" | "timed_out" | "cancelled")
        ) {
            return Err(ReconcileError::WorkPresent);
        }
        let admission: Option<String> =
            sqlx::query_scalar("SELECT state FROM task_admissions WHERE task_id=?")
                .bind(&t.task_id)
                .fetch_optional(&mut *c)
                .await
                .map_err(|_| ReconcileError::Database)?;
        if admission.as_deref() != Some("terminal") {
            return Err(ReconcileError::WorkPresent);
        }
        let open: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_work_queue WHERE task_id=? AND state NOT IN ('completed','expired','superseded')").bind(&t.task_id).fetch_one(&mut *c).await.map_err(|_| ReconcileError::Database)?;
        if open != 0 {
            return Err(ReconcileError::WorkPresent);
        }
        let open: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_leases WHERE task_id=? AND state IN ('active','stopping','recovery_needed')").bind(&t.task_id).fetch_one(&mut *c).await.map_err(|_| ReconcileError::Database)?;
        if open != 0 {
            return Err(ReconcileError::WorkPresent);
        }
        let open: i64 = sqlx::query_scalar("SELECT count(*) FROM task_continuations WHERE task_id=? AND state NOT IN ('terminal','released')").bind(&t.task_id).fetch_one(&mut *c).await.map_err(|_| ReconcileError::Database)?;
        if open != 0 {
            return Err(ReconcileError::WorkPresent);
        }
        let disp: Option<String> = sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE delivery_id=? AND task_id=? AND attempt=?").bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).fetch_optional(&mut *c).await.map_err(|_| ReconcileError::Database)?;
        if !matches!(
            disp.as_deref(),
            Some("prepared" | "acknowledged" | "outcome_unknown" | "terminal")
        ) {
            return Err(ReconcileError::WorkPresent);
        }
    }
    for t in tuples {
        let ack = sqlx::query("UPDATE deliveries SET acknowledged_at=COALESCE(acknowledged_at,?) WHERE delivery_id=? AND task_id=? AND attempt=?").bind(now).bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).execute(&mut *c).await.map_err(|_| ReconcileError::Database)?;
        if ack.rows_affected() != 1 {
            return Err(ReconcileError::Fenced);
        }
        let changed = sqlx::query("UPDATE delivery_dispositions SET state='terminal', reason_code='offline_reconciled' WHERE delivery_id=? AND task_id=? AND attempt=? AND state IN ('prepared','acknowledged','outcome_unknown')").bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).execute(&mut *c).await.map_err(|_| ReconcileError::Database)?.rows_affected();
        if changed == 0 {
            let state: Option<String> = sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE delivery_id=? AND task_id=? AND attempt=?").bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).fetch_optional(&mut *c).await.map_err(|_| ReconcileError::Database)?;
            if state.as_deref() != Some("terminal") {
                return Err(ReconcileError::Fenced);
            }
        }
    }
    Ok(tuples.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{SqliteRepository, connect, migrate, plan_recovery};
    use uuid::Uuid;

    async fn fixture(disposition: Option<&str>) -> (PathBuf, DeliveryTuple) {
        let path = std::env::temp_dir().join(format!("t033-{}.db", Uuid::now_v7()));
        let pool = connect(&path).await.unwrap();
        migrate(&pool).await.unwrap();
        let ep = Uuid::now_v7().to_string();
        let conv = Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?, 'acp',1,'[]')").bind(&ep).bind(Uuid::now_v7().to_string()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
            .bind(&conv)
            .execute(&pool)
            .await
            .unwrap();
        let task = Uuid::now_v7().to_string();
        let delivery = Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'x',1,0,0,0)").bind(&task).bind(&task).bind(&ep).bind(&ep).bind(&conv).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'completed','2025-01-01T00:00:00Z','{}')").bind(Uuid::now_v7().to_string()).bind(&task).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,created_at,updated_at) VALUES(?,'terminal',0,'2025-01-01T00:00:00Z','2025-01-01T00:00:00Z')").bind(&task).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2025-01-01T00:00:00Z')").bind(&delivery).bind(&task).bind(&ep).execute(&pool).await.unwrap();
        if let Some(state) = disposition {
            sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES(?,?,1,?)").bind(&delivery).bind(&task).bind(state).execute(&pool).await.unwrap();
        }
        pool.close().await;
        (
            path,
            DeliveryTuple {
                delivery_id: delivery,
                task_id: task,
                attempt: 1,
            },
        )
    }
    #[test]
    fn strict_tuple_parser_rejects_unknown_and_accepts_multiple() {
        let a = [
            "reconcile-delivery",
            "--database",
            "x",
            "--delivery",
            "d1",
            "t1",
            "1",
            "--delivery",
            "d2",
            "t2",
            "2",
        ];
        let os = a.iter().map(std::ffi::OsString::from).collect::<Vec<_>>();
        let (_, tuples) = parse_args(&os).unwrap();
        assert_eq!(tuples.len(), 2);
        let bad = [
            "reconcile-delivery",
            "--database",
            "x",
            "--unknown",
            "v",
            "--delivery",
            "d",
            "t",
            "1",
        ];
        let os = bad.iter().map(std::ffi::OsString::from).collect::<Vec<_>>();
        assert_eq!(parse_args(&os), Err(ReconcileError::InvalidArguments));
    }

    #[tokio::test]
    async fn sqlite_reconciliation_updates_exact_rows_and_production_plan_excludes_delivery() {
        let (path, tuple) = fixture(Some("prepared")).await;
        let before = connect(&path).await.unwrap();
        let repo = SqliteRepository::new(before.clone());
        let plan_before = plan_recovery(&repo).await.unwrap();
        assert_eq!(plan_before.unacknowledged.len(), 1);
        before.close().await;
        assert_eq!(
            reconcile(&path, std::slice::from_ref(&tuple), "2025-01-02T00:00:00Z").await,
            Ok(1)
        );
        let after = connect(&path).await.unwrap();
        let repo = SqliteRepository::new(after.clone());
        let plan_after = plan_recovery(&repo).await.unwrap();
        assert!(plan_after.unacknowledged.is_empty());
        let state: String =
            sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE delivery_id=?")
                .bind(&tuple.delivery_id)
                .fetch_one(&after)
                .await
                .unwrap();
        assert_eq!(state, "terminal");
        assert_eq!(
            reconcile(&path, std::slice::from_ref(&tuple), "2025-01-03T00:00:00Z").await,
            Ok(1)
        );
        after.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn missing_disposition_and_partial_multi_tuple_roll_back() {
        let (path, good) = fixture(Some("prepared")).await;
        let (path2, missing) = fixture(None).await;
        assert_eq!(
            reconcile(&path2, std::slice::from_ref(&missing), "now").await,
            Err(ReconcileError::WorkPresent)
        );
        let (path3, second) = fixture(Some("prepared")).await;
        assert_eq!(
            reconcile(&path3, &[good.clone(), second.clone()], "now").await,
            Err(ReconcileError::WorkPresent)
        );
        let db = connect(&path3).await.unwrap();
        let ack: i64 =
            sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE acknowledged_at IS NOT NULL")
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(ack, 0);
        db.close().await;
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path2);
        let _ = std::fs::remove_file(path3);
    }
}
