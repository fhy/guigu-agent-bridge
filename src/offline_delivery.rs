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
    reconcile_tx_inner(c, tuples, now, false).await
}

#[cfg(test)]
async fn reconcile_tx_with_fence(
    c: &mut SqliteConnection,
    tuples: &[DeliveryTuple],
    now: &str,
) -> Result<usize, ReconcileError> {
    reconcile_tx_inner(c, tuples, now, true).await
}

async fn reconcile_tx_inner(
    c: &mut SqliteConnection,
    tuples: &[DeliveryTuple],
    now: &str,
    pre_cas_fence: bool,
) -> Result<usize, ReconcileError> {
    #[cfg(not(test))]
    let _ = pre_cas_fence;
    let before = planner_counts(c).await?;
    let mut newly_acknowledged = 0_i64;
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
    #[cfg(test)]
    if pre_cas_fence {
        sqlx::query("UPDATE deliveries SET acknowledged_at='fenced-by-test' WHERE delivery_id=?")
            .bind(&tuples[0].delivery_id)
            .execute(&mut *c)
            .await
            .map_err(|_| ReconcileError::Database)?;
    }
    for t in tuples {
        let was_ack: Option<String> = sqlx::query_scalar("SELECT acknowledged_at FROM deliveries WHERE delivery_id=? AND task_id=? AND attempt=?").bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).fetch_one(&mut *c).await.map_err(|_| ReconcileError::Database)?;
        if was_ack.is_none() {
            let ack = sqlx::query("UPDATE deliveries SET acknowledged_at=? WHERE delivery_id=? AND task_id=? AND attempt=? AND acknowledged_at IS NULL").bind(now).bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).execute(&mut *c).await.map_err(|_| ReconcileError::Database)?;
            if ack.rows_affected() != 1 {
                return Err(ReconcileError::Fenced);
            }
            newly_acknowledged += 1;
        }
        let changed = sqlx::query("UPDATE delivery_dispositions SET state='terminal', reason_code='offline_reconciled' WHERE delivery_id=? AND task_id=? AND attempt=? AND state IN ('prepared','acknowledged','outcome_unknown')").bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).execute(&mut *c).await.map_err(|_| ReconcileError::Database)?.rows_affected();
        if changed == 0 {
            let state: Option<String> = sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE delivery_id=? AND task_id=? AND attempt=?").bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).fetch_optional(&mut *c).await.map_err(|_| ReconcileError::Database)?;
            if state.as_deref() != Some("terminal") {
                return Err(ReconcileError::Fenced);
            }
        }
    }
    let after = planner_counts(c).await?;
    if after.0 != before.0 || after.2 != before.2 || after.1 != before.1 - newly_acknowledged {
        return Err(ReconcileError::Fenced);
    }
    Ok(tuples.len())
}

async fn planner_counts(c: &mut SqliteConnection) -> Result<(i64, i64, i64), ReconcileError> {
    let unfinished: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks t LEFT JOIN task_events e ON e.task_id=t.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=t.task_id) WHERE e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled')").fetch_one(&mut *c).await.map_err(|_| ReconcileError::Database)?;
    let unack: i64 =
        sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE acknowledged_at IS NULL")
            .fetch_one(&mut *c)
            .await
            .map_err(|_| ReconcileError::Database)?;
    let awaiting: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries d LEFT JOIN task_events e ON e.task_id=d.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=d.task_id) WHERE d.acknowledged_at IS NOT NULL AND (e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled'))").fetch_one(&mut *c).await.map_err(|_| ReconcileError::Database)?;
    Ok((unfinished, unack, awaiting))
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

    #[tokio::test]
    async fn event_and_admission_matrix_and_fenced_transaction_are_fail_closed() {
        for status in [
            "running",
            "ready",
            "enqueued",
            "dispatching",
            "completed",
            "failed",
            "timed_out",
            "cancelled",
        ] {
            let (path, tuple) = fixture(Some("prepared")).await;
            let pool = connect(&path).await.unwrap();
            if status == "ready" || status == "enqueued" || status == "dispatching" {
                sqlx::query("UPDATE task_admissions SET state=? WHERE task_id=?")
                    .bind(status)
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            } else if status == "running" {
                sqlx::query("UPDATE task_events SET status='running' WHERE task_id=?")
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            } else {
                sqlx::query("UPDATE task_events SET status=? WHERE task_id=?")
                    .bind(status)
                    .bind(&tuple.task_id)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            pool.close().await;
            let result = reconcile(&path, std::slice::from_ref(&tuple), "now").await;
            if status == "completed"
                || status == "failed"
                || status == "timed_out"
                || status == "cancelled"
            {
                assert_eq!(result, Ok(1));
            } else {
                assert_eq!(result, Err(ReconcileError::WorkPresent));
            }
            let _ = std::fs::remove_file(path);
        }
        let (path, tuple) = fixture(Some("prepared")).await;
        let mut conn = SqliteConnection::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            reconcile_tx_with_fence(&mut conn, std::slice::from_ref(&tuple), "now").await,
            Err(ReconcileError::Fenced)
        );
        sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
        let _ = conn.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn busy_and_process_cli_paths_are_fixed_and_redacted() {
        let (path, tuple) = fixture(Some("prepared")).await;
        let mut lock = SqliteConnection::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut lock)
            .await
            .unwrap();
        assert_eq!(
            reconcile(&path, std::slice::from_ref(&tuple), "now").await,
            Err(ReconcileError::Busy)
        );
        sqlx::query("ROLLBACK").execute(&mut lock).await.unwrap();
        let _ = lock.close().await;
        let exe = std::env::var("CARGO_BIN_EXE_guigu-agent-bridge")
            .unwrap_or_else(|_| "target/debug/guigu-agent-bridge".into());
        let db = path.to_str().unwrap().to_owned();
        let out = tokio::process::Command::new(exe)
            .args([
                "reconcile-delivery",
                "--database",
                db.as_str(),
                "--delivery",
                tuple.delivery_id.as_str(),
                tuple.task_id.as_str(),
                "1",
            ])
            .output()
            .await
            .unwrap();
        assert!(out.status.success());
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("reconcile-delivery result=reconciled")
        );
        assert!(out.stderr.is_empty());
        let bad = tokio::process::Command::new(
            std::env::var("CARGO_BIN_EXE_guigu-agent-bridge")
                .unwrap_or_else(|_| "target/debug/guigu-agent-bridge".into()),
        )
        .args([
            "reconcile-delivery",
            "--database",
            db.as_str(),
            "--delivery",
            "wrong",
            tuple.task_id.as_str(),
            "1",
        ])
        .output()
        .await
        .unwrap();
        assert!(!bad.status.success());
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&bad.stdout),
            String::from_utf8_lossy(&bad.stderr)
        );
        assert!(!text.contains("wrong") && !text.contains("SELECT") && !text.contains(&db));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn closed_work_and_disposition_state_matrix_is_fail_closed() {
        for state in ["queued", "paused", "claimed", "running", "recovery_needed"] {
            let (path, tuple) = fixture(Some("prepared")).await;
            let pool = connect(&path).await.unwrap();
            let endpoint: String =
                sqlx::query_scalar("SELECT target_endpoint_id FROM deliveries WHERE delivery_id=?")
                    .bind(&tuple.delivery_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            sqlx::query("INSERT INTO agent_work_queue(queue_id,task_id,delivery_id,target_endpoint_id,room_id,sender_endpoint_id,idempotency_key,body_hash,lane,state,sequence,revision,created_at,updated_at) VALUES('q',?,?,?,?,?,'k','h','ordinary',?,1,0,'2025-01-01T00:00:00Z','2025-01-01T00:00:00Z')").bind(&tuple.task_id).bind(&tuple.delivery_id).bind(&endpoint).bind("room").bind(&endpoint).bind(state).execute(&pool).await.unwrap();
            pool.close().await;
            assert_eq!(
                reconcile(&path, std::slice::from_ref(&tuple), "now").await,
                Err(ReconcileError::WorkPresent)
            );
            let _ = std::fs::remove_file(path);
        }
        for state in ["active", "released", "recovery_needed"] {
            let (path, tuple) = fixture(Some("prepared")).await;
            let pool = connect(&path).await.unwrap();
            sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('r',?, 'o',1,?,'2025-01-01T00:00:00Z','2025-01-01T00:00:00Z','2025-01-02T00:00:00Z')").bind(&tuple.task_id).bind(state).execute(&pool).await.unwrap();
            pool.close().await;
            let result = reconcile(&path, std::slice::from_ref(&tuple), "now").await;
            if state == "released" {
                assert_eq!(result, Ok(1));
            } else {
                assert_eq!(result, Err(ReconcileError::WorkPresent));
            }
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn malformed_schema_and_t032_composition_are_fail_closed() {
        let path = std::env::temp_dir().join(format!("t033-malformed-{}", Uuid::now_v7()));
        std::fs::write(&path, b"not sqlite").unwrap();
        let tuple = DeliveryTuple {
            delivery_id: "d".into(),
            task_id: "t".into(),
            attempt: 1,
        };
        assert_eq!(
            reconcile(&path, &[tuple], "now").await,
            Err(ReconcileError::Database)
        );
        let _ = std::fs::remove_file(path);
        let (path, tuple) = fixture(Some("prepared")).await;
        let pool = connect(&path).await.unwrap();
        let unresolved: i64 =
            sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE acknowledged_at IS NULL")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(unresolved, 1);
        pool.close().await;
        assert_eq!(
            reconcile(&path, std::slice::from_ref(&tuple), "now").await,
            Ok(1)
        );
        let pool = connect(&path).await.unwrap();
        let unresolved: i64 =
            sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE acknowledged_at IS NULL")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(unresolved, 0);
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }
}
