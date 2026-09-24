//! Explicit, offline stale-runtime recovery. This path is never used by normal startup.

use std::path::Path;

use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RecoveryError {
    #[error("recovery rejected: invalid arguments")]
    InvalidArguments,
    #[error("recovery rejected: database error")]
    Database,
    #[error("recovery rejected: owner identity")]
    OwnerIdentity,
    #[error("recovery rejected: multiple active owners")]
    MultipleOwners,
    #[error("recovery rejected: work is present")]
    WorkPresent,
    #[error("recovery rejected: database is busy")]
    Busy,
    #[error("recovery rejected: fencing conflict")]
    Fenced,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RecoveryOutcome {
    Recovered,
    AlreadyStopped,
}

pub async fn recover_stale_runtime(
    database: impl AsRef<Path>,
    owner_token: &str,
    fingerprint: &str,
    now: &str,
) -> Result<RecoveryOutcome, RecoveryError> {
    if owner_token.is_empty() || fingerprint.is_empty() || now.is_empty() {
        return Err(RecoveryError::InvalidArguments);
    }
    let options = SqliteConnectOptions::new()
        .filename(database.as_ref())
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(5));
    let mut conn = SqliteConnection::connect_with(&options)
        .await
        .map_err(|_| RecoveryError::Database)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut conn)
        .await
        .map_err(|e| {
            if is_busy(&e) {
                RecoveryError::Busy
            } else {
                RecoveryError::Database
            }
        })?;
    let result = recover_in_transaction(&mut conn, owner_token, fingerprint, now).await;
    match result {
        Ok(value) => {
            sqlx::query("COMMIT")
                .execute(&mut conn)
                .await
                .map_err(|_| RecoveryError::Database)?;
            Ok(value)
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut conn).await;
            Err(error)
        }
    }
}

async fn recover_in_transaction(
    conn: &mut SqliteConnection,
    token: &str,
    fingerprint: &str,
    now: &str,
) -> Result<RecoveryOutcome, RecoveryError> {
    let owners = sqlx::query(
        "SELECT state, process_fingerprint FROM runtime_instances WHERE instance_token=?",
    )
    .bind(token)
    .fetch_all(&mut *conn)
    .await
    .map_err(|_| RecoveryError::Database)?;
    let active_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM runtime_instances WHERE state IN ('active','stopping')",
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|_| RecoveryError::Database)?;
    if active_count > 1 {
        return Err(RecoveryError::MultipleOwners);
    }
    let Some(owner) = owners.first() else {
        return Err(RecoveryError::OwnerIdentity);
    };
    let state: String = owner
        .try_get("state")
        .map_err(|_| RecoveryError::Database)?;
    let stored: String = owner
        .try_get("process_fingerprint")
        .map_err(|_| RecoveryError::Database)?;
    if stored != fingerprint {
        return Err(RecoveryError::OwnerIdentity);
    }
    if state == "stopped" {
        return Ok(RecoveryOutcome::AlreadyStopped);
    }
    if state != "active" && state != "stopping" {
        return Err(RecoveryError::OwnerIdentity);
    }

    let bad = |count: i64| {
        if count == 0 {
            Ok(())
        } else {
            Err(RecoveryError::WorkPresent)
        }
    };
    bad(sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM task_admissions WHERE state NOT IN ('terminal','recovery_needed')",
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|_| RecoveryError::Database)?)?;
    bad(sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM agent_work_queue WHERE state NOT IN ('completed','expired','superseded')",
    ).fetch_one(&mut *conn).await.map_err(|_| RecoveryError::Database)?)?;
    bad(sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM execution_leases WHERE state IN ('active','stopping','recovery_needed')",
    ).fetch_one(&mut *conn).await.map_err(|_| RecoveryError::Database)?)?;
    bad(sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM task_continuations WHERE state NOT IN ('terminal','released')",
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|_| RecoveryError::Database)?)?;
    bad(sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM deliveries d LEFT JOIN delivery_dispositions p ON p.delivery_id=d.delivery_id WHERE d.acknowledged_at IS NULL OR p.state IS NULL OR p.state IN ('prepared','acknowledged','outcome_unknown')",
    ).fetch_one(&mut *conn).await.map_err(|_| RecoveryError::Database)?)?;
    bad(sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM tasks t LEFT JOIN task_events e ON e.task_id=t.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=t.task_id) WHERE e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled')",
    ).fetch_one(&mut *conn).await.map_err(|_| RecoveryError::Database)?)?;
    bad(sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM deliveries WHERE acknowledged_at IS NULL",
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|_| RecoveryError::Database)?)?;
    bad(sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM deliveries d LEFT JOIN task_events e ON e.task_id=d.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=d.task_id) WHERE d.acknowledged_at IS NOT NULL AND (e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled'))",
    ).fetch_one(&mut *conn).await.map_err(|_| RecoveryError::Database)?)?;

    let updated = sqlx::query(
        "UPDATE runtime_instances SET state='stopped', heartbeat_at=? WHERE instance_token=? AND process_fingerprint=? AND state IN ('active','stopping')",
    )
    .bind(now)
    .bind(token)
    .bind(fingerprint)
    .execute(&mut *conn)
    .await
    .map_err(|_| RecoveryError::Database)?;
    if updated.rows_affected() != 1 {
        return Err(RecoveryError::Fenced);
    }
    Ok(RecoveryOutcome::Recovered)
}

fn is_busy(error: &sqlx::Error) -> bool {
    error.to_string().to_ascii_lowercase().contains("busy")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate};
    use sqlx::SqlitePool;
    use uuid::Uuid;

    async fn fixture() -> (std::path::PathBuf, SqlitePool) {
        let path = std::env::temp_dir().join(format!("t032-{}.db", Uuid::now_v7()));
        let pool = connect(&path).await.unwrap();
        migrate(&pool).await.unwrap();
        sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('legacy','t','t','active','fp')")
            .execute(&pool).await.unwrap();
        (path, pool)
    }

    #[tokio::test]
    async fn clean_legacy_owner_is_recovered_and_idempotent() {
        let (path, pool) = fixture().await;
        assert_eq!(
            recover_stale_runtime(&path, "legacy", "fp", "now")
                .await
                .unwrap(),
            RecoveryOutcome::Recovered
        );
        assert_eq!(
            recover_stale_runtime(&path, "legacy", "fp", "later")
                .await
                .unwrap(),
            RecoveryOutcome::AlreadyStopped
        );
        let state: String =
            sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token='legacy'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "stopped");
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn wrong_identity_and_unbound_queue_fail_closed() {
        let (path, pool) = fixture().await;
        assert_eq!(
            recover_stale_runtime(&path, "legacy", "wrong", "now").await,
            Err(RecoveryError::OwnerIdentity)
        );
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('ep','agent','acp',1,'[]')").execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO conversations(conversation_id,participants_json) VALUES('conv','[]')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','ep','ep','conv','x',1,0,0,0)").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES('del','task',1,'ep','t')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO agent_work_queue(queue_id,task_id,delivery_id,target_endpoint_id,room_id,sender_endpoint_id,idempotency_key,body_hash,lane,state,sequence,revision,created_at,updated_at) VALUES('q','task','del','ep','room','ep','key','hash','ordinary','queued',1,0,'t','t')").execute(&pool).await.unwrap();
        assert_eq!(
            recover_stale_runtime(&path, "legacy", "fp", "now").await,
            Err(RecoveryError::WorkPresent)
        );
        let state: String =
            sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token='legacy'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "active");
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }
}
