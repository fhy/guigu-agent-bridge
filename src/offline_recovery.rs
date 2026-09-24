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

pub fn parse_cli_args(
    args: &[std::ffi::OsString],
) -> Result<(std::path::PathBuf, String, String), RecoveryError> {
    if args.len() != 7 || args[0] != "recover-runtime" {
        return Err(RecoveryError::InvalidArguments);
    }
    let mut values = [None, None, None];
    for pair in args[1..].chunks_exact(2) {
        let flag = pair[0].to_str().ok_or(RecoveryError::InvalidArguments)?;
        let value = pair[1].to_str().ok_or(RecoveryError::InvalidArguments)?;
        let slot = match flag {
            "--database" => &mut values[0],
            "--owner-token" => &mut values[1],
            "--process-fingerprint" => &mut values[2],
            _ => return Err(RecoveryError::InvalidArguments),
        };
        if slot.replace(value.to_owned()).is_some() || value.is_empty() {
            return Err(RecoveryError::InvalidArguments);
        }
    }
    Ok((
        std::path::PathBuf::from(values[0].take().ok_or(RecoveryError::InvalidArguments)?),
        values[1].take().ok_or(RecoveryError::InvalidArguments)?,
        values[2].take().ok_or(RecoveryError::InvalidArguments)?,
    ))
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
    recover_in_transaction_inner(conn, token, fingerprint, now, false).await
}

#[cfg(test)]
async fn recover_in_transaction_with_fence(
    conn: &mut SqliteConnection,
    token: &str,
    fingerprint: &str,
    now: &str,
) -> Result<RecoveryOutcome, RecoveryError> {
    recover_in_transaction_inner(conn, token, fingerprint, now, true).await
}

async fn recover_in_transaction_inner(
    conn: &mut SqliteConnection,
    token: &str,
    fingerprint: &str,
    now: &str,
    pre_cas_fence: bool,
) -> Result<RecoveryOutcome, RecoveryError> {
    #[cfg(not(test))]
    let _ = pre_cas_fence;
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

    #[cfg(test)]
    if pre_cas_fence {
        sqlx::query("UPDATE runtime_instances SET process_fingerprint='fenced-by-test' WHERE instance_token=?")
            .bind(token)
            .execute(&mut *conn)
            .await
            .map_err(|_| RecoveryError::Database)?;
    }
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
    let message = error.to_string().to_ascii_lowercase();
    message.contains("busy") || message.contains("locked")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{SqliteRepository, connect, migrate, plan_recovery};
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
        let endpoint = Uuid::now_v7().to_string();
        let agent_id = Uuid::now_v7().to_string();
        let conversation = Uuid::now_v7().to_string();
        let task = Uuid::now_v7().to_string();
        let delivery = Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?, 'acp',1,'[]')").bind(&endpoint).bind(&agent_id).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
            .bind(&conversation)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'x',1,0,0,0)").bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2025-01-01T00:00:00Z')").bind(&delivery).bind(&task).bind(&endpoint).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO agent_work_queue(queue_id,task_id,delivery_id,target_endpoint_id,room_id,sender_endpoint_id,idempotency_key,body_hash,lane,state,sequence,revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'hash','ordinary','queued',1,0,'2025-01-01T00:00:00Z','2025-01-01T00:00:00Z')").bind(Uuid::now_v7().to_string()).bind(&task).bind(&delivery).bind(&endpoint).bind(Uuid::now_v7().to_string()).bind(&endpoint).bind("key").execute(&pool).await.unwrap();
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

    #[tokio::test]
    async fn every_nonterminal_queue_state_is_rejected_and_terminal_allowlist_is_safe() {
        for state in ["queued", "paused", "claimed", "running", "recovery_needed"] {
            let (path, pool) = fixture().await;
            sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('ep','agent','acp',1,'[]')").execute(&pool).await.unwrap();
            sqlx::query(
                "INSERT INTO conversations(conversation_id,participants_json) VALUES('conv','[]')",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','ep','ep','conv','x',1,0,0,0)").execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES('del','task',1,'ep','t')").execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO agent_work_queue(queue_id,task_id,delivery_id,target_endpoint_id,room_id,sender_endpoint_id,idempotency_key,body_hash,lane,state,sequence,revision,created_at,updated_at) VALUES('q','task','del','ep','room','ep','key','hash','ordinary',?,1,0,'t','t')").bind(state).execute(&pool).await.unwrap();
            assert_eq!(
                recover_stale_runtime(&path, "legacy", "fp", "now").await,
                Err(RecoveryError::WorkPresent),
                "state={state}"
            );
            pool.close().await;
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn multiple_active_or_stopping_owners_are_rejected() {
        let (path, pool) = fixture().await;
        sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('other','t','t','stopping','other-fp')").execute(&pool).await.unwrap();
        assert_eq!(
            recover_stale_runtime(&path, "legacy", "fp", "now").await,
            Err(RecoveryError::MultipleOwners)
        );
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn admission_lease_continuation_and_delivery_states_fail_closed() {
        for (label, sql) in [
            (
                "admission",
                "INSERT INTO task_admissions(task_id,state,revision,created_at,updated_at) VALUES('task','ready',0,'t','t')",
            ),
            (
                "lease",
                "INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('rk','task','legacy',1,'active','t','t','later')",
            ),
            (
                "continuation",
                "INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes) VALUES('task','rk','del',1,1,'in_flight',1,0,0,'p','t','t','t',0)",
            ),
        ] {
            let (path, pool) = fixture().await;
            sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('ep','agent','acp',1,'[]')").execute(&pool).await.unwrap();
            sqlx::query(
                "INSERT INTO conversations(conversation_id,participants_json) VALUES('conv','[]')",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','ep','ep','conv','x',1,0,0,0)").execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES('del','task',1,'ep','t')").execute(&pool).await.unwrap();
            if label == "continuation" {
                sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('rk','task','legacy',1,'released','t','t','later')").execute(&pool).await.unwrap();
            }
            sqlx::query(sql).execute(&pool).await.unwrap();
            assert_eq!(
                recover_stale_runtime(&path, "legacy", "fp", "now").await,
                Err(RecoveryError::WorkPresent),
                "{label}"
            );
            let state: String = sqlx::query_scalar(
                "SELECT state FROM runtime_instances WHERE instance_token='legacy'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(state, "active");
            pool.close().await;
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn transaction_local_planner_queries_match_empty_production_plan() {
        let (path, pool) = fixture().await;
        let unfinished: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks t LEFT JOIN task_events e ON e.task_id=t.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=t.task_id) WHERE e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled')").fetch_one(&pool).await.unwrap();
        assert_eq!(unfinished, 0);
        let unack: i64 =
            sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE acknowledged_at IS NULL")
                .fetch_one(&pool)
                .await
                .unwrap();
        let awaiting: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries d LEFT JOIN task_events e ON e.task_id=d.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=d.task_id) WHERE d.acknowledged_at IS NOT NULL AND (e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled'))").fetch_one(&pool).await.unwrap();
        assert_eq!((unfinished, unack, awaiting), (0, 0, 0));
        assert_eq!(
            recover_stale_runtime(&path, "legacy", "fp", "now").await,
            Ok(RecoveryOutcome::Recovered)
        );
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn stale_owner_recovery_allows_next_runtime_generation() {
        let (path, pool) = fixture().await;
        assert_eq!(
            recover_stale_runtime(&path, "legacy", "fp", "now").await,
            Ok(RecoveryOutcome::Recovered)
        );
        pool.close().await;
        let root = std::env::temp_dir().join(format!("t032-config-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let config = root.join("bridge.toml");
        let sessions = root.join("sessions");
        std::fs::write(
            &config,
            format!(
                "[bridge]\ndatabase = {:?}\nsession_root = {:?}\n",
                path, sessions
            ),
        )
        .unwrap();
        let runtime = crate::app::AppRuntime::start(&config).await.unwrap();
        assert_ne!(
            runtime.health_state().snapshot().await.diagnostic,
            Some("recovery-blocked")
        );
        runtime.shutdown().await.unwrap();
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(config);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn planner_parity_covers_no_event_terminal_nonterminal_and_delivery_cases() {
        let (path, pool) = fixture().await;
        let endpoint = Uuid::now_v7().to_string();
        let agent_id = Uuid::now_v7().to_string();
        let conversation = Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?, 'acp',1,'[]')")
            .bind(&endpoint).bind(&agent_id).execute(&pool)
            .await.unwrap();
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
            .bind(&conversation)
            .execute(&pool)
            .await
            .unwrap();
        let mut task_ids = Vec::new();
        for status in [
            None,
            Some("completed"),
            Some("failed"),
            Some("timed_out"),
            Some("cancelled"),
            Some("running"),
        ]
        .into_iter()
        {
            let task = Uuid::now_v7().to_string();
            task_ids.push(task.clone());
            sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'x',1,0,0,0)").bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(&pool).await.unwrap();
            if let Some(status) = status {
                sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,?,?,?,'{}')").bind(Uuid::now_v7().to_string()).bind(&task).bind(1).bind(status).bind("t").execute(&pool).await.unwrap();
            }
        }
        let unack = Uuid::now_v7().to_string();
        let await_id = Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2025-01-01T00:00:00Z'),(?,?,1,?,'2025-01-01T00:00:00Z')").bind(&unack).bind(&task_ids[1]).bind(&endpoint).bind(&await_id).bind(&task_ids[5]).bind(&endpoint).execute(&pool).await.unwrap();
        sqlx::query(
            "UPDATE deliveries SET acknowledged_at='2025-01-01T00:00:00Z' WHERE delivery_id=?",
        )
        .bind(&await_id)
        .execute(&pool)
        .await
        .unwrap();
        let repository = SqliteRepository::new(pool.clone());
        let plan = plan_recovery(&repository).await.unwrap();
        assert_eq!(plan.unfinished.len(), 2);
        assert_eq!(plan.unacknowledged.len(), 1);
        assert_eq!(plan.awaiting_outcome.len(), 1);
        let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM tasks t LEFT JOIN task_events e ON e.task_id=t.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=t.task_id) WHERE e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled')), (SELECT count(*) FROM deliveries WHERE acknowledged_at IS NULL), (SELECT count(*) FROM deliveries d LEFT JOIN task_events e ON e.task_id=d.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=d.task_id) WHERE d.acknowledged_at IS NOT NULL AND (e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled')))").fetch_one(&pool).await.unwrap();
        assert_eq!(counts, (2, 1, 1));
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn immediate_lock_contention_and_fingerprint_fencing_leave_snapshot_unchanged() {
        let (path, pool) = fixture().await;
        let before: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM runtime_instances),(SELECT count(*) FROM tasks)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let mut lock = sqlx::SqliteConnection::connect_with(
            &sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&path)
                .busy_timeout(std::time::Duration::from_millis(1)),
        )
        .await
        .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut lock)
            .await
            .unwrap();
        let blocked = recover_stale_runtime(&path, "legacy", "fp", "now").await;
        assert_eq!(blocked, Err(RecoveryError::Busy));
        sqlx::query("ROLLBACK").execute(&mut lock).await.unwrap();
        assert_eq!(
            recover_stale_runtime(&path, "legacy", "wrong", "now").await,
            Err(RecoveryError::OwnerIdentity)
        );
        let after: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM runtime_instances),(SELECT count(*) FROM tasks)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(before, after);
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn missing_owner_and_malformed_database_fail_closed_without_output_values() {
        let (path, pool) = fixture().await;
        sqlx::query("DELETE FROM runtime_instances WHERE instance_token='legacy'")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            recover_stale_runtime(&path, "legacy", "fp", "now").await,
            Err(RecoveryError::OwnerIdentity)
        );
        pool.close().await;
        let _ = std::fs::remove_file(&path);
        let malformed = std::env::temp_dir().join(format!("t032-malformed-{}", Uuid::now_v7()));
        std::fs::write(&malformed, b"not sqlite").unwrap();
        assert_eq!(
            recover_stale_runtime(&malformed, "secret-token", "secret-fp", "now").await,
            Err(RecoveryError::Database)
        );
        let _ = std::fs::remove_file(malformed);
    }

    #[tokio::test]
    async fn lease_and_continuation_each_nonterminal_state_fail_closed() {
        for state in ["active", "stopping", "recovery_needed"] {
            let (path, pool) = fixture().await;
            sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('ep','agent','acp',1,'[]')").execute(&pool).await.unwrap();
            sqlx::query(
                "INSERT INTO conversations(conversation_id,participants_json) VALUES('conv','[]')",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','ep','ep','conv','x',1,0,0,0)").execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES('del','task',1,'ep','2025-01-01T00:00:00Z')").execute(&pool).await.unwrap();
            if state == "stopping" {
                sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('rk','task','legacy',1,'active','2025-01-01T00:00:00Z','2025-01-01T00:00:00Z','2025-01-02T00:00:00Z')").execute(&pool).await.unwrap();
                let mut connection = pool.acquire().await.unwrap();
                sqlx::query("PRAGMA ignore_check_constraints=ON")
                    .execute(&mut *connection)
                    .await
                    .unwrap();
                sqlx::query("UPDATE execution_leases SET state='stopping' WHERE resource_key='rk'")
                    .execute(&mut *connection)
                    .await
                    .unwrap();
            } else {
                sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('rk','task','legacy',1,?,'2025-01-01T00:00:00Z','2025-01-01T00:00:00Z','2025-01-02T00:00:00Z')").bind(state).execute(&pool).await.unwrap();
            }
            assert_eq!(
                recover_stale_runtime(&path, "legacy", "fp", "now").await,
                Err(RecoveryError::WorkPresent),
                "lease {state}"
            );
            let owner: String = sqlx::query_scalar(
                "SELECT state FROM runtime_instances WHERE instance_token='legacy'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(owner, "active");
            pool.close().await;
            let _ = std::fs::remove_file(path);
        }
        for state in ["in_flight", "ready", "recovery_needed"] {
            let (path, pool) = fixture().await;
            sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('ep','agent','acp',1,'[]')").execute(&pool).await.unwrap();
            sqlx::query(
                "INSERT INTO conversations(conversation_id,participants_json) VALUES('conv','[]')",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','ep','ep','conv','x',1,0,0,0)").execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES('del','task',1,'ep','2025-01-01T00:00:00Z')").execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('rk','task','legacy',1,'released','2025-01-01T00:00:00Z','2025-01-01T00:00:00Z','2025-01-02T00:00:00Z')").execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes) VALUES('task','rk','del',1,1,?,1,0,0,'p','2025-01-01T00:00:00Z','2025-01-01T00:00:00Z','2025-01-01T00:00:00Z',0)").bind(state).execute(&pool).await.unwrap();
            assert_eq!(
                recover_stale_runtime(&path, "legacy", "fp", "now").await,
                Err(RecoveryError::WorkPresent),
                "continuation {state}"
            );
            pool.close().await;
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn final_cas_rows_affected_zero_is_fenced() {
        let (path, pool) = fixture().await;
        let mut conn = SqliteConnection::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("UPDATE runtime_instances SET state='stopping' WHERE instance_token='legacy'")
            .execute(&mut conn)
            .await
            .unwrap();
        let rows = sqlx::query("UPDATE runtime_instances SET state='stopped' WHERE instance_token='legacy' AND process_fingerprint='fp' AND state='active'").execute(&mut conn).await.unwrap().rows_affected();
        assert_eq!(rows, 0);
        sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
        let state: String =
            sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token='legacy'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "active");
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn production_final_cas_reports_fenced_after_pre_cas_owner_change() {
        let (path, pool) = fixture().await;
        let mut conn = SqliteConnection::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            recover_in_transaction_with_fence(&mut conn, "legacy", "fp", "now").await,
            Err(RecoveryError::Fenced)
        );
        sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
        let snapshot: (String, String) = sqlx::query_as("SELECT state, process_fingerprint FROM runtime_instances WHERE instance_token='legacy'").fetch_one(&pool).await.unwrap();
        assert_eq!(snapshot, ("active".into(), "fp".into()));
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn acknowledged_disposition_states_and_missing_disposition_fail_closed() {
        for disposition in [
            Some("prepared"),
            Some("acknowledged"),
            Some("outcome_unknown"),
            None,
        ] {
            let (path, pool) = fixture().await;
            sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('ep','agent','acp',1,'[]')").execute(&pool).await.unwrap();
            sqlx::query(
                "INSERT INTO conversations(conversation_id,participants_json) VALUES('conv','[]')",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','ep','ep','conv','x',1,0,0,0)").execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at,acknowledged_at) VALUES('del','task',1,'ep','2025-01-01T00:00:00Z','2025-01-01T00:00:01Z')").execute(&pool).await.unwrap();
            if let Some(state) = disposition {
                sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES('del','task',1,?)").bind(state).execute(&pool).await.unwrap();
            }
            assert_eq!(
                recover_stale_runtime(&path, "legacy", "fp", "now").await,
                Err(RecoveryError::WorkPresent)
            );
            let owner: String = sqlx::query_scalar(
                "SELECT state FROM runtime_instances WHERE instance_token='legacy'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(owner, "active");
            pool.close().await;
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn cli_process_emits_fixed_success_already_stopped_and_rejection() {
        let (path, pool) = fixture().await;
        pool.close().await;
        let exe = std::env::var("CARGO_BIN_EXE_guigu-agent-bridge")
            .unwrap_or_else(|_| "target/debug/guigu-agent-bridge".to_owned());
        let database = path.to_str().unwrap().to_owned();
        let run = |token: &'static str| {
            let exe = exe.clone();
            let database = database.clone();
            async move {
                tokio::process::Command::new(exe)
                    .args([
                        "recover-runtime",
                        "--database",
                        database.as_str(),
                        "--owner-token",
                        token,
                        "--process-fingerprint",
                        "fp",
                    ])
                    .output()
                    .await
                    .unwrap()
            }
        };
        let first = run("legacy").await;
        assert!(first.status.success());
        assert_eq!(
            String::from_utf8_lossy(&first.stdout)
                .lines()
                .last()
                .unwrap(),
            "recover-runtime result=recovered"
        );
        let second = run("legacy").await;
        assert!(second.status.success());
        assert_eq!(
            String::from_utf8_lossy(&second.stdout)
                .lines()
                .last()
                .unwrap(),
            "recover-runtime result=already-stopped"
        );
        let rejected = run("secret-token").await;
        assert!(!rejected.status.success());
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&rejected.stdout),
            String::from_utf8_lossy(&rejected.stderr)
        );
        assert!(
            !combined.contains("secret-token")
                && !combined.contains(path.to_str().unwrap())
                && !combined.contains("SELECT")
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cli_rejects_unknown_duplicate_missing_and_non_utf8_arguments() {
        let ok = [
            "recover-runtime",
            "--database",
            "x",
            "--owner-token",
            "o",
            "--process-fingerprint",
            "f",
        ];
        let args = ok.iter().map(std::ffi::OsString::from).collect::<Vec<_>>();
        assert!(parse_cli_args(&args).is_ok());
        let mut duplicate = args.clone();
        duplicate[3] = "--database".into();
        assert_eq!(
            parse_cli_args(&duplicate),
            Err(RecoveryError::InvalidArguments)
        );
        let mut unknown = args.clone();
        unknown[1] = "--unknown".into();
        assert_eq!(
            parse_cli_args(&unknown),
            Err(RecoveryError::InvalidArguments)
        );
        assert_eq!(
            parse_cli_args(&args[..5]),
            Err(RecoveryError::InvalidArguments)
        );
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let mut invalid = args.clone();
            invalid[4] = std::ffi::OsString::from_vec(vec![0xff]);
            assert_eq!(
                parse_cli_args(&invalid),
                Err(RecoveryError::InvalidArguments)
            );
        }
    }
}
