//! Read-only selection of an exact T037 pre-acceptance tuple.
use crate::models::TaskEventPayload;
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SelectionError {
    #[error("tuple selection rejected: invalid arguments")]
    InvalidArguments,
    #[error("tuple selection rejected: empty")]
    Empty,
    #[error("tuple selection rejected: multiple")]
    Multiple,
    #[error("tuple selection rejected: malformed")]
    Malformed,
    #[error("tuple selection rejected: conflicting")]
    Conflicting,
    #[error("tuple selection rejected: nonterminal")]
    Nonterminal,
    #[error("tuple selection rejected: busy")]
    Busy,
    #[error("tuple selection rejected: database")]
    Database,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedTuple {
    pub delivery_id: String,
    pub task_id: String,
    pub attempt: i64,
}

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, SelectionError> {
    if args.len() != 3 || args[0] != "select-preacceptance" || args[1] != "--database" {
        return Err(SelectionError::InvalidArguments);
    }
    let path = args[2].to_str().ok_or(SelectionError::InvalidArguments)?;
    if path.is_empty() {
        return Err(SelectionError::InvalidArguments);
    }
    Ok(PathBuf::from(path))
}

/// Select exactly one T037-eligible tuple from one consistent read snapshot.
/// This function never writes, migrates, retries, or invokes the recovery command.
pub async fn select(database: impl AsRef<Path>) -> Result<SelectedTuple, SelectionError> {
    let opts = SqliteConnectOptions::new()
        .filename(database.as_ref())
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(5));
    let mut c = SqliteConnection::connect_with(&opts)
        .await
        .map_err(|_| SelectionError::Database)?;
    sqlx::query("BEGIN").execute(&mut c).await.map_err(|e| {
        if is_busy(&e) {
            SelectionError::Busy
        } else {
            SelectionError::Database
        }
    })?;
    let result = select_tx(&mut c).await;
    let _ = sqlx::query("ROLLBACK").execute(&mut c).await;
    let _ = c.close().await;
    result
}

async fn select_tx(c: &mut SqliteConnection) -> Result<SelectedTuple, SelectionError> {
    let rows = sqlx::query("SELECT d.delivery_id,d.task_id,d.attempt,d.acknowledged_at FROM deliveries d ORDER BY d.delivery_id")
        .fetch_all(&mut *c).await.map_err(|_| SelectionError::Database)?;
    let mut eligible = Vec::new();
    for row in rows {
        if row
            .try_get::<Option<String>, _>("acknowledged_at")
            .map_err(|_| SelectionError::Malformed)?
            .is_some()
        {
            continue;
        }
        let tuple = SelectedTuple {
            delivery_id: row
                .try_get("delivery_id")
                .map_err(|_| SelectionError::Malformed)?,
            task_id: row
                .try_get("task_id")
                .map_err(|_| SelectionError::Malformed)?,
            attempt: row
                .try_get("attempt")
                .map_err(|_| SelectionError::Malformed)?,
        };
        if tuple.attempt < 1 {
            return Err(SelectionError::Malformed);
        }
        if prove_candidate(c, &tuple).await? {
            eligible.push(tuple);
        }
    }
    match eligible.len() {
        0 => Err(SelectionError::Empty),
        1 => Ok(eligible.remove(0)),
        _ => Err(SelectionError::Multiple),
    }
}

async fn prove_candidate(
    c: &mut SqliteConnection,
    t: &SelectedTuple,
) -> Result<bool, SelectionError> {
    let event = sqlx::query(
        "SELECT status,payload FROM task_events WHERE task_id=? ORDER BY seq DESC LIMIT 1",
    )
    .bind(&t.task_id)
    .fetch_optional(&mut *c)
    .await
    .map_err(|_| SelectionError::Database)?
    .ok_or(SelectionError::Nonterminal)?;
    if event
        .try_get::<String, _>("status")
        .map_err(|_| SelectionError::Malformed)?
        != "dispatched"
    {
        return Ok(false);
    }
    let payload: String = event
        .try_get("payload")
        .map_err(|_| SelectionError::Malformed)?;
    if !matches!(serde_json::from_str::<TaskEventPayload>(&payload), Ok(TaskEventPayload::Dispatched { delivery_id, attempt }) if delivery_id.to_string() == t.delivery_id && i64::from(attempt) == t.attempt)
    {
        return Err(SelectionError::Malformed);
    }
    let admission =
        sqlx::query("SELECT state,runtime_instance,revision FROM task_admissions WHERE task_id=?")
            .bind(&t.task_id)
            .fetch_optional(&mut *c)
            .await
            .map_err(|_| SelectionError::Database)?;
    let Some(admission) = admission else {
        return Ok(false);
    };
    let runtime_instance: String = admission
        .try_get::<Option<String>, _>("runtime_instance")
        .map_err(|_| SelectionError::Malformed)?
        .ok_or(SelectionError::Conflicting)?;
    let admission_revision = admission
        .try_get::<i64, _>("revision")
        .map_err(|_| SelectionError::Malformed)?;
    if admission
        .try_get::<String, _>("state")
        .map_err(|_| SelectionError::Malformed)?
        != "dispatching"
        || admission_revision < 0
    {
        return Ok(false);
    }
    let lease = sqlx::query("SELECT resource_key,owner_token,fence,expires_at FROM execution_leases WHERE task_id=? AND state='recovery_needed'")
        .bind(&t.task_id).fetch_optional(&mut *c).await.map_err(|_| SelectionError::Database)?;
    let Some(lease) = lease else {
        return Ok(false);
    };
    let lease_resource: String = lease
        .try_get("resource_key")
        .map_err(|_| SelectionError::Malformed)?;
    let lease_owner: String = lease
        .try_get("owner_token")
        .map_err(|_| SelectionError::Malformed)?;
    let lease_fence = lease
        .try_get::<i64, _>("fence")
        .map_err(|_| SelectionError::Malformed)?;
    let lease_expiry: String = lease
        .try_get("expires_at")
        .map_err(|_| SelectionError::Malformed)?;
    if lease_resource.is_empty()
        || lease_owner.is_empty()
        || lease_fence < 1
        || lease_expiry.is_empty()
        || lease_owner != runtime_instance
    {
        return Err(SelectionError::Conflicting);
    }
    if lease
        .try_get::<String, _>("resource_key")
        .map_err(|_| SelectionError::Malformed)?
        .is_empty()
        || lease
            .try_get::<String, _>("owner_token")
            .map_err(|_| SelectionError::Malformed)?
            .is_empty()
        || lease
            .try_get::<i64, _>("fence")
            .map_err(|_| SelectionError::Malformed)?
            < 0
        || lease
            .try_get::<String, _>("expires_at")
            .map_err(|_| SelectionError::Malformed)?
            .is_empty()
    {
        return Err(SelectionError::Malformed);
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_leases WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_one(&mut *c)
        .await
        .map_err(|_| SelectionError::Database)?;
    if count != 1 {
        return Err(SelectionError::Conflicting);
    }
    let disposition: Option<String> = sqlx::query_scalar(
        "SELECT state FROM delivery_dispositions WHERE delivery_id=? AND task_id=? AND attempt=?",
    )
    .bind(&t.delivery_id)
    .bind(&t.task_id)
    .bind(t.attempt)
    .fetch_optional(&mut *c)
    .await
    .map_err(|_| SelectionError::Database)?;
    if disposition.as_deref() != Some("prepared") {
        return Ok(false);
    }
    let evidence: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT session_id,child_fingerprint,reap_status FROM delivery_dispositions WHERE delivery_id=? AND task_id=? AND attempt=?",
    ).bind(&t.delivery_id).bind(&t.task_id).bind(t.attempt).fetch_one(&mut *c).await.map_err(|_| SelectionError::Database)?;
    if evidence.0.is_some() || evidence.1.is_some() || evidence.2.is_some() {
        return Ok(false);
    }
    let queue: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_work_queue WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_one(&mut *c)
        .await
        .map_err(|_| SelectionError::Database)?;
    let continuation: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_continuations WHERE task_id=?")
            .bind(&t.task_id)
            .fetch_one(&mut *c)
            .await
            .map_err(|_| SelectionError::Database)?;
    if queue != 0 || continuation != 0 {
        return Ok(false);
    }
    let unfinished: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks t LEFT JOIN task_events e ON e.task_id=t.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=t.task_id) WHERE t.task_id=? AND (e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled'))")
        .bind(&t.task_id).fetch_one(&mut *c).await.map_err(|_| SelectionError::Database)?;
    let unacknowledged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM deliveries WHERE delivery_id=? AND acknowledged_at IS NULL",
    )
    .bind(&t.delivery_id)
    .fetch_one(&mut *c)
    .await
    .map_err(|_| SelectionError::Database)?;
    let awaiting_outcome: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries d WHERE d.delivery_id=? AND d.acknowledged_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM task_events e WHERE e.task_id=d.task_id AND e.status IN ('completed','failed','timed_out','cancelled'))")
        .bind(&t.delivery_id).fetch_one(&mut *c).await.map_err(|_| SelectionError::Database)?;
    if unfinished != 1 || unacknowledged != 1 || awaiting_outcome != 0 || admission_revision < 0 {
        return Err(SelectionError::Conflicting);
    }
    Ok(true)
}

fn is_busy(error: &sqlx::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    text.contains("busy") || text.contains("locked")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate};
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use uuid::Uuid;

    async fn eligible_fixture(path: &Path, suffix: &str, expiry: &str) -> SelectedTuple {
        let pool = connect(path).await.unwrap();
        migrate(&pool).await.unwrap();
        let endpoint = Uuid::now_v7().to_string();
        let conversation = Uuid::now_v7().to_string();
        let task = Uuid::now_v7().to_string();
        let delivery = Uuid::now_v7().to_string();
        let runtime = Uuid::now_v7().to_string();
        let resource = format!("resource-{suffix}");
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?, 'acp',1,'[]')")
            .bind(&endpoint).bind(Uuid::now_v7().to_string()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
            .bind(&conversation)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'fixture',1,0,0,0)")
            .bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(&pool).await.unwrap();
        let payload = serde_json::to_string(&TaskEventPayload::Dispatched {
            delivery_id: delivery.parse().unwrap(),
            attempt: 1,
        })
        .unwrap();
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'dispatched','2026-01-01T00:00:00Z',?)")
            .bind(Uuid::now_v7().to_string()).bind(&task).bind(payload).execute(&pool).await.unwrap();
        let runtime_state = if suffix == "two" { "stopped" } else { "active" };
        sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES(?,'t','t',?,'fixture')")
            .bind(&runtime).bind(runtime_state).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',4,?,'t','t')")
            .bind(&task).bind(&runtime).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2026-01-01T00:00:00Z')")
            .bind(&delivery).bind(&task).bind(&endpoint).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'prepared','fixture')")
            .bind(&delivery).bind(&task).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,7,'recovery_needed','t','t',?)")
            .bind(&resource).bind(&task).bind(&runtime).bind(expiry).execute(&pool).await.unwrap();
        pool.close().await;
        SelectedTuple {
            delivery_id: delivery,
            task_id: task,
            attempt: 1,
        }
    }

    #[tokio::test]
    async fn empty_selection_is_redacted_and_read_only() {
        let path = std::env::temp_dir().join(format!("t038-empty-{}.db", Uuid::now_v7()));
        let pool = connect(&path).await.unwrap();
        migrate(&pool).await.unwrap();
        pool.close().await;
        assert_eq!(select(&path).await, Err(SelectionError::Empty));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn expired_bound_tuple_is_selected_and_snapshot_is_unchanged() {
        let path = std::env::temp_dir().join(format!("t038-eligible-{}.db", Uuid::now_v7()));
        let tuple = eligible_fixture(&path, "expired", "2000-01-01T00:00:00Z").await;
        let before: String = sqlx::query_scalar(
            "SELECT group_concat(name, ',') FROM sqlite_master WHERE type='table'",
        )
        .fetch_one(&connect(&path).await.unwrap())
        .await
        .unwrap();
        assert_eq!(select(&path).await.unwrap(), tuple);
        let pool = connect(&path).await.unwrap();
        let after: String = sqlx::query_scalar(
            "SELECT group_concat(name, ',') FROM sqlite_master WHERE type='table'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(before, after);
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn multiple_malformed_and_conflicting_candidates_fail_closed() {
        let path = std::env::temp_dir().join(format!("t038-multiple-{}.db", Uuid::now_v7()));
        let _ = eligible_fixture(&path, "one", "2099-01-01T00:00:00Z").await;
        let _ = eligible_fixture(&path, "two", "2099-01-01T00:00:00Z").await;
        assert_eq!(select(&path).await, Err(SelectionError::Multiple));
        let pool = connect(&path).await.unwrap();
        sqlx::query("UPDATE task_events SET payload='{}'")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        assert!(matches!(
            select(&path).await,
            Err(SelectionError::Malformed | SelectionError::Empty)
        ));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn queue_and_continuation_evidence_rejects_candidate() {
        let path = std::env::temp_dir().join(format!("t038-blocked-{}.db", Uuid::now_v7()));
        let tuple = eligible_fixture(&path, "blocked", "2099-01-01T00:00:00Z").await;
        let pool = connect(&path).await.unwrap();
        sqlx::query("INSERT INTO agent_queue_counters(target_endpoint_id,next_sequence,capacity,updated_at) SELECT target_endpoint_id,1,1,'t' FROM deliveries WHERE delivery_id=?")
            .bind(&tuple.delivery_id).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO agent_work_queue(queue_id,task_id,delivery_id,target_endpoint_id,room_id,sender_endpoint_id,idempotency_key,body_hash,lane,state,sequence,revision,created_at,updated_at) SELECT 'q',task_id,delivery_id,target_endpoint_id,'r',target_endpoint_id,'i','h','ordinary','queued',1,0,'t','t' FROM deliveries WHERE delivery_id=?")
            .bind(&tuple.delivery_id).execute(&pool).await.unwrap();
        pool.close().await;
        assert_eq!(select(&path).await, Err(SelectionError::Empty));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn selection_to_use_drift_is_rejected_on_reselection() {
        let path = std::env::temp_dir().join(format!("t038-drift-{}.db", Uuid::now_v7()));
        let tuple = eligible_fixture(&path, "drift", "2099-01-01T00:00:00Z").await;
        assert_eq!(select(&path).await.unwrap(), tuple);
        let pool = connect(&path).await.unwrap();
        sqlx::query(
            "UPDATE deliveries SET acknowledged_at='2026-01-02T00:00:00Z' WHERE delivery_id=?",
        )
        .bind(&tuple.delivery_id)
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        assert_eq!(select(&path).await, Err(SelectionError::Empty));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn cross_binding_and_prepared_post_acceptance_evidence_fail_closed() {
        let path = std::env::temp_dir().join(format!("t038-binding-{}.db", Uuid::now_v7()));
        let tuple = eligible_fixture(&path, "binding", "2099-01-01T00:00:00Z").await;
        let pool = connect(&path).await.unwrap();
        sqlx::query("UPDATE execution_leases SET owner_token='other-owner' WHERE task_id=?")
            .bind(&tuple.task_id)
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        assert_eq!(select(&path).await, Err(SelectionError::Conflicting));
        let pool = connect(&path).await.unwrap();
        sqlx::query("UPDATE execution_leases SET owner_token=(SELECT runtime_instance FROM task_admissions WHERE task_id=?) WHERE task_id=?")
            .bind(&tuple.task_id).bind(&tuple.task_id).execute(&pool).await.unwrap();
        sqlx::query(
            "UPDATE delivery_dispositions SET session_id='session-evidence' WHERE delivery_id=?",
        )
        .bind(&tuple.delivery_id)
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        assert_eq!(select(&path).await, Err(SelectionError::Empty));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn latest_nonterminal_event_matches_production_planner() {
        let path = std::env::temp_dir().join(format!("t038-latest-event-{}.db", Uuid::now_v7()));
        let tuple = eligible_fixture(&path, "latest", "2099-01-01T00:00:00Z").await;
        let pool = connect(&path).await.unwrap();
        let payload = serde_json::to_string(&TaskEventPayload::Dispatched {
            delivery_id: tuple.delivery_id.parse().unwrap(),
            attempt: 1,
        })
        .unwrap();
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,2,'dispatched','t',?)")
            .bind(Uuid::now_v7().to_string()).bind(&tuple.task_id).bind(payload).execute(&pool).await.unwrap();
        pool.close().await;
        assert_eq!(select(&path).await.unwrap(), tuple);
        let _ = std::fs::remove_file(path);
    }

    #[cfg(unix)]
    #[test]
    fn parser_is_strict_and_never_echoes_values() {
        let ok = vec![
            "select-preacceptance".into(),
            "--database".into(),
            "/private/production.db".into(),
        ];
        assert_eq!(
            parse_args(&ok).unwrap(),
            PathBuf::from("/private/production.db")
        );
        let duplicate = vec![
            "select-preacceptance".into(),
            "--database".into(),
            "a".into(),
            "--database".into(),
            "b".into(),
        ];
        assert_eq!(
            parse_args(&duplicate),
            Err(SelectionError::InvalidArguments)
        );
        let empty = vec![
            "select-preacceptance".into(),
            "--database".into(),
            "".into(),
        ];
        assert_eq!(parse_args(&empty), Err(SelectionError::InvalidArguments));
        let non_utf8 = vec![
            "select-preacceptance".into(),
            "--database".into(),
            std::ffi::OsString::from_vec(vec![0xff]),
        ];
        assert_eq!(parse_args(&non_utf8), Err(SelectionError::InvalidArguments));
    }
}
