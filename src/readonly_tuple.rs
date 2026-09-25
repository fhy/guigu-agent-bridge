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
    if admission
        .try_get::<String, _>("state")
        .map_err(|_| SelectionError::Malformed)?
        != "dispatching"
        || admission
            .try_get::<Option<String>, _>("runtime_instance")
            .map_err(|_| SelectionError::Malformed)?
            .is_none()
        || admission
            .try_get::<i64, _>("revision")
            .map_err(|_| SelectionError::Malformed)?
            < 0
    {
        return Ok(false);
    }
    let lease = sqlx::query("SELECT resource_key,owner_token,fence,expires_at FROM execution_leases WHERE task_id=? AND state='recovery_needed'")
        .bind(&t.task_id).fetch_optional(&mut *c).await.map_err(|_| SelectionError::Database)?;
    let Some(lease) = lease else {
        return Ok(false);
    };
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

    #[tokio::test]
    async fn empty_selection_is_redacted_and_read_only() {
        let path = std::env::temp_dir().join(format!("t038-empty-{}.db", Uuid::now_v7()));
        let pool = connect(&path).await.unwrap();
        migrate(&pool).await.unwrap();
        pool.close().await;
        assert_eq!(select(&path).await, Err(SelectionError::Empty));
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
