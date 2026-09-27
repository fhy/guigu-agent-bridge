use crate::models::TaskEventPayload;
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Category {
    Eligible,
    InvalidArguments,
    Empty,
    Multiple,
    Malformed,
    NonActionableOrphan,
    ActiveOwner,
    Fenced,
    Busy,
    Database,
}

impl Category {
    pub const fn status(self) -> u8 {
        match self {
            Self::Eligible => 0,
            Self::InvalidArguments => 2,
            Self::Empty => 3,
            Self::Multiple => 4,
            Self::Malformed => 5,
            Self::NonActionableOrphan => 6,
            Self::ActiveOwner => 7,
            Self::Fenced => 8,
            Self::Busy => 9,
            Self::Database => 10,
        }
    }
    pub const fn slug(self) -> &'static str {
        match self {
            Self::Eligible => "eligible",
            Self::InvalidArguments => "invalid-arguments",
            Self::Empty => "empty",
            Self::Multiple => "multiple",
            Self::Malformed => "malformed",
            Self::NonActionableOrphan => "non-actionable-orphan",
            Self::ActiveOwner => "active-owner",
            Self::Fenced => "fenced",
            Self::Busy => "busy",
            Self::Database => "database",
        }
    }
}

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, Category> {
    if args.len() != 3 || args[0] != "reconcile-lease-owner" || args[1] != "--database" {
        return Err(Category::InvalidArguments);
    }
    let value = args[2].to_str().ok_or(Category::InvalidArguments)?;
    if value.is_empty() {
        return Err(Category::InvalidArguments);
    }
    Ok(PathBuf::from(value))
}

pub async fn diagnose(database: impl AsRef<Path>) -> Category {
    let opts = SqliteConnectOptions::new()
        .filename(database)
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(5));
    let mut c = match SqliteConnection::connect_with(&opts).await {
        Ok(c) => c,
        Err(_) => return Category::Database,
    };
    if sqlx::query("BEGIN").execute(&mut c).await.is_err() {
        let _ = c.close().await;
        return Category::Busy;
    }
    let result = diagnose_read(&mut c).await;
    let _ = sqlx::query("ROLLBACK").execute(&mut c).await;
    let _ = c.close().await;
    result
}

async fn diagnose_read(c: &mut SqliteConnection) -> Category {
    let rows = match sqlx::query(
        "SELECT d.delivery_id,d.task_id,d.attempt,d.acknowledged_at FROM deliveries d",
    )
    .fetch_all(&mut *c)
    .await
    {
        Ok(rows) => rows,
        Err(error) => return classify_error(&error),
    };
    if rows.is_empty() {
        return Category::Empty;
    }
    let mut candidates = 0usize;
    let mut orphan = false;
    let mut active = false;
    let mut fenced = false;
    for row in rows {
        let delivery_id: String = match row.try_get("delivery_id") {
            Ok(value) => value,
            Err(_) => return Category::Malformed,
        };
        let attempt: i64 = match row.try_get("attempt") {
            Ok(value) => value,
            Err(_) => return Category::Malformed,
        };
        let task: String = match row.try_get("task_id") {
            Ok(v) => v,
            Err(_) => return Category::Malformed,
        };
        let ack: Option<String> = match row.try_get("acknowledged_at") {
            Ok(v) => v,
            Err(_) => return Category::Malformed,
        };
        if ack.is_some() {
            continue;
        }
        let admission = match sqlx::query(
            "SELECT state,runtime_instance,revision FROM task_admissions WHERE task_id=?",
        )
        .bind(&task)
        .fetch_optional(&mut *c)
        .await
        {
            Ok(v) => v,
            Err(error) => return classify_error(&error),
        };
        let Some(admission) = admission else { continue };
        let state: String = match admission.try_get("state") {
            Ok(v) => v,
            Err(_) => return Category::Malformed,
        };
        let runtime: Option<String> = match admission.try_get("runtime_instance") {
            Ok(v) => v,
            Err(_) => return Category::Malformed,
        };
        let revision: i64 = match admission.try_get("revision") {
            Ok(v) => v,
            Err(_) => return Category::Malformed,
        };
        if state != "dispatching" || revision < 0 {
            continue;
        }
        let latest: Option<(String, String)> = match sqlx::query_as(
            "SELECT status,payload FROM task_events WHERE task_id=? ORDER BY seq DESC LIMIT 1",
        )
        .bind(&task)
        .fetch_optional(&mut *c)
        .await
        {
            Ok(v) => v,
            Err(error) => return classify_error(&error),
        };
        let Some((status, payload)) = latest else {
            continue;
        };
        if status != "dispatched" {
            return Category::Malformed;
        }
        match serde_json::from_str::<TaskEventPayload>(&payload) {
            Ok(TaskEventPayload::Dispatched {
                delivery_id: event_delivery,
                attempt: event_attempt,
            }) if event_delivery.to_string() == delivery_id
                && i64::from(event_attempt) == attempt => {}
            _ => return Category::Malformed,
        }
        let disposition: Option<String> =
            match sqlx::query_scalar("SELECT state FROM delivery_dispositions WHERE delivery_id=? AND task_id=? AND attempt=?")
                .bind(&delivery_id).bind(&task).bind(attempt)
                .fetch_optional(&mut *c)
                .await
            {
                Ok(v) => v,
                Err(error) => return classify_error(&error),
            };
        if disposition.as_deref() != Some("prepared") {
            fenced = true;
            continue;
        }
        let evidence: i64 = match sqlx::query_scalar("SELECT count(*) FROM delivery_dispositions WHERE delivery_id=? AND task_id=? AND attempt=? AND (session_id IS NOT NULL OR child_fingerprint IS NOT NULL OR reap_status IS NOT NULL)").bind(&delivery_id).bind(&task).bind(attempt).fetch_one(&mut *c).await { Ok(v) => v, Err(error) => return classify_error(&error) };
        let queue: i64 =
            match sqlx::query_scalar("SELECT count(*) FROM agent_work_queue WHERE task_id=?")
                .bind(&task)
                .fetch_one(&mut *c)
                .await
            {
                Ok(v) => v,
                Err(error) => return classify_error(&error),
            };
        let continuation: i64 =
            match sqlx::query_scalar("SELECT count(*) FROM task_continuations WHERE task_id=?")
                .bind(&task)
                .fetch_one(&mut *c)
                .await
            {
                Ok(v) => v,
                Err(error) => return classify_error(&error),
            };
        if evidence != 0 || queue != 0 || continuation != 0 {
            fenced = true;
            continue;
        }
        let unfinished: i64 = match sqlx::query_scalar("SELECT count(*) FROM tasks t LEFT JOIN task_events e ON e.task_id=t.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=t.task_id) WHERE t.task_id=? AND (e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled'))").bind(&task).fetch_one(&mut *c).await { Ok(v) => v, Err(error) => return classify_error(&error) };
        let unacknowledged: i64 = match sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE delivery_id=? AND task_id=? AND attempt=? AND acknowledged_at IS NULL").bind(&delivery_id).bind(&task).bind(attempt).fetch_one(&mut *c).await { Ok(v) => v, Err(error) => return classify_error(&error) };
        let awaiting: i64 = match sqlx::query_scalar("SELECT count(*) FROM deliveries d LEFT JOIN task_events e ON e.task_id=d.task_id AND e.seq=(SELECT MAX(seq) FROM task_events WHERE task_id=d.task_id) WHERE d.delivery_id=? AND d.task_id=? AND d.attempt=? AND d.acknowledged_at IS NOT NULL AND (e.status IS NULL OR e.status NOT IN ('completed','failed','timed_out','cancelled'))").bind(&delivery_id).bind(&task).bind(attempt).fetch_one(&mut *c).await { Ok(v) => v, Err(error) => return classify_error(&error) };
        if unfinished != 1 || unacknowledged != 1 || awaiting != 0 {
            fenced = true;
            continue;
        }
        let leases = match sqlx::query(
            "SELECT owner_token,fence,state,expires_at FROM execution_leases WHERE task_id=?",
        )
        .bind(&task)
        .fetch_all(&mut *c)
        .await
        {
            Ok(v) => v,
            Err(error) => return classify_error(&error),
        };
        if leases.len() != 1 {
            fenced = true;
            continue;
        }
        let lease = &leases[0];
        let owner: String = match lease.try_get("owner_token") {
            Ok(v) => v,
            Err(_) => return Category::Malformed,
        };
        let fence: i64 = match lease.try_get("fence") {
            Ok(v) => v,
            Err(_) => return Category::Malformed,
        };
        let lease_state: String = match lease.try_get("state") {
            Ok(v) => v,
            Err(_) => return Category::Malformed,
        };
        let expiry: String = match lease.try_get("expires_at") {
            Ok(v) => v,
            Err(_) => return Category::Malformed,
        };
        if owner.is_empty() || fence < 1 || expiry.is_empty() {
            fenced = true;
            continue;
        }
        let owner_state: Option<String> =
            match sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token=?")
                .bind(&owner)
                .fetch_optional(&mut *c)
                .await
            {
                Ok(v) => v,
                Err(_) => return Category::Database,
            };
        if matches!(owner_state.as_deref(), Some("active") | Some("stopping")) {
            active = true;
            continue;
        }
        if owner_state.is_none() {
            if runtime.is_some() && lease_state == "recovery_needed" {
                orphan = true;
            } else {
                fenced = true;
            }
            continue;
        }
        if runtime.as_deref() != Some(owner.as_str()) || lease_state != "recovery_needed" {
            fenced = true;
            continue;
        }
        candidates += 1;
    }
    if active {
        return Category::ActiveOwner;
    }
    if candidates > 1 {
        return Category::Multiple;
    }
    if candidates == 1 {
        return Category::Eligible;
    }
    if orphan {
        return Category::NonActionableOrphan;
    }
    if fenced {
        return Category::Fenced;
    }
    Category::Empty
}

fn classify_error(error: &sqlx::Error) -> Category {
    let text = error.to_string().to_ascii_lowercase();
    if text.contains("busy") || text.contains("locked") {
        Category::Busy
    } else if text.contains("no such table")
        || text.contains("no such column")
        || text.contains("datatype")
        || text.contains("constraint")
    {
        Category::Malformed
    } else {
        Category::Database
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate};
    use uuid::Uuid;
    #[test]
    fn categories_have_frozen_statuses() {
        assert_eq!(Category::NonActionableOrphan.status(), 6);
        assert_eq!(Category::Database.slug(), "database");
    }
    #[test]
    fn parser_rejects_duplicates_and_non_utf8() {
        assert_eq!(
            parse_args(&["reconcile-lease-owner".into(), "--database".into()]),
            Err(Category::InvalidArguments)
        );
    }

    #[tokio::test]
    async fn empty_snapshot_is_read_only_and_repeatable() {
        let path = std::env::temp_dir().join(format!("t042-{}.sqlite", Uuid::now_v7()));
        let pool = connect(&path).await.unwrap();
        migrate(&pool).await.unwrap();
        pool.close().await;
        assert_eq!(diagnose(&path).await, Category::Empty);
        assert_eq!(diagnose(&path).await, Category::Empty);
        let _ = std::fs::remove_file(path);
    }
}
