//! Explicit offline closure for a proven pre-acceptance dispatch refusal.
use crate::models::{EventId, TaskEvent, TaskEventPayload, TaskId, TaskStatus};
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
    #[error("pre-acceptance recovery already reconciled")]
    AlreadyReconciled,
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
        "SELECT seq,status,payload FROM task_events WHERE task_id=? ORDER BY seq DESC LIMIT 1",
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
    let event_seq: i64 = row
        .try_get("seq")
        .map_err(|_| PreAcceptanceError::Database)?;
    let task_version: i64 = sqlx::query_scalar("SELECT version FROM tasks WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_one(&mut *c)
        .await
        .map_err(|_| PreAcceptanceError::NotEligible)?;
    if event_seq != task_version + 1 {
        return Err(PreAcceptanceError::NotEligible);
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
    let l=sqlx::query("SELECT resource_key,owner_token,fence,expires_at FROM execution_leases WHERE task_id=? AND state='recovery_needed'").bind(&t.task_id).fetch_one(&mut *c).await.map_err(|_|PreAcceptanceError::Fenced)?;
    let resource: String = l
        .try_get("resource_key")
        .map_err(|_| PreAcceptanceError::Database)?;
    let token: String = l
        .try_get("owner_token")
        .map_err(|_| PreAcceptanceError::Database)?;
    let fence: i64 = l
        .try_get("fence")
        .map_err(|_| PreAcceptanceError::Database)?;
    let expires_at: String = l
        .try_get("expires_at")
        .map_err(|_| PreAcceptanceError::Database)?;
    let task_version: i64 = sqlx::query_scalar("SELECT version FROM tasks WHERE task_id=?")
        .bind(&t.task_id)
        .fetch_one(&mut *c)
        .await
        .map_err(|_| PreAcceptanceError::Fenced)?;
    let seq: i64 =
        sqlx::query_scalar("SELECT coalesce(max(seq),0)+1 FROM task_events WHERE task_id=?")
            .bind(&t.task_id)
            .fetch_one(&mut *c)
            .await
            .map_err(|_| PreAcceptanceError::Database)?;
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: t
            .task_id
            .parse::<TaskId>()
            .map_err(|_| PreAcceptanceError::NotEligible)?,
        seq: u64::try_from(seq).map_err(|_| PreAcceptanceError::NotEligible)?,
        status: TaskStatus::Failed,
        timestamp: chrono::DateTime::parse_from_rfc3339(now)
            .map_err(|_| PreAcceptanceError::InvalidArguments)?
            .with_timezone(&chrono::Utc),
        payload: TaskEventPayload::Failed {
            error: "pre_acceptance_auth_failure".into(),
        },
    };
    crate::terminal_closure::apply(
        c,
        crate::terminal_closure::ClosureContext {
            task_id: &t.task_id,
            resource_key: &resource,
            event: &event,
            now,
            delivery: crate::terminal_closure::DeliveryBinding::OfflineExact {
                delivery_id: &t.delivery_id,
                attempt: t.attempt,
            },
            admission: crate::terminal_closure::AdmissionMode::Offline {
                runtime_owner: owner.as_deref(),
                revision: rev,
            },
            task_version: crate::terminal_closure::TaskVersionMode::OfflineExpected(task_version),
            disposition_reason: Some("pre_acceptance_auth_failure"),
        },
        crate::terminal_closure::LeaseMode::OfflineRecoveryNeeded {
            owner: &token,
            fence,
            expires_at: &expires_at,
        },
    )
    .await
    .map_err(|e| match e {
        crate::terminal_closure::ClosureError::Fenced => PreAcceptanceError::Fenced,
        crate::terminal_closure::ClosureError::AdmissionStale => PreAcceptanceError::Fenced,
        crate::terminal_closure::ClosureError::Malformed => PreAcceptanceError::NotEligible,
        crate::terminal_closure::ClosureError::Database(_) => PreAcceptanceError::Database,
    })
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

    #[tokio::test]
    async fn real_sqlite_success_uses_shared_core_and_preserves_unrelated_fields() {
        let path = std::env::temp_dir().join(format!("t037-{}.db", uuid::Uuid::now_v7()));
        let pool = crate::storage::connect(&path).await.unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        let endpoint = uuid::Uuid::now_v7().to_string();
        let conversation = uuid::Uuid::now_v7().to_string();
        let task = uuid::Uuid::now_v7().to_string();
        let delivery = uuid::Uuid::now_v7().to_string();
        let resource = "resource-t037";
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?, 'acp',1,'[]')").bind(&endpoint).bind(uuid::Uuid::now_v7().to_string()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
            .bind(&conversation)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'secret sentinel',1,0,0,0)").bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation).execute(&pool).await.unwrap();
        let dispatched = serde_json::to_string(&TaskEventPayload::Dispatched {
            delivery_id: delivery.parse().unwrap(),
            attempt: 1,
        })
        .unwrap();
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'dispatched',? ,?)").bind(uuid::Uuid::now_v7().to_string()).bind(&task).bind("2026-01-01T00:00:00Z").bind(dispatched).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',4,NULL,'t','t')").bind(&task).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2026-01-01T00:00:00Z')").bind(&delivery).bind(&task).bind(&endpoint).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'prepared','before')").bind(&delivery).bind(&task).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,7,'recovery_needed','t','t','2099-01-01T00:00:00Z')").bind(resource).bind(&task).bind("owner").execute(&pool).await.unwrap();
        pool.close().await;
        let tuple = PreAcceptanceTuple {
            delivery_id: delivery.clone(),
            task_id: task.clone(),
            attempt: 1,
        };
        assert_eq!(
            recover(&path, &[tuple], "2026-01-02T00:00:00Z").await,
            Ok(1)
        );
        let after = crate::storage::connect(&path).await.unwrap();
        let snapshot: (String, i64, String, String, String, String) = sqlx::query_as("SELECT t.text,t.version,a.state,d.acknowledged_at,p.state,l.state FROM tasks t JOIN task_admissions a ON a.task_id=t.task_id JOIN deliveries d ON d.task_id=t.task_id JOIN delivery_dispositions p ON p.task_id=t.task_id JOIN execution_leases l ON l.task_id=t.task_id WHERE t.task_id=?").bind(&task).fetch_one(&after).await.unwrap();
        assert_eq!(snapshot.0, "secret sentinel");
        assert_eq!(snapshot.1, 1);
        assert_eq!(snapshot.2, "terminal");
        assert!(!snapshot.3.is_empty());
        assert_eq!(snapshot.4, "terminal");
        assert_eq!(snapshot.5, "released");
        after.close().await;
        let _ = std::fs::remove_file(path);
    }
}
