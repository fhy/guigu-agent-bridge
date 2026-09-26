//! In-process, redacted composition of T038 selection and T037 recovery.
use crate::{offline_preacceptance, readonly_tuple};
#[cfg(test)]
use sqlx::Connection;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SelectedRecoveryError {
    #[error("selected pre-acceptance rejected: invalid arguments")]
    InvalidArguments,
    #[error("selected pre-acceptance rejected: empty")]
    Empty,
    #[error("selected pre-acceptance rejected: selection")]
    Selection,
    #[error("selected pre-acceptance rejected: recovery")]
    Recovery,
}

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, SelectedRecoveryError> {
    if args.len() != 3 || args[0] != "recover-selected-preacceptance" || args[1] != "--database" {
        return Err(SelectedRecoveryError::InvalidArguments);
    }
    let path = args[2]
        .to_str()
        .ok_or(SelectedRecoveryError::InvalidArguments)?;
    if path.is_empty() {
        return Err(SelectedRecoveryError::InvalidArguments);
    }
    Ok(PathBuf::from(path))
}

pub async fn recover(database: impl AsRef<Path>) -> Result<usize, SelectedRecoveryError> {
    recover_inner(database, false).await
}

#[cfg(test)]
pub async fn recover_with_ack_drift(
    database: impl AsRef<Path>,
) -> Result<usize, SelectedRecoveryError> {
    recover_inner(database, true).await
}

#[cfg(test)]
pub async fn recover_with_ack_drift_snapshot(
    database: impl AsRef<Path>,
) -> (Result<usize, SelectedRecoveryError>, Vec<u8>) {
    let selected = match readonly_tuple::select(database.as_ref()).await {
        Ok(value) => value,
        Err(_error) => return (Err(SelectedRecoveryError::Selection), Vec::new()),
    };
    let opts = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(database.as_ref())
        .foreign_keys(true);
    let mut connection = sqlx::SqliteConnection::connect_with(&opts).await.unwrap();
    sqlx::query("UPDATE deliveries SET acknowledged_at='2026-01-02T00:00:00Z' WHERE delivery_id=?")
        .bind(&selected.delivery_id)
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    let baseline = std::fs::read(database.as_ref()).unwrap();
    let tuple = offline_preacceptance::PreAcceptanceTuple {
        delivery_id: selected.delivery_id,
        task_id: selected.task_id,
        attempt: selected.attempt,
    };
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let result = offline_preacceptance::recover(database, &[tuple], &now)
        .await
        .map(|outcome| match outcome {
            offline_preacceptance::PreAcceptanceOutcome::Recovered { count } => count,
            offline_preacceptance::PreAcceptanceOutcome::AlreadyReconciled => 0,
        })
        .map_err(|_| SelectedRecoveryError::Recovery);
    (result, baseline)
}

async fn recover_inner(
    database: impl AsRef<Path>,
    inject_ack_drift: bool,
) -> Result<usize, SelectedRecoveryError> {
    #[cfg(not(test))]
    let _ = inject_ack_drift;
    let selected =
        readonly_tuple::select(database.as_ref())
            .await
            .map_err(|error| match error {
                readonly_tuple::SelectionError::Empty => SelectedRecoveryError::Empty,
                _ => SelectedRecoveryError::Selection,
            })?;
    let tuple = offline_preacceptance::PreAcceptanceTuple {
        delivery_id: selected.delivery_id,
        task_id: selected.task_id,
        attempt: selected.attempt,
    };
    #[cfg(test)]
    if inject_ack_drift {
        let opts = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(database.as_ref())
            .foreign_keys(true);
        let mut connection = sqlx::SqliteConnection::connect_with(&opts)
            .await
            .map_err(|_| SelectedRecoveryError::Recovery)?;
        sqlx::query(
            "UPDATE deliveries SET acknowledged_at='2026-01-02T00:00:00Z' WHERE delivery_id=?",
        )
        .bind(&tuple.delivery_id)
        .execute(&mut connection)
        .await
        .map_err(|_| SelectedRecoveryError::Recovery)?;
        connection
            .close()
            .await
            .map_err(|_| SelectedRecoveryError::Recovery)?;
    }
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    match offline_preacceptance::recover(database, &[tuple], &now).await {
        Ok(offline_preacceptance::PreAcceptanceOutcome::Recovered { count }) => Ok(count),
        Ok(offline_preacceptance::PreAcceptanceOutcome::AlreadyReconciled) => {
            Err(SelectedRecoveryError::Empty)
        }
        Err(_) => Err(SelectedRecoveryError::Recovery),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate};
    use uuid::Uuid;

    async fn eligible_db(path: &std::path::Path) -> (String, String) {
        let pool = connect(path).await.unwrap();
        migrate(&pool).await.unwrap();
        let e = Uuid::now_v7().to_string();
        let c = Uuid::now_v7().to_string();
        let t = Uuid::now_v7().to_string();
        let d = Uuid::now_v7().to_string();
        let r = Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?, 'acp',1,'[]')").bind(&e).bind(Uuid::now_v7().to_string()).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
            .bind(&c)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'x',1,0,0,0)").bind(&t).bind(&t).bind(&e).bind(&e).bind(&c).execute(&pool).await.unwrap();
        let p = serde_json::to_string(&crate::models::TaskEventPayload::Dispatched {
            delivery_id: d.parse().unwrap(),
            attempt: 1,
        })
        .unwrap();
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'dispatched','2026-01-01T00:00:00Z',?)").bind(Uuid::now_v7().to_string()).bind(&t).bind(p).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES(?,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','active','fp')").bind(&r).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',0,?,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").bind(&t).bind(&r).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES(?,?,1,?,'2026-01-01T00:00:00Z')").bind(&d).bind(&t).bind(&e).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'prepared','x')").bind(&d).bind(&t).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES(?,?,?,1,'recovery_needed','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2099-01-01T00:00:00Z')").bind(format!("res-{t}")).bind(&t).bind(&r).execute(&pool).await.unwrap();
        pool.close().await;
        (d, t)
    }
    #[test]
    fn parser_rejects_shape_without_echoing_values() {
        let args = vec!["recover-selected-preacceptance".into(), "--database".into()];
        assert_eq!(
            parse_args(&args),
            Err(SelectedRecoveryError::InvalidArguments)
        );
    }

    #[tokio::test]
    async fn empty_database_is_stable_and_never_retries_t037() {
        let path = std::env::temp_dir().join(format!("t039-empty-{}.db", Uuid::now_v7()));
        let pool = connect(&path).await.unwrap();
        migrate(&pool).await.unwrap();
        pool.close().await;
        let before = std::fs::read(&path).unwrap();
        assert_eq!(recover(&path).await, Err(SelectedRecoveryError::Empty));
        let after = std::fs::read(&path).unwrap();
        assert_eq!(before, after);
        assert_eq!(recover(&path).await, Err(SelectedRecoveryError::Empty));
        assert_eq!(before, std::fs::read(&path).unwrap());
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn eligible_combined_path_closes_once_then_empty() {
        let path = std::env::temp_dir().join(format!("t039-success-{}.db", Uuid::now_v7()));
        let _ = eligible_db(&path).await;
        assert_eq!(recover(&path).await, Ok(1));
        assert_eq!(recover(&path).await, Err(SelectedRecoveryError::Empty));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn selection_to_use_ack_drift_is_fenced_and_snapshot_unchanged() {
        let path = std::env::temp_dir().join(format!("t039-drift-{}.db", Uuid::now_v7()));
        let _ = eligible_db(&path).await;
        let (result, before) = recover_with_ack_drift_snapshot(&path).await;
        assert_eq!(result, Err(SelectedRecoveryError::Recovery));
        assert_eq!(before, std::fs::read(&path).unwrap());
        let _ = std::fs::remove_file(path);
    }
}
