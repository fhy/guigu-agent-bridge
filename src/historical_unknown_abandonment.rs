use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum AbandonmentError {
    #[error("invalid arguments")]
    InvalidArguments,
    #[error("schema unavailable")]
    Schema,
    #[error("target unavailable or conflicting")]
    Conflict,
    #[error("database failure")]
    Database,
    #[error("commit completed but post-commit guard verification failed")]
    CommittedVerification,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Request {
    pub database: PathBuf,
    pub config: PathBuf,
    pub mode: Mode,
    pub decision_id: String,
    pub task_id: String,
    pub delivery_id: String,
    pub resource_key: String,
    pub owner_token: String,
    pub fence: i64,
    pub revision: i64,
    pub generation: String,
    pub reason: String,
    pub policy_ref: String,
    pub authorization_ref: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    DryRun,
    Apply,
    Verify,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailurePoint {
    AfterAudit,
    AfterContinuation,
}

pub(crate) fn parse_args(args: &[std::ffi::OsString]) -> Result<Request, AbandonmentError> {
    let mut it = args.iter();
    if it.next().and_then(|x| x.to_str()) != Some("historical-unknown-abandon") {
        return Err(AbandonmentError::InvalidArguments);
    }
    let mut mode = None;
    let mut vals = std::collections::HashMap::new();
    while let Some(k) = it.next() {
        let k = k.to_str().ok_or(AbandonmentError::InvalidArguments)?;
        let v = it
            .next()
            .and_then(|x| x.to_str())
            .ok_or(AbandonmentError::InvalidArguments)?;
        match k {
            "--mode" => {
                if mode.replace(v).is_some() {
                    return Err(AbandonmentError::InvalidArguments);
                }
            }
            "--database"
            | "--config"
            | "--decision-id"
            | "--task-id"
            | "--delivery-id"
            | "--resource-key"
            | "--owner-token"
            | "--fence"
            | "--revision"
            | "--generation"
            | "--reason"
            | "--policy-ref"
            | "--authorization-ref" => {
                if vals
                    .insert(k.trim_start_matches('-'), v.to_owned())
                    .is_some()
                {
                    return Err(AbandonmentError::InvalidArguments);
                }
            }
            _ => return Err(AbandonmentError::InvalidArguments),
        }
    }
    let mode = match mode {
        Some("dry-run") => Mode::DryRun,
        Some("apply") => Mode::Apply,
        Some("verify") => Mode::Verify,
        _ => return Err(AbandonmentError::InvalidArguments),
    };
    macro_rules! req {
        ($n:literal) => {
            vals.remove($n).ok_or(AbandonmentError::InvalidArguments)?
        };
    }
    Ok(Request {
        database: PathBuf::from(req!("database")),
        config: PathBuf::from(req!("config")),
        mode,
        decision_id: req!("decision-id"),
        task_id: req!("task-id"),
        delivery_id: req!("delivery-id"),
        resource_key: req!("resource-key"),
        owner_token: req!("owner-token"),
        fence: req!("fence")
            .parse()
            .map_err(|_| AbandonmentError::InvalidArguments)?,
        revision: req!("revision")
            .parse()
            .map_err(|_| AbandonmentError::InvalidArguments)?,
        generation: req!("generation"),
        reason: req!("reason"),
        policy_ref: req!("policy-ref"),
        authorization_ref: req!("authorization-ref"),
    })
}

async fn connect(path: &Path) -> Result<SqliteConnection, AbandonmentError> {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .foreign_keys(true)
            .busy_timeout(std::time::Duration::from_secs(10)),
    )
    .await
    .map_err(|_| AbandonmentError::Database)
}

pub(crate) async fn run(req: Request) -> Result<&'static str, AbandonmentError> {
    run_with_failure(req, None).await
}

async fn run_with_failure(
    req: Request,
    failure: Option<FailurePoint>,
) -> Result<&'static str, AbandonmentError> {
    let guard = if matches!(req.mode, Mode::Apply) {
        Some(
            crate::runtime_admission::Guard::acquire(
                &req.database,
                &req.config,
                "manual-abandonment",
            )
            .map_err(|_| AbandonmentError::Conflict)?,
        )
    } else {
        None
    };
    let mut c = connect(&req.database).await?;
    if !matches!(req.mode, Mode::Apply) {
        sqlx::query("PRAGMA query_only=ON")
            .execute(&mut c)
            .await
            .map_err(|_| AbandonmentError::Database)?;
    }
    let exists:i64=sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='historical_unknown_abandonments'").fetch_one(&mut c).await.map_err(|_|AbandonmentError::Schema)?;
    if exists != 1 {
        return Err(AbandonmentError::Schema);
    }
    let active: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM runtime_instances WHERE state IN ('active','stopping')",
    )
    .fetch_one(&mut c)
    .await
    .map_err(|_| AbandonmentError::Database)?;
    if active != 0 {
        return Err(AbandonmentError::Conflict);
    }
    let retained = sqlx::query(
        "SELECT decision_id,owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,reason,policy_ref,authorization_ref,not_replayed FROM historical_unknown_abandonments WHERE task_id=? AND delivery_id=? AND resource_key=?",
    )
    .bind(&req.task_id)
    .bind(&req.delivery_id)
    .bind(&req.resource_key)
    .fetch_optional(&mut c)
    .await
    .map_err(|_| AbandonmentError::Database)?;
    if let Some(retained) = retained {
        let matches_input = retained.get::<String, _>(0) == req.decision_id
            && retained.get::<String, _>(1) == req.owner_token
            && retained.get::<i64, _>(2) == req.fence
            && retained.get::<i64, _>(3) == req.revision
            && retained.get::<String, _>(6) == req.generation
            && retained.get::<String, _>(8) == req.reason
            && retained.get::<String, _>(9) == req.policy_ref
            && retained.get::<String, _>(10) == req.authorization_ref
            && retained.get::<i64, _>(11) == 1;
        if !matches_input {
            return Err(AbandonmentError::Conflict);
        }
        let post_state = sqlx::query(
            "SELECT t.version,a.revision,l.state,c.revision,c.state,d.state,d.reason_code FROM tasks t JOIN task_admissions a ON a.task_id=t.task_id JOIN execution_leases l ON l.task_id=t.task_id AND l.resource_key=? JOIN task_continuations c ON c.task_id=t.task_id AND c.resource_key=? JOIN deliveries v ON v.task_id=t.task_id AND v.delivery_id=? JOIN delivery_dispositions d ON d.delivery_id=v.delivery_id AND d.attempt=v.attempt WHERE t.task_id=?",
        )
        .bind(&req.resource_key)
        .bind(&req.resource_key)
        .bind(&req.delivery_id)
        .bind(&req.task_id)
        .fetch_optional(&mut c)
        .await
        .map_err(|_| AbandonmentError::Database)?;
        let post_matches = post_state.is_some_and(|row| {
            row.get::<i64, _>(0) == retained.get::<i64, _>(3)
                && row.get::<i64, _>(1) == retained.get::<i64, _>(4)
                && row.get::<String, _>(2) == "released"
                && row.get::<i64, _>(3) == retained.get::<i64, _>(7)
                && row.get::<String, _>(4) == "terminal"
                && row.get::<String, _>(5) == "terminal"
                && row.get::<String, _>(6) == "manual_abandonment"
        });
        if !post_matches {
            return Err(AbandonmentError::Conflict);
        }
        return Ok(match req.mode {
            Mode::DryRun => "dry-run already applied",
            Mode::Apply => "already applied",
            Mode::Verify => "verified post-state",
        });
    }
    let eligible = "SELECT t.version,a.revision,a.state,d.state,d.reason_code,v.attempt,l.state,l.owner_token,l.fence,c.revision,c.state,c.runtime_generation FROM tasks t JOIN task_admissions a ON a.task_id=t.task_id JOIN runtime_instances r ON r.instance_token=a.runtime_instance JOIN deliveries v ON v.task_id=t.task_id AND v.delivery_id=? JOIN delivery_dispositions d ON d.delivery_id=v.delivery_id JOIN execution_leases l ON l.task_id=t.task_id AND l.resource_key=? JOIN task_continuations c ON c.task_id=t.task_id WHERE t.task_id=? AND r.state='stopped' AND v.attempt=d.attempt AND t.version=? AND c.revision=? AND c.runtime_generation=? AND l.owner_token=? AND l.fence=? AND l.state='recovery_needed' AND c.state='recovery_needed' AND d.state='outcome_unknown'";
    let row = sqlx::query(eligible)
        .bind(&req.delivery_id)
        .bind(&req.resource_key)
        .bind(&req.task_id)
        .bind(req.revision)
        .bind(req.revision)
        .bind(&req.generation)
        .bind(&req.owner_token)
        .bind(req.fence)
        .fetch_optional(&mut c)
        .await
        .map_err(|_| AbandonmentError::Database)?
        .ok_or(AbandonmentError::Conflict)?;
    let before_task = row.get::<i64, _>(0);
    let before_adm = row.get::<i64, _>(1);
    let before_cont = row.get::<i64, _>(9);
    match req.mode {
        Mode::DryRun => Ok("dry-run eligible"),
        Mode::Verify => Ok("verify eligible pre-state"),
        Mode::Apply => {
            if guard.as_ref().and_then(|g| g.revalidate().ok()).is_none() {
                return Err(AbandonmentError::Conflict);
            }
            let mut tx = c
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|_| AbandonmentError::Database)?;
            let audit=sqlx::query("INSERT INTO historical_unknown_abandonments(decision_id,task_id,delivery_id,resource_key,owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,disposition,reason,policy_ref,authorization_ref,not_replayed,created_at) SELECT ?,?,?,?,?,?,?,?,?,?,?, 'manual_abandonment',?,?,?,1,datetime('now') WHERE NOT EXISTS (SELECT 1 FROM historical_unknown_abandonments WHERE task_id=? AND delivery_id=? AND resource_key=?)")
   .bind(&req.decision_id).bind(&req.task_id).bind(&req.delivery_id).bind(&req.resource_key).bind(&req.owner_token).bind(req.fence).bind(before_task).bind(before_adm).bind(before_cont).bind(&req.generation).bind(before_cont+1).bind(&req.reason).bind(&req.policy_ref).bind(&req.authorization_ref).bind(&req.task_id).bind(&req.delivery_id).bind(&req.resource_key).execute(&mut *tx).await.map_err(|_|AbandonmentError::Database)?;
            if audit.rows_affected() != 1 {
                return Err(AbandonmentError::Conflict);
            }
            if failure == Some(FailurePoint::AfterAudit) {
                return Err(AbandonmentError::Database);
            }
            let l=sqlx::query("UPDATE execution_leases SET state='released',heartbeat_at=datetime('now') WHERE task_id=? AND resource_key=? AND owner_token=? AND fence=? AND state='recovery_needed'").bind(&req.task_id).bind(&req.resource_key).bind(&req.owner_token).bind(req.fence).execute(&mut *tx).await.map_err(|_|AbandonmentError::Database)?;
            let k=sqlx::query("UPDATE task_continuations SET state='terminal',revision=revision+1,heartbeat_at=datetime('now') WHERE task_id=? AND resource_key=? AND lease_fence=? AND revision=? AND runtime_generation=? AND state='recovery_needed'").bind(&req.task_id).bind(&req.resource_key).bind(req.fence).bind(req.revision).bind(&req.generation).execute(&mut *tx).await.map_err(|_|AbandonmentError::Database)?;
            if failure == Some(FailurePoint::AfterContinuation) {
                return Err(AbandonmentError::Database);
            }
            let d=sqlx::query("UPDATE delivery_dispositions SET state='terminal',reason_code='manual_abandonment' WHERE task_id=? AND delivery_id=? AND attempt=? AND state='outcome_unknown'").bind(&req.task_id).bind(&req.delivery_id).bind(row.get::<i64,_>(5)).execute(&mut *tx).await.map_err(|_|AbandonmentError::Database)?;
            if l.rows_affected() != 1 || k.rows_affected() != 1 || d.rows_affected() != 1 {
                return Err(AbandonmentError::Conflict);
            }
            tx.commit().await.map_err(|_| AbandonmentError::Database)?;
            if guard.as_ref().and_then(|g| g.revalidate().ok()).is_none() {
                return Err(AbandonmentError::CommittedVerification);
            }
            Ok("applied")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect as storage_connect, migrate};
    use std::ffi::OsString;

    fn valid_args() -> Vec<OsString> {
        [
            "historical-unknown-abandon",
            "--mode",
            "dry-run",
            "--database",
            "db.sqlite",
            "--config",
            "config.toml",
            "--decision-id",
            "decision",
            "--task-id",
            "task",
            "--delivery-id",
            "delivery",
            "--resource-key",
            "resource",
            "--owner-token",
            "owner",
            "--fence",
            "3",
            "--revision",
            "4",
            "--generation",
            "generation",
            "--reason",
            "reason",
            "--policy-ref",
            "policy",
            "--authorization-ref",
            "authorization",
        ]
        .into_iter()
        .map(OsString::from)
        .collect()
    }

    async fn fixture() -> (tempfile::TempDir, Request) {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("bridge.db");
        let config = root.path().join("bridge.toml");
        std::fs::write(&config, "").unwrap();
        let pool = storage_connect(&database).await.unwrap();
        migrate(&pool).await.unwrap();
        let now = "2026-10-20T00:00:00Z";
        for query in [
            "INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('endpoint','worker','acp',1,'[]')",
            "INSERT INTO conversations(conversation_id,participants_json) VALUES('conversation','[]')",
            "INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('runtime',?,?, 'stopped','fixture')",
            "INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','endpoint','endpoint','conversation','fixture',1,0,0,1)",
            "INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES('task','terminal',1,'runtime',?,?)",
            "INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES('delivery','task',1,'endpoint',?)",
            "INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES('delivery','task',1,'outcome_unknown','unknown')",
            "INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource','task','owner',7,'recovery_needed',?,?,?)",
            "INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes) VALUES('task','resource','delivery',7,1,'recovery_needed',1,0,0,'prompt',?,?,?,0)",
        ] {
            let mut statement = sqlx::query(query);
            if query.contains("runtime_instances") || query.contains("task_admissions") {
                statement = statement.bind(now).bind(now);
            } else if query.contains("deliveries") {
                statement = statement.bind(now);
            } else if query.contains("execution_leases") {
                statement = statement.bind(now).bind(now).bind("2099-01-01T00:00:00Z");
            } else if query.contains("task_continuations") {
                statement = statement.bind(now).bind(now).bind(now);
            }
            statement.execute(&pool).await.unwrap();
        }
        pool.close().await;
        (
            root,
            Request {
                database,
                config,
                mode: Mode::Apply,
                decision_id: "decision".into(),
                task_id: "task".into(),
                delivery_id: "delivery".into(),
                resource_key: "resource".into(),
                owner_token: "owner".into(),
                fence: 7,
                revision: 1,
                generation: "generation.v1:strict".into(),
                reason: "operator decision".into(),
                policy_ref: "T096".into(),
                authorization_ref: "fixture".into(),
            },
        )
    }

    #[tokio::test]
    async fn apply_reopen_verify_and_repeat_are_post_state_based() {
        let (_root, request) = fixture().await;
        let database = request.database.clone();
        assert_eq!(run(request.clone()).await.unwrap(), "applied");

        let mut verify = request.clone();
        verify.mode = Mode::Verify;
        assert_eq!(run(verify).await.unwrap(), "verified post-state");
        assert_eq!(run(request.clone()).await.unwrap(), "already applied");

        let pool = storage_connect(&database).await.unwrap();
        for (table, count) in [
            (
                "task_events",
                sqlx::query_scalar("SELECT count(*) FROM task_events"),
            ),
            (
                "transport_receipts",
                sqlx::query_scalar("SELECT count(*) FROM transport_receipts"),
            ),
            (
                "projection_outbox",
                sqlx::query_scalar("SELECT count(*) FROM projection_outbox"),
            ),
            (
                "agent_work_queue",
                sqlx::query_scalar("SELECT count(*) FROM agent_work_queue"),
            ),
        ] {
            let count: i64 = count.fetch_one(&pool).await.unwrap();
            assert_eq!(count, 0, "repeat must not create {table} rows");
        }
        let audit_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(audit_count, 1);
        pool.close().await;

        let mut conflict = request;
        conflict.decision_id = "other-decision".into();
        assert_eq!(run(conflict).await, Err(AbandonmentError::Conflict));
    }

    #[tokio::test]
    async fn stale_target_refuses_without_audit_or_side_effects() {
        let (_root, mut request) = fixture().await;
        let database = request.database.clone();
        request.fence = 8;
        assert_eq!(run(request).await, Err(AbandonmentError::Conflict));
        let pool = storage_connect(&database).await.unwrap();
        let audit_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments")
                .fetch_one(&pool)
                .await
                .unwrap();
        let lease_state: String = sqlx::query_scalar("SELECT state FROM execution_leases")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(audit_count, 0);
        assert_eq!(lease_state, "recovery_needed");
        pool.close().await;
    }

    #[tokio::test]
    async fn injected_failures_roll_back_audit_and_all_target_rows_after_reopen() {
        for failure in [FailurePoint::AfterAudit, FailurePoint::AfterContinuation] {
            let (_root, request) = fixture().await;
            let database = request.database.clone();
            let config = request.config.clone();
            assert_eq!(
                run_with_failure(request, Some(failure)).await,
                Err(AbandonmentError::Database)
            );
            let pool = storage_connect(&database).await.unwrap();
            let audit_count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let lease_state: String = sqlx::query_scalar("SELECT state FROM execution_leases")
                .fetch_one(&pool)
                .await
                .unwrap();
            let continuation: (i64, String) =
                sqlx::query_as("SELECT revision,state FROM task_continuations")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let disposition: (String, String) =
                sqlx::query_as("SELECT state,reason_code FROM delivery_dispositions")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(audit_count, 0);
            assert_eq!(lease_state, "recovery_needed");
            assert_eq!(continuation, (1, "recovery_needed".into()));
            assert_eq!(disposition, ("outcome_unknown".into(), "unknown".into()));
            pool.close().await;
            let _ = std::fs::remove_file(config);
        }
    }

    #[test]
    fn parser_accepts_each_explicit_mode() {
        for mode in ["dry-run", "apply", "verify"] {
            let mut args = valid_args();
            args[2] = OsString::from(mode);
            assert_eq!(parse_args(&args).unwrap().mode, mode.parse_mode());
        }
    }

    #[test]
    fn parser_rejects_unknown_or_duplicate_options() {
        let mut unknown = valid_args();
        unknown.push(OsString::from("--unexpected"));
        unknown.push(OsString::from("value"));
        assert_eq!(
            parse_args(&unknown),
            Err(AbandonmentError::InvalidArguments)
        );

        let mut duplicate = valid_args();
        duplicate.extend([OsString::from("--mode"), OsString::from("verify")]);
        assert_eq!(
            parse_args(&duplicate),
            Err(AbandonmentError::InvalidArguments)
        );
    }

    #[test]
    fn parser_rejects_malformed_or_incomplete_selectors() {
        let mut missing = valid_args();
        missing.truncate(missing.len() - 2);
        assert_eq!(
            parse_args(&missing),
            Err(AbandonmentError::InvalidArguments)
        );

        let mut invalid_number = valid_args();
        let fence_value = invalid_number
            .iter()
            .position(|value| value == "3")
            .unwrap();
        invalid_number[fence_value] = OsString::from("not-a-fence");
        assert_eq!(
            parse_args(&invalid_number),
            Err(AbandonmentError::InvalidArguments)
        );
    }

    trait ParseMode {
        fn parse_mode(self) -> Mode;
    }

    impl ParseMode for &str {
        fn parse_mode(self) -> Mode {
            match self {
                "dry-run" => Mode::DryRun,
                "apply" => Mode::Apply,
                "verify" => Mode::Verify,
                _ => unreachable!(),
            }
        }
    }
}
