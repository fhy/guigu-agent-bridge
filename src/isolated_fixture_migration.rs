use std::{
    fs::OpenOptions,
    io::Read,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

use crate::legacy_owner_reconciliation::{
    Error, RuntimeAbsence, Sidecars, classify_runtime_absence_authenticated, digest, ensure_marker,
    path_identity, preflight_publication, preflight_runtime_absence_authenticated, preflight_unit,
    publish, publish_new,
};

const O_NOFOLLOW: i32 = 0o400000;
const RESPONSE_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0x7297ec04_1338_57b4_9bc4_f0ff9f84fa5a);
const PREPARE: &str = "prepare-isolated-legacy-fixture-migration-manifest";
const MIGRATE: &str = "migrate-isolated-legacy-fixture";

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Migrated,
    AlreadyMigrated,
}

#[derive(Debug)]
pub struct PrepareArgs {
    root: PathBuf,
    config: PathBuf,
    database: PathBuf,
    unit: String,
    manifest: PathBuf,
    evidence: PathBuf,
    operation_id: String,
    commit_timestamp: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    operation_id: String,
    isolated_root: String,
    marker: String,
    config: String,
    config_identity: String,
    database: String,
    database_identity: String,
    binary: String,
    binary_identity: String,
    unit: String,
    evidence: String,
    pre_state_digest: String,
    post_state_digest: String,
    active_owner_digest: String,
    stopping_owner_digest: String,
    continuation_digest: String,
    receipt_digest: String,
    sibling_receipt_digest: String,
    commit_timestamp: String,
    manifest_digest: String,
}

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, Error> {
    if args.len() != 3 || args[0] != MIGRATE || args[1] != "--manifest" {
        return Err(Error::Rejected);
    }
    Ok(PathBuf::from(&args[2]))
}

pub fn parse_prepare_args(args: &[std::ffi::OsString]) -> Result<PrepareArgs, Error> {
    const FLAGS: &[&str] = &[
        "--root",
        "--config",
        "--database",
        "--unit",
        "--manifest",
        "--evidence",
        "--operation-id",
        "--commit-timestamp",
    ];
    if args.len() != 17 || args[0] != PREPARE {
        return Err(Error::Rejected);
    }
    let mut values = Vec::with_capacity(FLAGS.len());
    for (index, flag) in FLAGS.iter().enumerate() {
        if args[index * 2 + 1] != *flag {
            return Err(Error::Rejected);
        }
        values.push(args[index * 2 + 2].clone());
    }
    let text = |index: usize| {
        values[index]
            .to_str()
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or(Error::Rejected)
    };
    let operation_id = text(6)?;
    uuid::Uuid::parse_str(&operation_id).map_err(|_| Error::Rejected)?;
    let commit_timestamp = text(7)?;
    if chrono::DateTime::parse_from_rfc3339(&commit_timestamp)
        .map_err(|_| Error::Rejected)?
        .with_timezone(&chrono::Utc)
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        != commit_timestamp
    {
        return Err(Error::Rejected);
    }
    Ok(PrepareArgs {
        root: PathBuf::from(&values[0]),
        config: PathBuf::from(&values[1]),
        database: PathBuf::from(&values[2]),
        unit: text(3)?,
        manifest: PathBuf::from(&values[4]),
        evidence: PathBuf::from(&values[5]),
        operation_id,
        commit_timestamp,
    })
}

pub fn prepare_cli_rejected(error: Error) -> Error {
    prepare_rejected("preflight", "arguments", error)
}

#[derive(Clone)]
struct Owner {
    token: String,
    started: String,
    heartbeat: String,
    state: String,
    fingerprint: String,
}

#[derive(Clone)]
struct Continuation {
    task: String,
    resource: String,
    delivery: String,
    fence: i64,
    revision: i64,
    state: String,
    next_turn: i64,
    completed_turns: i64,
    no_progress: i64,
    prompt: String,
    started: String,
    heartbeat: String,
    last_progress: String,
    output_bytes: i64,
    generation: String,
    generation_version: i64,
}

#[derive(Clone)]
struct Facts {
    active: Owner,
    stopping: Owner,
    continuation: Continuation,
}

fn valid_legacy(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with("pid:")
        && value.is_ascii()
        && !value.chars().any(char::is_whitespace)
}

const OPEN_SESSION_COUNT: &str = "SELECT count(*) FROM sessions s
WHERE s.state <> 'closed'
  AND NOT (s.state = 'ready' AND EXISTS (
    SELECT 1
    FROM delivery_dispositions p
    JOIN deliveries d ON d.delivery_id = p.delivery_id
      AND d.task_id = p.task_id AND d.attempt = p.attempt
    JOIN tasks t ON t.task_id = d.task_id
    JOIN task_admissions a ON a.task_id = t.task_id
    JOIN execution_leases l ON l.task_id = t.task_id
    JOIN task_continuations c ON c.task_id = t.task_id
      AND c.resource_key = l.resource_key
      AND c.delivery_id = d.delivery_id
      AND c.lease_fence = l.fence
    WHERE p.session_id = s.session_id
      AND d.target_endpoint_id = s.endpoint_id
      AND t.conversation_id = s.conversation_id
      AND p.state = 'terminal'
      AND p.child_fingerprint IS NOT NULL
      AND p.reap_status = 'reaped'
      AND p.reap_at IS NOT NULL
      AND d.acknowledged_at IS NOT NULL
      AND a.state = 'terminal'
      AND a.runtime_instance = l.owner_token
      AND l.state = 'released'
      AND c.state = 'blocked'
      AND t.version + 1 = (
        SELECT max(e.seq) FROM task_events e WHERE e.task_id = t.task_id
      )
      AND (
        SELECT e.status FROM task_events e
        WHERE e.task_id = t.task_id ORDER BY e.seq DESC LIMIT 1
      ) IN ('completed','failed','timed_out','cancelled')
  ))";

async fn open_session_count(c: &mut SqliteConnection) -> Result<i64, Error> {
    sqlx::query_scalar(OPEN_SESSION_COUNT)
        .fetch_one(c)
        .await
        .map_err(|_| Error::Database)
}

type EligibilityResult<T> = Result<T, (Error, &'static str)>;

fn rejected(cause: &'static str) -> (Error, &'static str) {
    (Error::Rejected, cause)
}

fn database_facts() -> (Error, &'static str) {
    (Error::Database, "database-facts")
}

async fn eligible(c: &mut SqliteConnection) -> EligibilityResult<Facts> {
    let owners = sqlx::query("SELECT instance_token,started_at,heartbeat_at,state,process_fingerprint FROM runtime_instances WHERE state IN ('active','stopping') ORDER BY state,instance_token")
        .fetch_all(&mut *c).await.map_err(|_| database_facts())?;
    if owners.len() != 2 {
        return Err(rejected("owner-topology"));
    }
    let owner = |row: &sqlx::sqlite::SqliteRow| -> Result<Owner, Error> {
        Ok(Owner {
            token: row.try_get(0).map_err(|_| Error::Database)?,
            started: row.try_get(1).map_err(|_| Error::Database)?,
            heartbeat: row.try_get(2).map_err(|_| Error::Database)?,
            state: row.try_get(3).map_err(|_| Error::Database)?,
            fingerprint: row.try_get(4).map_err(|_| Error::Database)?,
        })
    };
    let mut active = None;
    let mut stopping = None;
    for row in &owners {
        let value = owner(row).map_err(|_| database_facts())?;
        if !valid_legacy(&value.fingerprint) {
            return Err(rejected("owner-identity"));
        }
        match value.state.as_str() {
            "active" if active.is_none() => active = Some(value),
            "stopping" if stopping.is_none() => stopping = Some(value),
            _ => return Err(rejected("owner-topology")),
        }
    }
    let active = active.ok_or_else(|| rejected("owner-topology"))?;
    let stopping = stopping.ok_or_else(|| rejected("owner-topology"))?;
    if stopping.started >= active.started || stopping.heartbeat > active.started {
        return Err(rejected("owner-ordering"));
    }
    let continuation_rows = sqlx::query("SELECT task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes,runtime_generation,runtime_generation_version FROM task_continuations")
        .fetch_all(&mut *c).await.map_err(|_|database_facts())?;
    if continuation_rows.len() != 1 {
        return Err(rejected("continuation-topology"));
    }
    let r = &continuation_rows[0];
    let continuation = Continuation {
        task: r.try_get(0).map_err(|_| database_facts())?,
        resource: r.try_get(1).map_err(|_| database_facts())?,
        delivery: r.try_get(2).map_err(|_| database_facts())?,
        fence: r.try_get(3).map_err(|_| database_facts())?,
        revision: r.try_get(4).map_err(|_| database_facts())?,
        state: r.try_get(5).map_err(|_| database_facts())?,
        next_turn: r.try_get(6).map_err(|_| database_facts())?,
        completed_turns: r.try_get(7).map_err(|_| database_facts())?,
        no_progress: r.try_get(8).map_err(|_| database_facts())?,
        prompt: r.try_get(9).map_err(|_| database_facts())?,
        started: r.try_get(10).map_err(|_| database_facts())?,
        heartbeat: r.try_get(11).map_err(|_| database_facts())?,
        last_progress: r.try_get(12).map_err(|_| database_facts())?,
        output_bytes: r.try_get(13).map_err(|_| database_facts())?,
        generation: r.try_get(14).map_err(|_| database_facts())?,
        generation_version: r.try_get(15).map_err(|_| database_facts())?,
    };
    let open_sessions = open_session_count(&mut *c)
        .await
        .map_err(|_| database_facts())?;
    if open_sessions != 0 {
        return Err(rejected("open-sessions"));
    }
    if continuation.state != "blocked" || continuation.revision < 2 {
        return Err(rejected("continuation-state"));
    }
    let lease: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_leases WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND state='released'")
        .bind(&continuation.resource).bind(&continuation.task).bind(&active.token).bind(continuation.fence).fetch_one(&mut *c).await.map_err(|_|database_facts())?;
    if lease != 1 {
        return Err(rejected("lease-binding"));
    }
    let admission: i64 = sqlx::query_scalar("SELECT count(*) FROM task_admissions WHERE task_id=? AND state='terminal' AND runtime_instance=?")
        .bind(&continuation.task).bind(&active.token).fetch_one(&mut *c).await.map_err(|_|database_facts())?;
    if admission != 1 {
        return Err(rejected("admission-binding"));
    }
    let delivery: i64 = sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE task_id=? AND delivery_id=? AND acknowledged_at IS NOT NULL")
        .bind(&continuation.task).bind(&continuation.delivery).fetch_one(&mut *c).await.map_err(|_|database_facts())?;
    if delivery != 1 {
        return Err(rejected("delivery-binding"));
    }
    let disposition: i64 = sqlx::query_scalar("SELECT count(*) FROM delivery_dispositions p JOIN deliveries d ON d.delivery_id=p.delivery_id AND d.task_id=p.task_id AND d.attempt=p.attempt WHERE p.task_id=? AND p.delivery_id=? AND p.state='terminal' AND ((p.child_fingerprint IS NULL AND p.reap_status IS NULL AND p.reap_at IS NULL) OR (p.child_fingerprint IS NOT NULL AND p.reap_status='reaped' AND p.reap_at IS NOT NULL))")
        .bind(&continuation.task).bind(&continuation.delivery).fetch_one(&mut *c).await.map_err(|_|database_facts())?;
    if disposition != 1 {
        return Err(rejected("child-reap"));
    }
    let terminal: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks t WHERE t.task_id=? AND t.version+1=(SELECT max(seq) FROM task_events WHERE task_id=t.task_id) AND (SELECT status FROM task_events WHERE task_id=t.task_id ORDER BY seq DESC LIMIT 1) IN ('completed','failed','timed_out','cancelled')")
        .bind(&continuation.task).fetch_one(&mut *c).await.map_err(|_|database_facts())?;
    if terminal != 1 {
        return Err(rejected("terminal-event"));
    }
    let response = sqlx::query("SELECT r.response_hash,r.output_bytes,r.outcome,e.payload FROM continuation_response_receipts r JOIN task_events e ON e.task_id=r.task_id WHERE r.task_id=? AND r.delivery_id=? AND r.resource_key=? AND r.lease_fence=? AND r.continuation_revision=? AND r.runtime_generation=? ORDER BY e.seq DESC LIMIT 1")
        .bind(&continuation.task).bind(&continuation.delivery).bind(&continuation.resource).bind(continuation.fence).bind(continuation.revision-1).bind(&continuation.generation).fetch_optional(&mut *c).await.map_err(|_|database_facts())?;
    let response_valid = if let Some(row) = response {
        let payload: String = row.try_get(3).map_err(|_| database_facts())?;
        let canonical = serde_json::to_string(
            &serde_json::from_str::<serde_json::Value>(&payload)
                .map_err(|_| rejected("response-receipt"))?,
        )
        .map_err(|_| rejected("response-receipt"))?;
        let expected = uuid::Uuid::new_v5(&RESPONSE_NAMESPACE, canonical.as_bytes()).to_string();
        row.try_get::<String, _>(0).map_err(|_| database_facts())? == expected
            && row.try_get::<i64, _>(1).map_err(|_| database_facts())?
                == i64::try_from(canonical.len()).map_err(|_| rejected("response-receipt"))?
            && row.try_get::<String, _>(2).map_err(|_| database_facts())? == "structured"
            && canonical == payload
    } else {
        false
    };
    let counts: (i64,i64,i64,i64,i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM execution_leases),(SELECT count(*) FROM task_admissions),(SELECT count(*) FROM deliveries),(SELECT count(*) FROM delivery_dispositions),(SELECT count(*) FROM agent_work_queue WHERE state NOT IN ('completed','expired','superseded')),(SELECT count(*) FROM workspace_claims),(SELECT count(*) FROM projection_outbox WHERE state IN ('pending','sending'))")
        .fetch_one(&mut *c).await.map_err(|_|database_facts())?;
    let stopping_refs: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM execution_leases WHERE owner_token=?)+(SELECT count(*) FROM task_admissions WHERE runtime_instance=?)+(SELECT count(*) FROM agent_work_queue WHERE runtime_owner=?)+(SELECT count(*) FROM workspace_claims WHERE runtime_owner=?)")
        .bind(&stopping.token).bind(&stopping.token).bind(&stopping.token).bind(&stopping.token).fetch_one(&mut *c).await.map_err(|_|database_facts())?;
    if !response_valid {
        return Err(rejected("response-receipt"));
    }
    if stopping_refs != 0 {
        return Err(rejected("stopping-references"));
    }
    if counts.0 != 1 {
        return Err(rejected("lease-topology"));
    }
    if counts.1 != 1 {
        return Err(rejected("admission-topology"));
    }
    if counts.2 != 1 || counts.3 != 1 {
        return Err(rejected("delivery-topology"));
    }
    if counts.4 != 0 {
        return Err(rejected("work-queue"));
    }
    if counts.5 != 0 {
        return Err(rejected("workspace-claim"));
    }
    if counts.6 != 0 {
        return Err(rejected("projection-outbox"));
    }
    Ok(Facts {
        active,
        stopping,
        continuation,
    })
}

async fn emit_eligibility_diagnostic(c: &mut SqliteConnection, rejected_cause: &'static str) {
    if std::env::var_os("T050_FIXTURE_MIGRATION_PREFLIGHT_DIAGNOSTIC").is_none()
        && std::env::var_os("T050_FIXTURE_MIGRATION_DIAGNOSTIC").is_none()
    {
        return;
    }
    if rejected_cause == "database-facts" {
        println!(
            "prepare-isolated-legacy-fixture-migration-manifest stage=eligibility cause=database-facts counts=owners=0,continuations=0,sessions=0,children=0,leases=0,admissions=0,work=0"
        );
        return;
    }
    let counts = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64)>(
        "SELECT
         (SELECT count(*) FROM runtime_instances WHERE state IN ('active','stopping')),
         (SELECT count(*) FROM task_continuations),
         (SELECT count(*) FROM delivery_dispositions WHERE
            NOT ((child_fingerprint IS NULL AND reap_status IS NULL AND reap_at IS NULL)
              OR (child_fingerprint IS NOT NULL AND reap_status='reaped' AND reap_at IS NOT NULL))),
         (SELECT count(*) FROM execution_leases),
         (SELECT count(*) FROM task_admissions),
         (SELECT count(*) FROM agent_work_queue WHERE state NOT IN ('completed','expired','superseded'))",
    )
    .fetch_one(&mut *c)
    .await;
    let sessions = open_session_count(&mut *c).await;
    let (cause, values) = match (counts, sessions) {
        (Ok((owners, continuations, children, leases, admissions, work)), Ok(sessions)) => (
            rejected_cause,
            format!(
                "owners={owners},continuations={continuations},sessions={sessions},children={children},leases={leases},admissions={admissions},work={work}"
            ),
        ),
        _ => (
            "database-facts",
            "owners=0,continuations=0,sessions=0,children=0,leases=0,admissions=0,work=0"
                .to_owned(),
        ),
    };
    println!(
        "prepare-isolated-legacy-fixture-migration-manifest stage=eligibility cause={cause} counts={values}"
    );
}

fn emit_prepare_diagnostic(stage: &str, cause: &str, counts: &str) {
    if std::env::var_os("T050_FIXTURE_MIGRATION_PREFLIGHT_DIAGNOSTIC").is_none() {
        return;
    }
    println!(
        "prepare-isolated-legacy-fixture-migration-manifest stage={stage} cause={cause} counts={counts}"
    );
}

fn prepare_rejected(stage: &str, cause: &str, error: Error) -> Error {
    emit_prepare_diagnostic(stage, cause, "failures=1");
    error
}

fn prepare_test_fault(label: &str) -> bool {
    #[cfg(debug_assertions)]
    {
        std::env::var("T050_FIXTURE_MIGRATION_TEST_PREPARE_FAULT").as_deref() == Ok(label)
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = label;
        false
    }
}

fn postflight_identity(path: &Path, executable: bool, cause: &str) -> Result<String, Error> {
    if prepare_test_fault(cause) {
        return Err(prepare_rejected("postflight", cause, Error::Rejected));
    }
    path_identity(path, executable).map_err(|error| prepare_rejected("postflight", cause, error))
}

fn owner_digest(owner: &Owner) -> String {
    digest(&[
        &owner.token,
        &owner.started,
        &owner.heartbeat,
        &owner.state,
        &owner.fingerprint,
    ])
}

fn continuation_digest(value: &Continuation) -> String {
    digest(&[
        &value.task,
        &value.resource,
        &value.delivery,
        &value.fence.to_string(),
        &value.revision.to_string(),
        &value.state,
        &value.next_turn.to_string(),
        &value.completed_turns.to_string(),
        &value.no_progress.to_string(),
        &value.prompt,
        &value.started,
        &value.heartbeat,
        &value.last_progress,
        &value.output_bytes.to_string(),
        &value.generation,
        &value.generation_version.to_string(),
    ])
}

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn snapshot(
    c: &mut SqliteConnection,
    projected: Option<(&Facts, &str)>,
) -> Result<String, Error> {
    const TABLES: &[(&str, &str, usize)] = &[
        (
            "runtime_instances",
            "SELECT quote(instance_token),quote(started_at),quote(heartbeat_at),quote(state),quote(process_fingerprint) FROM runtime_instances ORDER BY instance_token",
            5,
        ),
        (
            "tasks",
            "SELECT quote(task_id),quote(root_task_id),quote(parent_task_id),quote(from_agent),quote(to_agent),quote(conversation_id),quote(reply_to),quote(text),quote(priority),quote(depth),quote(hops),quote(deadline),quote(version) FROM tasks ORDER BY task_id",
            13,
        ),
        (
            "task_events",
            "SELECT quote(event_id),quote(task_id),quote(seq),quote(status),quote(timestamp),quote(payload) FROM task_events ORDER BY task_id,seq",
            6,
        ),
        (
            "deliveries",
            "SELECT quote(delivery_id),quote(task_id),quote(attempt),quote(target_endpoint_id),quote(dispatched_at),quote(acknowledged_at) FROM deliveries ORDER BY delivery_id",
            6,
        ),
        (
            "task_admissions",
            "SELECT quote(task_id),quote(state),quote(revision),quote(runtime_instance),quote(reply_room),quote(reply_thread_root),quote(reply_event_id),quote(monitor_room),quote(monitor_generation),quote(render_version),quote(created_at),quote(updated_at) FROM task_admissions ORDER BY task_id",
            12,
        ),
        (
            "sessions",
            "SELECT quote(session_id),quote(endpoint_id),quote(conversation_id),quote(cwd),quote(backend_id),quote(protocol_version),quote(state),quote(created_at),quote(updated_at),quote(additional_directories),quote(compatibility_version),quote(compatibility_hash) FROM sessions ORDER BY session_id,endpoint_id",
            12,
        ),
        (
            "delivery_dispositions",
            "SELECT quote(delivery_id),quote(task_id),quote(attempt),quote(state),quote(session_id),quote(child_fingerprint),quote(reap_status),quote(reap_at),quote(reason_code) FROM delivery_dispositions ORDER BY delivery_id",
            9,
        ),
        (
            "execution_leases",
            "SELECT quote(resource_key),quote(task_id),quote(owner_token),quote(fence),quote(state),quote(acquired_at),quote(heartbeat_at),quote(expires_at) FROM execution_leases ORDER BY resource_key",
            8,
        ),
        (
            "task_continuations",
            "SELECT quote(task_id),quote(resource_key),quote(delivery_id),quote(lease_fence),quote(revision),quote(state),quote(next_turn),quote(completed_turns),quote(consecutive_no_progress),quote(next_prompt),quote(started_at),quote(heartbeat_at),quote(last_progress_at),quote(observed_output_bytes),quote(runtime_generation),quote(runtime_generation_version) FROM task_continuations ORDER BY task_id",
            16,
        ),
        (
            "continuation_response_receipts",
            "SELECT quote(idempotency_key),quote(task_id),quote(delivery_id),quote(resource_key),quote(lease_fence),quote(continuation_revision),quote(response_hash),quote(outcome),quote(output_bytes),quote(runtime_generation),quote(created_at) FROM continuation_response_receipts ORDER BY idempotency_key",
            11,
        ),
        (
            "agent_work_queue",
            "SELECT quote(queue_id),quote(task_id),quote(delivery_id),quote(state),quote(revision),quote(runtime_owner),quote(owner_fence),quote(send_started),quote(updated_at) FROM agent_work_queue ORDER BY queue_id",
            9,
        ),
        (
            "workspace_claims",
            "SELECT quote(claim_id),quote(runtime_owner),quote(task_id),quote(owner_fence),quote(state),quote(updated_at) FROM workspace_claims ORDER BY claim_id",
            6,
        ),
        (
            "projection_outbox",
            "SELECT quote(source_kind),quote(source_id),quote(projection),quote(state),quote(claim_owner),quote(claim_revision),quote(updated_at) FROM projection_outbox ORDER BY source_kind,source_id,projection",
            7,
        ),
    ];
    let mut hash = Sha256::new();
    for pragma in ["PRAGMA schema_version", "PRAGMA user_version"] {
        let value: i64 = sqlx::query_scalar(pragma)
            .fetch_one(&mut *c)
            .await
            .map_err(|_| Error::Database)?;
        hash.update(value.to_be_bytes());
    }
    for (table, sql, width) in TABLES {
        hash.update((*table).as_bytes());
        let rows = sqlx::query(*sql)
            .fetch_all(&mut *c)
            .await
            .map_err(|_| Error::Database)?;
        for row in rows {
            let mut values = Vec::with_capacity(*width);
            for index in 0..*width {
                values.push(
                    row.try_get::<String, _>(index)
                        .map_err(|_| Error::Database)?,
                );
            }
            if let Some((facts, stamp)) = projected {
                if *table == "runtime_instances"
                    && (values[0] == quoted(&facts.active.token)
                        || values[0] == quoted(&facts.stopping.token))
                {
                    values[2] = quoted(stamp);
                    values[3] = quoted("stopped");
                }
                if *table == "task_continuations" && values[0] == quoted(&facts.continuation.task) {
                    values[4] = (facts.continuation.revision + 1).to_string();
                    values[5] = quoted("terminal");
                    values[11] = quoted(stamp);
                }
            }
            for value in values {
                hash.update((value.len() as u64).to_be_bytes());
                hash.update(value.as_bytes());
            }
        }
    }
    Ok(format!("{:x}", hash.finalize()))
}

async fn sibling_digest(c: &mut SqliteConnection, operation: &str) -> Result<String, Error> {
    let rows=sqlx::query("SELECT operation_id,receipt_digest FROM isolated_fixture_migration_receipts WHERE operation_id<>? ORDER BY operation_id").bind(operation).fetch_all(&mut *c).await.map_err(|_|Error::Database)?;
    let mut values = Vec::new();
    for row in rows {
        values.push(row.try_get::<String, _>(0).map_err(|_| Error::Database)?);
        values.push(row.try_get::<String, _>(1).map_err(|_| Error::Database)?);
    }
    Ok(digest(
        &values.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

fn manifest_digest(m: &Manifest) -> String {
    digest(&[
        &m.operation_id,
        &m.isolated_root,
        &m.marker,
        &m.config,
        &m.config_identity,
        &m.database,
        &m.database_identity,
        &m.binary,
        &m.binary_identity,
        &m.unit,
        &m.evidence,
        &m.pre_state_digest,
        &m.post_state_digest,
        &m.active_owner_digest,
        &m.stopping_owner_digest,
        &m.continuation_digest,
        &m.sibling_receipt_digest,
        &m.commit_timestamp,
    ])
}
fn receipt_digest(m: &Manifest) -> String {
    digest(&[
        &m.operation_id,
        &m.manifest_digest,
        &m.pre_state_digest,
        &m.post_state_digest,
        &m.active_owner_digest,
        &m.stopping_owner_digest,
        &m.continuation_digest,
        &m.sibling_receipt_digest,
        &m.commit_timestamp,
        "migrated",
    ])
}

fn read_manifest(path: &Path) -> Result<(Manifest, Vec<u8>), Error> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW)
        .open(path)
        .map_err(|_| Error::Rejected)?;
    let before = file.metadata().map_err(|_| Error::Rejected)?;
    if !before.is_file() || before.mode() & 0o777 != 0o600 || before.nlink() != 1 {
        return Err(Error::Rejected);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|_| Error::Rejected)?;
    let after = file.metadata().map_err(|_| Error::Rejected)?;
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.mode() != after.mode()
        || after.nlink() != 1
    {
        return Err(Error::Rejected);
    }
    Ok((
        serde_json::from_slice(&bytes).map_err(|_| Error::Rejected)?,
        bytes,
    ))
}

fn authenticate(m: &Manifest, path: &Path, binary: &Path) -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    if m.unit != "guigu-codex-test.service"
        || manifest_digest(m) != m.manifest_digest
        || receipt_digest(m) != m.receipt_digest
    {
        return Err(Error::Rejected);
    }
    let root = Path::new(&m.isolated_root);
    if root.canonicalize().map_err(|_| Error::Rejected)? != root
        || std::fs::symlink_metadata(root)
            .map_err(|_| Error::Rejected)?
            .mode()
            & 0o022
            != 0
    {
        return Err(Error::Rejected);
    }
    if path.parent() != Some(root)
        || Path::new(&m.database).parent() != Some(root)
        || Path::new(&m.evidence).parent() != Some(root)
        || Path::new(&m.marker) != root.join(".t050-fixture-migration-marker")
    {
        return Err(Error::Rejected);
    }
    if Path::new(&m.binary) != binary
        || path_identity(binary, true)? != m.binary_identity
        || path_identity(Path::new(&m.config), true)? != m.config_identity
        || path_identity(Path::new(&m.database), false)? != m.database_identity
    {
        return Err(Error::Rejected);
    }
    let marker = format!(
        "t050-fixture-migration-v1\noperation_id={}\nunit={}\n",
        m.operation_id, m.unit
    );
    let (observed, _) = read_file(Path::new(&m.marker))?;
    if observed != marker.as_bytes() {
        return Err(Error::Rejected);
    }
    Ok(())
}
fn read_file(path: &Path) -> Result<(Vec<u8>, std::fs::Metadata), Error> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW)
        .open(path)
        .map_err(|_| Error::Rejected)?;
    let meta = file.metadata().map_err(|_| Error::Rejected)?;
    if !meta.is_file() || meta.mode() & 0o777 != 0o600 || meta.nlink() != 1 {
        return Err(Error::Rejected);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|_| Error::Rejected)?;
    Ok((bytes, meta))
}

fn precommit_pause() -> Result<(), Error> {
    #[cfg(debug_assertions)]
    {
        if let Some(value) = std::env::var_os("T050_FIXTURE_MIGRATION_TEST_PRECOMMIT") {
            let path = PathBuf::from(value);
            publish_new(&path, b"ready\n")?;
            while !path.with_extension("continue").exists() {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
    Ok(())
}

pub async fn prepare_manifest(args: PrepareArgs) -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    let root = args
        .root
        .canonicalize()
        .map_err(|_| prepare_rejected("preflight", "root-path", Error::Rejected))?;
    if root != args.root
        || (if prepare_test_fault("root-metadata") {
            Err(Error::Rejected)
        } else {
            std::fs::symlink_metadata(&root).map_err(|_| Error::Rejected)
        })
        .map_err(|_| prepare_rejected("preflight", "root-path", Error::Rejected))?
        .mode()
            & 0o022
            != 0
        || args.unit != "guigu-codex-test.service"
        || args.database.parent() != Some(root.as_path())
        || args.manifest.parent() != Some(root.as_path())
        || args.evidence.parent() != Some(root.as_path())
    {
        return Err(prepare_rejected(
            "preflight",
            "root-contract",
            Error::Rejected,
        ));
    }
    let config = args
        .config
        .canonicalize()
        .map_err(|_| prepare_rejected("preflight", "config-path", Error::Rejected))?;
    let database = args
        .database
        .canonicalize()
        .map_err(|_| prepare_rejected("preflight", "database-path", Error::Rejected))?;
    let binary = if prepare_test_fault("binary-current-exe") {
        Err(std::io::Error::other(
            "synthetic current executable failure",
        ))
    } else {
        std::env::current_exe()
    }
    .map_err(|_| prepare_rejected("preflight", "binary-path", Error::Rejected))?
    .canonicalize()
    .and_then(|path| {
        if prepare_test_fault("binary-canonicalize") {
            Err(std::io::Error::other(
                "synthetic executable canonicalization failure",
            ))
        } else {
            Ok(path)
        }
    })
    .map_err(|_| prepare_rejected("preflight", "binary-path", Error::Rejected))?;
    if config != args.config || database != args.database {
        return Err(prepare_rejected(
            "preflight",
            "path-identity",
            Error::Rejected,
        ));
    }
    preflight_publication(&args.manifest)
        .map_err(|error| prepare_rejected("publication", "manifest-target", error))?;
    preflight_publication(&args.evidence)
        .map_err(|error| prepare_rejected("publication", "evidence-target", error))?;
    let admission = crate::runtime_admission::Guard::acquire(&database, &config, "fixture-prepare")
        .map_err(|_| prepare_rejected("preflight", "admission-lock", Error::Rejected))?;
    preflight_unit(&args.unit)
        .map_err(|error| prepare_rejected("preflight", "unit-state", error))?;
    match classify_runtime_absence_authenticated(
        &binary,
        &config,
        Some(std::process::id()),
        &admission,
    ) {
        Ok(RuntimeAbsence::Clean) => {}
        Ok(RuntimeAbsence::Process) => {
            return Err(prepare_rejected(
                "preflight",
                "runtime-process",
                Error::Rejected,
            ));
        }
        Ok(RuntimeAbsence::Listener) => {
            return Err(prepare_rejected(
                "preflight",
                "runtime-listener",
                Error::Rejected,
            ));
        }
        Err(unknown) => {
            return Err(prepare_rejected(
                "preflight",
                unknown.cause(),
                Error::Rejected,
            ));
        }
    }
    let sidecars = Sidecars::capture(&database)
        .map_err(|error| prepare_rejected("sqlite-open", "sidecar-auth", error))?;
    let mut c = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(false)
            .foreign_keys(true),
    )
    .await
    .map_err(|_| prepare_rejected("sqlite-open", "connect", Error::Database))?;
    sidecars
        .revalidate(&database)
        .map_err(|error| prepare_rejected("sqlite-open", "sidecar-revalidation", error))?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut c)
        .await
        .map_err(|_| prepare_rejected("sqlite-open", "begin", Error::Database))?;
    let facts = match eligible(&mut c).await {
        Ok(facts) => facts,
        Err((error, cause)) => {
            emit_prepare_diagnostic("eligibility", "durable-facts", "failures=1");
            emit_eligibility_diagnostic(&mut c, cause).await;
            let _ = sqlx::query("ROLLBACK").execute(&mut c).await;
            return Err(error);
        }
    };
    let pre = snapshot(&mut c, None)
        .await
        .map_err(|error| prepare_rejected("sqlite-read", "pre-snapshot", error))?;
    let post = snapshot(&mut c, Some((&facts, &args.commit_timestamp)))
        .await
        .map_err(|error| prepare_rejected("sqlite-read", "post-snapshot", error))?;
    let siblings = sibling_digest(&mut c, &args.operation_id)
        .await
        .map_err(|error| prepare_rejected("sqlite-read", "sibling-receipts", error))?;
    sqlx::query("ROLLBACK")
        .execute(&mut c)
        .await
        .map_err(|_| prepare_rejected("sqlite-close", "rollback", Error::Database))?;
    c.close()
        .await
        .map_err(|_| prepare_rejected("sqlite-close", "connection", Error::Database))?;
    #[cfg(debug_assertions)]
    if std::env::var_os("T050_FIXTURE_MIGRATION_TEST_REPLACE_SIDECAR_AFTER_CLOSE").is_some() {
        use std::os::unix::fs::PermissionsExt;

        let wal = PathBuf::from(format!("{}-wal", database.to_string_lossy()));
        std::fs::write(&wal, b"replacement").map_err(|_| Error::Rejected)?;
        std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| Error::Rejected)?;
    }
    sidecars
        .revalidate(&database)
        .map_err(|error| prepare_rejected("sqlite-close", "sidecar-revalidation", error))?;
    admission
        .revalidate()
        .map_err(|_| prepare_rejected("postflight", "admission-lock", Error::Rejected))?;
    preflight_unit(&args.unit)
        .map_err(|error| prepare_rejected("postflight", "unit-state", error))?;
    preflight_runtime_absence_authenticated(&binary, &config, Some(std::process::id()), &admission)
        .map_err(|_| prepare_rejected("postflight", "runtime-absence", Error::Rejected))?;
    let mut m = Manifest {
        operation_id: args.operation_id,
        isolated_root: root.to_string_lossy().into_owned(),
        marker: root
            .join(".t050-fixture-migration-marker")
            .to_string_lossy()
            .into_owned(),
        config: config.to_string_lossy().into_owned(),
        config_identity: postflight_identity(&config, true, "config-identity")?,
        database: database.to_string_lossy().into_owned(),
        database_identity: postflight_identity(&database, false, "database-identity")?,
        binary: binary.to_string_lossy().into_owned(),
        binary_identity: postflight_identity(&binary, true, "binary-identity")?,
        unit: args.unit,
        evidence: args.evidence.to_string_lossy().into_owned(),
        pre_state_digest: pre,
        post_state_digest: post,
        active_owner_digest: owner_digest(&facts.active),
        stopping_owner_digest: owner_digest(&facts.stopping),
        continuation_digest: continuation_digest(&facts.continuation),
        receipt_digest: String::new(),
        sibling_receipt_digest: siblings,
        commit_timestamp: args.commit_timestamp,
        manifest_digest: String::new(),
    };
    m.manifest_digest = manifest_digest(&m);
    m.receipt_digest = receipt_digest(&m);
    let manifest = if prepare_test_fault("manifest-serialization") {
        Err(Error::Rejected)
    } else {
        serde_json::to_vec(&m).map_err(|_| Error::Rejected)
    }
    .map_err(|error| prepare_rejected("publication", "manifest-serialization", error))?;
    let marker = format!(
        "t050-fixture-migration-v1\noperation_id={}\nunit={}\n",
        m.operation_id, m.unit
    );
    ensure_marker(Path::new(&m.marker), marker.as_bytes())
        .map_err(|error| prepare_rejected("publication", "marker", error))?;
    publish_new(&args.manifest, &manifest)
        .map_err(|error| prepare_rejected("publication", "manifest", error))?;
    Ok(())
}

pub async fn migrate(path: impl AsRef<Path>) -> Result<Outcome, Error> {
    let path = path.as_ref();
    let (m, bytes) = read_manifest(path)?;
    let binary = std::env::current_exe()
        .map_err(|_| Error::Rejected)?
        .canonicalize()
        .map_err(|_| Error::Rejected)?;
    authenticate(&m, path, &binary)?;
    let admission = crate::runtime_admission::Guard::acquire(
        Path::new(&m.database),
        Path::new(&m.config),
        "fixture-migrate",
    )
    .map_err(|_| Error::Rejected)?;
    preflight_unit(&m.unit)?;
    preflight_runtime_absence_authenticated(&binary, Path::new(&m.config), None, &admission)?;
    preflight_publication(Path::new(&m.evidence))?;
    let sidecars = Sidecars::capture(Path::new(&m.database))?;
    let mut c = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&m.database)
            .create_if_missing(false)
            .foreign_keys(true),
    )
    .await
    .map_err(|_| Error::Database)?;
    sidecars.revalidate(Path::new(&m.database))?;
    let (_, again) = read_manifest(path)?;
    if again != bytes {
        return Err(Error::Rejected);
    };
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut c)
        .await
        .map_err(|_| Error::Database)?;
    let result = execute_transaction(&mut c, &m).await;
    match result {
        Ok(outcome) => {
            admission.revalidate().map_err(|_| Error::Rejected)?;
            preflight_unit(&m.unit)?;
            preflight_runtime_absence_authenticated(
                &binary,
                Path::new(&m.config),
                None,
                &admission,
            )?;
            sqlx::query("COMMIT")
                .execute(&mut c)
                .await
                .map_err(|_| Error::Database)?;
            sidecars.revalidate(Path::new(&m.database))?;
            #[cfg(debug_assertions)]
            if std::env::var_os("T050_FIXTURE_MIGRATION_TEST_LOSE_EVIDENCE").is_some() {
                return Err(Error::Rejected);
            }
            let manifest_hash = format!("{:x}", Sha256::digest(bytes));
            let evidence = format!(
                "t050-fixture-migration status=0 result={} manifest={} receipt={}\n",
                if outcome == Outcome::Migrated {
                    "migrated"
                } else {
                    "already-migrated"
                },
                manifest_hash,
                m.receipt_digest
            );
            publish(Path::new(&m.evidence), evidence.as_bytes())?;
            Ok(outcome)
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut c).await;
            sidecars.revalidate(Path::new(&m.database))?;
            Err(error)
        }
    }
}

async fn execute_transaction(c: &mut SqliteConnection, m: &Manifest) -> Result<Outcome, Error> {
    if sibling_digest(c, &m.operation_id).await? != m.sibling_receipt_digest {
        return Err(Error::Rejected);
    }
    if let Some(row) = sqlx::query("SELECT manifest_digest,pre_state_digest,post_state_digest,active_owner_digest,stopping_owner_digest,continuation_digest,receipt_digest,sibling_receipt_digest,commit_timestamp,result FROM isolated_fixture_migration_receipts WHERE operation_id=?")
        .bind(&m.operation_id).fetch_optional(&mut *c).await.map_err(|_|Error::Database)? {
        let expected = [&m.manifest_digest,&m.pre_state_digest,&m.post_state_digest,&m.active_owner_digest,&m.stopping_owner_digest,&m.continuation_digest,&m.receipt_digest,&m.sibling_receipt_digest,&m.commit_timestamp];
        for (index, value) in expected.iter().enumerate() {
            if row.try_get::<String, _>(index).map_err(|_| Error::Database)? != **value {
                return Err(Error::Rejected);
            }
        }
        let result: String = row.try_get(9).map_err(|_| Error::Database)?;
        if result != "migrated" || snapshot(c, None).await? != m.post_state_digest {
            return Err(Error::Rejected);
        }
        return Ok(Outcome::AlreadyMigrated);
    }
    if snapshot(c, None).await? != m.pre_state_digest {
        return Err(Error::Rejected);
    }
    let facts = eligible(c).await.map_err(|(error, _)| error)?;
    if owner_digest(&facts.active) != m.active_owner_digest
        || owner_digest(&facts.stopping) != m.stopping_owner_digest
        || continuation_digest(&facts.continuation) != m.continuation_digest
    {
        return Err(Error::Rejected);
    }
    update_owner(c, &facts.active, "active", &m.commit_timestamp).await?;
    fail_after("active")?;
    update_owner(c, &facts.stopping, "stopping", &m.commit_timestamp).await?;
    fail_after("stopping")?;
    update_continuation(c, &facts.continuation, &m.commit_timestamp).await?;
    drift_session(c).await?;
    if snapshot(c, None).await? != m.post_state_digest {
        return Err(Error::Rejected);
    }
    fail_after("continuation")?;
    if !eligible_post(c, &facts, &m.commit_timestamp).await? {
        return Err(Error::Rejected);
    }
    sqlx::query(
        "INSERT INTO isolated_fixture_migration_receipts VALUES(?,?,?,?,?,?,?,?,?,?, 'migrated')",
    )
    .bind(&m.operation_id)
    .bind(&m.manifest_digest)
    .bind(&m.pre_state_digest)
    .bind(&m.post_state_digest)
    .bind(&m.active_owner_digest)
    .bind(&m.stopping_owner_digest)
    .bind(&m.continuation_digest)
    .bind(&m.receipt_digest)
    .bind(&m.sibling_receipt_digest)
    .bind(&m.commit_timestamp)
    .execute(&mut *c)
    .await
    .map_err(|_| Error::Database)?;
    precommit_pause()?;
    Ok(Outcome::Migrated)
}

async fn update_owner(
    c: &mut SqliteConnection,
    owner: &Owner,
    state: &str,
    stamp: &str,
) -> Result<(), Error> {
    let changed=sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token=? AND started_at=? AND heartbeat_at=? AND state=? AND process_fingerprint=?")
        .bind(stamp).bind(&owner.token).bind(&owner.started).bind(&owner.heartbeat).bind(state)
        .bind(&owner.fingerprint).execute(&mut *c).await.map_err(|_|Error::Database)?;
    if changed.rows_affected() != 1 {
        return Err(Error::Rejected);
    }
    Ok(())
}

async fn update_continuation(
    c: &mut SqliteConnection,
    v: &Continuation,
    stamp: &str,
) -> Result<(), Error> {
    let changed=sqlx::query("UPDATE task_continuations SET state='terminal',revision=revision+1,heartbeat_at=? WHERE task_id=? AND resource_key=? AND delivery_id=? AND lease_fence=? AND revision=? AND state='blocked' AND next_turn=? AND completed_turns=? AND consecutive_no_progress=? AND next_prompt=? AND started_at=? AND heartbeat_at=? AND last_progress_at=? AND observed_output_bytes=? AND runtime_generation=? AND runtime_generation_version=?")
        .bind(stamp).bind(&v.task).bind(&v.resource).bind(&v.delivery).bind(v.fence).bind(v.revision)
        .bind(v.next_turn).bind(v.completed_turns).bind(v.no_progress).bind(&v.prompt).bind(&v.started)
        .bind(&v.heartbeat).bind(&v.last_progress).bind(v.output_bytes).bind(&v.generation)
        .bind(v.generation_version).execute(&mut *c).await.map_err(|_|Error::Database)?;
    if changed.rows_affected() != 1 {
        return Err(Error::Rejected);
    }
    Ok(())
}

async fn eligible_post(
    c: &mut SqliteConnection,
    before: &Facts,
    stamp: &str,
) -> Result<bool, Error> {
    let owners:i64=sqlx::query_scalar("SELECT count(*) FROM runtime_instances WHERE instance_token IN (?,?) AND state='stopped' AND heartbeat_at=?").bind(&before.active.token).bind(&before.stopping.token).bind(stamp).fetch_one(&mut *c).await.map_err(|_|Error::Database)?;
    let v = &before.continuation;
    let continuation:i64=sqlx::query_scalar("SELECT count(*) FROM task_continuations WHERE task_id=? AND resource_key=? AND delivery_id=? AND lease_fence=? AND revision=? AND state='terminal' AND heartbeat_at=?").bind(&v.task).bind(&v.resource).bind(&v.delivery).bind(v.fence).bind(v.revision+1).bind(stamp).fetch_one(&mut *c).await.map_err(|_|Error::Database)?;
    Ok(owners == 2 && continuation == 1)
}

fn fail_after(stage: &str) -> Result<(), Error> {
    #[cfg(debug_assertions)]
    if std::env::var("T050_FIXTURE_MIGRATION_TEST_FAIL_AFTER").as_deref() == Ok(stage) {
        return Err(Error::Rejected);
    }
    Ok(())
}

async fn drift_session(c: &mut SqliteConnection) -> Result<(), Error> {
    #[cfg(debug_assertions)]
    if std::env::var_os("T050_FIXTURE_MIGRATION_TEST_DRIFT_SESSION").is_some() {
        sqlx::query("UPDATE sessions SET updated_at='2026-10-04T00:00:01.000000000Z'")
            .execute(c)
            .await
            .map_err(|_| Error::Database)?;
    }
    Ok(())
}
