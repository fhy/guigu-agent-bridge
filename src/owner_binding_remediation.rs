use std::{ffi::OsString, path::Path};

use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

const COMMAND: &str = "remediate-isolated-owner-bindings";
const CONTRACT_VERSION: i64 = 2;
const MIGRATION_VERSION: i64 = 14;
const EXPECTED_LIVE_SOURCE_SET_SHA256: &str =
    "fdae494127ac9acf616ce16d0e0f5cb9a0ab3d3ac1cc8974e5525bddb45cb10f";
const OPERATION_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0xb0fa_773f_3de1_5cd4_a1af_9110_f33c_c7ae);
const MIGRATION: &[u8] =
    include_bytes!("../migrations/0014_isolated_owner_binding_remediation.sql");

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Remediated,
    AlreadyRemediated,
    Adopted,
}

impl Outcome {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Remediated => "remediated",
            Self::AlreadyRemediated => "already-remediated",
            Self::Adopted => "adopted",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Failure {
    pub cause: &'static str,
    pub substage: &'static str,
}

impl Failure {
    const fn new(cause: &'static str, substage: &'static str) -> Self {
        Self { cause, substage }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Args {
    snapshot: String,
    source_set: String,
    observed_live_source_set: String,
}

pub fn parse_args(args: &[OsString]) -> Result<Args, Failure> {
    if args.len() != 7
        || args[0] != COMMAND
        || args[1] != "--expected-backup-sha256"
        || args[3] != "--expected-source-set-sha256"
        || args[5] != "--expected-live-source-set-sha256"
    {
        return Err(Failure::new("arguments", "none"));
    }
    let snapshot = exact_digest(&args[2])?;
    let source_set = exact_digest(&args[4])?;
    let observed_live_source_set = exact_digest(&args[6])?;
    if observed_live_source_set != EXPECTED_LIVE_SOURCE_SET_SHA256 {
        return Err(Failure::new("arguments", "none"));
    }
    Ok(Args {
        snapshot,
        source_set,
        observed_live_source_set,
    })
}

fn exact_digest(value: &OsString) -> Result<String, Failure> {
    let value = value
        .to_str()
        .filter(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        })
        .ok_or_else(|| Failure::new("arguments", "none"))?;
    Ok(value.to_owned())
}

fn framed_digest(domain: &[u8], values: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    for value in values {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

fn operation_id(args: &Args) -> String {
    let mut framed = b"GGT050-OWNER-BINDING-REMEDIATION-V2\0".to_vec();
    for value in [
        &args.snapshot,
        &args.source_set,
        &args.observed_live_source_set,
    ] {
        framed.extend_from_slice(&(value.len() as u64).to_be_bytes());
        framed.extend_from_slice(value.as_bytes());
    }
    uuid::Uuid::new_v5(&OPERATION_NAMESPACE, &framed).to_string()
}

fn migration_checksum() -> String {
    format!("{:x}", Sha256::digest(MIGRATION))
}

#[derive(Clone)]
struct Owner {
    token: String,
    started_at: String,
    heartbeat_at: String,
    state: String,
    fingerprint: String,
}

#[derive(Clone)]
struct Continuation {
    task_id: String,
    resource_key: String,
    delivery_id: String,
    lease_fence: i64,
    revision: i64,
    state: String,
    next_turn: i64,
    completed_turns: i64,
    consecutive_no_progress: i64,
    next_prompt: String,
    started_at: String,
    heartbeat_at: String,
    last_progress_at: String,
    observed_output_bytes: i64,
    runtime_generation: String,
    runtime_generation_version: i64,
}

#[derive(Clone)]
struct Lease {
    resource_key: String,
    task_id: String,
    owner_token: String,
    fence: i64,
    state: String,
    acquired_at: String,
    heartbeat_at: String,
    expires_at: String,
}

#[derive(Clone)]
struct Admission {
    task_id: String,
    state: String,
    revision: i64,
    runtime_instance: String,
    reply_room: Option<String>,
    reply_thread_root: Option<String>,
    reply_event_id: Option<String>,
    monitor_room: Option<String>,
    monitor_generation: Option<String>,
    render_version: Option<String>,
    created_at: String,
    updated_at: String,
}

struct Facts {
    active: Owner,
    stopping: Owner,
    continuation: Continuation,
    lease: Lease,
    admission: Admission,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Boundary {
    BeforePublication,
    AfterPublicationBegin,
    AfterWitnessDdl,
    AfterReceiptDdl,
    AfterLedgerInsert,
    AfterWitnessInsert,
    BeforePublicationCommit,
    AfterPublicationReread,
    BeforeBusinessCommit,
    AfterBusinessReread,
}

type BoundaryCheck<'a> = dyn FnMut(Boundary) -> Result<(), Failure> + 'a;

fn valid_owner(owner: &Owner) -> bool {
    !owner.fingerprint.is_empty()
        && !owner.fingerprint.starts_with("pid:")
        && owner.fingerprint.is_ascii()
        && !owner.fingerprint.chars().any(char::is_whitespace)
}

async fn load_facts(c: &mut SqliteConnection) -> Result<Facts, Failure> {
    let owners = sqlx::query("SELECT instance_token,started_at,heartbeat_at,state,process_fingerprint FROM runtime_instances WHERE state IN ('active','stopping') ORDER BY state,instance_token")
        .fetch_all(&mut *c).await.map_err(|_| Failure::new("database", "read"))?;
    if owners.len() != 2 {
        return Err(Failure::new("eligibility", "owner-topology"));
    }
    let decode_owner = |row: &sqlx::sqlite::SqliteRow| -> Result<Owner, Failure> {
        Ok(Owner {
            token: row
                .try_get(0)
                .map_err(|_| Failure::new("database", "decode"))?,
            started_at: row
                .try_get(1)
                .map_err(|_| Failure::new("database", "decode"))?,
            heartbeat_at: row
                .try_get(2)
                .map_err(|_| Failure::new("database", "decode"))?,
            state: row
                .try_get(3)
                .map_err(|_| Failure::new("database", "decode"))?,
            fingerprint: row
                .try_get(4)
                .map_err(|_| Failure::new("database", "decode"))?,
        })
    };
    let mut active = None;
    let mut stopping = None;
    for row in &owners {
        let owner = decode_owner(row)?;
        if !valid_owner(&owner) {
            return Err(Failure::new("eligibility", "owner-identity"));
        }
        match owner.state.as_str() {
            "active" if active.is_none() => active = Some(owner),
            "stopping" if stopping.is_none() => stopping = Some(owner),
            _ => return Err(Failure::new("eligibility", "owner-topology")),
        }
    }
    let active = active.ok_or_else(|| Failure::new("eligibility", "owner-topology"))?;
    let stopping = stopping.ok_or_else(|| Failure::new("eligibility", "owner-topology"))?;
    if stopping.started_at >= active.started_at || stopping.heartbeat_at > active.started_at {
        return Err(Failure::new("eligibility", "owner-ordering"));
    }

    let rows = sqlx::query("SELECT task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes,runtime_generation,runtime_generation_version FROM task_continuations")
        .fetch_all(&mut *c).await.map_err(|_| Failure::new("database", "read"))?;
    if rows.len() != 1 {
        return Err(Failure::new("eligibility", "continuation"));
    }
    let row = &rows[0];
    let continuation = Continuation {
        task_id: get(row, 0)?,
        resource_key: get(row, 1)?,
        delivery_id: get(row, 2)?,
        lease_fence: get(row, 3)?,
        revision: get(row, 4)?,
        state: get(row, 5)?,
        next_turn: get(row, 6)?,
        completed_turns: get(row, 7)?,
        consecutive_no_progress: get(row, 8)?,
        next_prompt: get(row, 9)?,
        started_at: get(row, 10)?,
        heartbeat_at: get(row, 11)?,
        last_progress_at: get(row, 12)?,
        observed_output_bytes: get(row, 13)?,
        runtime_generation: get(row, 14)?,
        runtime_generation_version: get(row, 15)?,
    };
    if continuation.state != "blocked" || continuation.revision < 2 {
        return Err(Failure::new("eligibility", "continuation"));
    }

    let leases = sqlx::query("SELECT resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at FROM execution_leases")
        .fetch_all(&mut *c).await.map_err(|_| Failure::new("database", "read"))?;
    if leases.len() != 1 {
        return Err(Failure::new("eligibility", "lease-shape"));
    }
    let row = &leases[0];
    let lease = Lease {
        resource_key: get(row, 0)?,
        task_id: get(row, 1)?,
        owner_token: get(row, 2)?,
        fence: get(row, 3)?,
        state: get(row, 4)?,
        acquired_at: get(row, 5)?,
        heartbeat_at: get(row, 6)?,
        expires_at: get(row, 7)?,
    };
    if lease.resource_key != continuation.resource_key
        || lease.task_id != continuation.task_id
        || lease.fence != continuation.lease_fence
        || lease.state != "released"
        || lease.owner_token != stopping.token
    {
        return Err(Failure::new("eligibility", "lease-shape"));
    }

    let admissions = sqlx::query("SELECT task_id,state,revision,runtime_instance,reply_room,reply_thread_root,reply_event_id,monitor_room,monitor_generation,render_version,created_at,updated_at FROM task_admissions")
        .fetch_all(&mut *c).await.map_err(|_| Failure::new("database", "read"))?;
    if admissions.len() != 1 {
        return Err(Failure::new("eligibility", "admission-shape"));
    }
    let row = &admissions[0];
    let admission = Admission {
        task_id: get(row, 0)?,
        state: get(row, 1)?,
        revision: get(row, 2)?,
        runtime_instance: get(row, 3)?,
        reply_room: get(row, 4)?,
        reply_thread_root: get(row, 5)?,
        reply_event_id: get(row, 6)?,
        monitor_room: get(row, 7)?,
        monitor_generation: get(row, 8)?,
        render_version: get(row, 9)?,
        created_at: get(row, 10)?,
        updated_at: get(row, 11)?,
    };
    if admission.task_id != continuation.task_id
        || admission.state != "terminal"
        || admission.runtime_instance != stopping.token
        || admission.revision == i64::MAX
    {
        return Err(Failure::new("eligibility", "admission-shape"));
    }

    let stopping_refs: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM execution_leases WHERE owner_token=?)+(SELECT count(*) FROM task_admissions WHERE runtime_instance=?)+(SELECT count(*) FROM agent_work_queue WHERE runtime_owner=?)+(SELECT count(*) FROM workspace_claims WHERE runtime_owner=?)")
        .bind(&stopping.token).bind(&stopping.token).bind(&stopping.token).bind(&stopping.token)
        .fetch_one(&mut *c).await.map_err(|_| Failure::new("database", "read"))?;
    if stopping_refs != 2 {
        return Err(Failure::new("eligibility", "downstream"));
    }
    Ok(Facts {
        active,
        stopping,
        continuation,
        lease,
        admission,
    })
}

fn get<T>(row: &sqlx::sqlite::SqliteRow, index: usize) -> Result<T, Failure>
where
    for<'r> T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get(index)
        .map_err(|_| Failure::new("database", "decode"))
}

fn owner_digest(v: &Owner) -> String {
    framed_digest(
        b"GGT050-OWNER-V1",
        &[
            &v.token,
            &v.started_at,
            &v.heartbeat_at,
            &v.state,
            &v.fingerprint,
        ],
    )
}
fn continuation_digest(v: &Continuation) -> String {
    framed_digest(
        b"GGT050-CONTINUATION-V1",
        &[
            &v.task_id,
            &v.resource_key,
            &v.delivery_id,
            &v.lease_fence.to_string(),
            &v.revision.to_string(),
            &v.state,
            &v.next_turn.to_string(),
            &v.completed_turns.to_string(),
            &v.consecutive_no_progress.to_string(),
            &v.next_prompt,
            &v.started_at,
            &v.heartbeat_at,
            &v.last_progress_at,
            &v.observed_output_bytes.to_string(),
            &v.runtime_generation,
            &v.runtime_generation_version.to_string(),
        ],
    )
}
fn lease_digest(v: &Lease) -> String {
    framed_digest(
        b"GGT050-LEASE-V1",
        &[
            &v.resource_key,
            &v.task_id,
            &v.owner_token,
            &v.fence.to_string(),
            &v.state,
            &v.acquired_at,
            &v.heartbeat_at,
            &v.expires_at,
        ],
    )
}
fn admission_digest(v: &Admission) -> String {
    framed_digest(
        b"GGT050-ADMISSION-V1",
        &[
            &v.task_id,
            &v.state,
            &v.revision.to_string(),
            &v.runtime_instance,
            v.reply_room.as_deref().unwrap_or("<null>"),
            v.reply_thread_root.as_deref().unwrap_or("<null>"),
            v.reply_event_id.as_deref().unwrap_or("<null>"),
            v.monitor_room.as_deref().unwrap_or("<null>"),
            v.monitor_generation.as_deref().unwrap_or("<null>"),
            v.render_version.as_deref().unwrap_or("<null>"),
            &v.created_at,
            &v.updated_at,
        ],
    )
}

fn witness_digest(operation: &str, args: &Args, business: &str) -> String {
    framed_digest(
        b"GGT050-OWNER-BINDING-WITNESS-V2",
        &[
            "2",
            operation,
            &args.snapshot,
            &args.source_set,
            &args.observed_live_source_set,
            "14",
            &migration_checksum(),
            business,
            business,
        ],
    )
}

async fn business_digest(c: &mut SqliteConnection) -> Result<String, Failure> {
    crate::isolated_fixture_migration::owner_binding_business_snapshot(c)
        .await
        .map_err(|_| Failure::new("database", "read"))
}

async fn sibling_digest(c: &mut SqliteConnection, operation: &str) -> Result<String, Failure> {
    let rows = sqlx::query("SELECT operation_id,receipt_digest FROM isolated_owner_binding_remediation_receipts WHERE operation_id<>? ORDER BY operation_id")
        .bind(operation).fetch_all(&mut *c).await.map_err(|_| Failure::new("database", "read"))?;
    let mut values = Vec::with_capacity(rows.len() * 2);
    for row in rows {
        values.push(get::<String>(&row, 0)?);
        values.push(get::<String>(&row, 1)?);
    }
    Ok(framed_digest(
        b"GGT050-OWNER-BINDING-SIBLINGS-V1",
        &values.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

#[derive(Clone)]
struct Receipt {
    operation_id: String,
    backup_snapshot: String,
    backup_source: String,
    pre: String,
    post: String,
    active: String,
    stopping: String,
    continuation: String,
    lease_before: String,
    lease_after: String,
    admission_before: String,
    admission_after: String,
    siblings: String,
    timestamp: String,
}

fn receipt_digest(v: &Receipt) -> String {
    framed_digest(
        b"GGT050-OWNER-BINDING-RECEIPT-V1",
        &[
            &v.operation_id,
            "1",
            &v.backup_snapshot,
            &v.backup_source,
            &v.pre,
            &v.post,
            &v.active,
            &v.stopping,
            &v.continuation,
            &v.lease_before,
            &v.lease_after,
            &v.admission_before,
            &v.admission_after,
            &v.siblings,
            &v.timestamp,
            "remediated",
        ],
    )
}

fn normalized_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn migration_table_sql(name: &str) -> Option<String> {
    let source = std::str::from_utf8(MIGRATION).ok()?;
    let marker = format!("CREATE TABLE {name}");
    let start = source.find(&marker)?;
    let rest = &source[start..];
    let end = rest.find(";")?;
    Some(normalized_sql(&rest[..end]))
}

async fn authenticate_migration_ledger(
    c: &mut SqliteConnection,
    expected_versions: usize,
) -> Result<(), Failure> {
    type LedgerRow = (i64, String, bool, Vec<u8>, String, i64, bool);
    let rows: Vec<LedgerRow> = sqlx::query_as(
        "SELECT version,description,success,checksum,installed_on,execution_time,typeof(installed_on)='text' AND length(installed_on)=19 AND strftime('%Y-%m-%d %H:%M:%S',installed_on)=installed_on FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&mut *c)
    .await
    .map_err(|_| Failure::new("migration", "ledger"))?;
    let contract = crate::storage::migration_contract();
    if expected_versions > contract.len()
        || rows.len() != expected_versions
        || rows.iter().zip(contract.iter()).any(
            |(
                (
                    version,
                    description,
                    success,
                    checksum,
                    _installed_on,
                    execution_time,
                    canonical_installed_on,
                ),
                expected,
            )| {
                *version != expected.0
                    || description != &expected.1
                    || !*success
                    || checksum != &expected.2
                    || (*version == MIGRATION_VERSION
                        && (*execution_time != -1 || !*canonical_installed_on))
            },
        )
    {
        return Err(Failure::new("migration", "compatibility"));
    }
    Ok(())
}

async fn authenticate_0014_schema(c: &mut SqliteConnection) -> Result<(), Failure> {
    authenticate_migration_ledger(c, MIGRATION_VERSION as usize).await?;
    authenticate_0014_tables(c).await
}

async fn authenticate_0014_tables(c: &mut SqliteConnection) -> Result<(), Failure> {
    for name in [
        "isolated_owner_binding_remediation_witness",
        "isolated_owner_binding_remediation_receipts",
    ] {
        let actual: Option<String> =
            sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE type='table' AND name=?")
                .bind(name)
                .fetch_optional(&mut *c)
                .await
                .map_err(|_| Failure::new("migration", "compatibility"))?;
        if actual.as_deref().map(normalized_sql) != migration_table_sql(name) {
            return Err(Failure::new("migration", "compatibility"));
        }
    }
    let object_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_schema WHERE name LIKE 'isolated_owner_binding_remediation_%'",
    )
    .fetch_one(&mut *c)
    .await
    .map_err(|_| Failure::new("migration", "compatibility"))?;
    if object_count != 2 {
        return Err(Failure::new("migration", "compatibility"));
    }
    Ok(())
}

async fn exact_legacy_absence(c: &mut SqliteConnection, business: &str) -> Result<bool, Failure> {
    if authenticate_migration_ledger(c, (MIGRATION_VERSION - 1) as usize)
        .await
        .is_err()
    {
        return Ok(false);
    }
    let objects: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_schema WHERE name LIKE 'isolated_owner_binding_remediation_%'",
    )
    .fetch_one(&mut *c)
    .await
    .map_err(|_| Failure::new("migration", "compatibility"))?;
    Ok(objects == 0 && business_digest(c).await? == business)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)] // `Unavailable` is exercised by the deterministic classifier matrix.
enum CommitObservation {
    Success,
    Error,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationState {
    LegacyAbsence,
    Complete,
    Other,
}

#[derive(Clone, Copy)]
enum PublicationMode {
    Normal,
    #[cfg(test)]
    Inject {
        observation: CommitObservation,
        state: PublicationState,
    },
}

fn classify_publication(
    observation: CommitObservation,
    state: PublicationState,
) -> Result<(), Failure> {
    match (observation, state) {
        (_, PublicationState::Complete) => Ok(()),
        (
            CommitObservation::Error | CommitObservation::Unavailable,
            PublicationState::LegacyAbsence,
        ) => Err(Failure::new("unknown-commit", "not-committed")),
        _ => Err(Failure::new("unknown-commit", "ambiguous")),
    }
}

async fn commit_publication(c: &mut SqliteConnection, _mode: PublicationMode) -> CommitObservation {
    #[cfg(test)]
    if let PublicationMode::Inject { observation, .. } = _mode {
        let _ = sqlx::query("COMMIT").execute(&mut *c).await;
        return observation;
    }
    #[cfg(debug_assertions)]
    match std::env::var("T050_OWNER_BINDING_TEST_PUBLICATION_COMMIT").as_deref() {
        Ok("success-absence") => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *c).await;
            return CommitObservation::Success;
        }
        Ok("error-absence") => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *c).await;
            return CommitObservation::Error;
        }
        Ok("unavailable-complete") => {
            let _ = sqlx::query("COMMIT").execute(&mut *c).await;
            return CommitObservation::Unavailable;
        }
        _ => {}
    }
    if sqlx::query("COMMIT").execute(&mut *c).await.is_ok() {
        CommitObservation::Success
    } else {
        CommitObservation::Error
    }
}

async fn fresh_publication_reread(
    database: &Path,
    operation: &str,
    args: &Args,
    retained_business: &str,
    boundary: &mut BoundaryCheck<'_>,
) -> Result<(SqliteConnection, PublicationState), Failure> {
    let mut reread = open_live(database)
        .await
        .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    boundary(Boundary::AfterPublicationReread)
        .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    let receipt_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM isolated_owner_binding_remediation_receipts",
    )
    .fetch_one(&mut reread)
    .await;
    let state = if exact_legacy_absence(&mut reread, retained_business)
        .await
        .unwrap_or(false)
    {
        PublicationState::LegacyAbsence
    } else if authenticate_0014_schema(&mut reread).await.is_ok()
        && exact_witness(&mut reread, operation, args, retained_business)
            .await
            .unwrap_or(false)
        && business_digest(&mut reread).await.as_deref() == Ok(retained_business)
        && matches!(receipt_count, Ok(0))
    {
        PublicationState::Complete
    } else {
        PublicationState::Other
    };
    Ok((reread, state))
}

async fn finish_publication_commit(
    observation: CommitObservation,
    old: SqliteConnection,
    database: &Path,
    operation: &str,
    args: &Args,
    retained_business: &str,
    boundary: &mut BoundaryCheck<'_>,
) -> Result<SqliteConnection, Failure> {
    drop(old);
    let (reread, state) =
        fresh_publication_reread(database, operation, args, retained_business, boundary).await?;
    classify_publication(observation, state)?;
    Ok(reread)
}

async fn open_live(database: &Path) -> Result<SqliteConnection, Failure> {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(database)
            .create_if_missing(false)
            .foreign_keys(true)
            .busy_timeout(std::time::Duration::from_secs(1)),
    )
    .await
    .map_err(|_| Failure::new("database", "connect"))
}

async fn exact_witness(
    c: &mut SqliteConnection,
    operation: &str,
    args: &Args,
    business: &str,
) -> Result<bool, Failure> {
    let rows=sqlx::query("SELECT contract_version,operation_id,backup_snapshot_sha256,backup_source_set_sha256,observed_live_source_set_sha256,migration_version,migration_checksum_digest,retained_pre0014_business_digest,migrated_pre_repair_business_digest,witness_digest FROM isolated_owner_binding_remediation_witness").fetch_all(&mut *c).await.map_err(|_|Failure::new("migration","compatibility"))?;
    if rows.is_empty() {
        return Ok(false);
    }
    if rows.len() != 1 {
        return Err(Failure::new("migration", "compatibility"));
    }
    let r = &rows[0];
    let values = (
        get::<i64>(r, 0)?,
        get::<String>(r, 1)?,
        get::<String>(r, 2)?,
        get::<String>(r, 3)?,
        get::<String>(r, 4)?,
        get::<i64>(r, 5)?,
        get::<String>(r, 6)?,
        get::<String>(r, 7)?,
        get::<String>(r, 8)?,
        get::<String>(r, 9)?,
    );
    if values
        != (
            CONTRACT_VERSION,
            operation.to_owned(),
            args.snapshot.clone(),
            args.source_set.clone(),
            args.observed_live_source_set.clone(),
            MIGRATION_VERSION,
            migration_checksum(),
            business.to_owned(),
            business.to_owned(),
            witness_digest(operation, args, business),
        )
    {
        return Err(Failure::new("migration", "compatibility"));
    }
    Ok(true)
}

async fn receipt_repeat(
    c: &mut SqliteConnection,
    operation: &str,
    args: &Args,
    pre_business: &str,
    current_business: &str,
    retained: &Facts,
) -> Result<Option<Outcome>, Failure> {
    let siblings = sibling_digest(c, operation).await?;
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM isolated_owner_binding_remediation_receipts")
            .fetch_one(&mut *c)
            .await
            .map_err(|_| Failure::new("database", "read"))?;
    let row=sqlx::query("SELECT pre_state_digest,post_state_digest,active_owner_digest,stopping_owner_digest,continuation_digest,lease_before_digest,lease_after_digest,admission_before_digest,admission_after_digest,sibling_receipt_digest,receipt_digest,commit_timestamp,result,contract_version,backup_snapshot_sha256,backup_source_set_sha256 FROM isolated_owner_binding_remediation_receipts WHERE operation_id=?")
        .bind(operation).fetch_optional(&mut *c).await.map_err(|_|Failure::new("database","read"))?;
    let Some(row) = row else {
        if count != 0 {
            return Err(Failure::new("receipt", "collision"));
        }
        return Ok(None);
    };
    if count != 1 {
        return Err(Failure::new("receipt", "collision"));
    }
    let receipt = Receipt {
        operation_id: operation.to_owned(),
        backup_snapshot: get(&row, 14)?,
        backup_source: get(&row, 15)?,
        pre: get(&row, 0)?,
        post: get(&row, 1)?,
        active: get(&row, 2)?,
        stopping: get(&row, 3)?,
        continuation: get(&row, 4)?,
        lease_before: get(&row, 5)?,
        lease_after: get(&row, 6)?,
        admission_before: get(&row, 7)?,
        admission_after: get(&row, 8)?,
        siblings: get(&row, 9)?,
        timestamp: get(&row, 11)?,
    };
    let stored: String = get(&row, 10)?;
    let result: String = get(&row, 12)?;
    let version: i64 = get(&row, 13)?;
    let mut lease_after = retained.lease.clone();
    lease_after.owner_token = retained.active.token.clone();
    let mut admission_after = retained.admission.clone();
    admission_after.runtime_instance = retained.active.token.clone();
    admission_after.revision = admission_after
        .revision
        .checked_add(1)
        .ok_or_else(|| Failure::new("receipt", "mismatch"))?;
    admission_after.updated_at = receipt.timestamp.clone();
    let lease_row = sqlx::query("SELECT resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at FROM execution_leases")
        .fetch_one(&mut *c).await.map_err(|_| Failure::new("receipt", "mismatch"))?;
    let current_lease = Lease {
        resource_key: get(&lease_row, 0)?,
        task_id: get(&lease_row, 1)?,
        owner_token: get(&lease_row, 2)?,
        fence: get(&lease_row, 3)?,
        state: get(&lease_row, 4)?,
        acquired_at: get(&lease_row, 5)?,
        heartbeat_at: get(&lease_row, 6)?,
        expires_at: get(&lease_row, 7)?,
    };
    let admission_row = sqlx::query("SELECT task_id,state,revision,runtime_instance,reply_room,reply_thread_root,reply_event_id,monitor_room,monitor_generation,render_version,created_at,updated_at FROM task_admissions")
        .fetch_one(&mut *c).await.map_err(|_| Failure::new("receipt", "mismatch"))?;
    let current_admission = Admission {
        task_id: get(&admission_row, 0)?,
        state: get(&admission_row, 1)?,
        revision: get(&admission_row, 2)?,
        runtime_instance: get(&admission_row, 3)?,
        reply_room: get(&admission_row, 4)?,
        reply_thread_root: get(&admission_row, 5)?,
        reply_event_id: get(&admission_row, 6)?,
        monitor_room: get(&admission_row, 7)?,
        monitor_generation: get(&admission_row, 8)?,
        render_version: get(&admission_row, 9)?,
        created_at: get(&admission_row, 10)?,
        updated_at: get(&admission_row, 11)?,
    };
    if version != 1
        || receipt.backup_snapshot != args.snapshot
        || receipt.backup_source != args.source_set
        || receipt.siblings != siblings
        || receipt.pre != pre_business
        || receipt.active != owner_digest(&retained.active)
        || receipt.stopping != owner_digest(&retained.stopping)
        || receipt.continuation != continuation_digest(&retained.continuation)
        || receipt.lease_before != lease_digest(&retained.lease)
        || receipt.lease_after != lease_digest(&lease_after)
        || receipt.admission_before != admission_digest(&retained.admission)
        || receipt.admission_after != admission_digest(&admission_after)
        || lease_digest(&current_lease) != lease_digest(&lease_after)
        || admission_digest(&current_admission) != admission_digest(&admission_after)
        || stored != receipt_digest(&receipt)
        || result != "remediated"
        || receipt.post != current_business
    {
        return Err(Failure::new("receipt", "mismatch"));
    }
    Ok(Some(Outcome::AlreadyRemediated))
}

async fn fresh_business_reread(
    database: &Path,
    operation: &str,
    args: &Args,
    retained_business: &str,
    retained_facts: &Facts,
    boundary: &mut BoundaryCheck<'_>,
) -> Result<(String, Option<Outcome>), Failure> {
    let mut reread = open_live(database)
        .await
        .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    authenticate_0014_schema(&mut reread)
        .await
        .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    boundary(Boundary::AfterBusinessReread)
        .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    let business = business_digest(&mut reread)
        .await
        .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    let receipt = receipt_repeat(
        &mut reread,
        operation,
        args,
        retained_business,
        &business,
        retained_facts,
    )
    .await
    .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    Ok((business, receipt))
}

pub async fn remediate_paths(
    database: &Path,
    retained: &Path,
    args: &Args,
    legacy_source_matches: bool,
) -> Result<Outcome, Failure> {
    let mut boundary = |_| Ok(());
    remediate_paths_with_boundaries(
        database,
        retained,
        args,
        legacy_source_matches,
        false,
        &mut boundary,
    )
    .await
}

async fn remediate_paths_with_commit_result(
    database: &Path,
    retained: &Path,
    args: &Args,
    legacy_source_matches: bool,
    commit_result_lost: bool,
) -> Result<Outcome, Failure> {
    let mut boundary = |_| Ok(());
    remediate_paths_with_boundaries(
        database,
        retained,
        args,
        legacy_source_matches,
        commit_result_lost,
        &mut boundary,
    )
    .await
}

async fn remediate_paths_with_boundaries(
    database: &Path,
    retained: &Path,
    args: &Args,
    legacy_source_matches: bool,
    commit_result_lost: bool,
    boundary: &mut BoundaryCheck<'_>,
) -> Result<Outcome, Failure> {
    remediate_paths_opening(
        database,
        retained,
        args,
        legacy_source_matches,
        commit_result_lost,
        boundary,
        PublicationMode::Normal,
    )
    .await
}

async fn remediate_paths_opening(
    database_open_path: &Path,
    retained: &Path,
    args: &Args,
    legacy_source_matches: bool,
    commit_result_lost: bool,
    boundary: &mut BoundaryCheck<'_>,
    publication_mode: PublicationMode,
) -> Result<Outcome, Failure> {
    let operation = operation_id(args);
    let mut retained_c = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(retained)
            .create_if_missing(false)
            .read_only(true)
            .immutable(true),
    )
    .await
    .map_err(|_| Failure::new("backup", "snapshot"))?;
    let retained_business = business_digest(&mut retained_c).await?;
    if exact_legacy_absence(&mut retained_c, &retained_business).await != Ok(true) {
        return Err(Failure::new("backup", "source-binding"));
    }
    let retained_facts = load_facts(&mut retained_c)
        .await
        .map_err(|_| Failure::new("backup", "source-binding"))?;
    retained_c
        .close()
        .await
        .map_err(|_| Failure::new("backup", "snapshot"))?;
    boundary(Boundary::BeforePublication)?;
    let mut c = open_live(database_open_path).await?;
    let latest: i64 = sqlx::query_scalar("SELECT coalesce(max(version),0) FROM _sqlx_migrations")
        .fetch_one(&mut c)
        .await
        .map_err(|_| Failure::new("migration", "ledger"))?;
    if latest == MIGRATION_VERSION - 1 {
        authenticate_migration_ledger(&mut c, (MIGRATION_VERSION - 1) as usize).await?;
        let live_business = business_digest(&mut c).await?;
        if exact_legacy_absence(&mut c, &retained_business).await != Ok(true) {
            return Err(Failure::new("migration", "compatibility"));
        }
        if live_business != retained_business {
            return Err(Failure::new("backup", "source-binding"));
        }
        // `legacy_source_matches` distinguishes the exact-byte and business-equivalent
        // phases; both are authorized only after the caller-bound observed digest check.
        let _legacy_phase_is_exact_bytes = legacy_source_matches;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut c)
            .await
            .map_err(|_| Failure::new("migration", "busy"))?;
        let publication = async {
            boundary(Boundary::AfterPublicationBegin)?;
            let migration = std::str::from_utf8(MIGRATION)
                .map_err(|_| Failure::new("migration", "apply"))?;
            let mut statements = migration.split(";\n\n");
            let witness_ddl = statements
                .next()
                .ok_or_else(|| Failure::new("migration", "apply"))?;
            let receipt_ddl = statements
                .next()
                .ok_or_else(|| Failure::new("migration", "apply"))?;
            if statements.next().is_some() {
                return Err(Failure::new("migration", "apply"));
            }
            #[cfg(test)]
            if matches!(
                publication_mode,
                PublicationMode::Inject {
                    state: PublicationState::LegacyAbsence,
                    ..
                }
            ) {
                return Ok(());
            }
            sqlx::raw_sql(witness_ddl)
                .execute(&mut c)
                .await
                .map_err(|_| Failure::new("migration", "apply"))?;
            boundary(Boundary::AfterWitnessDdl)?;
            #[cfg(test)]
            if matches!(
                publication_mode,
                PublicationMode::Inject {
                    state: PublicationState::Other,
                    ..
                }
            ) {
                return Ok(());
            }
            sqlx::raw_sql(receipt_ddl)
                .execute(&mut c)
                .await
                .map_err(|_| Failure::new("migration", "apply"))?;
            boundary(Boundary::AfterReceiptDdl)?;
            authenticate_0014_tables(&mut c).await?;
            let receipts: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM isolated_owner_binding_remediation_receipts",
            )
            .fetch_one(&mut c)
            .await
            .map_err(|_| Failure::new("migration", "compatibility"))?;
            if receipts != 0 {
                return Err(Failure::new("migration", "compatibility"));
            }
            let contract = crate::storage::migration_contract();
            let migration = contract
                .iter()
                .find(|entry| entry.0 == MIGRATION_VERSION)
                .ok_or_else(|| Failure::new("migration", "compatibility"))?;
            let inserted = sqlx::query("INSERT INTO _sqlx_migrations(version,description,success,checksum,execution_time) VALUES(?1,?2,TRUE,?3,-1)")
                .bind(migration.0)
                .bind(&migration.1)
                .bind(&migration.2)
                .execute(&mut c)
                .await
                .map_err(|_| Failure::new("migration", "apply"))?;
            if inserted.rows_affected() != 1 {
                return Err(Failure::new("migration", "apply"));
            }
            boundary(Boundary::AfterLedgerInsert)?;
            authenticate_migration_ledger(&mut c, MIGRATION_VERSION as usize).await?;
            let inserted = sqlx::query("INSERT INTO isolated_owner_binding_remediation_witness(singleton,contract_version,operation_id,backup_snapshot_sha256,backup_source_set_sha256,observed_live_source_set_sha256,migration_version,migration_checksum_digest,retained_pre0014_business_digest,migrated_pre_repair_business_digest,witness_digest) VALUES(1,2,?,?,?,?,14,?,?,?,?)")
                .bind(&operation)
                .bind(&args.snapshot)
                .bind(&args.source_set)
                .bind(&args.observed_live_source_set)
                .bind(migration_checksum())
                .bind(&retained_business)
                .bind(&retained_business)
                .bind(witness_digest(&operation, args, &retained_business))
                .execute(&mut c)
                .await
                .map_err(|_| Failure::new("migration", "apply"))?;
            if inserted.rows_affected() != 1
                || !exact_witness(&mut c, &operation, args, &retained_business).await?
                || business_digest(&mut c).await? != retained_business
            {
                return Err(Failure::new("migration", "compatibility"));
            }
            boundary(Boundary::AfterWitnessInsert)?;
            Ok::<(), Failure>(())
        }
        .await;
        if let Err(error) = publication {
            let rollback = sqlx::query("ROLLBACK").execute(&mut c).await;
            drop(c);
            let mut reread = open_live(database_open_path)
                .await
                .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
            boundary(Boundary::AfterPublicationReread)
                .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
            if rollback.is_ok()
                && exact_legacy_absence(&mut reread, &retained_business).await == Ok(true)
            {
                return Err(error);
            }
            return Err(Failure::new("unknown-commit", "ambiguous"));
        }
        if let Err(error) = boundary(Boundary::BeforePublicationCommit) {
            let rollback = sqlx::query("ROLLBACK").execute(&mut c).await;
            drop(c);
            let mut reread = open_live(database_open_path)
                .await
                .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
            boundary(Boundary::AfterPublicationReread)
                .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
            if rollback.is_ok()
                && exact_legacy_absence(&mut reread, &retained_business).await == Ok(true)
            {
                return Err(error);
            }
            return Err(Failure::new("unknown-commit", "ambiguous"));
        }
        let observation = commit_publication(&mut c, publication_mode).await;
        c = finish_publication_commit(
            observation,
            c,
            database_open_path,
            &operation,
            args,
            &retained_business,
            boundary,
        )
        .await?;
    } else if latest == MIGRATION_VERSION {
        authenticate_0014_schema(&mut c).await?;
        if !exact_witness(&mut c, &operation, args, &retained_business).await? {
            return Err(Failure::new("migration", "compatibility"));
        }
    } else {
        return Err(Failure::new("migration", "compatibility"));
    }
    let current = business_digest(&mut c).await?;
    if let Some(outcome) = receipt_repeat(
        &mut c,
        &operation,
        args,
        &retained_business,
        &current,
        &retained_facts,
    )
    .await?
    {
        c.close()
            .await
            .map_err(|_| Failure::new("database", "read"))?;
        return Ok(outcome);
    }
    if current != retained_business {
        return Err(Failure::new("receipt", "post-state"));
    }
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut c)
        .await
        .map_err(|_| Failure::new("database", "busy"))?;
    let result = execute_business(
        &mut c,
        &operation,
        args,
        &retained_business,
        &retained_facts,
    )
    .await;
    if let Err(error) = result {
        let rollback = sqlx::query("ROLLBACK").execute(&mut c).await;
        drop(c);
        let (durable, receipt) = fresh_business_reread(
            database_open_path,
            &operation,
            args,
            &retained_business,
            &retained_facts,
            boundary,
        )
        .await?;
        if rollback.is_ok() && receipt.is_none() && durable == retained_business {
            return Err(error);
        }
        return Err(Failure::new("unknown-commit", "ambiguous"));
    }
    boundary(Boundary::BeforeBusinessCommit)?;
    let commit = sqlx::query("COMMIT").execute(&mut c).await;
    drop(c);
    let (post, receipt) = fresh_business_reread(
        database_open_path,
        &operation,
        args,
        &retained_business,
        &retained_facts,
        boundary,
    )
    .await?;
    match receipt {
        Some(_) if commit.is_err() || commit_result_lost => Ok(Outcome::Adopted),
        Some(_) => Ok(Outcome::Remediated),
        None if post == retained_business => Err(Failure::new("unknown-commit", "not-committed")),
        None => Err(Failure::new("unknown-commit", "ambiguous")),
    }
}

pub async fn run_production(args: &Args) -> Result<Outcome, Failure> {
    use std::os::fd::AsRawFd as _;
    #[cfg(debug_assertions)]
    if let (Some(database), Some(retained)) = (
        std::env::var_os("T050_OWNER_BINDING_TEST_DATABASE"),
        std::env::var_os("T050_OWNER_BINDING_TEST_RETAINED"),
    ) {
        let source_matches = std::env::var_os("T050_OWNER_BINDING_TEST_SOURCE_MATCH")
            .is_some_and(|value| value == "1");
        return remediate_paths_with_commit_result(
            Path::new(&database),
            Path::new(&retained),
            args,
            source_matches,
            std::env::var_os("T050_OWNER_BINDING_TEST_LOST_COMMIT").is_some(),
        )
        .await;
    }
    use crate::isolated_state_backup::{CONFIG, DATABASE};
    let database = Path::new(DATABASE);
    let config = Path::new(CONFIG);
    let guard =
        crate::runtime_admission::Guard::acquire(database, config, "owner-binding-remediation")
            .map_err(|_| Failure::new("runtime", "admission-lock"))?;
    crate::legacy_owner_reconciliation::preflight_unit("guigu-codex-test.service")
        .map_err(|_| Failure::new("runtime", "unit-state"))?;
    let binary = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|_| Failure::new("identity", "binary"))?;
    crate::legacy_owner_reconciliation::preflight_runtime_absence_authenticated(
        &binary, config, None, &guard,
    )
    .map_err(|_| Failure::new("runtime", "runtime-unknown"))?;
    let mut anchor =
        crate::isolated_state_backup::owner_binding_anchor(&args.snapshot, &args.source_set)
            .await?;
    if anchor.source_set_hex() != args.observed_live_source_set {
        return Err(Failure::new("backup", "source-binding"));
    }
    let live_source_matches = anchor.source_set_hex() == args.source_set;
    let retained_fd = std::path::PathBuf::from(format!(
        "/proc/self/fd/{}",
        anchor.snapshot_fd().as_raw_fd()
    ));
    let live_fd =
        std::path::PathBuf::from(format!("/proc/self/fd/{}", anchor.live_fd().as_raw_fd()));
    let mut boundary = |point| {
        if point == Boundary::AfterPublicationReread {
            anchor.refresh_live_sidecars()?;
        }
        anchor.revalidate()?;
        guard
            .revalidate()
            .map_err(|_| Failure::new("runtime", "admission-lock"))?;
        crate::legacy_owner_reconciliation::preflight_unit("guigu-codex-test.service")
            .map_err(|_| Failure::new("runtime", "unit-state"))?;
        crate::legacy_owner_reconciliation::preflight_runtime_absence_authenticated(
            &binary, config, None, &guard,
        )
        .map_err(|_| Failure::new("runtime", "runtime-unknown"))
    };
    remediate_paths_opening(
        &live_fd,
        &retained_fd,
        args,
        live_source_matches,
        false,
        &mut boundary,
        PublicationMode::Normal,
    )
    .await
}

async fn execute_business(
    c: &mut SqliteConnection,
    operation: &str,
    args: &Args,
    pre: &str,
    retained: &Facts,
) -> Result<(), Failure> {
    let facts = load_facts(c).await?;
    if owner_digest(&facts.active) != owner_digest(&retained.active)
        || owner_digest(&facts.stopping) != owner_digest(&retained.stopping)
        || continuation_digest(&facts.continuation) != continuation_digest(&retained.continuation)
        || lease_digest(&facts.lease) != lease_digest(&retained.lease)
        || admission_digest(&facts.admission) != admission_digest(&retained.admission)
    {
        return Err(Failure::new("eligibility", "owner-identity"));
    }
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let mut lease_after = facts.lease.clone();
    lease_after.owner_token = facts.active.token.clone();
    let mut admission_after = facts.admission.clone();
    admission_after.runtime_instance = facts.active.token.clone();
    admission_after.revision += 1;
    admission_after.updated_at = timestamp.clone();
    let changed=sqlx::query("UPDATE execution_leases SET owner_token=? WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND state=? AND acquired_at=? AND heartbeat_at=? AND expires_at=?")
        .bind(&facts.active.token).bind(&facts.lease.resource_key).bind(&facts.lease.task_id).bind(&facts.lease.owner_token).bind(facts.lease.fence).bind(&facts.lease.state).bind(&facts.lease.acquired_at).bind(&facts.lease.heartbeat_at).bind(&facts.lease.expires_at).execute(&mut *c).await.map_err(|_|Failure::new("cas","lease"))?;
    if changed.rows_affected() != 1 {
        return Err(Failure::new("cas", "lease"));
    }
    let changed=sqlx::query("UPDATE task_admissions SET runtime_instance=?,revision=revision+1,updated_at=? WHERE task_id=? AND state=? AND revision=? AND runtime_instance=? AND reply_room IS ? AND reply_thread_root IS ? AND reply_event_id IS ? AND monitor_room IS ? AND monitor_generation IS ? AND render_version IS ? AND created_at=? AND updated_at=?")
        .bind(&facts.active.token).bind(&timestamp).bind(&facts.admission.task_id).bind(&facts.admission.state).bind(facts.admission.revision).bind(&facts.admission.runtime_instance).bind(&facts.admission.reply_room).bind(&facts.admission.reply_thread_root).bind(&facts.admission.reply_event_id).bind(&facts.admission.monitor_room).bind(&facts.admission.monitor_generation).bind(&facts.admission.render_version).bind(&facts.admission.created_at).bind(&facts.admission.updated_at).execute(&mut *c).await.map_err(|_|Failure::new("cas","admission"))?;
    if changed.rows_affected() != 1 {
        return Err(Failure::new("cas", "admission"));
    }
    crate::isolated_fixture_migration::owner_binding_eligible(c)
        .await
        .map_err(|_| Failure::new("cas", "post-state"))?;
    let post = business_digest(c).await?;
    let siblings = sibling_digest(c, operation).await?;
    let receipt = Receipt {
        operation_id: operation.to_owned(),
        backup_snapshot: args.snapshot.clone(),
        backup_source: args.source_set.clone(),
        pre: pre.to_owned(),
        post: post.clone(),
        active: owner_digest(&facts.active),
        stopping: owner_digest(&facts.stopping),
        continuation: continuation_digest(&facts.continuation),
        lease_before: lease_digest(&facts.lease),
        lease_after: lease_digest(&lease_after),
        admission_before: admission_digest(&facts.admission),
        admission_after: admission_digest(&admission_after),
        siblings,
        timestamp,
    };
    let digest = receipt_digest(&receipt);
    let inserted=sqlx::query("INSERT INTO isolated_owner_binding_remediation_receipts(operation_id,contract_version,backup_snapshot_sha256,backup_source_set_sha256,pre_state_digest,post_state_digest,active_owner_digest,stopping_owner_digest,continuation_digest,lease_before_digest,lease_after_digest,admission_before_digest,admission_after_digest,sibling_receipt_digest,receipt_digest,commit_timestamp,result) VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,'remediated')")
        .bind(&receipt.operation_id).bind(&receipt.backup_snapshot).bind(&receipt.backup_source).bind(&receipt.pre).bind(&receipt.post).bind(&receipt.active).bind(&receipt.stopping).bind(&receipt.continuation).bind(&receipt.lease_before).bind(&receipt.lease_after).bind(&receipt.admission_before).bind(&receipt.admission_after).bind(&receipt.siblings).bind(digest).bind(&receipt.timestamp).execute(&mut *c).await.map_err(|_|Failure::new("cas","receipt"))?;
    if inserted.rows_affected() != 1 {
        return Err(Failure::new("cas", "receipt"));
    }
    if business_digest(c).await? != post {
        return Err(Failure::new("cas", "post-state"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate};
    use sqlx::Executor;

    async fn open(path: &Path) -> SqliteConnection {
        SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(false),
        )
        .await
        .unwrap()
    }

    async fn fixture() -> (
        tempfile::TempDir,
        std::path::PathBuf,
        std::path::PathBuf,
        Args,
    ) {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("live.db");
        let retained = root.path().join("retained.db");
        let pool = connect(&database).await.unwrap();
        migrate(&pool).await.unwrap();
        for sql in [
            "INSERT INTO agents VALUES('agent-a','a','acp',1,NULL,'[]'),('agent-b','b','acp',1,NULL,'[]')",
            "INSERT INTO conversations VALUES('conversation',NULL,NULL,NULL,'[]')",
            "INSERT INTO tasks VALUES('task','task',NULL,'agent-a','agent-b','conversation',NULL,'closed fixture',5,0,0,NULL,1)",
            "INSERT INTO task_events VALUES('event-1','task',1,'running','2026-10-01T00:00:00.000000000Z','{}'),('event-2','task',2,'failed','2026-10-01T00:00:03.000000000Z','{}')",
            "INSERT INTO deliveries VALUES('delivery','task',1,'agent-b','2026-10-01T00:00:01.000000000Z','2026-10-01T00:00:03.000000000Z')",
            "INSERT INTO runtime_instances VALUES('owner-s','2026-09-30T23:00:00.000000000Z','2026-09-30T23:30:00.000000000Z','stopping','legacy-stopping'),('owner-a','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','active','legacy-active')",
            "INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES('task','terminal',4,'owner-s','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z')",
            "INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES('delivery','task',1,'terminal')",
            "INSERT INTO sessions(session_id,endpoint_id,conversation_id,cwd,backend_id,protocol_version,state,created_at,updated_at) VALUES('closed-session','agent-b','conversation','/tmp','agent-b@1',1,'closed','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z')",
            "INSERT INTO execution_leases VALUES('resource','task','owner-s',7,'released','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','2026-10-01T00:00:03.000000000Z')",
            "INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes,runtime_generation,runtime_generation_version) VALUES('task','resource','delivery',7,2,'blocked',1,1,0,'','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','2026-10-01T00:00:03.000000000Z',12,'generation.v1:strict',1)",
        ] {
            pool.execute(sql).await.unwrap();
        }
        let response_hash = uuid::Uuid::new_v5(
            &uuid::Uuid::from_u128(0x7297ec04_1338_57b4_9bc4_f0ff9f84fa5a),
            b"{}",
        )
        .to_string();
        sqlx::query("INSERT INTO continuation_response_receipts VALUES('receipt','task','delivery','resource',7,1,?,'structured',2,'generation.v1:strict','2026-10-01T00:00:03.000000000Z')")
            .bind(response_hash).execute(&pool).await.unwrap();
        pool.execute("DROP TABLE isolated_owner_binding_remediation_receipts")
            .await
            .unwrap();
        pool.execute("DROP TABLE isolated_owner_binding_remediation_witness")
            .await
            .unwrap();
        pool.execute("DELETE FROM _sqlx_migrations WHERE version=14")
            .await
            .unwrap();
        pool.close().await;
        std::fs::copy(&database, &retained).unwrap();
        let args = Args {
            snapshot: "01".repeat(32),
            source_set: "02".repeat(32),
            observed_live_source_set: EXPECTED_LIVE_SOURCE_SET_SHA256.to_owned(),
        };
        (root, database, retained, args)
    }

    #[test]
    fn argv_and_identity_are_fixed() {
        let hash = "01".repeat(32);
        let valid = vec![
            COMMAND.into(),
            "--expected-backup-sha256".into(),
            hash.clone().into(),
            "--expected-source-set-sha256".into(),
            hash.clone().into(),
            "--expected-live-source-set-sha256".into(),
            EXPECTED_LIVE_SOURCE_SET_SHA256.into(),
        ];
        let args = parse_args(&valid).unwrap();
        assert_eq!(operation_id(&args), operation_id(&args));
        for bad in [
            vec![],
            vec![COMMAND.into()],
            vec![
                COMMAND.into(),
                "--wrong".into(),
                hash.clone().into(),
                "--expected-live-source-set-sha256".into(),
                EXPECTED_LIVE_SOURCE_SET_SHA256.into(),
                "--expected-source-set-sha256".into(),
                hash.clone().into(),
            ],
        ] {
            assert_eq!(parse_args(&bad), Err(Failure::new("arguments", "none")));
        }
    }

    #[tokio::test]
    async fn migrated_prestate_repairs_atomically_and_exact_repeat_is_read_only() {
        let (_root, database, retained, args) = fixture().await;
        assert_eq!(
            remediate_paths(&database, &retained, &args, false)
                .await
                .unwrap(),
            Outcome::Remediated
        );
        let mut c = open(&database).await;
        let lease: String = sqlx::query_scalar("SELECT owner_token FROM execution_leases")
            .fetch_one(&mut c)
            .await
            .unwrap();
        let admission: (String, i64) =
            sqlx::query_as("SELECT runtime_instance,revision FROM task_admissions")
                .fetch_one(&mut c)
                .await
                .unwrap();
        let counts: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM isolated_owner_binding_remediation_witness),(SELECT count(*) FROM isolated_owner_binding_remediation_receipts)")
            .fetch_one(&mut c).await.unwrap();
        assert_eq!(lease, "owner-a");
        assert_eq!(admission, ("owner-a".to_owned(), 5));
        assert_eq!(counts, (1, 1));
        c.close().await.unwrap();
        let before = std::fs::metadata(&database).unwrap().len();
        assert_eq!(
            remediate_paths(&database, &retained, &args, false)
                .await
                .unwrap(),
            Outcome::AlreadyRemediated
        );
        assert_eq!(std::fs::metadata(&database).unwrap().len(), before);
    }

    #[tokio::test]
    async fn arbitrary_post_migration_business_drift_refuses_before_witness() {
        let (_root, database, retained, args) = fixture().await;
        let pool = connect(&database).await.unwrap();
        migrate(&pool).await.unwrap();
        pool.close().await;
        let mut c = open(&database).await;
        sqlx::query("UPDATE execution_leases SET fence=8")
            .execute(&mut c)
            .await
            .unwrap();
        c.close().await.unwrap();
        assert_eq!(
            remediate_paths(&database, &retained, &args, false).await,
            Err(Failure::new("migration", "compatibility"))
        );
        let mut c = open(&database).await;
        let counts:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM isolated_owner_binding_remediation_witness),(SELECT count(*) FROM isolated_owner_binding_remediation_receipts)").fetch_one(&mut c).await.unwrap();
        assert_eq!(counts, (0, 0));
    }

    #[tokio::test]
    async fn witness_and_receipt_drift_fail_closed() {
        let (_root, database, retained, args) = fixture().await;
        remediate_paths(&database, &retained, &args, false)
            .await
            .unwrap();
        let mut c = open(&database).await;
        sqlx::query("UPDATE isolated_owner_binding_remediation_receipts SET receipt_digest=?")
            .bind("ff".repeat(32))
            .execute(&mut c)
            .await
            .unwrap();
        c.close().await.unwrap();
        assert_eq!(
            remediate_paths(&database, &retained, &args, false).await,
            Err(Failure::new("receipt", "mismatch"))
        );
    }

    #[tokio::test]
    async fn witness_drift_and_commit_result_loss_have_fixed_classification() {
        let (_root, database, retained, args) = fixture().await;
        assert_eq!(
            remediate_paths_with_commit_result(&database, &retained, &args, false, true)
                .await
                .unwrap(),
            Outcome::Adopted
        );
        let mut c = open(&database).await;
        sqlx::query("UPDATE isolated_owner_binding_remediation_witness SET witness_digest=?")
            .bind("ff".repeat(32))
            .execute(&mut c)
            .await
            .unwrap();
        c.close().await.unwrap();
        assert_eq!(
            remediate_paths(&database, &retained, &args, false).await,
            Err(Failure::new("migration", "compatibility"))
        );
    }

    #[tokio::test]
    async fn legacy_first_entry_applies_0014_then_witness_and_repair() {
        let (_root, database, retained, args) = fixture().await;
        assert_eq!(
            remediate_paths(&database, &retained, &args, true)
                .await
                .unwrap(),
            Outcome::Remediated
        );
        let mut c = open(&database).await;
        let state: (i64, i64, String, String) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM isolated_owner_binding_remediation_witness),(SELECT count(*) FROM isolated_owner_binding_remediation_receipts),(SELECT owner_token FROM execution_leases),(SELECT runtime_instance FROM task_admissions)",
        )
        .fetch_one(&mut c)
        .await
        .unwrap();
        assert_eq!(state, (1, 1, "owner-a".to_owned(), "owner-a".to_owned()));
    }

    #[tokio::test]
    async fn second_cas_failure_rolls_back_first_cas_and_receipt() {
        let (_root, database, retained, args) = fixture().await;
        let mut c = open(&database).await;
        sqlx::query("CREATE TRIGGER t050_reject_admission BEFORE UPDATE ON task_admissions BEGIN SELECT RAISE(ABORT, 'synthetic'); END")
            .execute(&mut c).await.unwrap();
        c.close().await.unwrap();
        assert_eq!(
            remediate_paths(&database, &retained, &args, false).await,
            Err(Failure::new("cas", "admission"))
        );
        let mut c = open(&database).await;
        let state: (String, String, i64) = sqlx::query_as(
            "SELECT (SELECT owner_token FROM execution_leases),(SELECT runtime_instance FROM task_admissions),(SELECT count(*) FROM isolated_owner_binding_remediation_receipts)",
        )
        .fetch_one(&mut c)
        .await
        .unwrap();
        assert_eq!(state, ("owner-s".to_owned(), "owner-s".to_owned(), 0));
    }

    #[tokio::test]
    async fn every_mutation_boundary_is_reached_in_order() {
        let (_root, database, retained, args) = fixture().await;
        let mut seen = Vec::new();
        let mut boundary = |point| {
            seen.push(point);
            Ok(())
        };
        assert_eq!(
            remediate_paths_with_boundaries(
                &database,
                &retained,
                &args,
                false,
                false,
                &mut boundary,
            )
            .await,
            Ok(Outcome::Remediated)
        );
        assert_eq!(
            seen,
            [
                Boundary::BeforePublication,
                Boundary::AfterPublicationBegin,
                Boundary::AfterWitnessDdl,
                Boundary::AfterReceiptDdl,
                Boundary::AfterLedgerInsert,
                Boundary::AfterWitnessInsert,
                Boundary::BeforePublicationCommit,
                Boundary::AfterPublicationReread,
                Boundary::BeforeBusinessCommit,
                Boundary::AfterBusinessReread,
            ]
        );
    }

    #[tokio::test]
    async fn boundary_replacement_is_ambiguous_and_stops_before_business_mutation() {
        let (root, database, retained, args) = fixture().await;
        let replacement = root.path().join("replacement.db");
        std::fs::copy(&database, &replacement).unwrap();
        let expected = std::fs::metadata(&database).unwrap();
        let mut boundary = |point| {
            if point == Boundary::BeforePublicationCommit {
                std::fs::rename(&database, root.path().join("original.db")).unwrap();
                std::fs::rename(&replacement, &database).unwrap();
            }
            let current = std::fs::metadata(&database).unwrap();
            use std::os::unix::fs::MetadataExt as _;
            if current.dev() != expected.dev() || current.ino() != expected.ino() {
                return Err(Failure::new("postflight", "database-identity"));
            }
            Ok(())
        };
        assert_eq!(
            remediate_paths_with_boundaries(
                &database,
                &retained,
                &args,
                false,
                false,
                &mut boundary,
            )
            .await,
            Err(Failure::new("unknown-commit", "ambiguous"))
        );
        let mut c = open(&database).await;
        let latest: i64 = sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(latest, 13);
    }

    #[tokio::test]
    async fn aba_swap_restore_never_opens_or_mutates_foreign_inode() {
        use std::os::fd::AsRawFd as _;

        let (root, database, retained, args) = fixture().await;
        let original = root.path().join("original.db");
        let foreign = root.path().join("foreign.db");
        let foreign_after = root.path().join("foreign-after.db");
        std::fs::copy(&database, &foreign).unwrap();
        let held = std::fs::File::open(&database).unwrap();
        let held_path = std::path::PathBuf::from(format!("/proc/self/fd/{}", held.as_raw_fd()));
        let mut boundary = |point| {
            if point == Boundary::BeforePublication {
                std::fs::rename(&database, &original).unwrap();
                std::fs::rename(&foreign, &database).unwrap();
            } else if point == Boundary::AfterPublicationBegin {
                std::fs::rename(&database, &foreign_after).unwrap();
                std::fs::rename(&original, &database).unwrap();
                return Err(Failure::new("postflight", "database-identity"));
            }
            Ok(())
        };
        assert_eq!(
            remediate_paths_opening(
                &held_path,
                &retained,
                &args,
                false,
                false,
                &mut boundary,
                PublicationMode::Normal,
            )
            .await,
            Err(Failure::new("postflight", "database-identity"))
        );
        let mut foreign_c = open(&foreign_after).await;
        let latest: i64 = sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&mut foreign_c)
            .await
            .unwrap();
        let objects: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE name LIKE 'isolated_owner_binding_remediation_%'")
            .fetch_one(&mut foreign_c).await.unwrap();
        assert_eq!((latest, objects), (13, 0));
    }

    #[tokio::test]
    async fn owned_live_fd_survives_publication_business_rereads_and_fd_pressure() {
        use std::os::{fd::AsRawFd as _, unix::fs::MetadataExt as _};

        for rollback in [false, true] {
            let (_root, database, retained, args) = fixture().await;
            if rollback {
                let mut c = open(&database).await;
                sqlx::query("CREATE TRIGGER t050_reject_admission BEFORE UPDATE ON task_admissions BEGIN SELECT RAISE(ABORT, 'synthetic'); END")
                    .execute(&mut c).await.unwrap();
                c.close().await.unwrap();
            }
            let held = std::fs::File::open(&database).unwrap();
            let held_path = std::path::PathBuf::from(format!("/proc/self/fd/{}", held.as_raw_fd()));
            let expected = std::fs::metadata(&database).unwrap();
            let mut pressure = Vec::new();
            let mut publication_rereads = 0;
            let mut business_rereads = 0;
            let mut boundary = |point| {
                if point == Boundary::AfterPublicationReread {
                    publication_rereads += 1;
                    for _ in 0..256 {
                        pressure.push(std::fs::File::open("/dev/null").unwrap());
                    }
                }
                if point == Boundary::AfterBusinessReread {
                    business_rereads += 1;
                }
                let current = std::fs::metadata(&held_path)
                    .map_err(|_| Failure::new("postflight", "database-identity"))?;
                if current.dev() != expected.dev() || current.ino() != expected.ino() {
                    return Err(Failure::new("postflight", "database-identity"));
                }
                Ok(())
            };
            let result = remediate_paths_opening(
                &held_path,
                &retained,
                &args,
                false,
                false,
                &mut boundary,
                PublicationMode::Normal,
            )
            .await;
            assert_eq!(
                result,
                if rollback {
                    Err(Failure::new("cas", "admission"))
                } else {
                    Ok(Outcome::Remediated)
                }
            );
            assert_eq!((publication_rereads, business_rereads), (1, 1));
            assert_eq!(std::fs::metadata(&held_path).unwrap().ino(), expected.ino());
        }
    }

    #[tokio::test]
    async fn lost_database_name_after_witness_commit_is_ambiguous() {
        let (root, database, retained, args) = fixture().await;
        let moved = root.path().join("moved.db");
        let mut boundary = |point| {
            if point == Boundary::BeforePublicationCommit {
                std::fs::rename(&database, &moved).unwrap();
            }
            Ok(())
        };
        assert_eq!(
            remediate_paths_with_boundaries(
                &database,
                &retained,
                &args,
                false,
                false,
                &mut boundary,
            )
            .await,
            Err(Failure::new("unknown-commit", "ambiguous"))
        );
    }

    #[tokio::test]
    async fn lost_database_name_after_business_commit_is_ambiguous() {
        let (root, database, retained, args) = fixture().await;
        let moved = root.path().join("moved.db");
        let mut boundary = |point| {
            if point == Boundary::BeforeBusinessCommit {
                std::fs::rename(&database, &moved).unwrap();
            }
            Ok(())
        };
        assert_eq!(
            remediate_paths_with_boundaries(
                &database,
                &retained,
                &args,
                false,
                false,
                &mut boundary,
            )
            .await,
            Err(Failure::new("unknown-commit", "ambiguous"))
        );
    }

    #[tokio::test]
    async fn exact_schema_and_ledger_drift_are_rejected() {
        for mutation in [0, 1, 2, 3, 4] {
            let (_root, database, retained, args) = fixture().await;
            let pool = connect(&database).await.unwrap();
            migrate(&pool).await.unwrap();
            pool.close().await;
            let mut c = open(&database).await;
            match mutation {
                0 => {
                    sqlx::query("DROP TABLE isolated_owner_binding_remediation_receipts")
                        .execute(&mut c)
                        .await
                        .unwrap();
                    sqlx::query("CREATE TABLE isolated_owner_binding_remediation_receipts(operation_id TEXT PRIMARY KEY NOT NULL) STRICT")
                        .execute(&mut c).await.unwrap();
                }
                1 => {
                    sqlx::query("DELETE FROM _sqlx_migrations WHERE version=7")
                        .execute(&mut c)
                        .await
                        .unwrap();
                }
                2 => {
                    sqlx::query("INSERT INTO _sqlx_migrations(version,description,installed_on,success,checksum,execution_time) SELECT 15,'hostile',installed_on,1,checksum,0 FROM _sqlx_migrations WHERE version=14").execute(&mut c).await.unwrap();
                }
                3 => {
                    sqlx::query(
                        "UPDATE _sqlx_migrations SET description='hostile' WHERE version=14",
                    )
                    .execute(&mut c)
                    .await
                    .unwrap();
                }
                _ => {
                    sqlx::query("UPDATE _sqlx_migrations SET checksum=? WHERE version=14")
                        .bind(vec![0_u8; 48])
                        .execute(&mut c)
                        .await
                        .unwrap();
                }
            }
            c.close().await.unwrap();
            assert_eq!(
                remediate_paths(&database, &retained, &args, false).await,
                Err(Failure::new("migration", "compatibility"))
            );
        }
    }

    #[tokio::test]
    async fn repeat_authenticates_receipt_prestate_and_actual_post_images() {
        for mutation in [0, 1] {
            let (_root, database, retained, args) = fixture().await;
            remediate_paths(&database, &retained, &args, false)
                .await
                .unwrap();
            let mut c = open(&database).await;
            if mutation == 0 {
                let row = sqlx::query("SELECT operation_id,backup_snapshot_sha256,backup_source_set_sha256,pre_state_digest,post_state_digest,active_owner_digest,stopping_owner_digest,continuation_digest,lease_before_digest,lease_after_digest,admission_before_digest,admission_after_digest,sibling_receipt_digest,commit_timestamp FROM isolated_owner_binding_remediation_receipts")
                    .fetch_one(&mut c).await.unwrap();
                let mut receipt = Receipt {
                    operation_id: get(&row, 0).unwrap(),
                    backup_snapshot: get(&row, 1).unwrap(),
                    backup_source: get(&row, 2).unwrap(),
                    pre: get(&row, 3).unwrap(),
                    post: get(&row, 4).unwrap(),
                    active: get(&row, 5).unwrap(),
                    stopping: get(&row, 6).unwrap(),
                    continuation: get(&row, 7).unwrap(),
                    lease_before: get(&row, 8).unwrap(),
                    lease_after: get(&row, 9).unwrap(),
                    admission_before: get(&row, 10).unwrap(),
                    admission_after: get(&row, 11).unwrap(),
                    siblings: get(&row, 12).unwrap(),
                    timestamp: get(&row, 13).unwrap(),
                };
                receipt.pre = "aa".repeat(32);
                let forged = receipt_digest(&receipt);
                sqlx::query("UPDATE isolated_owner_binding_remediation_receipts SET pre_state_digest=?,receipt_digest=?")
                .bind(&receipt.pre)
                .bind(forged)
                .execute(&mut c)
                .await
                .unwrap();
            } else {
                sqlx::query(
                    "UPDATE execution_leases SET heartbeat_at='2026-10-01T00:00:04.000000000Z'",
                )
                .execute(&mut c)
                .await
                .unwrap();
            }
            c.close().await.unwrap();
            assert_eq!(
                remediate_paths(&database, &retained, &args, false).await,
                Err(Failure::new("receipt", "mismatch"))
            );
        }
    }

    #[tokio::test]
    async fn successful_rollback_uses_exactly_one_fresh_reread() {
        let (_root, database, retained, args) = fixture().await;
        let mut c = open(&database).await;
        sqlx::query("CREATE TRIGGER t050_reject_admission BEFORE UPDATE ON task_admissions BEGIN SELECT RAISE(ABORT, 'synthetic'); END")
            .execute(&mut c).await.unwrap();
        c.close().await.unwrap();
        let mut rereads = 0;
        let mut boundary = |point| {
            if point == Boundary::AfterBusinessReread {
                rereads += 1;
            }
            Ok(())
        };
        assert_eq!(
            remediate_paths_with_boundaries(
                &database,
                &retained,
                &args,
                false,
                false,
                &mut boundary,
            )
            .await,
            Err(Failure::new("cas", "admission"))
        );
        assert_eq!(rereads, 1);
    }

    #[tokio::test]
    async fn rollback_failure_and_reread_failure_are_ambiguous() {
        for rollback_aborts_transaction in [false, true] {
            let (_root, database, retained, args) = fixture().await;
            let mut c = open(&database).await;
            let trigger = if rollback_aborts_transaction {
                "CREATE TRIGGER t050_reject_admission BEFORE UPDATE ON task_admissions BEGIN SELECT RAISE(ROLLBACK, 'synthetic'); END"
            } else {
                "CREATE TRIGGER t050_reject_admission BEFORE UPDATE ON task_admissions BEGIN SELECT RAISE(ABORT, 'synthetic'); END"
            };
            sqlx::query(trigger).execute(&mut c).await.unwrap();
            c.close().await.unwrap();
            let mut boundary = |point| {
                if !rollback_aborts_transaction && point == Boundary::AfterBusinessReread {
                    return Err(Failure::new("postflight", "database-identity"));
                }
                Ok(())
            };
            assert_eq!(
                remediate_paths_with_boundaries(
                    &database,
                    &retained,
                    &args,
                    false,
                    false,
                    &mut boundary,
                )
                .await,
                Err(Failure::new("unknown-commit", "ambiguous"))
            );
        }
    }

    #[test]
    fn publication_commit_classifier_covers_all_nine_cells() {
        for observation in [
            CommitObservation::Success,
            CommitObservation::Error,
            CommitObservation::Unavailable,
        ] {
            assert_eq!(
                classify_publication(observation, PublicationState::Complete),
                Ok(())
            );
            assert_eq!(
                classify_publication(observation, PublicationState::Other),
                Err(Failure::new("unknown-commit", "ambiguous"))
            );
        }
        assert_eq!(
            classify_publication(CommitObservation::Success, PublicationState::LegacyAbsence),
            Err(Failure::new("unknown-commit", "ambiguous"))
        );
        for observation in [CommitObservation::Error, CommitObservation::Unavailable] {
            assert_eq!(
                classify_publication(observation, PublicationState::LegacyAbsence),
                Err(Failure::new("unknown-commit", "not-committed"))
            );
        }
    }

    #[tokio::test]
    async fn publication_nine_cells_use_one_fresh_sqlite_reread_with_counts() {
        for expected_state in [
            PublicationState::LegacyAbsence,
            PublicationState::Complete,
            PublicationState::Other,
        ] {
            for observation in [
                CommitObservation::Success,
                CommitObservation::Error,
                CommitObservation::Unavailable,
            ] {
                let (_root, database, retained, args) = fixture().await;
                let retained_before = std::fs::read(&retained).unwrap();
                let mut publication_attempts = 0;
                let mut commit_attempts = 0;
                let mut fresh_opens = 0;
                let mut boundary = |point| {
                    if point == Boundary::AfterPublicationBegin {
                        publication_attempts += 1;
                    }
                    if point == Boundary::BeforePublicationCommit {
                        commit_attempts += 1;
                    }
                    if point == Boundary::AfterPublicationReread {
                        fresh_opens += 1;
                    }
                    Ok(())
                };
                let result = remediate_paths_opening(
                    &database,
                    &retained,
                    &args,
                    false,
                    false,
                    &mut boundary,
                    PublicationMode::Inject {
                        observation,
                        state: expected_state,
                    },
                )
                .await;
                assert_eq!(
                    (publication_attempts, commit_attempts, fresh_opens),
                    (1, 1, 1)
                );
                assert_eq!(publication_attempts - 1, 0);
                assert_eq!(
                    result,
                    match (observation, expected_state) {
                        (_, PublicationState::Complete) => Ok(Outcome::Remediated),
                        (
                            CommitObservation::Error | CommitObservation::Unavailable,
                            PublicationState::LegacyAbsence,
                        ) => Err(Failure::new("unknown-commit", "not-committed")),
                        _ => Err(Failure::new("unknown-commit", "ambiguous")),
                    }
                );
                let mut reread = open(&database).await;
                let base_counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM sqlite_schema WHERE name IN ('isolated_owner_binding_remediation_witness','isolated_owner_binding_remediation_receipts')),(SELECT count(*) FROM execution_leases WHERE owner_token='owner-a'),(SELECT count(*) FROM task_admissions WHERE runtime_instance='owner-a')")
                    .fetch_one(&mut reread).await.unwrap();
                let receipt_counts = if expected_state == PublicationState::Complete {
                    sqlx::query_as("SELECT (SELECT count(*) FROM isolated_owner_binding_remediation_witness),(SELECT count(*) FROM isolated_owner_binding_remediation_receipts)")
                        .fetch_one(&mut reread).await.unwrap()
                } else {
                    (0_i64, 0_i64)
                };
                let complete = i64::from(expected_state == PublicationState::Complete);
                assert_eq!(
                    base_counts.0,
                    match expected_state {
                        PublicationState::LegacyAbsence => 0,
                        PublicationState::Other => 1,
                        PublicationState::Complete => 2,
                    }
                );
                assert_eq!(
                    (
                        receipt_counts.0,
                        receipt_counts.1,
                        base_counts.1,
                        base_counts.2
                    ),
                    (complete, complete, complete, complete)
                );
                assert_eq!(std::fs::read(&retained).unwrap(), retained_before);
                assert!(
                    !std::path::PathBuf::from(format!("{}-journal", database.display())).exists()
                );
            }
        }
    }

    #[tokio::test]
    async fn precommit_faults_rollback_every_partial_publication() {
        for fault in [
            Boundary::AfterPublicationBegin,
            Boundary::AfterWitnessDdl,
            Boundary::AfterReceiptDdl,
            Boundary::AfterLedgerInsert,
            Boundary::AfterWitnessInsert,
            Boundary::BeforePublicationCommit,
        ] {
            let (_root, database, retained, args) = fixture().await;
            let mut fresh_opens = 0;
            let mut boundary = |point| {
                if point == Boundary::AfterPublicationReread {
                    fresh_opens += 1;
                }
                if point == fault {
                    return Err(Failure::new("postflight", "database-identity"));
                }
                Ok(())
            };
            assert_eq!(
                remediate_paths_with_boundaries(
                    &database,
                    &retained,
                    &args,
                    false,
                    false,
                    &mut boundary,
                )
                .await,
                Err(Failure::new("postflight", "database-identity"))
            );
            assert_eq!(fresh_opens, 1);
            let mut c = open(&database).await;
            let latest: i64 = sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations")
                .fetch_one(&mut c)
                .await
                .unwrap();
            let objects: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE name LIKE 'isolated_owner_binding_remediation_%'")
                .fetch_one(&mut c).await.unwrap();
            let owners: (String, String) = sqlx::query_as("SELECT (SELECT owner_token FROM execution_leases),(SELECT runtime_instance FROM task_admissions)")
                .fetch_one(&mut c).await.unwrap();
            assert_eq!((latest, objects), (13, 0));
            assert_eq!(owners, ("owner-s".to_owned(), "owner-s".to_owned()));
        }
    }

    #[tokio::test]
    async fn hostile_durable_publication_shapes_never_enter_business_mutation() {
        for hostile in 0..5 {
            let (_root, database, retained, args) = fixture().await;
            let pool = connect(&database).await.unwrap();
            migrate(&pool).await.unwrap();
            match hostile {
                0 => {}
                1 => {
                    pool.execute("DROP TABLE isolated_owner_binding_remediation_receipts")
                        .await
                        .unwrap();
                }
                2 => {
                    pool.execute("DELETE FROM _sqlx_migrations WHERE version=14")
                        .await
                        .unwrap();
                }
                3 => {
                    pool.execute(
                        "UPDATE _sqlx_migrations SET description='hostile' WHERE version=14",
                    )
                    .await
                    .unwrap();
                }
                _ => {
                    pool.execute("CREATE TABLE isolated_owner_binding_remediation_extra(value INTEGER) STRICT").await.unwrap();
                }
            };
            pool.close().await;
            assert_eq!(
                remediate_paths(&database, &retained, &args, false).await,
                Err(Failure::new("migration", "compatibility"))
            );
            let mut c = open(&database).await;
            let owners: (String, String) = sqlx::query_as("SELECT (SELECT owner_token FROM execution_leases),(SELECT runtime_instance FROM task_admissions)")
                .fetch_one(&mut c).await.unwrap();
            assert_eq!(owners, ("owner-s".to_owned(), "owner-s".to_owned()));
        }
    }

    #[tokio::test]
    async fn atomic_publication_has_v2_witness_exact_ledger_and_is_migrator_noop() {
        let (_root, database, retained, args) = fixture().await;
        remediate_paths(&database, &retained, &args, false)
            .await
            .unwrap();
        let mut c = open(&database).await;
        let ledger: (String, bool, i64, String) = sqlx::query_as(
            "SELECT description,success,execution_time,installed_on FROM _sqlx_migrations WHERE version=14",
        )
        .fetch_one(&mut c)
        .await
        .unwrap();
        assert_eq!(ledger.0, "isolated owner binding remediation");
        assert!(ledger.1);
        assert_eq!(ledger.2, -1);
        assert_eq!(ledger.3.len(), 19);
        let witness: (i64, String) = sqlx::query_as(
            "SELECT contract_version,observed_live_source_set_sha256 FROM isolated_owner_binding_remediation_witness",
        )
        .fetch_one(&mut c)
        .await
        .unwrap();
        assert_eq!(witness, (2, EXPECTED_LIVE_SOURCE_SET_SHA256.to_owned()));
        c.close().await.unwrap();

        let pool = connect(&database).await.unwrap();
        migrate(&pool).await.unwrap();
        let execution_time: i64 =
            sqlx::query_scalar("SELECT execution_time FROM _sqlx_migrations WHERE version=14")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(execution_time, -1);
    }

    #[tokio::test]
    async fn forged_observed_authority_and_recomputed_witness_are_rejected() {
        let (_root, database, retained, args) = fixture().await;
        remediate_paths(&database, &retained, &args, false)
            .await
            .unwrap();
        let hostile = Args {
            snapshot: args.snapshot.clone(),
            source_set: args.source_set.clone(),
            observed_live_source_set: "03".repeat(32),
        };
        let hostile_operation = operation_id(&hostile);
        let mut c = open(&database).await;
        let retained_business = {
            let mut retained_c = open(&retained).await;
            business_digest(&mut retained_c).await.unwrap()
        };
        sqlx::query("UPDATE isolated_owner_binding_remediation_witness SET operation_id=?,observed_live_source_set_sha256=?,witness_digest=?")
            .bind(&hostile_operation)
            .bind(&hostile.observed_live_source_set)
            .bind(witness_digest(&hostile_operation, &hostile, &retained_business))
            .execute(&mut c)
            .await
            .unwrap();
        c.close().await.unwrap();
        assert_eq!(
            remediate_paths(&database, &retained, &args, false).await,
            Err(Failure::new("migration", "compatibility"))
        );
    }
}
