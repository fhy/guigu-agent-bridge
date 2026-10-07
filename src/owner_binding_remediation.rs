use std::{ffi::OsString, path::Path};

use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

const COMMAND: &str = "remediate-isolated-owner-bindings";
const CONTRACT_VERSION: i64 = 1;
const MIGRATION_VERSION: i64 = 14;
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
}

pub fn parse_args(args: &[OsString]) -> Result<Args, Failure> {
    if args.len() != 5
        || args[0] != COMMAND
        || args[1] != "--expected-backup-sha256"
        || args[3] != "--expected-source-set-sha256"
    {
        return Err(Failure::new("arguments", "none"));
    }
    let snapshot = exact_digest(&args[2])?;
    let source_set = exact_digest(&args[4])?;
    Ok(Args {
        snapshot,
        source_set,
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
    let framed = format!(
        "GGT050-OWNER-BINDING-REMEDIATION-V1\0{}\0{}",
        args.snapshot, args.source_set
    );
    uuid::Uuid::new_v5(&OPERATION_NAMESPACE, framed.as_bytes()).to_string()
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
    BeforeMigration,
    AfterMigration,
    BeforeWitnessCommit,
    AfterWitnessReread,
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
        b"GGT050-OWNER-BINDING-WITNESS-V1",
        &[
            "1",
            operation,
            &args.snapshot,
            &args.source_set,
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

async fn authenticate_0014_schema(c: &mut SqliteConnection) -> Result<(), Failure> {
    let rows: Vec<(i64, bool)> =
        sqlx::query_as("SELECT version,success FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&mut *c)
            .await
            .map_err(|_| Failure::new("migration", "ledger"))?;
    if rows.len() != MIGRATION_VERSION as usize
        || rows
            .iter()
            .enumerate()
            .any(|(index, (version, success))| *version != index as i64 + 1 || !*success)
    {
        return Err(Failure::new("migration", "compatibility"));
    }
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
    Ok(())
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
    let rows=sqlx::query("SELECT contract_version,operation_id,backup_snapshot_sha256,backup_source_set_sha256,migration_version,migration_checksum_digest,retained_pre0014_business_digest,migrated_pre_repair_business_digest,witness_digest FROM isolated_owner_binding_remediation_witness").fetch_all(&mut *c).await.map_err(|_|Failure::new("migration","compatibility"))?;
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
        get::<i64>(r, 4)?,
        get::<String>(r, 5)?,
        get::<String>(r, 6)?,
        get::<String>(r, 7)?,
        get::<String>(r, 8)?,
    );
    if values
        != (
            CONTRACT_VERSION,
            operation.to_owned(),
            args.snapshot.clone(),
            args.source_set.clone(),
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
    let retained_facts = load_facts(&mut retained_c)
        .await
        .map_err(|_| Failure::new("backup", "source-binding"))?;
    retained_c
        .close()
        .await
        .map_err(|_| Failure::new("backup", "snapshot"))?;
    boundary(Boundary::BeforeMigration)?;
    let pool = crate::storage::connect(database)
        .await
        .map_err(|_| Failure::new("database", "connect"))?;
    let (applied, latest, total): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM _sqlx_migrations WHERE version=14 AND success=1),coalesce((SELECT max(version) FROM _sqlx_migrations),0),(SELECT count(*) FROM _sqlx_migrations WHERE success=1)",
    )
            .fetch_one(&pool)
            .await
            .map_err(|_| Failure::new("migration", "ledger"))?;
    if latest > MIGRATION_VERSION || (latest == 13 && total != 13) || (latest == 14 && total != 14)
    {
        return Err(Failure::new("migration", "compatibility"));
    }
    if applied == 0 {
        let mut live = pool
            .acquire()
            .await
            .map_err(|_| Failure::new("database", "connect"))?;
        let live_business = business_digest(&mut live).await?;
        if !legacy_source_matches || live_business != retained_business {
            return Err(Failure::new("backup", "source-binding"));
        }
        drop(live);
    } else if applied != 1 {
        return Err(Failure::new("migration", "compatibility"));
    }
    crate::storage::migrate(&pool)
        .await
        .map_err(|_| Failure::new("migration", "apply"))?;
    pool.close().await;
    boundary(Boundary::AfterMigration)?;
    let mut c = open_live(database).await?;
    authenticate_0014_schema(&mut c).await?;
    let live_business = business_digest(&mut c).await?;
    if !exact_witness(&mut c, &operation, args, &retained_business).await? {
        let receipts: i64 =
            sqlx::query_scalar("SELECT count(*) FROM isolated_owner_binding_remediation_receipts")
                .fetch_one(&mut c)
                .await
                .map_err(|_| Failure::new("database", "read"))?;
        if receipts != 0 || live_business != retained_business {
            return Err(Failure::new("migration", "compatibility"));
        }
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut c)
            .await
            .map_err(|_| Failure::new("database", "busy"))?;
        let inserted = sqlx::query(
            "INSERT INTO isolated_owner_binding_remediation_witness VALUES(1,1,?,?,?,14,?,?,?,?)",
        )
        .bind(&operation)
        .bind(&args.snapshot)
        .bind(&args.source_set)
        .bind(migration_checksum())
        .bind(&retained_business)
        .bind(&retained_business)
        .bind(witness_digest(&operation, args, &retained_business))
        .execute(&mut c)
        .await;
        if !matches!(inserted, Ok(ref done) if done.rows_affected() == 1) {
            if sqlx::query("ROLLBACK").execute(&mut c).await.is_err() {
                return Err(Failure::new("unknown-commit", "ambiguous"));
            }
            return Err(Failure::new("migration", "apply"));
        }
        boundary(Boundary::BeforeWitnessCommit)?;
        let commit = sqlx::query("COMMIT").execute(&mut c).await;
        drop(c);
        let mut reread = open_live(database)
            .await
            .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
        authenticate_0014_schema(&mut reread)
            .await
            .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
        boundary(Boundary::AfterWitnessReread)
            .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
        let reread_business = business_digest(&mut reread)
            .await
            .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
        let witness = exact_witness(&mut reread, &operation, args, &retained_business)
            .await
            .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
        if !witness || reread_business != retained_business {
            return Err(Failure::new(
                "unknown-commit",
                if commit.is_err() && !witness {
                    "not-committed"
                } else {
                    "ambiguous"
                },
            ));
        }
        c = reread;
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
        if sqlx::query("ROLLBACK").execute(&mut c).await.is_err() {
            return Err(Failure::new("unknown-commit", "ambiguous"));
        }
        return Err(error);
    }
    boundary(Boundary::BeforeBusinessCommit)?;
    let commit = sqlx::query("COMMIT").execute(&mut c).await;
    drop(c);
    let mut reread = open_live(database)
        .await
        .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    authenticate_0014_schema(&mut reread)
        .await
        .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    boundary(Boundary::AfterBusinessReread)
        .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    let post = business_digest(&mut reread)
        .await
        .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
    let receipt = receipt_repeat(
        &mut reread,
        &operation,
        args,
        &retained_business,
        &post,
        &retained_facts,
    )
    .await
    .map_err(|_| Failure::new("unknown-commit", "ambiguous"))?;
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
    let live_source_matches = anchor.source_set_hex() == args.source_set;
    let retained_fd = std::path::PathBuf::from(format!(
        "/proc/self/fd/{}",
        anchor.snapshot_fd().as_raw_fd()
    ));
    let mut boundary = |point| {
        if point == Boundary::AfterMigration {
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
    remediate_paths_with_boundaries(
        database,
        &retained_fd,
        args,
        live_source_matches,
        false,
        &mut boundary,
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
        pool.close().await;
        std::fs::copy(&database, &retained).unwrap();
        let args = Args {
            snapshot: "01".repeat(32),
            source_set: "02".repeat(32),
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
        for path in [&database, &retained] {
            let mut c = open(path).await;
            sqlx::query("DROP TABLE isolated_owner_binding_remediation_receipts")
                .execute(&mut c)
                .await
                .unwrap();
            sqlx::query("DROP TABLE isolated_owner_binding_remediation_witness")
                .execute(&mut c)
                .await
                .unwrap();
            sqlx::query("DELETE FROM _sqlx_migrations WHERE version=14")
                .execute(&mut c)
                .await
                .unwrap();
            c.close().await.unwrap();
        }
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
                Boundary::BeforeMigration,
                Boundary::AfterMigration,
                Boundary::BeforeWitnessCommit,
                Boundary::AfterWitnessReread,
                Boundary::BeforeBusinessCommit,
                Boundary::AfterBusinessReread,
            ]
        );
    }

    #[tokio::test]
    async fn boundary_replacement_stops_before_witness_or_business_mutation() {
        let (root, database, retained, args) = fixture().await;
        let replacement = root.path().join("replacement.db");
        std::fs::copy(&database, &replacement).unwrap();
        let expected = std::fs::metadata(&database).unwrap();
        let mut boundary = |point| {
            if point == Boundary::AfterMigration {
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
            Err(Failure::new("postflight", "database-identity"))
        );
        let mut c = open(&database).await;
        let counts: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM isolated_owner_binding_remediation_witness),(SELECT count(*) FROM isolated_owner_binding_remediation_receipts)")
            .fetch_one(&mut c).await.unwrap();
        assert_eq!(counts, (0, 0));
    }

    #[tokio::test]
    async fn lost_database_name_after_witness_commit_is_ambiguous() {
        let (root, database, retained, args) = fixture().await;
        let moved = root.path().join("moved.db");
        let mut boundary = |point| {
            if point == Boundary::BeforeWitnessCommit {
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
        for mutation in [0, 1, 2] {
            let (_root, database, retained, args) = fixture().await;
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
                _ => {
                    sqlx::query("INSERT INTO _sqlx_migrations(version,description,installed_on,success,checksum,execution_time) SELECT 15,'hostile',installed_on,1,checksum,0 FROM _sqlx_migrations WHERE version=14").execute(&mut c).await.unwrap();
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
}
