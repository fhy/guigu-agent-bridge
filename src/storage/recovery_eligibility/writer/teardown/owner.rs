use sqlx::{SqliteConnection, SqlitePool};

use super::{RecoveryScope, RowIdentity, begin, complete, inserted, prepare, refused};
use crate::storage::{ReliabilityError, ReliabilityStore, RuntimeOwnerEvidence};

pub(super) async fn require_current_process(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    token: &str,
) -> Result<(), sqlx::Error> {
    let observation = RuntimeOwnerEvidence::capture().ok_or_else(refused)?;
    let fingerprint = RuntimeOwnerEvidence::fingerprint().ok_or_else(refused)?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_instances r JOIN runtime_owner_evidence e USING(instance_token) WHERE r.instance_token=? AND r.state IN ('active','stopping') AND r.process_fingerprint=? AND e.is_current=1 AND e.state IN ('live','scan_pending') AND e.process_fingerprint=r.process_fingerprint AND e.pid=? AND e.proc_starttime=? AND e.boot_id=?")
        .bind(token).bind(fingerprint).bind(observation.pid).bind(observation.proc_starttime).bind(observation.boot_id).fetch_one(&mut *connection).await?;
    if count != 1 {
        return Err(refused());
    }
    let identity = super::super::text_identity("runtime_instances", token)?;
    prepare(connection, scope, &identity).await?;
    let revision = sqlx::query_scalar("SELECT evidence_revision FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1")
        .bind(token).fetch_one(&mut *connection).await?;
    let identity = evidence_identity(connection, token, revision).await?;
    prepare(connection, scope, &identity).await?;
    Ok(())
}

async fn evidence_identity(
    connection: &mut SqliteConnection,
    token: &str,
    revision: i64,
) -> Result<RowIdentity, sqlx::Error> {
    let key = sqlx::query_scalar("SELECT json_array('text:'||hex(CAST(instance_token AS BLOB)),'integer:'||hex(CAST(runtime_generation AS BLOB)),'integer:'||hex(CAST(evidence_revision AS BLOB))) FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
        .bind(token).bind(revision).fetch_one(connection).await?;
    Ok(RowIdentity {
        table: "runtime_owner_evidence".into(),
        key,
    })
}

pub(crate) async fn stopping_runtime(
    pool: &SqlitePool,
    scope: Option<&RecoveryScope>,
    token: &str,
    now: &str,
) -> Result<bool, sqlx::Error> {
    let scope = scope.ok_or_else(refused)?;
    let mut transaction = begin(pool, scope).await?;
    if scope
        .matrix_fence
        .get()
        .is_none_or(|fence| fence.owner != token)
    {
        return Err(refused());
    }
    require_current_process(&mut transaction, scope, token).await?;
    let identity = super::super::text_identity("runtime_instances", token)?;
    let runtime = prepare(&mut transaction, scope, &identity).await?;
    let revision: i64 = sqlx::query_scalar("SELECT evidence_revision FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1 AND state='live'")
        .bind(token).fetch_one(&mut *transaction).await?;
    let next = revision.checked_add(1).ok_or_else(refused)?;
    let changed = sqlx::query("UPDATE runtime_instances SET state='stopping',heartbeat_at=? WHERE instance_token=? AND state='active'")
        .bind(now).bind(token).execute(&mut *transaction).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    complete(&mut transaction, scope, runtime).await?;
    let identity = evidence_identity(&mut transaction, token, revision).await?;
    let evidence = prepare(&mut transaction, scope, &identity).await?;
    let changed = sqlx::query("UPDATE runtime_owner_evidence SET is_current=0 WHERE instance_token=? AND evidence_revision=? AND is_current=1 AND state='live'")
        .bind(token).bind(revision).execute(&mut *transaction).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    complete(&mut transaction, scope, evidence).await?;
    sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) SELECT instance_token,runtime_generation,?,'scan_pending',pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,1,max_age_seconds FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
        .bind(next).bind(token).bind(revision).execute(&mut *transaction).await?;
    let identity = evidence_identity(&mut transaction, token, next).await?;
    inserted(&mut transaction, scope, &identity).await?;
    transaction.commit().await?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn finish_controlled_stop(
    pool: &SqlitePool,
    scope: Option<&RecoveryScope>,
    store: &ReliabilityStore,
    token: &str,
    now: &str,
    observation: &RuntimeOwnerEvidence,
    joined: &crate::acp::JoinedChildren,
) -> Result<bool, ReliabilityError> {
    let scope = scope.ok_or_else(refused)?;
    let mut transaction = begin(pool, scope).await?;
    if scope
        .matrix_fence
        .get()
        .is_none_or(|fence| fence.owner != token)
    {
        return Err(refused().into());
    }
    if crate::storage::owner_restart::restart_eligible(&mut transaction, token).await? {
        let identity = super::super::text_identity("runtime_instances", token)?;
        prepare(&mut transaction, scope, &identity).await?;
        let revision = sqlx::query_scalar("SELECT evidence_revision FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1")
            .bind(token).fetch_one(&mut *transaction).await?;
        let identity = evidence_identity(&mut transaction, token, revision).await?;
        prepare(&mut transaction, scope, &identity).await?;
        return Ok(true);
    }
    require_current_process(&mut transaction, scope, token).await?;
    let Some(record) = store
        .prepare_controlled_stop(&mut transaction, token, now, observation, joined)
        .await?
    else {
        return Ok(false);
    };
    let revision = record.source_revision;
    let next = revision.checked_add(1).ok_or_else(refused)?;
    let snapshot = serde_json::to_string(&record).map_err(|_| refused())?;
    let identity = super::super::text_identity("runtime_instances", token)?;
    let runtime = prepare(&mut transaction, scope, &identity).await?;
    let identity = evidence_identity(&mut transaction, token, revision).await?;
    let evidence = prepare(&mut transaction, scope, &identity).await?;
    let changed = sqlx::query("UPDATE runtime_owner_evidence SET is_current=0 WHERE instance_token=? AND evidence_revision=? AND is_current=1 AND state='scan_pending'")
        .bind(token).bind(revision).execute(&mut *transaction).await?;
    if changed.rows_affected() != 1 {
        return Err(refused().into());
    }
    complete(&mut transaction, scope, evidence).await?;
    sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) SELECT instance_token,runtime_generation,?,'observing',pid,proc_starttime,process_fingerprint,boot_id,?,?,?,'closed_joined',0,'inaccessible',?,1,max_age_seconds FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
        .bind(next).bind(observation.boottime_ns).bind(now).bind(record.epoch).bind(snapshot).bind(token).bind(revision).execute(&mut *transaction).await?;
    let identity = evidence_identity(&mut transaction, token, next).await?;
    inserted(&mut transaction, scope, &identity).await?;
    let changed = sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token=? AND state='stopping' AND process_fingerprint=?")
        .bind(now).bind(token).bind(record.fingerprint).execute(&mut *transaction).await?;
    if changed.rows_affected() != 1 {
        return Err(refused().into());
    }
    complete(&mut transaction, scope, runtime).await?;
    transaction.commit().await?;
    tracing::info!(
        phase = "owner-controlled-stop",
        owner = token,
        cleanup_complete = true,
        stream_held = true,
        containment = false,
        "controlled cleanup completed; refused stream still blocks startup"
    );
    Ok(true)
}
