use sqlx::SqlitePool;

use super::{
    RecoveryScope, begin, complete, evidence_identity, inserted, prepare, refused,
    require_current_process,
};
use crate::storage::{ReliabilityError, ReliabilityStore, RuntimeOwnerEvidence};

pub(crate) async fn stopping_runtime(
    pool: &SqlitePool,
    scope: Option<&RecoveryScope>,
    token: &str,
    now: &str,
) -> Result<bool, sqlx::Error> {
    let scope = scope.ok_or_else(refused)?;
    let fingerprint = scope.runtime_fingerprint().await?;
    let mut transaction = begin(pool, scope).await?;
    if scope
        .matrix_fence
        .get()
        .is_none_or(|fence| fence.owner != token)
    {
        return Err(refused());
    }
    require_current_process(&mut transaction, scope, token, &fingerprint).await?;
    let observation = RuntimeOwnerEvidence::capture().ok_or_else(refused)?;
    if !crate::storage::owner_observation::current_timestamp(now) {
        return Err(refused());
    }
    let monotonic: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1 AND boottime_ns<=? AND julianday(captured_at)<=julianday(?)")
        .bind(token).bind(observation.boottime_ns).bind(now).fetch_one(&mut *transaction).await?;
    if monotonic != 1 {
        return Err(refused());
    }
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
    sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) SELECT instance_token,runtime_generation,?,'scan_pending',pid,proc_starttime,process_fingerprint,boot_id,?,?,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,1,max_age_seconds FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
        .bind(next).bind(observation.boottime_ns).bind(now).bind(token).bind(revision).execute(&mut *transaction).await?;
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
    let fingerprint = scope.runtime_fingerprint().await?;
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
    require_current_process(&mut transaction, scope, token, &fingerprint).await?;
    let Some(record) = store
        .prepare_controlled_stop(
            &mut transaction,
            token,
            now,
            observation,
            joined,
            &fingerprint,
        )
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
