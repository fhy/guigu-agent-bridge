use super::*;

pub(super) async fn retire_leases(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    state: &State,
    now: &str,
) -> Result<(), sqlx::Error> {
    for (component, owner, epoch, status, expires) in &state.leases {
        let id = text_identity("matrix_component_leases", component)?;
        let mutation = lineage::prepare(c, s, &id).await?;
        let changed=sqlx::query("UPDATE matrix_component_leases SET epoch=?,state='expired',expires_at=?,updated_at=? WHERE component=? AND owner_token=? AND epoch=? AND state=? AND expires_at=?")
            .bind(epoch.checked_add(1).ok_or_else(refused)?).bind(now).bind(now).bind(component).bind(owner).bind(epoch).bind(status).bind(expires).execute(&mut *c).await?;
        if changed.rows_affected() != 1 {
            return Err(refused());
        }
        lineage::complete(c, s, mutation).await?;
    }
    Ok(())
}

pub(super) async fn invocations_clear(c: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    for query in [
        "SELECT count(*) FROM matrix_recovery_receipts WHERE state!='dispositioned'",
        "SELECT count(*) FROM sessions s WHERE s.state!='closed' AND NOT EXISTS(SELECT 1 FROM delivery_dispositions d WHERE d.session_id=s.session_id AND d.state='terminal')",
    ] {
        if sqlx::query_scalar::<_, i64>(query)
            .fetch_one(&mut *c)
            .await?
            != 0
        {
            return Err(state::refuse("unresolved-invocation-or-effect"));
        }
    }
    let pids: Vec<i64> =
        sqlx::query_scalar("SELECT child_pid FROM session_owner_bindings WHERE state!='reaped'")
            .fetch_all(c)
            .await?;
    for pid in pids {
        // PID reuse is a refusal, not evidence that this invocation was reaped.
        if pid <= 0 {
            return Err(state::refuse("bound-child-not-excluded"));
        }
        match std::fs::symlink_metadata(format!("/proc/{pid}")) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            _ => return Err(state::refuse("bound-child-not-excluded")),
        }
    }
    Ok(())
}

pub(super) async fn complete_batch(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    state: &State,
    proof: &Exclusion,
    payloads: &[(String, Value)],
    now: &str,
) -> Result<(), sqlx::Error> {
    let Some(batch) = &state.batch else {
        return if payloads.is_empty() {
            Ok(())
        } else {
            Err(refused())
        };
    };
    crate::matrix::succession_payload::verify(batch, payloads)
        .map_err(|_| state::refuse("payload-drift-or-actionable"))?;
    let mutation =
        lineage::prepare(c, s, &text_identity("matrix_recovery_batches", &batch.id)?).await?;
    let mut receipt_parents = super::receipt_lineage::Parents::default();
    for (room, value) in payloads {
        super::coverage::publication_deadline(proof)?;
        disposition(c, s, state, proof, room, value, now, &mut receipt_parents).await?;
    }
    let covered = serde_json::to_string(&batch.expected).map_err(|_| refused())?;
    let changed=sqlx::query("UPDATE matrix_recovery_batches SET covered_events_json=?,state='terminal',updated_at=? WHERE batch_id=? AND owner_token=? AND owner_epoch=? AND response_digest=? AND state='processing'")
        .bind(covered).bind(now).bind(&batch.id).bind(&batch.owner).bind(batch.epoch).bind(&batch.digest).execute(&mut *c).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    lineage::complete(c, s, mutation).await
}

#[allow(clippy::too_many_arguments)]
async fn disposition(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    state: &State,
    proof: &Exclusion,
    room: &str,
    value: &Value,
    now: &str,
    parents: &mut super::receipt_lineage::Parents,
) -> Result<(), sqlx::Error> {
    let batch = state.batch.as_ref().ok_or_else(refused)?;
    let event = value
        .get("event_id")
        .and_then(Value::as_str)
        .ok_or_else(refused)?;
    let digest = format!("{:x}", Sha256::digest(value.to_string().as_bytes()));
    let id = composite_text_identity(
        "matrix_recovery_receipts",
        &[&proof.account, room, event, &state.operation],
    )?;
    let existing:Option<(String,String)>=sqlx::query_as("SELECT payload_digest,state FROM matrix_recovery_receipts WHERE account_id=? AND room_id=? AND event_id=? AND cutover_operation_id=?")
        .bind(&proof.account).bind(room).bind(event).bind(&state.operation).fetch_optional(&mut *c).await?;
    if let Some(existing) = existing {
        if existing != (digest, "dispositioned".into()) {
            return Err(refused());
        }
        return super::super::super::require_lineage(c, &s.context(), EffectDomain::Matrix, &id)
            .await;
    }
    if batch.covered.iter().any(|e| e == event) {
        return Err(state::refuse("covered-without-receipt"));
    }
    let effects: i64 =
        sqlx::query_scalar("SELECT count(*) FROM transport_receipts WHERE external_event_id=?")
            .bind(event)
            .fetch_one(&mut *c)
            .await?;
    if effects != 0 {
        return Err(state::refuse("unreceipted-effect"));
    }
    if let Some(config) = &proof.decision_config {
        crate::matrix::ingress_decision::no_effect(c, config, room, value).await?;
    }
    // reserve-before-deliver is preserved. Here delivery is solely the normal
    // adapter's ignored-event result, so the whole disposition can be atomic.
    sqlx::query("INSERT INTO matrix_recovery_receipts(account_id,room_id,event_id,cutover_operation_id,payload_digest,state,batch_id,created_at,updated_at) VALUES(?,?,?,?,?,'pending',?,?,?)")
        .bind(&proof.account).bind(room).bind(event).bind(&state.operation).bind(digest).bind(&batch.id).bind(now).bind(now).execute(&mut *c).await?;
    sqlx::query("UPDATE matrix_recovery_receipts SET state='processing' WHERE account_id=? AND room_id=? AND event_id=? AND cutover_operation_id=? AND state='pending'")
        .bind(&proof.account).bind(room).bind(event).bind(&state.operation).execute(&mut *c).await?;
    if !crate::matrix::succession_payload::ignored(value, room, &proof.account)
        .map_err(|_| refused())?
        && proof.decision_config.is_none()
        && !proof.unrouted.iter().any(|e| e == event)
        && !proof
            .rebuild
            .as_ref()
            .is_some_and(|r| r.unrouted.iter().any(|e| e == event))
    {
        return Err(refused());
    }
    sqlx::query("UPDATE matrix_recovery_receipts SET state='dispositioned' WHERE account_id=? AND room_id=? AND event_id=? AND cutover_operation_id=? AND state='processing'")
        .bind(&proof.account).bind(room).bind(event).bind(&state.operation).execute(&mut *c).await?;
    parents.inserted(c, s, &id).await
}

pub(super) async fn retire_owner(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    state: &State,
    record: &str,
    now: &str,
) -> Result<(), sqlx::Error> {
    let owner = &state.owner;
    let id = text_identity("runtime_instances", &owner.token)?;
    let mutation = lineage::prepare(c, s, &id).await?;
    let id = evidence_id(c, &owner.token, owner.revision).await?;
    let previous = lineage::prepare(c, s, &id).await?;
    let changed = if state.history_hold_owner {
        sqlx::query("UPDATE runtime_owner_evidence SET is_current=0 WHERE instance_token=? AND runtime_generation=? AND evidence_revision=? AND is_current=1 AND state='observing' AND process_fingerprint=? AND barrier_state='closed_joined' AND in_flight_spawns=0 AND child_scan_state='inaccessible'")
            .bind(&owner.token).bind(owner.generation).bind(owner.revision).bind(&owner.fingerprint).execute(&mut *c).await?
    } else {
        sqlx::query("UPDATE runtime_owner_evidence SET is_current=0 WHERE instance_token=? AND runtime_generation=? AND evidence_revision=? AND is_current=1 AND state IN ('live','scan_pending','unknown') AND process_fingerprint=? AND barrier_state!='closed_joined'")
            .bind(&owner.token).bind(owner.generation).bind(owner.revision).bind(&owner.fingerprint).execute(&mut *c).await?
    };
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    lineage::complete(c, s, previous).await?;
    let next = owner.revision.checked_add(1).ok_or_else(refused)?;
    // Preserve historical evidence. A new succession record uses an open barrier:
    // it proves current exclusion, never a new historical join or controlled stop.
    // The former closed-joined record remains intact at its original revision.
    sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) SELECT instance_token,runtime_generation,?,'observing',pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,CASE WHEN ? THEN 'open' ELSE barrier_state END,in_flight_spawns,child_scan_state,?,1,max_age_seconds FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
        .bind(next).bind(state.history_hold_owner).bind(record).bind(&owner.token).bind(owner.revision).execute(&mut *c).await?;
    let id = evidence_id(c, &owner.token, next).await?;
    lineage::inserted(c, s, &id).await?;
    let changed=sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token=? AND state IN ('active','stopping','stopped') AND process_fingerprint=?")
        .bind(now).bind(&owner.token).bind(&owner.fingerprint).execute(&mut *c).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    lineage::complete(c, s, mutation).await
}
async fn evidence_id(
    c: &mut SqliteConnection,
    owner: &str,
    revision: i64,
) -> Result<super::super::RowIdentity, sqlx::Error> {
    let key=sqlx::query_scalar("SELECT json_array('text:'||hex(CAST(instance_token AS BLOB)),'integer:'||hex(CAST(runtime_generation AS BLOB)),'integer:'||hex(CAST(evidence_revision AS BLOB))) FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?").bind(owner).bind(revision).fetch_one(c).await?;
    Ok(super::super::RowIdentity {
        table: "runtime_owner_evidence".into(),
        key,
    })
}
