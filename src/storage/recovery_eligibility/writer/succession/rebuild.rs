use super::*;

pub(super) fn effective(state: &State, proof: &Exclusion) -> Result<State, sqlx::Error> {
    let mut effective = state.clone();
    match &proof.rebuild {
        Some(rebuild) => {
            let batch = &rebuild.batch;
            if state.hold.as_deref() != Some("refused")
                || state.batch.is_some()
                || batch.previous != state.watermark
                || batch.request.as_deref() != Some(&state.watermark)
                || batch.owner != state.owner.token
                || batch.response.is_empty()
                || !batch.covered.is_empty()
                || !state.leases.iter().any(|(c, o, e, _, _)| {
                    c == "MatrixSync" && o == &batch.owner && e == &batch.epoch
                })
            {
                return Err(refused());
            }
            effective.batch = Some(batch.clone());
        }
        None if state.hold.as_deref() == Some("refused") => return Err(refused()),
        None => (),
    }
    Ok(effective)
}

pub(super) async fn prepare(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    state: &State,
    proof: &Exclusion,
    now: &str,
) -> Result<State, sqlx::Error> {
    let effective = effective(state, proof)?;
    let Some(rebuild) = &proof.rebuild else {
        return Ok(effective);
    };
    for anchor in &rebuild.anchors {
        let valid:i64=sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_receipts r JOIN matrix_recovery_batches b ON b.batch_id=r.batch_id WHERE r.account_id=? AND r.room_id=? AND r.event_id=? AND r.payload_digest=? AND r.batch_id=? AND r.cutover_operation_id=? AND r.state='dispositioned' AND b.state='terminal'")
            .bind(&proof.account).bind(&anchor.room).bind(&anchor.event).bind(&anchor.digest).bind(&anchor.batch).bind(&state.operation).fetch_one(&mut *c).await?;
        if valid != 1 {
            return Err(refused());
        }
    }
    let b = &rebuild.batch;
    sqlx::query("INSERT INTO matrix_recovery_batches(batch_id,request_token,previous_watermark,response_token,response_digest,component,owner_token,owner_epoch,expected_events_json,covered_events_json,state,created_at,updated_at) VALUES(?,?,?,?,?,'MatrixSync',?,?,?,'[]','processing',?,?)")
        .bind(&b.id).bind(&b.request).bind(&b.previous).bind(&b.response).bind(&b.digest).bind(&b.owner).bind(b.epoch)
        .bind(serde_json::to_string(&b.expected).map_err(|_|refused())?).bind(now).bind(now).execute(&mut *c).await?;
    lineage::inserted(c, s, &text_identity("matrix_recovery_batches", &b.id)?).await?;
    Ok(effective)
}
