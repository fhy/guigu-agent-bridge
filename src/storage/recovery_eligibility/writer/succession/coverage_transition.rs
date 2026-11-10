use super::*;
use crate::matrix::coverage::Coverage;

pub(super) fn transition(old: Option<&Coverage>, next: &Coverage) -> Result<(), sqlx::Error> {
    let Some(old) = old else {
        return initializing(next);
    };
    if old.status == "published" {
        return initializing(next);
    }
    if old.state != next.state
        || old.pairing != next.pairing
        || old.batch_id != next.batch_id
        || old.created_ms != next.created_ms
        || old.deadline_ms != next.deadline_ms
        || old.pages.len() > next.pages.len()
        || next.pages[..old.pages.len()] != old.pages
        || next.segments < old.segments
        || next.segments > old.segments + 1
        || next.attempts < old.attempts
        || next.sync_attempts < old.sync_attempts
        || next.publications < old.publications
        || (next.segments == old.segments
            && (next.segment_start != old.segment_start
                || next.segment_pages < old.segment_pages
                || next.segment_events < old.segment_events
                || next.segment_bytes < old.segment_bytes
                || next.segment_attempts < old.segment_attempts))
        || next.attempts > old.attempts + 1
        || next.sync_attempts > old.sync_attempts + 1
        || next.publications > old.publications + 1
        || next.pages.len() > old.pages.len() + 1
        || next.elapsed_ms < old.elapsed_ms
        || next.clock.utc_ms < old.clock.utc_ms
        || next.status == "published"
        || (old.status == "refused" && next.status != "refused")
    {
        return Err(state::refuse("coverage-immutable-drift"));
    }
    if let Some(before) = &old.target {
        let after = next
            .target
            .as_ref()
            .ok_or_else(|| state::refuse("coverage-target-drift"))?;
        if before.next != after.next
            || before.rooms.len() != after.rooms.len()
            || before.rooms.iter().zip(&after.rooms).any(|(a, b)| {
                a.id != b.id
                    || a.initial != b.initial
                    || a.anchor != b.anchor
                    || a.origin != b.origin
            })
        {
            return Err(state::refuse("coverage-target-drift"));
        }
    }
    Ok(())
}
pub(super) async fn reusable(
    c: &mut SqliteConnection,
    old: &Coverage,
    next: &Coverage,
) -> Result<(), sqlx::Error> {
    if old.status != "published" {
        return Ok(());
    }
    if next.status != "initializing"
        || old.state.operation != next.state.operation
        || old.state.native_generation != next.state.native_generation
        || old.state.watermark == next.state.watermark
        || old.state.owner.token == next.state.owner.token
    {
        return Err(state::refuse("coverage-reuse-binding"));
    }
    let valid:i64=sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_batches WHERE batch_id=? AND response_token=? AND response_digest=? AND state='terminal' AND expected_events_json=covered_events_json")
        .bind(&old.batch_id).bind(&old.final_response).bind(&old.response_digest).fetch_one(&mut *c).await?;
    if valid != 1 {
        return Err(state::refuse("coverage-reuse-unconfirmed"));
    }
    let checkpoint: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_batches b JOIN offline_recovery_rows c ON c.table_name='matrix_recovery_batches' AND c.canonical_pk=json_array('text:'||hex(CAST(b.batch_id AS BLOB))) WHERE c.matrix_operation_id=? AND b.response_token=? AND b.state='terminal' AND b.expected_events_json=b.covered_events_json")
        .bind(&next.state.operation).bind(&next.state.watermark).fetch_one(&mut *c).await?;
    if checkpoint == 0 {
        return Err(state::refuse("coverage-reuse-checkpoint"));
    }
    Ok(())
}
pub(super) async fn reparent(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    m: &mut super::super::Mutation,
) -> Result<(), sqlx::Error> {
    let snapshot = super::super::inventory::snapshot(c, &m.identity).await?;
    let mut parents = Vec::new();
    for id in snapshot.parents {
        super::super::super::require_lineage(c, &s.context(), EffectDomain::Matrix, &id).await?;
        let p = super::super::inventory::snapshot(c, &id).await?;
        parents.push(super::super::ParentIdentity {
            identity: id,
            revision: p.revision,
            digest: p.digest,
            role: super::super::ParentRole::Effectful,
        });
    }
    let encoded = serde_json::to_string(&parents).map_err(|_| refused())?;
    let changed=sqlx::query("UPDATE offline_recovery_rows SET parent_relations=? WHERE table_name='matrix_recovery_coverage' AND canonical_pk=? AND original_revision=? AND original_digest=? AND parent_relations=?")
        .bind(&encoded).bind(&m.identity.key).bind(&m.revision).bind(&m.digest).bind(&m.parents).execute(c).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    m.parents = encoded;
    Ok(())
}
pub(super) async fn publication(
    c: &mut SqliteConnection,
    state: &State,
    proof: &Exclusion,
    payloads: &[(String, Value)],
) -> Result<(), sqlx::Error> {
    let Some(row) = &proof.coverage else {
        return Ok(());
    };
    let config = proof
        .decision_config
        .as_ref()
        .ok_or_else(|| state::refuse("coverage-config-absent"))?;
    let rebuild = proof
        .rebuild
        .as_ref()
        .ok_or_else(|| state::refuse("coverage-rebuild-absent"))?;
    let target = row
        .target
        .as_ref()
        .ok_or_else(|| state::refuse("coverage-target-absent"))?;
    if &row.state != state
        || row.pairing != proof.pairing(config)?
        || row.status != "ready"
        || row.batch_id != rebuild.batch.id
        || target.next != rebuild.batch.response
        || row.payloads()? != payloads
        || target
            .rooms
            .iter()
            .filter_map(|r| r.anchor.clone())
            .collect::<Vec<_>>()
            != rebuild.anchors
        || super::coverage::read(c).await?.as_ref() != Some(row)
    {
        return Err(state::refuse("coverage-publication-binding"));
    }
    Ok(())
}

fn initializing(row: &Coverage) -> Result<(), sqlx::Error> {
    if row.status != "initializing"
        || row.target.is_some()
        || !row.pages.is_empty()
        || row.segments != 1
        || row.attempts != 0
        || row.sync_attempts != 0
        || row.publications != 0
        || row.segment_pages != 0
        || row.segment_events != 0
        || row.segment_bytes != 0
        || row.segment_attempts != 0
        || row.elapsed_ms != 0
        || row.created_ms != row.clock.utc_ms
        || row.segment_start != row.clock
        || row.reservation.is_some()
        || row.refusal.is_some()
        || row.final_response.is_some()
        || row.response_digest.is_some()
    {
        return Err(state::refuse("coverage-initializing"));
    }
    Ok(())
}
