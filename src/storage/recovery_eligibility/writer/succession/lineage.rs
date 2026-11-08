use super::super::{EffectDomain, Mutation, RecoveryScope, RowIdentity, inventory, refused};
use sqlx::{Row, SqliteConnection};

pub(super) async fn prepare(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    id: &RowIdentity,
) -> Result<Mutation, sqlx::Error> {
    if !matches!(
        id.table.as_str(),
        "runtime_instances"
            | "runtime_owner_evidence"
            | "matrix_component_leases"
            | "matrix_recovery_control"
            | "matrix_recovery_batches"
    ) {
        return Err(refused());
    }
    super::super::super::require_lineage(c, &s.context(), EffectDomain::Matrix, id).await?;
    let row=sqlx::query("SELECT original_revision,original_digest,parent_relations FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=? AND disposition IN ('eligible_new','eligible_preserved')").bind(&id.table).bind(&id.key).fetch_one(c).await?;
    Ok(Mutation {
        identity: id.clone(),
        revision: row.get(0),
        digest: row.get(1),
        parents: row.get(2),
        released_lease: None,
    })
}

pub(super) async fn complete(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    m: Mutation,
) -> Result<(), sqlx::Error> {
    let snapshot = inventory::snapshot(c, &m.identity).await?;
    let changed=sqlx::query("UPDATE offline_recovery_rows SET original_revision=?,original_digest=? WHERE table_name=? AND canonical_pk=? AND original_revision=? AND original_digest=? AND parent_relations=? AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(&snapshot.revision).bind(&snapshot.digest).bind(&m.identity.table).bind(&m.identity.key).bind(&m.revision).bind(&m.digest).bind(&m.parents).execute(&mut *c).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    super::super::mutation::refresh_references(c, s, &m, &snapshot).await?;
    super::super::super::require_lineage(c, &s.context(), EffectDomain::Matrix, &m.identity).await
}

pub(super) async fn inserted(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    id: &RowIdentity,
) -> Result<(), sqlx::Error> {
    if !matches!(
        id.table.as_str(),
        "runtime_owner_evidence" | "matrix_recovery_receipts" | "matrix_recovery_batches"
    ) {
        return Err(refused());
    }
    let snapshot = inventory::snapshot(c, id).await?;
    let mut parents = Vec::new();
    for id in &snapshot.parents {
        super::super::super::require_lineage(c, &s.context(), EffectDomain::Matrix, id).await?;
        let parent = inventory::snapshot(c, id).await?;
        parents.push(super::super::ParentIdentity {
            identity: id.clone(),
            revision: parent.revision,
            digest: parent.digest,
            role: super::super::ParentRole::Effectful,
        });
    }
    let encoded = serde_json::to_string(&parents).map_err(|_| refused())?;
    super::super::insert_classification(c, s, id, &snapshot, &encoded, true).await?;
    super::super::super::require_lineage(c, &s.context(), EffectDomain::Matrix, id).await
}

// The old restart reconciler added only a hold and timestamp outside the scoped
// writer. Reconstruct its exact prior row and verify the original classification.
// This is uncommitted until the complete recovery publication succeeds.
pub(super) async fn reconstruct_control(
    c: &mut SqliteConnection,
    state: &crate::native_succession::state::State,
) -> Result<(), sqlx::Error> {
    if state.hold.is_none() {
        return Ok(());
    }
    let times:Vec<String>=sqlx::query_scalar("SELECT DISTINCT updated_at FROM matrix_recovery_batches WHERE response_token=? AND state='terminal'").bind(&state.watermark).fetch_all(&mut *c).await?;
    if times.len() != 1 {
        return Err(refused());
    }
    let changed = if state.hold.as_deref() == Some("refused") {
        sqlx::query("UPDATE matrix_recovery_control SET phase='active',restart_hold_state=NULL,restart_hold_reason=NULL,updated_at=? WHERE control_id=1 AND cutover_operation_id=? AND watermark=? AND phase='refused' AND restart_hold_state='refused' AND restart_hold_event_id IS NULL AND restart_hold_reason='rebuild-history-gap'")
            .bind(&times[0]).bind(&state.operation).bind(&state.watermark).execute(&mut *c).await?
    } else {
        sqlx::query("UPDATE matrix_recovery_control SET restart_hold_state=NULL,restart_hold_event_id=NULL,restart_hold_reason=NULL,updated_at=? WHERE control_id=1 AND cutover_operation_id=? AND watermark=? AND restart_hold_state='processing' AND restart_hold_event_id=? AND restart_hold_reason='incomplete response coverage requires reconciliation'")
        .bind(&times[0]).bind(&state.operation).bind(&state.watermark).bind(format!("batch:{}",state.batch.as_ref().ok_or_else(refused)?.id)).execute(&mut *c).await?
    };
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    let id = control();
    let actual = inventory::snapshot(c, &id).await?;
    let expected:(String,String)=sqlx::query_as("SELECT original_revision,original_digest FROM offline_recovery_rows WHERE table_name='matrix_recovery_control' AND canonical_pk=?").bind(&id.key).fetch_one(c).await?;
    if expected != (actual.revision, actual.digest) {
        return Err(refused());
    }
    Ok(())
}
pub(super) fn control() -> RowIdentity {
    RowIdentity {
        table: "matrix_recovery_control".into(),
        key: "[\"integer:31\"]".into(),
    }
}
