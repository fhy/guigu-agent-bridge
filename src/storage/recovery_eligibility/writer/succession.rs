use super::{
    EffectDomain, EligibilityContext, RecoveryScope, composite_text_identity, refused,
    text_identity,
};
use crate::native_succession::{
    process::Exclusion,
    state::{self, State},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Connection, SqliteConnection, SqlitePool};
use std::path::Path;
#[cfg(test)]
mod boundary_tests;
mod control_time;
#[cfg(test)]
mod controlled_hold_tests;
pub(crate) mod coverage;
#[cfg(test)]
mod coverage_boundary_tests;
#[cfg(test)]
mod coverage_cost_fixture;
#[cfg(test)]
mod coverage_cost_tests;
#[cfg(test)]
mod coverage_crash_tests;
#[cfg(test)]
mod coverage_resume_tests;
#[cfg(test)]
mod coverage_tests;
mod coverage_transition;
mod lineage;
mod pre_accept;
#[cfg(test)]
mod pre_accept_tests;
#[cfg(test)]
mod projection_tests;
mod rebuild;
#[cfg(test)]
mod rebuild_tests;
mod receipt_lineage;
mod rows;
pub(crate) mod stop_metadata;
#[cfg(test)]
pub(crate) mod tests;

pub(crate) async fn publish(
    pool: &SqlitePool,
    state: &State,
    proof: &Exclusion,
    payloads: &[(String, Value)],
    revalidate: impl Fn() -> Result<(), sqlx::Error>,
) -> Result<(), sqlx::Error> {
    coverage::publication_deadline(proof)?;
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    if crate::native_succession::state::read(&mut tx)
        .await?
        .as_ref()
        != Some(state)
        || proof.owner != state.owner
    {
        return Err(refused());
    }
    revalidate()?;
    lineage::reconstruct_control(&mut tx, state)
        .await
        .map_err(|_| state::refuse("control-preimage"))?;
    let scope = RecoveryScope::new(
        EligibilityContext::NonAdmin {
            generation: state.native_generation.clone(),
            revision: state.revision,
            matrix_enabled: true,
            matrix_operation: Some(state.operation.clone()),
        },
        EffectDomain::Local,
        None,
    );
    super::super::authority::validate_identity(&mut tx, &scope.context())
        .await
        .map_err(|_| state::refuse("context-identity"))?;
    super::super::native_context::validate(&mut tx, &scope.context())
        .await
        .map_err(|_| state::refuse("native-context"))?;
    if let Some(witness) = &proof.stop_metadata {
        stop_metadata::apply(&mut tx, &scope, state, witness).await?;
    }
    super::super::bootstrap::readback::inventory(&mut tx, &scope.context())
        .await
        .map_err(|_| state::refuse("inventory-before"))?;
    coverage_transition::publication(&mut tx, state, proof, payloads).await?;
    let now = chrono::Utc::now().to_rfc3339();
    rows::retire_leases(&mut tx, &scope, state, &now)
        .await
        .map_err(|_| state::refuse("retire-leases"))?;
    pre_accept::close(&mut tx, &scope, state, &now).await?;
    if !crate::storage::owner_restart::guards::work_clear(&mut tx).await? {
        return Err(state::refuse("business-effects"));
    }
    rows::invocations_clear(&mut tx).await?;
    let control = lineage::prepare(&mut tx, &scope, &lineage::control()).await?;
    let effective = rebuild::prepare(&mut tx, &scope, state, proof, &now)
        .await
        .map_err(|_| state::refuse("rebuild-prepare"))?;
    rows::complete_batch(&mut tx, &scope, &effective, proof, payloads, &now)
        .await
        .map_err(|error| match error {
            sqlx::Error::Protocol(ref reason)
                if reason == "native-succession/coverage-publication-deadline" =>
            {
                error
            }
            _ => state::refuse("complete-batch"),
        })?;
    let record = record(state, proof)?;
    coverage::finish(&mut tx, &scope, proof).await?;
    rows::retire_owner(&mut tx, &scope, state, &record, &now)
        .await
        .map_err(|_| state::refuse("retire-owner"))?;
    let watermark = effective
        .batch
        .as_ref()
        .map_or(state.watermark.as_str(), |b| b.response.as_str());
    let changed=sqlx::query("UPDATE matrix_recovery_control SET watermark=?,updated_at=?,restart_hold_state=NULL,restart_hold_event_id=NULL,restart_hold_reason=NULL WHERE control_id=1 AND cutover_operation_id=? AND watermark=? AND restart_hold_state IS NULL")
        .bind(watermark).bind(&now).bind(&state.operation).bind(&state.watermark).execute(&mut *tx).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    lineage::complete(&mut tx, &scope, control).await?;
    let changed=sqlx::query("UPDATE runtime_eligibility_context SET expected_b_digest=?,revision=revision+1 WHERE singleton=1 AND generation=? AND revision=? AND compatibility_operation_id=? AND expected_b_digest=?")
        .bind(format!("{:x}",Sha256::digest(watermark.as_bytes()))).bind(&state.native_generation).bind(state.revision).bind(&state.operation).bind(format!("{:x}",Sha256::digest(state.watermark.as_bytes()))).execute(&mut *tx).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    let next = EligibilityContext::NonAdmin {
        generation: state.native_generation.clone(),
        revision: state.revision.checked_add(1).ok_or_else(refused)?,
        matrix_enabled: true,
        matrix_operation: Some(state.operation.clone()),
    };
    super::super::authority::validate_identity(&mut tx, &next).await?;
    super::super::native_context::validate(&mut tx, &next).await?;
    super::super::bootstrap::readback::inventory(&mut tx, &next)
        .await
        .map_err(|_| state::refuse("inventory-after"))?;
    revalidate()?;
    coverage::publication_deadline(proof)?;
    tx.commit().await
}

fn record(state: &State, proof: &Exclusion) -> Result<String, sqlx::Error> {
    serde_json::to_string(&json!({"kind":"safe-succession-v1","source":state,"exclusion":proof,"historical_joined":false})).map_err(|_|refused())
}

pub(crate) async fn confirmed(
    database: &Path,
    state: &State,
    proof: &Exclusion,
) -> Result<bool, sqlx::Error> {
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(database)
        .read_only(true)
        .create_if_missing(false);
    let mut c = SqliteConnection::connect_with(&options).await?;
    let evidence:Option<(String,String,String,String)>=sqlx::query_as("SELECT r.state,e.state,e.barrier_state,e.child_snapshot FROM runtime_instances r JOIN runtime_owner_evidence e USING(instance_token) WHERE r.instance_token=? AND e.is_current=1 AND e.evidence_revision=? AND r.process_fingerprint=?")
        .bind(&state.owner.token).bind(state.owner.revision.checked_add(1).ok_or_else(refused)?).bind(&state.owner.fingerprint).fetch_optional(&mut c).await?;
    if evidence.as_ref().is_none_or(|(r, e, barrier, s)| {
        r != "stopped"
            || e != "observing"
            || barrier == "closed_joined"
            || s != &record(state, proof).unwrap_or_default()
    }) {
        return Ok(false);
    }
    let checkpoint:Option<(String,Option<String>)>=sqlx::query_as("SELECT watermark,restart_hold_state FROM matrix_recovery_control WHERE control_id=1 AND cutover_operation_id=?").bind(&state.operation).fetch_optional(&mut c).await?;
    if !coverage::confirmed(&mut c, proof).await? {
        return Ok(false);
    }
    let effective = rebuild::effective(state, proof)?;
    let watermark = effective
        .batch
        .as_ref()
        .map_or(state.watermark.as_str(), |b| b.response.as_str());
    if checkpoint.as_ref() != Some(&(watermark.into(), None)) {
        return Ok(false);
    }
    if let Some(batch) = &effective.batch {
        let valid:i64=sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_batches WHERE batch_id=? AND state='terminal' AND response_digest=? AND expected_events_json=covered_events_json AND NOT EXISTS(SELECT 1 FROM matrix_recovery_receipts WHERE batch_id=? AND state!='dispositioned')")
            .bind(&batch.id).bind(&batch.digest).bind(&batch.id).fetch_one(&mut c).await?;
        if valid != 1 {
            return Ok(false);
        }
    }
    for (component, owner, epoch, _, _) in &state.leases {
        let valid:i64=sqlx::query_scalar("SELECT count(*) FROM matrix_component_leases WHERE component=? AND owner_token=? AND epoch=? AND state='expired'").bind(component).bind(owner).bind(epoch.checked_add(1).ok_or_else(refused)?).fetch_one(&mut c).await?;
        if valid != 1 {
            return Ok(false);
        }
    }
    let context = EligibilityContext::NonAdmin {
        generation: state.native_generation.clone(),
        revision: state.revision.checked_add(1).ok_or_else(refused)?,
        matrix_enabled: true,
        matrix_operation: Some(state.operation.clone()),
    };
    super::super::authority::validate_identity(&mut c, &context).await?;
    super::super::native_context::validate(&mut c, &context).await?;
    super::super::bootstrap::readback::inventory(&mut c, &context).await?;
    if !pre_accept::confirmed(&mut c, state).await? {
        return Ok(false);
    }
    c.close().await?;
    Ok(true)
}
