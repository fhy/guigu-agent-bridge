use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

use super::{
    EffectDomain, EligibilityContext, RecoveryScope, RowIdentity, ScopedTransaction, authority,
    inventory, refused,
};

pub(crate) struct ControlMutation {
    identity: RowIdentity,
    revision: String,
    digest: String,
    phase: String,
    watermark: Option<String>,
}

pub(crate) async fn prepare_control(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
) -> Result<Option<ControlMutation>, sqlx::Error> {
    let Some(scope) = scope else {
        return Ok(None);
    };
    let context = scope.context();
    authority::validate(connection, &context, scope.domain, scope.fence.as_ref()).await?;
    if !matches!(
        context,
        EligibilityContext::NonAdmin {
            matrix_enabled: true,
            matrix_operation: Some(_),
            ..
        }
    ) {
        return Err(refused());
    }
    let identity = RowIdentity {
        table: "matrix_recovery_control".into(),
        key: "[\"integer:31\"]".into(),
    };
    let row = sqlx::query("SELECT original_revision,original_digest,parent_relations,disposition,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id,origin_generation FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=?")
        .bind(&identity.table).bind(&identity.key).fetch_one(&mut *connection).await?;
    context.matches_classification(&row)?;
    let snapshot = inventory::snapshot(connection, &identity).await?;
    if !matches!(
        row.try_get::<&str, _>(3)?,
        "eligible_new" | "eligible_preserved"
    ) || row.try_get::<&str, _>(0)? != snapshot.revision
        || row.try_get::<&str, _>(1)? != snapshot.digest
        || row.try_get::<Option<&str>, _>(8)? != context.matrix_operation()
        || row.try_get::<&str, _>(2)? != "[]"
        || !snapshot.parents.is_empty()
        || (scope.domain == EffectDomain::Local && scope.fence.is_some())
    {
        return Err(refused());
    }
    let (phase, watermark): (String, Option<String>) = sqlx::query_as(
        "SELECT phase,watermark FROM matrix_recovery_control WHERE control_id=1 AND restart_hold_state IS NULL",
    ).fetch_one(connection).await?;
    Ok(Some(ControlMutation {
        identity,
        revision: snapshot.revision,
        digest: snapshot.digest,
        phase,
        watermark,
    }))
}

pub(crate) async fn complete_control(
    transaction: &mut ScopedTransaction<'_>,
    scope: Option<&RecoveryScope>,
    mutation: Option<ControlMutation>,
) -> Result<(), sqlx::Error> {
    let (Some(scope), Some(mutation)) = (scope, mutation) else {
        return Ok(());
    };
    let connection: &mut SqliteConnection = transaction;
    let mut context = scope.context();
    let EligibilityContext::NonAdmin {
        generation,
        revision,
        matrix_operation,
        ..
    } = &mut context
    else {
        return Err(refused());
    };
    let (phase, watermark): (String, String) = sqlx::query_as(
        "SELECT phase,watermark FROM matrix_recovery_control WHERE control_id=1 AND cutover_operation_id=? AND restart_hold_state IS NULL",
    ).bind(matrix_operation.as_deref()).fetch_one(&mut *connection).await?;
    if phase != "active"
        || watermark.is_empty()
        || !matches!(mutation.phase.as_str(), "baseline" | "active")
    {
        return Err(refused());
    }
    let previous_digest = mutation
        .watermark
        .as_ref()
        .map(|token| format!("{:x}", Sha256::digest(token.as_bytes())));
    let digest = format!("{:x}", Sha256::digest(watermark.as_bytes()));
    let next_revision = revision.checked_add(1).ok_or_else(refused)?;
    let changed = sqlx::query("UPDATE runtime_eligibility_context SET matrix_phase='active',expected_b_digest=?,revision=revision+1 WHERE singleton=1 AND generation=? AND revision=? AND state='ready' AND matrix_enabled=1 AND compatibility_operation_id IS ? AND matrix_phase=? AND expected_b_digest IS ?")
        .bind(digest).bind(generation.as_str()).bind(*revision).bind(matrix_operation.as_deref()).bind(&mutation.phase).bind(previous_digest).execute(&mut *connection).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    *revision = next_revision;
    let snapshot = inventory::snapshot(connection, &mutation.identity).await?;
    let changed = sqlx::query("UPDATE offline_recovery_rows SET original_revision=?,original_digest=? WHERE table_name=? AND canonical_pk=? AND original_revision=? AND original_digest=? AND parent_relations='[]' AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(snapshot.revision).bind(snapshot.digest).bind(&mutation.identity.table).bind(&mutation.identity.key).bind(mutation.revision).bind(mutation.digest).execute(&mut *connection).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    authority::validate(connection, &context, scope.domain, scope.fence.as_ref()).await?;
    transaction.stage_revision(scope, next_revision)
}
