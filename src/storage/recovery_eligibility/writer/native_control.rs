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
    prepare_control_row(connection, scope).await.map(Some)
}

async fn prepare_control_row(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
) -> Result<ControlMutation, sqlx::Error> {
    let context = scope.context();
    if !matches!(
        context,
        EligibilityContext::NonAdmin {
            matrix_enabled: true,
            matrix_operation: Some(_),
            ..
        } | EligibilityContext::Admin {
            matrix_enabled: true,
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
    Ok(ControlMutation {
        identity,
        revision: snapshot.revision,
        digest: snapshot.digest,
        phase,
        watermark,
    })
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
    let (phase, watermark): (String, String) = sqlx::query_as(
        "SELECT phase,watermark FROM matrix_recovery_control WHERE control_id=1 AND cutover_operation_id=? AND restart_hold_state IS NULL",
    ).bind(context.matrix_operation()).fetch_one(&mut *connection).await?;
    if phase != "active"
        || watermark.is_empty()
        || !matches!(mutation.phase.as_str(), "baseline" | "active")
    {
        return Err(refused());
    }
    let next_revision = advance(connection, &mut context, &mutation, &watermark).await?;
    let snapshot = inventory::snapshot(connection, &mutation.identity).await?;
    let changed = sqlx::query("UPDATE offline_recovery_rows SET original_revision=?,original_digest=? WHERE table_name=? AND canonical_pk=? AND original_revision=? AND original_digest=? AND parent_relations='[]' AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(snapshot.revision).bind(snapshot.digest).bind(&mutation.identity.table).bind(&mutation.identity.key).bind(mutation.revision).bind(mutation.digest).execute(&mut *connection).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    authority::validate(connection, &context, scope.domain, scope.fence.as_ref()).await?;
    transaction.stage_revision(scope, next_revision)
}

async fn advance(
    connection: &mut SqliteConnection,
    context: &mut EligibilityContext,
    mutation: &ControlMutation,
    watermark: &str,
) -> Result<i64, sqlx::Error> {
    let previous_digest = mutation
        .watermark
        .as_ref()
        .map(|token| format!("{:x}", Sha256::digest(token.as_bytes())));
    let digest = format!("{:x}", Sha256::digest(watermark.as_bytes()));
    let (revision, changed) = match context {
        EligibilityContext::NonAdmin {
            generation,
            revision,
            matrix_operation,
            ..
        } => {
            let changed = sqlx::query("UPDATE runtime_eligibility_context SET matrix_phase='active',expected_b_digest=?,revision=revision+1 WHERE singleton=1 AND generation=? AND revision=? AND state='ready' AND matrix_enabled=1 AND compatibility_operation_id IS ? AND matrix_phase=? AND expected_b_digest IS ?")
                .bind(digest).bind(generation.as_str()).bind(*revision).bind(matrix_operation.as_deref()).bind(&mutation.phase).bind(previous_digest).execute(&mut *connection).await?;
            (revision, changed)
        }
        EligibilityContext::Admin {
            generation,
            root,
            operation,
            plan,
            revision,
            matrix_operation,
            ..
        } => {
            if mutation.phase != "active" {
                return Err(refused());
            }
            let changed = sqlx::query("UPDATE offline_recovery_binding SET revision=revision+1 WHERE singleton=1 AND phase='active' AND db_generation=? AND root_digest=? AND admin_operation_id=? AND plan_digest=? AND native_operation_id=? AND revision=?")
                .bind(generation.as_str()).bind(root.as_str()).bind(operation.as_str()).bind(plan.as_str()).bind(matrix_operation.as_str()).bind(*revision).execute(&mut *connection).await?;
            (revision, changed)
        }
    };
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    *revision = revision.checked_add(1).ok_or_else(refused)?;
    Ok(*revision)
}

/// Publish a restrictive hold from an authenticated unheld preimage. No checkpoint
/// moves and no new effect authority is granted; ordinary scope validation rejects
/// this control after commit. The pre-hold context is retained for supported recovery.
pub(crate) async fn complete_hold(
    transaction: &mut ScopedTransaction<'_>,
    scope: Option<&RecoveryScope>,
    mutation: Option<ControlMutation>,
) -> Result<(), sqlx::Error> {
    let (Some(scope), Some(mutation)) = (scope, mutation) else {
        return Ok(());
    };
    let context = scope.context();
    authority::validate_identity(transaction, &context).await?;
    let row = sqlx::query("SELECT phase,watermark,restart_hold_state,restart_hold_reason FROM matrix_recovery_control WHERE control_id=1 AND cutover_operation_id=?")
        .bind(context.matrix_operation()).fetch_one(&mut **transaction).await?;
    let phase: &str = row.try_get(0)?;
    let hold: Option<&str> = row.try_get(2)?;
    if mutation.phase != "active"
        || row.try_get::<Option<String>, _>(1)? != mutation.watermark
        || !matches!(hold, Some("pending" | "processing" | "unknown" | "refused"))
        || row.try_get::<Option<&str>, _>(3)?.is_none_or(str::is_empty)
        || !(phase == mutation.phase || (phase == "refused" && hold == Some("refused")))
    {
        return Err(refused());
    }
    let snapshot = inventory::snapshot(transaction, &mutation.identity).await?;
    let changed = sqlx::query("UPDATE offline_recovery_rows SET original_revision=?,original_digest=? WHERE table_name=? AND canonical_pk=? AND original_revision=? AND original_digest=? AND parent_relations='[]' AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(snapshot.revision).bind(snapshot.digest).bind(&mutation.identity.table).bind(&mutation.identity.key).bind(mutation.revision).bind(mutation.digest).execute(&mut **transaction).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    Ok(())
}

// A response may already contain a durably refused receipt. Publishing its hold
// must not require effect authority that this receipt deliberately revoked.
// This transaction grants only control refusal, never receipt/work mutation.
pub(crate) async fn begin_response_refusal<'a>(
    pool: &'a sqlx::SqlitePool,
    scope: Option<&RecoveryScope>,
    operation: &str,
    owner: &str,
    epoch: i64,
) -> Result<(ScopedTransaction<'a>, Option<ControlMutation>), sqlx::Error> {
    let Some(scope) = scope else {
        return Ok((super::begin(pool, None).await?, None));
    };
    let guard = scope.serial.clone().lock_owned().await;
    let mut raw = pool.begin_with("BEGIN IMMEDIATE").await?;
    let context = scope.context();
    if !authority::validate_identity(&mut raw, &context).await?
        || context.matrix_operation() != Some(operation)
    {
        return Err(refused());
    }
    super::super::native_context::validate(&mut raw, &context).await?;
    for fence in scope.fence.iter().chain(scope.matrix_fence.get()) {
        if fence.component != "MatrixSync" || fence.owner != owner || fence.epoch != epoch {
            return Err(refused());
        }
    }
    let live: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM matrix_component_leases WHERE component='MatrixSync' AND owner_token=? AND epoch=? AND state='active' AND julianday(expires_at)>julianday('now'))")
        .bind(owner).bind(epoch).fetch_one(&mut *raw).await?;
    if !live {
        return Err(refused());
    }
    let mutation = prepare_control_row(&mut raw, scope).await?;
    Ok((
        ScopedTransaction::new(raw, Some(scope.revision.clone()), Some(guard)),
        Some(mutation),
    ))
}
