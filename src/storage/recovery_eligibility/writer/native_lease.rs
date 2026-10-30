use sqlx::{Row, SqliteConnection};

use super::{EligibilityContext, RecoveryScope, RowIdentity, authority, inventory, refused};

#[cfg(test)]
#[path = "native_lease_tests.rs"]
mod tests;

pub(crate) struct LeaseMutation {
    identity: RowIdentity,
    revision: String,
    digest: String,
}

async fn validate(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    component: &str,
) -> Result<(), sqlx::Error> {
    let context = scope.context();
    authority::validate(connection, &context, scope.domain, scope.fence.as_ref()).await?;
    let enabled = match &context {
        EligibilityContext::NonAdmin { matrix_enabled, .. }
        | EligibilityContext::Admin { matrix_enabled, .. } => *matrix_enabled,
    };
    if !enabled {
        return Err(refused());
    }
    let baseline = component == "MatrixSync"
        && scope.domain == super::EffectDomain::Local
        && scope.fence.is_none()
        && matches!(
            context,
            EligibilityContext::NonAdmin {
                matrix_enabled: true,
                matrix_operation: Some(_),
                ..
            }
        );
    let active: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_control WHERE control_id=1 AND cutover_operation_id=? AND restart_hold_state IS NULL AND ((phase='active' AND watermark IS NOT NULL AND length(watermark)>0) OR (? AND phase='baseline' AND watermark IS NULL AND NOT EXISTS(SELECT 1 FROM matrix_recovery_batches) AND NOT EXISTS(SELECT 1 FROM matrix_recovery_receipts)))")
        .bind(context.matrix_operation().ok_or_else(refused)?).bind(baseline).fetch_one(connection).await?;
    if active != 1 {
        return Err(refused());
    }
    Ok(())
}

pub(crate) async fn prepare_lease(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    component: &str,
) -> Result<Option<LeaseMutation>, sqlx::Error> {
    let Some(scope) = scope else { return Ok(None) };
    validate(connection, scope, component).await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM matrix_component_leases WHERE component=?)",
    )
    .bind(component)
    .fetch_one(&mut *connection)
    .await?;
    if !exists {
        return Ok(None);
    }
    let identity = super::text_identity("matrix_component_leases", component)?;
    let row = sqlx::query("SELECT original_revision,original_digest,parent_relations,disposition,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id,origin_generation FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=?")
        .bind(&identity.table).bind(&identity.key).fetch_optional(&mut *connection).await?.ok_or_else(refused)?;
    let context = scope.context();
    context.matches_classification(&row)?;
    let snapshot = inventory::snapshot(connection, &identity).await?;
    if !matches!(
        row.try_get::<&str, _>(3)?,
        "eligible_new" | "eligible_preserved"
    ) || row.try_get::<&str, _>(0)? != snapshot.revision
        || row.try_get::<&str, _>(1)? != snapshot.digest
        || row.try_get::<&str, _>(2)? != "[]"
        || row.try_get::<Option<&str>, _>(8)? != context.matrix_operation()
        || !snapshot.parents.is_empty()
    {
        return Err(refused());
    }
    Ok(Some(LeaseMutation {
        identity,
        revision: snapshot.revision,
        digest: snapshot.digest,
    }))
}

pub(crate) async fn complete_lease(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    component: &str,
    mutation: Option<LeaseMutation>,
) -> Result<(), sqlx::Error> {
    let Some(scope) = scope else { return Ok(()) };
    validate(connection, scope, component).await?;
    let identity = super::text_identity("matrix_component_leases", component)?;
    let snapshot = inventory::snapshot(connection, &identity).await?;
    if !snapshot.parents.is_empty() {
        return Err(refused());
    }
    if let Some(mutation) = mutation {
        if identity != mutation.identity {
            return Err(refused());
        }
        let changed = sqlx::query("UPDATE offline_recovery_rows SET original_revision=?,original_digest=? WHERE table_name=? AND canonical_pk=? AND original_revision=? AND original_digest=? AND parent_relations='[]' AND disposition IN ('eligible_new','eligible_preserved')")
            .bind(&snapshot.revision).bind(&snapshot.digest).bind(&identity.table).bind(&identity.key).bind(mutation.revision).bind(mutation.digest).execute(&mut *connection).await?;
        if changed.rows_affected() != 1 {
            return Err(refused());
        }
    } else {
        super::insert_classification(connection, scope, &identity, &snapshot, "[]", true).await?;
    }
    Ok(())
}

pub(crate) async fn complete_expired_lease(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    component: &str,
    mutation: Option<LeaseMutation>,
) -> Result<(), sqlx::Error> {
    let Some(scope) = scope else { return Ok(()) };
    let expired: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_component_leases WHERE component=? AND state='active' AND julianday(expires_at)<=julianday('now')")
        .bind(component).fetch_one(&mut *connection).await?;
    let identity = super::text_identity("matrix_component_leases", component)?;
    if expired != 1
        || mutation
            .as_ref()
            .is_none_or(|mutation| mutation.identity != identity)
    {
        return Err(refused());
    }
    let mut control_scope = scope.clone();
    control_scope.domain = super::EffectDomain::Local;
    control_scope.fence = None;
    complete_lease(connection, Some(&control_scope), component, mutation).await
}
