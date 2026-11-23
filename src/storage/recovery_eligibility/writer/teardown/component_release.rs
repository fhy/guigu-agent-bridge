use sqlx::SqlitePool;

use super::{RecoveryScope, begin, complete, prepare, refused, require_current_process};

pub(crate) async fn release_component_lease(
    pool: &SqlitePool,
    scope: &RecoveryScope,
    component: &str,
    owner: &str,
    epoch: i64,
    now: &str,
) -> Result<(), sqlx::Error> {
    if !matches!(component, "MatrixSync" | "OutboxDrain") || epoch <= 0 {
        return Err(refused());
    }
    let fence = scope
        .fence
        .as_ref()
        .or_else(|| scope.matrix_fence.get())
        .ok_or_else(refused)?;
    if fence.component != component || fence.owner != owner || fence.epoch != epoch {
        return Err(refused());
    }
    let fingerprint = scope.runtime_fingerprint().await?;
    let mut transaction = begin(pool, scope).await?;
    require_current_process(&mut transaction, scope, owner, &fingerprint).await?;
    let identity = super::super::text_identity("matrix_component_leases", component)?;
    let mutation = prepare(&mut transaction, scope, &identity).await?;
    let changed = sqlx::query("UPDATE matrix_component_leases SET state='expired',expires_at=?,updated_at=? WHERE component=? AND owner_token=? AND epoch=? AND state='active'")
        .bind(now).bind(now).bind(component).bind(owner).bind(epoch).execute(&mut *transaction).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    complete(&mut transaction, scope, mutation).await?;
    transaction.commit().await?;
    tracing::info!(
        stage = "component-teardown",
        component,
        owner,
        epoch,
        committed = true,
        stream_held = true,
        "exact component released without restoring effect authority"
    );
    Ok(())
}
