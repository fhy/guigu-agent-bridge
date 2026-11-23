use sqlx::{Row, SqliteConnection};

use super::{EffectDomain, EligibilityContext, ParentIdentity, inventory, refused};

pub(super) async fn validate(
    connection: &mut SqliteConnection,
    context: &EligibilityContext,
    domain: EffectDomain,
    parent: &ParentIdentity,
) -> Result<(), sqlx::Error> {
    let EligibilityContext::Admin {
        generation,
        root,
        operation,
        plan,
        matrix_operation,
        matrix_enabled: true,
        ..
    } = context
    else {
        return Err(refused());
    };
    if domain != EffectDomain::Matrix
        || !matches!(parent.identity.table.as_str(), "agents" | "conversations")
    {
        return Err(refused());
    }
    let anchor = sqlx::query("SELECT anchor.original_revision,anchor.original_digest FROM offline_recovery_metadata_anchors AS anchor JOIN offline_recovery_rows AS classified ON classified.table_name=anchor.table_name AND classified.canonical_pk=anchor.canonical_pk WHERE anchor.table_name=? AND anchor.canonical_pk=? AND anchor.role='metadata_anchor' AND anchor.db_generation=? AND anchor.root_digest=? AND anchor.admin_operation_id=? AND anchor.plan_digest=? AND classified.context_kind='admin' AND classified.origin_generation=? AND classified.matrix_operation_id=? AND classified.admin_operation_id=anchor.admin_operation_id AND classified.plan_digest=anchor.plan_digest AND classified.original_revision=anchor.original_revision AND classified.original_digest=anchor.original_digest AND classified.disposition IN ('terminal_evidenced','quarantined_no_effect')")
        .bind(&parent.identity.table).bind(&parent.identity.key).bind(generation)
        .bind(root).bind(operation).bind(plan).bind(matrix_operation).bind(matrix_operation).fetch_optional(&mut *connection).await?
        .ok_or_else(refused)?;
    let revision: &str = anchor.try_get(0)?;
    let digest: &str = anchor.try_get(1)?;
    if revision != parent.revision || digest != parent.digest {
        return Err(refused());
    }
    let snapshot = inventory::snapshot(connection, &parent.identity).await?;
    if snapshot.revision != revision || snapshot.digest != digest || !snapshot.parents.is_empty() {
        return Err(refused());
    }
    Ok(())
}
