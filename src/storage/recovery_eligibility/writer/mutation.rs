use sqlx::{Row, SqliteConnection};

use super::{
    ParentIdentity, ParentRole, RecoveryScope, RowIdentity, authority, inventory, refused,
    require_row,
};

pub(crate) struct Mutation {
    identity: RowIdentity,
    revision: String,
    digest: String,
    parents: String,
    released_lease: Option<(String, i64)>,
}

pub(crate) async fn complete_refused_receipt(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    mutation: Option<Mutation>,
) -> Result<(), sqlx::Error> {
    let (Some(scope), Some(mutation)) = (scope, mutation) else {
        return Ok(());
    };
    if mutation.identity.table != "matrix_recovery_receipts" {
        return Err(refused());
    }
    let snapshot = inventory::snapshot(connection, &mutation.identity).await?;
    let declared: Vec<ParentIdentity> =
        serde_json::from_str(&mutation.parents).map_err(|_| refused())?;
    if declared.len() != snapshot.parents.len()
        || snapshot
            .parents
            .iter()
            .any(|parent| !declared.iter().any(|previous| previous.identity == *parent))
    {
        return Err(refused());
    }
    let row = sqlx::query("SELECT original_revision,original_digest,parent_relations,disposition,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id,origin_generation FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=?")
        .bind(&mutation.identity.table).bind(&mutation.identity.key).fetch_one(&mut *connection).await?;
    scope.context().matches_classification(&row)?;
    let refused_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_receipts WHERE json_array('text:'||hex(CAST(account_id AS BLOB)),'text:'||hex(CAST(room_id AS BLOB)),'text:'||hex(CAST(event_id AS BLOB)),'text:'||hex(CAST(cutover_operation_id AS BLOB)))=? AND state='refused'")
        .bind(&mutation.identity.key).fetch_one(&mut *connection).await?;
    if refused_rows != 1 {
        return Err(refused());
    }
    let changed = sqlx::query("UPDATE offline_recovery_rows SET original_revision=?,original_digest=?,disposition='refused_hold' WHERE table_name=? AND canonical_pk=? AND original_revision=? AND original_digest=? AND parent_relations=? AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(snapshot.revision).bind(snapshot.digest).bind(&mutation.identity.table).bind(&mutation.identity.key).bind(mutation.revision).bind(mutation.digest).bind(mutation.parents).execute(connection).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    Ok(())
}

pub(crate) async fn prepare_mutation(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    identity: &RowIdentity,
) -> Result<Option<Mutation>, sqlx::Error> {
    let Some(scope) = scope else {
        return Ok(None);
    };
    let routed = super::effect_scope::resolve(connection, scope, identity).await?;
    let scope = &routed;
    require_row(
        connection,
        &scope.context(),
        scope.domain,
        scope.fence.as_ref(),
        identity,
    )
    .await?;
    let row = sqlx::query("SELECT original_revision,original_digest,parent_relations FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=? AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(&identity.table).bind(&identity.key).fetch_optional(&mut *connection).await?.ok_or_else(refused)?;
    let released_lease = if identity.table == "execution_leases" {
        sqlx::query_as("SELECT task_id,fence FROM execution_leases WHERE json_array('text:'||hex(CAST(resource_key AS BLOB)))=? AND state='released'")
            .bind(&identity.key).fetch_optional(&mut *connection).await?
    } else {
        None
    };
    Ok(Some(Mutation {
        identity: identity.clone(),
        revision: row.try_get(0)?,
        digest: row.try_get(1)?,
        parents: row.try_get(2)?,
        released_lease,
    }))
}

pub(crate) async fn complete_mutation(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    mutation: Option<Mutation>,
) -> Result<(), sqlx::Error> {
    complete(connection, scope, mutation, false).await
}

pub(crate) async fn complete_reassigned_lease(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    mutation: Option<Mutation>,
) -> Result<(), sqlx::Error> {
    complete(connection, scope, mutation, true).await
}

async fn complete(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    mutation: Option<Mutation>,
    reassign: bool,
) -> Result<(), sqlx::Error> {
    let (Some(scope), Some(mutation)) = (scope, mutation) else {
        return Ok(());
    };
    let routed = super::effect_scope::resolve(connection, scope, &mutation.identity).await?;
    let scope = &routed;
    authority::validate(
        connection,
        &scope.context(),
        scope.domain,
        scope.fence.as_ref(),
    )
    .await?;
    let snapshot = inventory::snapshot(connection, &mutation.identity).await?;
    let mut declared: Vec<ParentIdentity> =
        serde_json::from_str(&mutation.parents).map_err(|_| refused())?;
    let parents = if reassign {
        validate_reassignment(connection, &mutation, &mut declared).await?;
        serde_json::to_string(
            &super::parents(connection, scope, &mutation.identity, &snapshot).await?,
        )
        .map_err(|_| refused())?
    } else {
        mutation.parents.clone()
    };
    if declared.len() != snapshot.parents.len()
        || snapshot
            .parents
            .iter()
            .any(|parent| !declared.iter().any(|old| old.identity == *parent))
    {
        return Err(refused());
    }
    let affected = sqlx::query("UPDATE offline_recovery_rows SET original_revision=?,original_digest=?,parent_relations=? WHERE table_name=? AND canonical_pk=? AND original_revision=? AND original_digest=? AND parent_relations=? AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(&snapshot.revision).bind(&snapshot.digest).bind(parents).bind(&mutation.identity.table)
        .bind(&mutation.identity.key).bind(&mutation.revision).bind(&mutation.digest)
        .bind(&mutation.parents).execute(&mut *connection).await?;
    if affected.rows_affected() != 1 {
        return Err(refused());
    }
    refresh_references(connection, scope, &mutation, &snapshot).await?;
    require_row(
        connection,
        &scope.context(),
        scope.domain,
        scope.fence.as_ref(),
        &mutation.identity,
    )
    .await
}

async fn validate_reassignment(
    connection: &mut SqliteConnection,
    mutation: &Mutation,
    declared: &mut [ParentIdentity],
) -> Result<(), sqlx::Error> {
    let (previous_task, previous_fence) = mutation.released_lease.as_ref().ok_or_else(refused)?;
    if mutation.identity.table != "execution_leases" {
        return Err(refused());
    }
    let (task, fence): (String, i64) = sqlx::query_as("SELECT task_id,fence FROM execution_leases WHERE json_array('text:'||hex(CAST(resource_key AS BLOB)))=? AND state='active'")
        .bind(&mutation.identity.key).fetch_one(&mut *connection).await?;
    if previous_fence.checked_add(1) != Some(fence) {
        return Err(refused());
    }
    let previous = super::text_identity("tasks", previous_task)?;
    let parent = declared
        .iter_mut()
        .find(|parent| parent.identity == previous)
        .ok_or_else(refused)?;
    parent.identity = super::text_identity("tasks", &task)?;
    Ok(())
}

async fn refresh_references(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    mutation: &Mutation,
    snapshot: &inventory::Snapshot,
) -> Result<(), sqlx::Error> {
    let rows = sqlx::query("SELECT original_revision,original_digest,parent_relations,disposition,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id,origin_generation,table_name,canonical_pk FROM offline_recovery_rows AS classification WHERE disposition IN ('eligible_new','eligible_preserved') AND NOT EXISTS (SELECT 1 FROM offline_recovery_workspace_deletions AS deletion WHERE deletion.table_name=classification.table_name AND deletion.canonical_pk=classification.canonical_pk)")
        .fetch_all(&mut *connection).await?;
    for row in rows {
        let encoded: String = row.try_get("parent_relations")?;
        let mut parents: Vec<ParentIdentity> =
            serde_json::from_str(&encoded).map_err(|_| refused())?;
        let mut changed = false;
        for parent in &mut parents {
            if parent.identity == mutation.identity {
                scope.context.matches_classification(&row)?;
                if parent.role != ParentRole::Effectful
                    || parent.revision != mutation.revision
                    || parent.digest != mutation.digest
                {
                    return Err(refused());
                }
                parent.revision = snapshot.revision.clone();
                parent.digest = snapshot.digest.clone();
                changed = true;
            }
        }
        if changed {
            let affected = sqlx::query("UPDATE offline_recovery_rows SET parent_relations=? WHERE table_name=? AND canonical_pk=? AND parent_relations=? AND original_revision=? AND original_digest=? AND disposition IN ('eligible_new','eligible_preserved')")
                .bind(serde_json::to_string(&parents).map_err(|_| refused())?)
                .bind(row.try_get::<&str, _>("table_name")?).bind(row.try_get::<&str, _>("canonical_pk")?)
                .bind(encoded).bind(row.try_get::<&str, _>("original_revision")?)
                .bind(row.try_get::<&str, _>("original_digest")?).execute(&mut *connection).await?;
            if affected.rows_affected() != 1 {
                return Err(refused());
            }
        }
    }
    Ok(())
}
