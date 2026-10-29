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
}

pub(crate) async fn prepare_mutation(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    identity: &RowIdentity,
) -> Result<Option<Mutation>, sqlx::Error> {
    let Some(scope) = scope else {
        return Ok(None);
    };
    require_row(
        connection,
        &scope.context(),
        scope.domain,
        scope.fence.as_ref(),
        identity,
    )
    .await?;
    let row = sqlx::query("SELECT original_revision,original_digest,parent_relations FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=? AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(&identity.table).bind(&identity.key).fetch_optional(connection).await?.ok_or_else(refused)?;
    Ok(Some(Mutation {
        identity: identity.clone(),
        revision: row.try_get(0)?,
        digest: row.try_get(1)?,
        parents: row.try_get(2)?,
    }))
}

pub(crate) async fn complete_mutation(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    mutation: Option<Mutation>,
) -> Result<(), sqlx::Error> {
    let (Some(scope), Some(mutation)) = (scope, mutation) else {
        return Ok(());
    };
    authority::validate(
        connection,
        &scope.context(),
        scope.domain,
        scope.fence.as_ref(),
    )
    .await?;
    let snapshot = inventory::snapshot(connection, &mutation.identity).await?;
    let declared: Vec<ParentIdentity> =
        serde_json::from_str(&mutation.parents).map_err(|_| refused())?;
    if declared.len() != snapshot.parents.len()
        || snapshot
            .parents
            .iter()
            .any(|parent| !declared.iter().any(|old| old.identity == *parent))
    {
        return Err(refused());
    }
    let affected = sqlx::query("UPDATE offline_recovery_rows SET original_revision=?,original_digest=? WHERE table_name=? AND canonical_pk=? AND original_revision=? AND original_digest=? AND parent_relations=? AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(&snapshot.revision).bind(&snapshot.digest).bind(&mutation.identity.table)
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

async fn refresh_references(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    mutation: &Mutation,
    snapshot: &inventory::Snapshot,
) -> Result<(), sqlx::Error> {
    let rows = sqlx::query("SELECT original_revision,original_digest,parent_relations,disposition,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id,origin_generation,table_name,canonical_pk FROM offline_recovery_rows WHERE disposition IN ('eligible_new','eligible_preserved')")
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
