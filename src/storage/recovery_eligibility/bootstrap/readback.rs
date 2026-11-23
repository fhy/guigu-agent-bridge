use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

use super::diagnostics::{refusal, stage};
use super::{EligibilityContext, Native, ParentIdentity, inventory as snapshots, refused};

pub(super) async fn context(
    connection: &mut SqliteConnection,
    enabled: bool,
    native: &Native,
) -> Result<EligibilityContext, sqlx::Error> {
    let row = sqlx::query("SELECT context.* FROM runtime_eligibility_context AS context JOIN runtime_eligibility_bootstrap AS bootstrap ON bootstrap.generation=context.generation AND bootstrap.bootstrap_nonce=context.bootstrap_nonce AND bootstrap.inventory_digest=context.bootstrap_inventory_digest WHERE context.singleton=1 AND context.state='ready' AND bootstrap.state='complete'")
        .fetch_optional(&mut *connection).await?.ok_or_else(refused)?;
    let stored_operation: Option<String> = row.try_get("compatibility_operation_id")?;
    let source: Option<String> = row.try_get("compatibility_source_digest")?;
    let phase: String = row.try_get("matrix_phase")?;
    let watermark: Option<String> = row.try_get("expected_b_digest")?;
    let expected_watermark = native
        .watermark
        .as_ref()
        .map(|token| format!("{:x}", Sha256::digest(token.as_bytes())));
    if stored_operation != native.operation
        || source != native.source
        || phase != native.phase
        || watermark != expected_watermark
    {
        tracing::debug!(
            phase = "bootstrap_nonadmin",
            stage = "context",
            operation_matches = stored_operation == native.operation,
            source_matches = source == native.source,
            phase_matches = phase == native.phase,
            watermark_matches = watermark == expected_watermark,
            "bootstrap context comparison"
        );
        return Err(refusal("context", "native-binding"));
    }
    let generation: String = row.try_get("generation")?;
    let mut revision: i64 = row.try_get("revision")?;
    if row.try_get::<bool, _>("matrix_enabled")? != enabled {
        let changed = sqlx::query("UPDATE runtime_eligibility_context SET matrix_enabled=?,revision=revision+1 WHERE singleton=1 AND generation=? AND revision=? AND state='ready'")
            .bind(enabled).bind(&generation).bind(revision).execute(connection).await?;
        if changed.rows_affected() != 1 {
            return Err(refused());
        }
        revision += 1;
    }
    Ok(EligibilityContext::NonAdmin {
        generation,
        revision,
        matrix_enabled: enabled,
        matrix_operation: stored_operation,
    })
}

pub(crate) async fn inventory(
    connection: &mut SqliteConnection,
    context: &EligibilityContext,
) -> Result<(), sqlx::Error> {
    let identities = snapshots::identities(connection)
        .await
        .map_err(|error| stage("identities", error))?;
    let deleted = super::super::deletion::validate(connection, context, &identities)
        .await
        .map_err(|error| stage("deletions", error))?;
    let rows = sqlx::query("SELECT original_revision,original_digest,parent_relations,disposition,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id,origin_generation,table_name,canonical_pk FROM offline_recovery_rows ORDER BY table_name,canonical_pk")
        .fetch_all(&mut *connection).await?;
    if rows.len() != identities.len() + deleted.len() {
        tracing::debug!(
            phase = "bootstrap_nonadmin",
            stage = "inventory",
            live = identities.len(),
            deleted = deleted.len(),
            classified = rows.len(),
            "bootstrap inventory counts"
        );
        return Err(refusal("inventory", "classification-cardinality"));
    }
    let indexed = rows
        .iter()
        .map(|r| {
            (
                (
                    r.get::<&str, _>("table_name"),
                    r.get::<&str, _>("canonical_pk"),
                ),
                r,
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut live = std::collections::BTreeMap::new();
    // This helper performs no writes: snapshot every row once on this connection.
    // Parent checks use that same inventory, not a cross-transaction/global cache.
    for id in &identities {
        let snapshot = snapshots::snapshot(connection, id)
            .await
            .map_err(|e| stage("snapshot", e))?;
        live.insert((id.table.as_str(), id.key.as_str()), snapshot);
    }
    for identity in &deleted {
        let row = *indexed
            .get(&(identity.table.as_str(), identity.key.as_str()))
            .ok_or_else(refused)?;
        context
            .matches_classification(row)
            .map_err(|error| stage("deleted-classification", error))?;
        if row
            .try_get::<Option<&str>, _>("matrix_operation_id")?
            .is_some_and(|operation| Some(operation) != context.matrix_operation())
        {
            return Err(refused());
        }
    }
    for identity in &identities {
        let row = *indexed
            .get(&(identity.table.as_str(), identity.key.as_str()))
            .ok_or_else(refused)?;
        context
            .matches_classification(row)
            .map_err(|error| stage("classification", error))?;
        if matches!(
            row.try_get::<&str, _>("disposition")?,
            "uncertain_hold" | "refused_hold"
        ) {
            return Err(refusal("classification", "held-disposition"));
        }
        let snapshot = live
            .get(&(identity.table.as_str(), identity.key.as_str()))
            .ok_or_else(refused)?;
        if row.try_get::<&str, _>("original_revision")? != snapshot.revision
            || row.try_get::<&str, _>("original_digest")? != snapshot.digest
        {
            tracing::debug!(
                phase = "bootstrap_nonadmin",
                stage = "snapshot",
                table = identity.table,
                revision_matches =
                    row.try_get::<&str, _>("original_revision")? == snapshot.revision,
                digest_matches = row.try_get::<&str, _>("original_digest")? == snapshot.digest,
                "bootstrap snapshot comparison"
            );
            return Err(refusal("snapshot", "row-binding"));
        }
        let parents: Vec<ParentIdentity> =
            serde_json::from_str(row.try_get("parent_relations")?).map_err(|_| refused())?;
        if parents.len() != snapshot.parents.len() {
            return Err(refusal("parents", "cardinality"));
        }
        for parent in parents {
            if parent.role != super::ParentRole::Effectful
                || !snapshot.parents.contains(&parent.identity)
            {
                return Err(refusal("parents", "role-or-identity"));
            }
            let actual = live
                .get(&(parent.identity.table.as_str(), parent.identity.key.as_str()))
                .ok_or_else(refused)?;
            if actual.revision != parent.revision || actual.digest != parent.digest {
                return Err(refusal("parents", "row-binding"));
            }
        }
        if snapshot.matrix
            && row.try_get::<Option<&str>, _>("matrix_operation_id")? != context.matrix_operation()
        {
            return Err(refused());
        }
    }
    Ok(())
}
