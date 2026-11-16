use sqlx::{Row, SqliteConnection, SqlitePool};

use super::{
    EffectDomain, Mutation, RecoveryScope, RowIdentity, ScopedTransaction, inventory, refused,
};
use crate::storage::RuntimeOwnerEvidence;

mod component_release;
mod stop_proof;
pub(crate) use component_release::release_component_lease;
pub(crate) use stop_proof::{finish_controlled_stop, stopping_runtime};

#[cfg(test)]
mod tests;

pub(crate) async fn is_refused(pool: &SqlitePool) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM matrix_recovery_control WHERE control_id=1 AND phase='refused' AND restart_hold_state='refused')")
        .fetch_one(pool).await
}

async fn require_current_process(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    token: &str,
    fingerprint: &str,
) -> Result<(), sqlx::Error> {
    let observation = RuntimeOwnerEvidence::capture().ok_or_else(refused)?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_instances r JOIN runtime_owner_evidence e USING(instance_token) WHERE r.instance_token=? AND r.state IN ('active','stopping') AND r.process_fingerprint=? AND e.is_current=1 AND e.state IN ('live','scan_pending') AND e.process_fingerprint=r.process_fingerprint AND e.pid=? AND e.proc_starttime=? AND e.boot_id=?")
        .bind(token).bind(fingerprint).bind(observation.pid).bind(observation.proc_starttime).bind(observation.boot_id).fetch_one(&mut *connection).await?;
    if count != 1 {
        return Err(refused());
    }
    let identity = super::text_identity("runtime_instances", token)?;
    prepare(connection, scope, &identity).await?;
    let revision = sqlx::query_scalar("SELECT evidence_revision FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1")
        .bind(token).fetch_one(&mut *connection).await?;
    let identity = evidence_identity(connection, token, revision).await?;
    prepare(connection, scope, &identity).await?;
    Ok(())
}

async fn evidence_identity(
    connection: &mut SqliteConnection,
    token: &str,
    revision: i64,
) -> Result<RowIdentity, sqlx::Error> {
    let key = sqlx::query_scalar("SELECT json_array('text:'||hex(CAST(instance_token AS BLOB)),'integer:'||hex(CAST(runtime_generation AS BLOB)),'integer:'||hex(CAST(evidence_revision AS BLOB))) FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
        .bind(token).bind(revision).fetch_one(connection).await?;
    Ok(RowIdentity {
        table: "runtime_owner_evidence".into(),
        key,
    })
}

async fn begin<'pool>(
    pool: &'pool SqlitePool,
    scope: &RecoveryScope,
) -> Result<ScopedTransaction<'pool>, sqlx::Error> {
    let guard = scope.serial.clone().lock_owned().await;
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let context = scope.context();
    if !super::authority::validate_identity(&mut transaction, &context).await? {
        return Err(refused());
    }
    let operation = context.matrix_operation().ok_or_else(refused)?;
    let reason: Option<String> = sqlx::query_scalar("SELECT c.restart_hold_reason FROM matrix_recovery_control c JOIN matrix_recovery_compatibility k ON k.native_operation_id=c.cutover_operation_id WHERE c.control_id=1 AND c.cutover_operation_id=? AND c.phase='refused' AND c.restart_hold_state='refused' AND c.watermark IS NOT NULL AND length(c.watermark)>0 AND k.decision='native'")
        .bind(operation).fetch_optional(&mut *transaction).await?;
    validate_native_refusal(&mut transaction, &context).await?;
    match reason.as_deref() {
        Some("invalid-or-unrecoverable-response") => {
            validate_refused_receipt(&mut transaction, scope).await?;
        }
        Some("rebuild-history-gap") => {
            // Rebuild never admits a response. Only a fully dispositioned old
            // stream qualifies for teardown; this does not grant effect authority.
            let unfinished: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM matrix_recovery_receipts WHERE state!='dispositioned') + (SELECT count(*) FROM matrix_recovery_batches WHERE state!='terminal')")
                .fetch_one(&mut *transaction).await?;
            if unfinished != 0 || scope.matrix_fence.get().is_none() {
                return Err(refused());
            }
        }
        _ => return Err(refused()),
    }
    validate_component_fences(&mut transaction, scope).await?;
    Ok(ScopedTransaction::new(
        transaction,
        Some(scope.revision.clone()),
        Some(guard),
    ))
}

async fn validate_component_fences(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
) -> Result<(), sqlx::Error> {
    for fence in scope
        .matrix_fence
        .get()
        .into_iter()
        .chain(scope.fence.iter())
    {
        let matching: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_component_leases WHERE component=? AND owner_token=? AND epoch=? AND state IN ('active','expired')")
            .bind(&fence.component).bind(&fence.owner).bind(fence.epoch).fetch_one(&mut *connection).await?;
        if matching != 1 {
            return Err(refused());
        }
        let identity = super::text_identity("matrix_component_leases", &fence.component)?;
        super::super::require_lineage(
            connection,
            &scope.context(),
            EffectDomain::Matrix,
            &identity,
        )
        .await?;
    }
    Ok(())
}

async fn validate_refused_receipt(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
) -> Result<(), sqlx::Error> {
    let fence = scope.matrix_fence.get().ok_or_else(refused)?;
    if fence.component != "MatrixSync" || fence.epoch <= 0 {
        return Err(refused());
    }
    let rows = sqlx::query("SELECT r.account_id,r.room_id,r.event_id,r.cutover_operation_id FROM matrix_recovery_receipts r JOIN matrix_recovery_batches b USING(batch_id) WHERE r.state='refused' AND r.refusal_code='missing-room-key-unrecoverable' AND r.cutover_operation_id=? AND b.component='MatrixSync' AND b.owner_token=? AND b.owner_epoch=? AND b.state='processing'")
        .bind(scope.context().matrix_operation()).bind(&fence.owner).bind(fence.epoch).fetch_all(&mut *connection).await?;
    if rows.is_empty() {
        return Err(refused());
    }
    for row in rows {
        let identity = super::composite_text_identity(
            "matrix_recovery_receipts",
            &[
                row.try_get(0)?,
                row.try_get(1)?,
                row.try_get(2)?,
                row.try_get(3)?,
            ],
        )?;
        let classification = sqlx::query("SELECT original_revision,original_digest,parent_relations,disposition,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id,origin_generation FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=?")
            .bind(&identity.table).bind(&identity.key).fetch_one(&mut *connection).await?;
        scope.context().matches_classification(&classification)?;
        let snapshot = inventory::snapshot(connection, &identity).await?;
        if classification.try_get::<&str, _>(0)? != snapshot.revision
            || classification.try_get::<&str, _>(1)? != snapshot.digest
            || classification.try_get::<&str, _>(3)? != "refused_hold"
            || classification.try_get::<Option<&str>, _>(8)? != scope.context().matrix_operation()
        {
            return Err(refused());
        }
        let parents: Vec<super::ParentIdentity> =
            serde_json::from_str(classification.try_get(2)?).map_err(|_| refused())?;
        if parents.len() != snapshot.parents.len() {
            return Err(refused());
        }
        for parent in parents {
            if parent.role != super::ParentRole::Effectful
                || !snapshot.parents.contains(&parent.identity)
            {
                return Err(refused());
            }
            super::super::require_lineage(
                connection,
                &scope.context(),
                EffectDomain::Matrix,
                &parent.identity,
            )
            .await?;
            let current = inventory::snapshot(connection, &parent.identity).await?;
            if current.revision != parent.revision || current.digest != parent.digest {
                return Err(refused());
            }
        }
    }
    Ok(())
}

async fn validate_native_refusal(
    connection: &mut SqliteConnection,
    context: &super::EligibilityContext,
) -> Result<(), sqlx::Error> {
    use sha2::{Digest, Sha256};
    if let super::EligibilityContext::NonAdmin { generation, .. } = context {
        let row = sqlx::query("SELECT x.expected_b_digest,x.compatibility_source_digest,c.watermark FROM runtime_eligibility_context x JOIN matrix_recovery_control c ON c.cutover_operation_id=x.compatibility_operation_id JOIN matrix_recovery_compatibility k ON k.native_operation_id=c.cutover_operation_id WHERE x.generation=? AND x.provenance_kind='validated_native' AND x.matrix_phase='active' AND k.source_state_digest=x.compatibility_source_digest")
            .bind(generation).fetch_optional(&mut *connection).await?.ok_or_else(refused)?;
        let watermark: String = row.try_get(2)?;
        if row.try_get::<String, _>(0)? != format!("{:x}", Sha256::digest(watermark.as_bytes()))
            || row.try_get::<String, _>(1)?
                != crate::storage::matrix_recovery_snapshot::snapshot(connection)
                    .await?
                    .digest
        {
            return Err(refused());
        }
    }
    Ok(())
}

async fn prepare(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    identity: &RowIdentity,
) -> Result<Mutation, sqlx::Error> {
    if !matches!(
        identity.table.as_str(),
        "runtime_instances" | "runtime_owner_evidence" | "matrix_component_leases"
    ) {
        return Err(refused());
    }
    super::super::require_lineage(connection, &scope.context(), EffectDomain::Matrix, identity)
        .await?;
    let row = sqlx::query("SELECT original_revision,original_digest,parent_relations FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=? AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(&identity.table).bind(&identity.key).fetch_one(connection).await?;
    Ok(Mutation {
        identity: identity.clone(),
        revision: row.try_get(0)?,
        digest: row.try_get(1)?,
        parents: row.try_get(2)?,
        released_lease: None,
    })
}

async fn complete(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    mutation: Mutation,
) -> Result<(), sqlx::Error> {
    let snapshot = inventory::snapshot(connection, &mutation.identity).await?;
    let changed = sqlx::query("UPDATE offline_recovery_rows SET original_revision=?,original_digest=? WHERE table_name=? AND canonical_pk=? AND original_revision=? AND original_digest=? AND parent_relations=? AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(&snapshot.revision).bind(&snapshot.digest).bind(&mutation.identity.table).bind(&mutation.identity.key)
        .bind(&mutation.revision).bind(&mutation.digest).bind(&mutation.parents).execute(&mut *connection).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    super::mutation::refresh_references(connection, scope, &mutation, &snapshot).await?;
    super::super::require_lineage(
        connection,
        &scope.context(),
        EffectDomain::Matrix,
        &mutation.identity,
    )
    .await
}

async fn inserted(
    connection: &mut SqliteConnection,
    scope: &RecoveryScope,
    identity: &RowIdentity,
) -> Result<(), sqlx::Error> {
    if identity.table != "runtime_owner_evidence" {
        return Err(refused());
    }
    let snapshot = inventory::snapshot(connection, identity).await?;
    let mut parents = Vec::new();
    for identity in &snapshot.parents {
        super::super::require_lineage(connection, &scope.context(), EffectDomain::Matrix, identity)
            .await?;
        let parent = inventory::snapshot(connection, identity).await?;
        parents.push(super::ParentIdentity {
            identity: identity.clone(),
            revision: parent.revision,
            digest: parent.digest,
            role: super::ParentRole::Effectful,
        });
    }
    let encoded = serde_json::to_string(&parents).map_err(|_| refused())?;
    super::insert_classification(connection, scope, identity, &snapshot, &encoded, true).await?;
    super::super::require_lineage(connection, &scope.context(), EffectDomain::Matrix, identity)
        .await
}
