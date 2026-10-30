use sqlx::{Row, SqliteConnection};

use super::{Failure, transition::Plan};
use crate::storage::recovery_eligibility::{ParentIdentity, ParentRole, inventory};

pub(super) async fn validate(
    connection: &mut SqliteConnection,
    plan: &Plan,
) -> Result<(), Failure> {
    let identities = inventory::identities(connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
    if identities.len() != plan.classifications.len() {
        return Err(Failure::ManualRequired);
    }
    let mut seen = std::collections::BTreeSet::new();
    for classification in &plan.classifications {
        let identity = inventory::RowIdentity {
            table: classification.table_name.clone(),
            key: classification.canonical_pk.clone(),
        };
        if !seen.insert((identity.table.clone(), identity.key.clone()))
            || !identities.contains(&identity)
            || classification.origin_generation != plan.native_operation_id
            || classification.matrix_operation_id != plan.native_operation_id
            || !matches!(
                classification.disposition.as_str(),
                "terminal_evidenced" | "quarantined_no_effect"
            )
            || classification.evidence_digest.len() != 64
            || !classification
                .evidence_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(Failure::ManualRequired);
        }
        let snapshot = inventory::snapshot(connection, &identity)
            .await
            .map_err(|_| Failure::ManualRequired)?;
        let (names, values) = inventory::values(connection, &identity)
            .await
            .map_err(|_| Failure::ManualRequired)?;
        let disposition =
            crate::storage::recovery_eligibility::bootstrap::lineage::row_disposition(
                connection, &identity, &names, &values,
            )
            .await
            .map_err(|_| Failure::ManualRequired)?;
        if disposition == "eligible_preserved"
            && classification.disposition != "quarantined_no_effect"
        {
            return Err(Failure::ManualRequired);
        }
        if snapshot.revision != classification.original_revision
            || snapshot.digest != classification.original_digest
        {
            return Err(Failure::ManualRequired);
        }
        let parents: Vec<ParentIdentity> =
            serde_json::from_value(classification.parent_relations.clone())
                .map_err(|_| Failure::ManualRequired)?;
        if parents.len() != snapshot.parents.len() {
            return Err(Failure::ManualRequired);
        }
        let mut seen_parents = std::collections::BTreeSet::new();
        for parent in parents {
            if !seen_parents.insert((parent.identity.table.clone(), parent.identity.key.clone()))
                || !snapshot.parents.contains(&parent.identity)
                || parent.role != ParentRole::Effectful
            {
                return Err(Failure::ManualRequired);
            }
            let actual = inventory::snapshot(connection, &parent.identity)
                .await
                .map_err(|_| Failure::ManualRequired)?;
            if actual.revision != parent.revision || actual.digest != parent.digest {
                return Err(Failure::ManualRequired);
            }
        }
    }
    Ok(())
}

pub(super) async fn anchors(
    connection: &mut SqliteConnection,
    plan: &Plan,
    operation: &str,
) -> Result<(), Failure> {
    for classification in &plan.classifications {
        if matches!(
            classification.table_name.as_str(),
            "agents" | "conversations"
        ) {
            let changed = sqlx::query("INSERT INTO offline_recovery_metadata_anchors(table_name,canonical_pk,role,db_generation,root_digest,admin_operation_id,plan_digest,original_revision,original_digest) SELECT ?,?,'metadata_anchor',db_generation,root_digest,admin_operation_id,plan_digest,?,? FROM offline_recovery_binding WHERE singleton=1 AND phase='transition_pending' AND admin_operation_id=? AND plan_digest=?")
                .bind(&classification.table_name).bind(&classification.canonical_pk)
                .bind(&classification.original_revision).bind(&classification.original_digest)
                .bind(operation).bind(&plan.plan_digest).execute(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
            if changed.rows_affected() != 1 {
                return Err(Failure::ManualRequired);
            }
        }
    }
    Ok(())
}

pub(super) async fn readback(
    connection: &mut SqliteConnection,
    plan: &Plan,
    operation: &str,
    active: bool,
) -> Result<(), Failure> {
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_rows")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
    if !active && count != plan.classifications.len() as i64 {
        return Err(Failure::ManualRequired);
    }
    for expected in &plan.classifications {
        let row = sqlx::query("SELECT * FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=? AND context_kind='admin' AND admin_operation_id=? AND plan_digest=? AND context_generation IS NULL")
            .bind(&expected.table_name).bind(&expected.canonical_pk).bind(operation).bind(&plan.plan_digest)
            .fetch_one(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
        for (column, value) in [
            ("origin_generation", &expected.origin_generation),
            ("original_revision", &expected.original_revision),
            ("original_digest", &expected.original_digest),
            ("disposition", &expected.disposition),
            ("evidence_digest", &expected.evidence_digest),
            ("matrix_operation_id", &expected.matrix_operation_id),
        ] {
            if row
                .try_get::<&str, _>(column)
                .map_err(|_| Failure::ManualRequired)?
                != value
            {
                return Err(Failure::ManualRequired);
            }
        }
        let parents: serde_json::Value = serde_json::from_str(
            row.try_get("parent_relations")
                .map_err(|_| Failure::ManualRequired)?,
        )
        .map_err(|_| Failure::ManualRequired)?;
        let identity = inventory::RowIdentity {
            table: expected.table_name.clone(),
            key: expected.canonical_pk.clone(),
        };
        let snapshot = inventory::snapshot(connection, &identity)
            .await
            .map_err(|_| Failure::ManualRequired)?;
        if parents != expected.parent_relations
            || snapshot.revision != expected.original_revision
            || snapshot.digest != expected.original_digest
        {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}
