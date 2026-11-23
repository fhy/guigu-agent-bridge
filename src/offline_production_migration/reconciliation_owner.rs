use std::collections::BTreeSet;

use sqlx::{Row, SqliteConnection};

use super::{
    Failure,
    reconciliation_records::{OwnerProof, Plan, RowImage},
};
use crate::storage::recovery_eligibility::inventory;

pub(super) async fn validate(
    connection: &mut SqliteConnection,
    plan: &Plan,
) -> Result<(), Failure> {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT instance_token,runtime_generation,evidence_revision FROM runtime_owner_evidence",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    let actual = rows.into_iter().collect::<BTreeSet<_>>();
    let expected = plan
        .d2_owner
        .iter()
        .map(|proof| {
            (
                proof.instance_token.clone(),
                proof.runtime_generation,
                proof.evidence_revision,
            )
        })
        .collect::<BTreeSet<_>>();
    if actual != expected || expected.len() != plan.d2_owner.len() {
        return Err(Failure::ManualRequired);
    }
    for proof in &plan.d2_owner {
        validate_one(connection, plan, proof).await?;
    }
    Ok(())
}

async fn validate_one(
    connection: &mut SqliteConnection,
    plan: &Plan,
    proof: &OwnerProof,
) -> Result<(), Failure> {
    let row = sqlx::query(
        "SELECT evidence.state,instance.state AS parent_state,
                evidence.process_fingerprint,instance.process_fingerprint AS parent_fingerprint,
                json_array(typeof(evidence.instance_token)||':'||hex(CAST(evidence.instance_token AS BLOB)),
                    typeof(runtime_generation)||':'||hex(CAST(runtime_generation AS BLOB)),
                    typeof(evidence_revision)||':'||hex(CAST(evidence_revision AS BLOB))) AS owner_key,
                json_array(typeof(instance.instance_token)||':'||hex(CAST(instance.instance_token AS BLOB))) AS parent_key
         FROM runtime_owner_evidence evidence JOIN runtime_instances instance USING(instance_token)
         WHERE evidence.instance_token=? AND runtime_generation=? AND evidence_revision=?",
    ).bind(&proof.instance_token).bind(proof.runtime_generation).bind(proof.evidence_revision)
        .fetch_optional(&mut *connection).await.map_err(|_| Failure::ManualRequired)?
        .ok_or(Failure::ManualRequired)?;
    let fingerprint: String = row.get("process_fingerprint");
    if !matches!(row.get::<&str, _>("state"), "live" | "reaped")
        || row.get::<&str, _>("parent_state") != "stopped"
        || fingerprint.trim().is_empty()
        || fingerprint != row.get::<String, _>("parent_fingerprint")
    {
        return Err(Failure::ManualRequired);
    }
    let owner = inventory::RowIdentity {
        table: "runtime_owner_evidence".into(),
        key: row.get("owner_key"),
    };
    let parent = inventory::RowIdentity {
        table: "runtime_instances".into(),
        key: row.get("parent_key"),
    };
    let snapshot = inventory::snapshot(connection, &owner)
        .await
        .map_err(|_| Failure::ManualRequired)?;
    let parent_snapshot = inventory::snapshot(connection, &parent)
        .await
        .map_err(|_| Failure::ManualRequired)?;
    if snapshot.digest != proof.evidence_digest
        || parent_snapshot.digest != proof.parent_digest
        || snapshot.parents != vec![parent.clone()]
    {
        return Err(Failure::ManualRequired);
    }
    validate_preservation(connection, plan, &owner).await?;
    validate_preservation(connection, plan, &parent).await
}

async fn validate_preservation(
    connection: &mut SqliteConnection,
    plan: &Plan,
    identity: &inventory::RowIdentity,
) -> Result<(), Failure> {
    let classification = plan
        .classifications
        .iter()
        .find(|item| item.table_name == identity.table && item.canonical_pk == identity.key)
        .ok_or(Failure::ManualRequired)?;
    let action = plan
        .actions
        .iter()
        .find(|item| item.table_name == identity.table && item.canonical_pk == identity.key)
        .ok_or(Failure::ManualRequired)?;
    let (names, values) = inventory::values(connection, identity)
        .await
        .map_err(|_| Failure::ManualRequired)?;
    let image = RowImage { names, values };
    let digest = super::records::encoded_digest(&(&image.names, &image.values))?;
    if classification.disposition != "terminal_evidenced"
        || classification.original_digest != digest
        || action.action_type != "preserve"
        || action.before.as_ref() != Some(&image)
        || action.after.as_ref() != Some(&image)
        || !action.set_columns.is_empty()
        || action.unchanged_columns != image.names
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}
