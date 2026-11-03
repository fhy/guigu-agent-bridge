use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

use super::{
    Failure,
    reconciliation_records::{Action, Classification, Plan, RowImage},
};
use crate::storage::recovery_eligibility::{ParentIdentity, ParentRole, inventory};

pub(super) async fn validate_plan(
    connection: &mut SqliteConnection,
    plan: &Plan,
) -> Result<Vec<inventory::RowIdentity>, Failure> {
    validate_digests(plan)?;
    let identities = inventory::identities(connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
    if identities.len() != plan.classifications.len() {
        return Err(Failure::ManualRequired);
    }
    let expected = plan
        .classifications
        .iter()
        .map(|item| (item.table_name.clone(), item.canonical_pk.clone()))
        .collect::<BTreeSet<_>>();
    let actual = identities
        .iter()
        .map(|item| (item.table.clone(), item.key.clone()))
        .collect::<BTreeSet<_>>();
    if expected != actual || expected.len() != plan.classifications.len() {
        return Err(Failure::ManualRequired);
    }
    for classification in &plan.classifications {
        validate_classification(connection, plan, classification).await?;
    }
    validate_actions(plan)?;
    validate_decisions(connection, plan).await?;
    Ok(identities)
}

fn validate_digests(plan: &Plan) -> Result<(), Failure> {
    if plan.protocol != 2
        || plan.operation_id.is_empty()
        || plan.native_operation_id.is_empty()
        || !hex_digest(&plan.plan_digest)
        || !hex_digest(&plan.grant_digest)
        || !hex_digest(&plan.action_digest)
        || !hex_digest(&plan.classification_digest)
        || plan.original_binding.phase != "identity_ready"
        || plan.original_binding.revision < 0
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

async fn validate_classification(
    connection: &mut SqliteConnection,
    plan: &Plan,
    classification: &Classification,
) -> Result<(), Failure> {
    if classification.context_kind != "admin"
        || classification.context_generation.is_some()
        || classification.admin_operation_id.as_deref() != Some(plan.operation_id.as_str())
        || classification.plan_digest.as_deref() != Some(plan.plan_digest.as_str())
        || classification.origin_generation != plan.native_operation_id
        || classification.matrix_operation_id.as_deref() != Some(plan.native_operation_id.as_str())
        || !matches!(
            classification.disposition.as_str(),
            "terminal_evidenced" | "quarantined_no_effect"
        )
        || !hex_digest(&classification.evidence_digest)
    {
        return Err(Failure::ManualRequired);
    }
    let identity = inventory::RowIdentity {
        table: classification.table_name.clone(),
        key: classification.canonical_pk.clone(),
    };
    let snapshot = inventory::snapshot(connection, &identity)
        .await
        .map_err(|_| Failure::ManualRequired)?;
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
    let mut seen = BTreeSet::new();
    for parent in parents {
        if parent.role != ParentRole::Effectful
            || !seen.insert((parent.identity.table.clone(), parent.identity.key.clone()))
            || !snapshot.parents.contains(&parent.identity)
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
    Ok(())
}

fn validate_actions(plan: &Plan) -> Result<(), Failure> {
    let mut seen = BTreeSet::new();
    let identities = plan
        .classifications
        .iter()
        .chain(&plan.inserted_classifications)
        .map(|item| (item.table_name.as_str(), item.canonical_pk.as_str()))
        .collect::<BTreeSet<_>>();
    for action in &plan.actions {
        if action.action_id.is_empty()
            || !seen.insert(action.action_id.as_str())
            || !hex_digest(&action.original_digest)
            || !identities.contains(&(action.table_name.as_str(), action.canonical_pk.as_str()))
            || action.set_columns.iter().any(|column| {
                action
                    .unchanged_columns
                    .iter()
                    .any(|unchanged| unchanged == column)
            })
        {
            return Err(Failure::ManualRequired);
        }
        validate_image(action.before.as_ref())?;
        validate_image(action.after.as_ref())?;
        if action.action_type == "insert" && action.before.is_some()
            || action.action_type == "delete" && action.after.is_some()
        {
            return Err(Failure::ManualRequired);
        }
    }
    if seen.len() != plan.actions.len() {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

fn validate_image(image: Option<&RowImage>) -> Result<(), Failure> {
    if let Some(image) = image
        && (image.names.len() != image.values.len()
            || image.names.windows(2).any(|pair| pair[0] >= pair[1]))
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

async fn validate_decisions(connection: &mut SqliteConnection, plan: &Plan) -> Result<(), Failure> {
    let uncertain: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM execution_leases WHERE state='recovery_needed')
         + (SELECT count(*) FROM task_admissions WHERE state='recovery_needed')
         + (SELECT count(*) FROM task_continuations WHERE state='recovery_needed')
         + (SELECT count(*) FROM delivery_dispositions WHERE state='outcome_unknown')",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    if uncertain > 0 && plan.d1_unknown_chain.is_none() {
        return Err(Failure::ManualRequired);
    }
    let owner_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM offline_recovery_rows
         WHERE table_name='runtime_owner_evidence'",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    if owner_count == 126 && plan.d2_owner.len() != 126 {
        return Err(Failure::ManualRequired);
    }
    let quarantined = plan
        .classifications
        .iter()
        .filter(|item| item.disposition == "quarantined_no_effect")
        .count();
    if quarantined == 7
        && plan.d1_quarantined.is_none()
        && plan
            .d1_unknown_chain
            .as_ref()
            .is_none_or(|decision| decision.references.len() != 7)
    {
        return Err(Failure::ManualRequired);
    }
    for proof in &plan.d2_owner {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runtime_owner_evidence evidence
             JOIN runtime_instances instance USING(instance_token)
             WHERE evidence.instance_token=? AND evidence.runtime_generation=?
               AND evidence.evidence_revision=? AND evidence.state='reaped'
               AND instance.state='stopped'",
        )
        .bind(&proof.instance_token)
        .bind(proof.runtime_generation)
        .bind(proof.evidence_revision)
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        if count != 1 || !hex_digest(&proof.evidence_digest) || !hex_digest(&proof.parent_digest) {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

pub(super) async fn verify_classifications(
    connection: &mut SqliteConnection,
    plan: &Plan,
    operation: &str,
) -> Result<(), Failure> {
    let expected = plan
        .classifications
        .iter()
        .chain(&plan.inserted_classifications)
        .collect::<Vec<_>>();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM offline_recovery_rows
         WHERE context_kind='admin' AND admin_operation_id=? AND plan_digest=?",
    )
    .bind(operation)
    .bind(&plan.plan_digest)
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    if count != expected.len() as i64 {
        return Err(Failure::ManualRequired);
    }
    for item in expected {
        let row = sqlx::query(
            "SELECT origin_generation,original_revision,original_digest,parent_relations,
                    disposition,evidence_digest,context_kind,context_generation,
                    admin_operation_id,plan_digest,matrix_operation_id
             FROM offline_recovery_rows
             WHERE table_name=? AND canonical_pk=?",
        )
        .bind(&item.table_name)
        .bind(&item.canonical_pk)
        .fetch_optional(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?
        .ok_or(Failure::ManualRequired)?;
        if row
            .try_get::<String, _>("origin_generation")
            .map_err(|_| Failure::ManualRequired)?
            != item.origin_generation
            || row
                .try_get::<String, _>("original_revision")
                .map_err(|_| Failure::ManualRequired)?
                != item.original_revision
            || row
                .try_get::<String, _>("original_digest")
                .map_err(|_| Failure::ManualRequired)?
                != item.original_digest
            || row
                .try_get::<String, _>("parent_relations")
                .map_err(|_| Failure::ManualRequired)?
                != serde_json::to_string(&item.parent_relations).map_err(|_| Failure::Preflight)?
            || row
                .try_get::<String, _>("disposition")
                .map_err(|_| Failure::ManualRequired)?
                != item.disposition
            || row
                .try_get::<String, _>("evidence_digest")
                .map_err(|_| Failure::ManualRequired)?
                != item.evidence_digest
            || row
                .try_get::<String, _>("context_kind")
                .map_err(|_| Failure::ManualRequired)?
                != item.context_kind
            || row
                .try_get::<Option<String>, _>("context_generation")
                .map_err(|_| Failure::ManualRequired)?
                != item.context_generation
            || row
                .try_get::<Option<String>, _>("admin_operation_id")
                .map_err(|_| Failure::ManualRequired)?
                != item.admin_operation_id
            || row
                .try_get::<Option<String>, _>("plan_digest")
                .map_err(|_| Failure::ManualRequired)?
                != item.plan_digest
            || row
                .try_get::<Option<String>, _>("matrix_operation_id")
                .map_err(|_| Failure::ManualRequired)?
                != item.matrix_operation_id
        {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

pub(super) async fn graph_digest(
    connection: &mut SqliteConnection,
    plan: &Plan,
) -> Result<String, Failure> {
    let mut rows: Vec<serde_json::Value> = Vec::new();
    for identity in plan
        .classifications
        .iter()
        .chain(&plan.inserted_classifications)
    {
        let row = inventory::values(
            connection,
            &inventory::RowIdentity {
                table: identity.table_name.clone(),
                key: identity.canonical_pk.clone(),
            },
        )
        .await
        .map_err(|_| Failure::ManualRequired)?;
        rows.push(serde_json::json!([&identity.table_name, &identity.canonical_pk, row]));
    }
    let marker: Option<(String, String, String)> = sqlx::query_as(
        "SELECT state,committed_graph_digest,commit_digest FROM r093_reconciliation_marker WHERE singleton=1",
    )
    .fetch_optional(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    let audit_count: i64 = sqlx::query_scalar("SELECT count(*) FROM r093_reconciliation_audit")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
    let anchor_count: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_metadata_anchors")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
    rows.push(serde_json::json!(["__marker__", "singleton", marker]));
    rows.push(serde_json::json!(["__audit__", "count", audit_count]));
    rows.push(serde_json::json!(["__anchors__", "count", anchor_count]));
    rows.sort_by_key(|row| row[0].as_str().unwrap_or_default().to_owned());
    let bytes = serde_json::to_vec(&rows).map_err(|_| Failure::Preflight)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub(super) fn hex_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn action_images_equal(action: &Action, actual: &(Vec<String>, Vec<String>)) -> bool {
    action
        .before
        .as_ref()
        .is_some_and(|image| image.names == actual.0 && image.values == actual.1)
}

pub(super) async fn verify_actions(
    connection: &mut SqliteConnection,
    plan: &Plan,
) -> Result<(), Failure> {
    for action in &plan.actions {
        if action.action_type != "preserve" {
            continue;
        }
        let actual = inventory::values(
            connection,
            &inventory::RowIdentity {
                table: action.table_name.clone(),
                key: action.canonical_pk.clone(),
            },
        )
        .await
        .map_err(|_| Failure::ManualRequired)?;
        if !action_images_equal(action, &actual) {
            return Err(Failure::ManualRequired);
        }
        if action.action_type == "preserve" && action.after.as_ref() != action.before.as_ref() {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}
