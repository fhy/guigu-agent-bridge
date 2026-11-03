use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

use super::{
    Failure,
    reconciliation_records::{Classification, Plan, RowImage},
};
use crate::storage::recovery_eligibility::{ParentIdentity, ParentRole, inventory};

type AuditGraphRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreTransitionGraph {
    rows: BTreeSet<(String, String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedPostGraph {
    rows: BTreeSet<(String, String, String)>,
}

impl PreTransitionGraph {
    pub(super) async fn capture(connection: &mut SqliteConnection) -> Result<Self, Failure> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT table_name,canonical_pk,original_digest FROM offline_recovery_rows ORDER BY table_name,canonical_pk",
        )
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        Ok(Self {
            rows: rows.into_iter().collect(),
        })
    }
}

impl ExpectedPostGraph {
    fn from_pre_state(pre: &PreTransitionGraph) -> Self {
        Self {
            rows: pre.rows.clone(),
        }
    }

    fn matches(&self, actual: &BTreeSet<(String, String, String)>) -> bool {
        &self.rows == actual
    }
}

type ControlRow = (i64, String, String, Option<String>);

fn expected_compatibility(original: &str, operation: &str) -> Result<serde_json::Value, Failure> {
    let mut value: serde_json::Value =
        serde_json::from_str(original).map_err(|_| Failure::ManualRequired)?;
    let fields = value.as_array_mut().ok_or(Failure::ManualRequired)?;
    if fields.len() != 15 {
        return Err(Failure::ManualRequired);
    }
    let was_held = fields[1] == "held";
    fields[1] = serde_json::Value::String("native".into());
    fields[12] = serde_json::Value::String(operation.into());
    fields[13] = serde_json::Value::String(
        if was_held {
            "administrative-transition"
        } else {
            "pristine-native"
        }
        .into(),
    );
    Ok(value)
}

fn verify_compatibility(actual: &str, original: &str, operation: &str) -> Result<(), Failure> {
    let actual: serde_json::Value =
        serde_json::from_str(actual).map_err(|_| Failure::ManualRequired)?;
    let expected = expected_compatibility(original, operation)?;
    if actual != expected {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

fn verify_controls(actual: &[ControlRow], operation: &str) -> Result<(), Failure> {
    if actual != [(1, operation.into(), "baseline".into(), None)] {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

fn verify_inventory(
    actual: &BTreeSet<(String, String)>,
    expected: &BTreeSet<(String, String)>,
) -> Result<(), Failure> {
    if actual != expected {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

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
    if uncertain > 0
        && plan.d1_unknown_chain.as_ref().is_none_or(|decision| {
            decision.decision_id.is_empty() || decision.references.is_empty()
        })
    {
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
        && plan.d1_quarantined.as_ref().is_none_or(|decision| {
            decision.decision_id.is_empty() || decision.references.len() != 7
        })
    {
        return Err(Failure::ManualRequired);
    }
    if plan.d1_unknown_chain.as_ref().is_some_and(|chain| {
        plan.d1_quarantined
            .as_ref()
            .is_some_and(|quarantine| chain.decision_id == quarantine.decision_id)
    }) {
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
        rows.push(serde_json::json!([
            &identity.table_name,
            &identity.canonical_pk,
            row
        ]));
    }
    let audit_rows: Vec<AuditGraphRow> = sqlx::query_as(
        "SELECT operation_id,action_id,action_type,table_name,canonical_pk,pre_digest,post_digest,evidence_digest,original_digest
         FROM r093_reconciliation_audit ORDER BY operation_id,action_id",
    ).fetch_all(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
    let anchor_rows: Vec<(String, String, String, String, String, String)> = sqlx::query_as(
        "SELECT table_name,canonical_pk,role,db_generation,root_digest,plan_digest
         FROM offline_recovery_metadata_anchors ORDER BY table_name,canonical_pk",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?
    .into_iter()
    .collect();
    let receipt_rows: Vec<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT table_name,canonical_pk,receipt_digest,runtime_owner,task_id
         FROM offline_recovery_workspace_deletions ORDER BY table_name,canonical_pk",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?
    .into_iter()
    .collect();
    rows.push(serde_json::json!(["__audit__", audit_rows]));
    rows.push(serde_json::json!(["__anchors__", anchor_rows]));
    rows.push(serde_json::json!(["__receipts__", receipt_rows]));
    rows.sort_by_key(|row| row[0].as_str().unwrap_or_default().to_owned());
    let bytes = serde_json::to_vec(&rows).map_err(|_| Failure::Preflight)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub(super) async fn verify_expected_graph(
    connection: &mut SqliteConnection,
    plan: &Plan,
    pre_state: Option<&PreTransitionGraph>,
) -> Result<(), Failure> {
    if let Some(pre_state) = pre_state {
        let expected_graph = ExpectedPostGraph::from_pre_state(pre_state);
        let current = PreTransitionGraph::capture(connection).await?;
        let unchanged = plan
            .actions
            .iter()
            .all(|action| action.action_type != "preserve");
        if unchanged && !pre_state.rows.is_empty() && !expected_graph.matches(&current.rows) {
            return Err(Failure::ManualRequired);
        }
    }
    if plan.native_operation_id.is_empty() {
        return Err(Failure::ManualRequired);
    }
    let original: Option<String> = sqlx::query_scalar(
        "SELECT json_array(singleton,decision,source_schema_version,source_state_digest,legacy_operation_id,legacy_phase,legacy_baseline,legacy_baseline_digest,legacy_watermark,legacy_revision,legacy_binding,legacy_archive_digest,native_operation_id,reason_code,created_at) FROM matrix_recovery_compatibility WHERE singleton=1",
    )
    .fetch_optional(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    let original = original.ok_or(Failure::ManualRequired)?;
    let actual_compatibility: String = sqlx::query_scalar(
        "SELECT json_array(singleton,decision,source_schema_version,source_state_digest,legacy_operation_id,legacy_phase,legacy_baseline,legacy_baseline_digest,legacy_watermark,legacy_revision,legacy_binding,legacy_archive_digest,native_operation_id,reason_code,created_at) FROM matrix_recovery_compatibility WHERE singleton=1",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    verify_compatibility(&actual_compatibility, &original, &plan.native_operation_id)?;
    let controls: Vec<ControlRow> = sqlx::query_as(
        "SELECT control_id,cutover_operation_id,phase,watermark FROM matrix_recovery_control ORDER BY control_id",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    verify_controls(&controls, &plan.native_operation_id)?;
    let mut allowed = plan
        .classifications
        .iter()
        .chain(&plan.inserted_classifications)
        .map(|item| (item.table_name.clone(), item.canonical_pk.clone()))
        .collect::<BTreeSet<_>>();
    allowed.insert(("matrix_recovery_control".into(), "[\"integer:31\"]".into()));
    let mut expected = BTreeSet::new();
    for action in &plan.actions {
        let key = (action.table_name.clone(), action.canonical_pk.clone());
        if !allowed.contains(&key) || !expected.insert(key.clone()) {
            return Err(Failure::ManualRequired);
        }
        let actual = inventory::values(
            connection,
            &inventory::RowIdentity {
                table: key.0.clone(),
                key: key.1.clone(),
            },
        )
        .await;
        let expected_image = match action.action_type.as_str() {
            "insert" => action.after.as_ref(),
            "update" | "preserve" => action.after.as_ref().or(action.before.as_ref()),
            "delete" => None,
            _ => return Err(Failure::ManualRequired),
        };
        match (action.action_type.as_str(), expected_image, actual) {
            ("delete", None, Err(_)) => {
                let receipt_count: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM offline_recovery_workspace_deletions
                     WHERE canonical_pk=? AND snapshot_json IS NOT NULL",
                )
                .bind(&key.1)
                .fetch_one(&mut *connection)
                .await
                .map_err(|_| Failure::ManualRequired)?;
                if receipt_count != 1 {
                    return Err(Failure::ManualRequired);
                }
            }
            (_, Some(image), Ok(actual)) if image.names == actual.0 && image.values == actual.1 => {
            }
            ("delete", _, Ok(_)) | (_, Some(_), Err(_)) => return Err(Failure::ManualRequired),
            _ => {}
        }
    }
    let actual = inventory::identities(connection)
        .await
        .map_err(|_| Failure::ManualRequired)?
        .into_iter()
        .map(|identity| (identity.table, identity.key))
        .collect::<BTreeSet<_>>();
    verify_inventory(&actual, &allowed)?;
    for classification in plan
        .classifications
        .iter()
        .chain(&plan.inserted_classifications)
    {
        let identity = inventory::RowIdentity {
            table: classification.table_name.clone(),
            key: classification.canonical_pk.clone(),
        };
        let snapshot = inventory::snapshot(connection, &identity)
            .await
            .map_err(|_| Failure::ManualRequired)?;
        let parents =
            serde_json::from_value::<Vec<ParentIdentity>>(classification.parent_relations.clone())
                .map_err(|_| Failure::ManualRequired)?;
        if parents.len() != snapshot.parents.len()
            || parents
                .iter()
                .any(|parent| !snapshot.parents.contains(&parent.identity))
        {
            return Err(Failure::ManualRequired);
        }
        for parent in parents {
            let parent_snapshot = inventory::snapshot(connection, &parent.identity)
                .await
                .map_err(|_| Failure::ManualRequired)?;
            if parent_snapshot.revision != parent.revision
                || parent_snapshot.digest != parent.digest
                || parent.role != ParentRole::Effectful
            {
                return Err(Failure::ManualRequired);
            }
        }
    }
    let expected_receipts = plan
        .workspaces
        .iter()
        .map(|workspace| {
            (
                "workspace_claims".to_owned(),
                workspace.canonical_pk.clone(),
                workspace.receipt_digest.clone(),
                workspace.runtime_owner.clone(),
                workspace.task_id.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    let actual_receipts: BTreeSet<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT table_name,canonical_pk,receipt_digest,runtime_owner,task_id
         FROM offline_recovery_workspace_deletions ORDER BY table_name,canonical_pk",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?
    .into_iter()
    .collect();
    if actual_receipts != expected_receipts {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

pub(super) fn hex_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) async fn verify_actions(
    connection: &mut SqliteConnection,
    plan: &Plan,
) -> Result<(), Failure> {
    for action in &plan.actions {
        let actual = inventory::values(
            connection,
            &inventory::RowIdentity {
                table: action.table_name.clone(),
                key: action.canonical_pk.clone(),
            },
        )
        .await
        .map_err(|_| Failure::ManualRequired)?;
        let expected = match action.action_type.as_str() {
            "preserve" => action.before.as_ref(),
            "update" => action.after.as_ref(),
            _ => None,
        };
        if expected.is_some()
            && !expected.is_some_and(|image| image.names == actual.0 && image.values == actual.1)
        {
            return Err(Failure::ManualRequired);
        }
        if action.action_type == "preserve" && action.after.as_ref() != action.before.as_ref() {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn original(decision: &str) -> String {
        serde_json::json!([
            1,
            decision,
            24,
            "source",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "pristine-native",
            "created"
        ])
        .to_string()
    }

    #[test]
    fn rejects_wrong_native_compatibility_value() {
        let expected = expected_compatibility(&original("native"), "operation").unwrap();
        let mut wrong = expected.clone();
        wrong[1] = serde_json::Value::String("held".into());
        assert!(matches!(
            verify_compatibility(&wrong.to_string(), &original("native"), "operation"),
            Err(Failure::ManualRequired)
        ));
    }

    #[test]
    fn rejects_extra_native_control_row() {
        let controls = vec![
            (1, "operation".into(), "baseline".into(), None),
            (2, "other".into(), "baseline".into(), None),
        ];
        assert!(matches!(
            verify_controls(&controls, "operation"),
            Err(Failure::ManualRequired)
        ));
    }

    #[test]
    fn rejects_unrelated_inventory_row() {
        let expected = BTreeSet::from([(String::from("agents"), String::from("agent-1"))]);
        let actual = BTreeSet::from([
            (String::from("agents"), String::from("agent-1")),
            (String::from("agents"), String::from("unrelated")),
        ]);
        assert!(matches!(
            verify_inventory(&actual, &expected),
            Err(Failure::ManualRequired)
        ));
    }
}
