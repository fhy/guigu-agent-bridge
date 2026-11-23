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
    String,
);
type AnchorGraphRow = (
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
type ReceiptGraphRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
);
type ClassificationGraphRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreTransitionGraph {
    rows: BTreeSet<(String, String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ExpectedPostGraph {
    rows: BTreeSet<(String, String, String)>,
    intent_digest: String,
}

type GraphRows = BTreeSet<(String, String, String)>;

impl PreTransitionGraph {
    pub(super) async fn capture(connection: &mut SqliteConnection) -> Result<Self, Failure> {
        let mut rows = BTreeSet::new();
        Self::capture_binding(&mut rows, connection).await?;
        Self::capture_bookkeeping(&mut rows, connection).await?;
        Self::capture_images(&mut rows, connection).await?;
        Self::capture_native(&mut rows, connection).await?;
        Ok(Self { rows })
    }

    async fn capture_binding(
        rows: &mut GraphRows,
        connection: &mut SqliteConnection,
    ) -> Result<(), Failure> {
        let binding_identity: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT singleton,protocol FROM offline_recovery_binding ORDER BY singleton",
        )
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        rows.insert((
            "binding_identity".into(),
            "set".into(),
            serde_json::to_string(&binding_identity).map_err(|_| Failure::Preflight)?,
        ));
        let binding = super::reconciliation::load_binding(connection).await?;
        rows.insert((
            "binding_cells".into(),
            "1".into(),
            serde_json::to_string(&binding).map_err(|_| Failure::Preflight)?,
        ));
        let inventory: Vec<(String, String, String)> = sqlx::query_as("SELECT table_name,canonical_pk,original_digest FROM offline_recovery_rows ORDER BY table_name,canonical_pk").fetch_all(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
        rows.extend(
            inventory
                .into_iter()
                .map(|(table, key, digest)| ("inventory".into(), format!("{table}:{key}"), digest)),
        );
        let binding: Vec<BindingGraphRow> = sqlx::query_as("SELECT singleton,phase,admin_operation_id,revision,plan_digest,transition_intent_digest,transition_complete_digest FROM offline_recovery_binding ORDER BY singleton").fetch_all(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
        for row in binding {
            rows.insert((
                "binding".into(),
                row.0.to_string(),
                serde_json::to_string(&row).map_err(|_| Failure::Preflight)?,
            ));
        }
        let control: Vec<String> = sqlx::query_scalar("SELECT json_array(control_id,cutover_operation_id,phase,watermark,updated_at,restart_hold_state,restart_hold_event_id,restart_hold_reason) FROM matrix_recovery_control ORDER BY control_id").fetch_all(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
        for row in control {
            rows.insert(("control".into(), "1".into(), row));
        }
        Ok(())
    }

    async fn capture_bookkeeping(
        rows: &mut GraphRows,
        connection: &mut SqliteConnection,
    ) -> Result<(), Failure> {
        let has_audit: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='r093_reconciliation_audit'").fetch_one(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
        let audit: Vec<AuditGraphRow> = if has_audit == 0 {
            Vec::new()
        } else {
            sqlx::query_as("SELECT operation_id,action_id,action_type,table_name,canonical_pk,pre_digest,post_digest,evidence_digest,original_digest,original_revision FROM r093_reconciliation_audit ORDER BY operation_id,action_id").fetch_all(&mut *connection).await.map_err(|_| Failure::ManualRequired)?
        };
        for row in audit {
            rows.insert((
                "audit".into(),
                format!("{}:{}", row.0, row.1),
                serde_json::to_string(&row).map_err(|_| Failure::Preflight)?,
            ));
        }
        let anchors: Vec<AnchorGraphRow> = sqlx::query_as(
            "SELECT table_name,canonical_pk,role,db_generation,root_digest,plan_digest,admin_operation_id,original_revision,original_digest
             FROM offline_recovery_metadata_anchors ORDER BY table_name,canonical_pk",
        )
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        rows.insert((
            "anchors".into(),
            "set".into(),
            serde_json::to_string(&anchors).map_err(|_| Failure::Preflight)?,
        ));
        let receipts: Vec<ReceiptGraphRow> = sqlx::query_as(
            "SELECT table_name,canonical_pk,receipt_digest,runtime_owner,task_id,classification_json,snapshot_json,context_json,authority_fence_json,owner_fence
             FROM offline_recovery_workspace_deletions ORDER BY table_name,canonical_pk",
        )
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        rows.insert((
            "receipts".into(),
            "set".into(),
            serde_json::to_string(&receipts).map_err(|_| Failure::Preflight)?,
        ));
        let cells: Vec<ClassificationGraphRow> = sqlx::query_as("SELECT table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id FROM offline_recovery_rows ORDER BY table_name,canonical_pk").fetch_all(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
        rows.insert((
            "cells".into(),
            "set".into(),
            serde_json::to_string(&cells).map_err(|_| Failure::Preflight)?,
        ));
        let parents: Vec<(String, String, String)> = cells
            .iter()
            .map(|row| (row.0.clone(), row.1.clone(), row.5.clone()))
            .collect();
        rows.insert((
            "parents".into(),
            "set".into(),
            serde_json::to_string(&parents).map_err(|_| Failure::Preflight)?,
        ));
        let deletions: Vec<(String, String)> = sqlx::query_as(
            "SELECT table_name,canonical_pk FROM offline_recovery_workspace_deletions ORDER BY table_name,canonical_pk",
        ).fetch_all(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
        rows.insert((
            "deletions".into(),
            "set".into(),
            serde_json::to_string(&deletions).map_err(|_| Failure::Preflight)?,
        ));
        Ok(())
    }

    async fn capture_images(
        rows: &mut GraphRows,
        connection: &mut SqliteConnection,
    ) -> Result<(), Failure> {
        for identity in inventory::identities(connection)
            .await
            .map_err(|_| Failure::ManualRequired)?
        {
            let (names, values) = inventory::values(connection, &identity)
                .await
                .map_err(|_| Failure::ManualRequired)?;
            let snapshot = inventory::snapshot(connection, &identity)
                .await
                .map_err(|_| Failure::ManualRequired)?;
            let mut parents = snapshot
                .parents
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| Failure::Preflight)?;
            parents.sort();
            rows.insert((
                "lineage".into(),
                serde_json::to_string(&identity).map_err(|_| Failure::Preflight)?,
                serde_json::to_string(&parents).map_err(|_| Failure::Preflight)?,
            ));
            rows.insert((
                "image".into(),
                serde_json::to_string(&identity).map_err(|_| Failure::Preflight)?,
                serde_json::to_string(&RowImage { names, values })
                    .map_err(|_| Failure::Preflight)?,
            ));
        }
        Ok(())
    }

    async fn capture_native(
        rows: &mut GraphRows,
        connection: &mut SqliteConnection,
    ) -> Result<(), Failure> {
        let compatibility: Vec<String> = sqlx::query_scalar("SELECT json_array(singleton,decision,source_schema_version,source_state_digest,legacy_operation_id,legacy_phase,legacy_baseline,legacy_baseline_digest,legacy_watermark,legacy_revision,legacy_binding,legacy_archive_digest,native_operation_id,reason_code,created_at) FROM matrix_recovery_compatibility ORDER BY singleton").fetch_all(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
        rows.insert((
            "compatibility".into(),
            "set".into(),
            serde_json::to_string(&compatibility).map_err(|_| Failure::Preflight)?,
        ));
        let legacy = crate::storage::matrix_recovery_snapshot::snapshot(connection)
            .await
            .map_err(|_| Failure::ManualRequired)?;
        rows.insert((
            "legacy".into(),
            legacy.digest,
            serde_json::to_string(&legacy.cutover).map_err(|_| Failure::Preflight)?,
        ));
        let audits: Vec<String> = sqlx::query_scalar("SELECT json_array(decision_id,task_id,delivery_id,resource_key,owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,disposition,reason,policy_ref,authorization_ref,not_replayed,created_at,session_id_digest,before_state_digest,post_state_digest,abandonment_kind,termination_proven) FROM historical_unknown_abandonments ORDER BY decision_id").fetch_all(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
        for audit in audits {
            let value: serde_json::Value =
                serde_json::from_str(&audit).map_err(|_| Failure::ManualRequired)?;
            rows.insert((
                "d1audit".into(),
                value[0].as_str().ok_or(Failure::ManualRequired)?.to_owned(),
                audit,
            ));
        }
        Ok(())
    }
}

impl ExpectedPostGraph {
    pub(super) fn from_pre_state(
        pre: &PreTransitionGraph,
        plan: &Plan,
        intent_digest: &str,
    ) -> Result<Self, Failure> {
        validate_digests(plan)?;
        validate_actions(plan)?;
        let mut rows = pre.rows.clone();
        Self::derive_d1(&mut rows, pre, plan)?;
        Self::validate_original_images(pre, plan)?;
        Self::derive_binding(&mut rows, pre, plan, intent_digest)?;
        Self::derive_inventory(&mut rows, plan, intent_digest)?;
        Self::derive_audit(&mut rows, plan)?;
        Self::derive_anchors(&mut rows, plan)?;
        Self::derive_receipts(&mut rows, plan)?;
        Self::derive_classifications(&mut rows, plan)?;
        Self::derive_images(&mut rows, pre, plan)?;
        Self::derive_native(&mut rows, pre, plan)?;
        Ok(Self {
            rows,
            intent_digest: intent_digest.to_owned(),
        })
    }

    fn derive_d1(
        rows: &mut GraphRows,
        pre: &PreTransitionGraph,
        plan: &Plan,
    ) -> Result<(), Failure> {
        if !pre
            .rows
            .contains(&("binding_identity".into(), "set".into(), "[[1,2]]".into()))
        {
            return Err(Failure::ManualRequired);
        }
        for workspace in &plan.workspaces {
            if !plan.actions.iter().any(|action| {
                action.action_type == "delete"
                    && action.table_name == "workspace_claims"
                    && action.canonical_pk == workspace.canonical_pk
                    && action.before.is_some()
                    && action.after.is_none()
            }) {
                return Err(Failure::ManualRequired);
            }
        }
        if let Some(audit) = super::reconciliation::expected_chain_audit(plan)? {
            if rows
                .iter()
                .any(|row| row.0 == "d1audit" && row.1 == plan.native_operation_id)
            {
                return Err(Failure::ManualRequired);
            }
            rows.insert(("d1audit".into(), plan.native_operation_id.clone(), audit));
        }
        Ok(())
    }

    fn validate_original_images(pre: &PreTransitionGraph, plan: &Plan) -> Result<(), Failure> {
        let mut source_keys = BTreeSet::new();
        for item in &plan.classifications {
            let identity = inventory::RowIdentity {
                table: item.table_name.clone(),
                key: item.canonical_pk.clone(),
            };
            let key = serde_json::to_string(&identity).map_err(|_| Failure::Preflight)?;
            if !source_keys.insert(key.clone()) {
                return Err(Failure::ManualRequired);
            }
            let image = pre
                .rows
                .iter()
                .find(|row| row.0 == "image" && row.1 == key)
                .ok_or(Failure::ManualRequired)?;
            let image: RowImage =
                serde_json::from_str(&image.2).map_err(|_| Failure::ManualRequired)?;
            if super::records::encoded_digest(&(&image.names, &image.values))?
                != item.original_digest
            {
                return Err(Failure::ManualRequired);
            }
        }
        if pre
            .rows
            .iter()
            .filter(|row| row.0 == "image")
            .map(|row| row.1.clone())
            .collect::<BTreeSet<_>>()
            != source_keys
        {
            return Err(Failure::ManualRequired);
        }
        Ok(())
    }

    fn derive_binding(
        rows: &mut GraphRows,
        pre: &PreTransitionGraph,
        plan: &Plan,
        intent_digest: &str,
    ) -> Result<(), Failure> {
        let original_binding = pre
            .rows
            .iter()
            .find(|row| row.0 == "binding_cells")
            .ok_or(Failure::ManualRequired)?;
        let original_binding: super::reconciliation_records::Binding =
            serde_json::from_str(&original_binding.2).map_err(|_| Failure::ManualRequired)?;
        if original_binding != plan.original_binding {
            return Err(Failure::ManualRequired);
        }
        let mut binding = original_binding;
        binding.schema_complete_digest = Some(plan.schema_complete_digest.clone());
        binding.plan_digest = Some(plan.plan_digest.clone());
        binding.identity_digest = Some(plan.identity_digest.clone());
        binding.classification_digest = Some(plan.classification_digest.clone());
        binding.transition_intent_digest = Some(intent_digest.to_owned());
        binding.native_operation_id = Some(plan.native_operation_id.clone());
        binding.phase = "transition_pending".into();
        binding.revision += 1;
        rows.retain(|row| row.0 != "binding_cells");
        rows.insert((
            "binding_cells".into(),
            "1".into(),
            serde_json::to_string(&binding).map_err(|_| Failure::Preflight)?,
        ));
        rows.insert((
            "marker_projection".into(),
            "1".into(),
            serde_json::json!([
                1,
                2,
                plan.operation_id,
                plan.root_digest,
                "committed",
                intent_digest,
                plan.grant_digest,
                plan.plan_digest,
                plan.action_digest,
                plan.archive_digest,
                plan.schema_complete_digest,
                plan.identity_digest,
                plan.original_binding.revision + 1,
                null,
                null,
                null
            ])
            .to_string(),
        ));
        Ok(())
    }

    fn derive_inventory(
        rows: &mut GraphRows,
        plan: &Plan,
        intent_digest: &str,
    ) -> Result<(), Failure> {
        let mut inventory = rows
            .iter()
            .filter(|(kind, _, _)| kind == "inventory")
            .map(|(_, key, value)| (("inventory".to_owned(), key.clone()), value.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        for item in plan
            .classifications
            .iter()
            .chain(&plan.inserted_classifications)
        {
            inventory.insert(
                (
                    "inventory".to_owned(),
                    format!("{}:{}", item.table_name, item.canonical_pk),
                ),
                item.original_digest.clone(),
            );
        }
        rows.retain(|(kind, _, _)| {
            !matches!(
                kind.as_str(),
                "anchors" | "receipts" | "cells" | "parents" | "deletions"
            )
        });
        rows.retain(|(kind, _, _)| kind != "inventory");
        rows.extend(
            inventory
                .into_iter()
                .map(|((kind, key), value)| (kind, key, value)),
        );

        let binding_key = ("binding".to_owned(), "1".to_owned());
        if let Some((_, _, encoded)) = rows
            .iter()
            .find(|(kind, key, _)| (kind, key) == (&binding_key.0, &binding_key.1))
        {
            let mut value: serde_json::Value =
                serde_json::from_str(encoded).map_err(|_| Failure::ManualRequired)?;
            let fields = value.as_array_mut().ok_or(Failure::ManualRequired)?;
            fields[1] = serde_json::Value::String("transition_pending".into());
            fields[3] = serde_json::json!(plan.original_binding.revision + 1);
            fields[4] = serde_json::json!(plan.plan_digest);
            fields[5] = serde_json::json!(intent_digest);
            fields[6] = serde_json::Value::Null;
            rows.retain(|row| !(row.0 == binding_key.0 && row.1 == binding_key.1));
            rows.insert((
                binding_key.0,
                binding_key.1,
                serde_json::to_string(&value).map_err(|_| Failure::Preflight)?,
            ));
        }

        rows.insert((
            "control".into(),
            "1".into(),
            serde_json::to_string(&(
                1_i64,
                plan.native_operation_id.clone(),
                "baseline".to_owned(),
                Option::<String>::None,
                &plan.native_at,
                Option::<String>::None,
                Option::<String>::None,
                Option::<String>::None,
            ))
            .map_err(|_| Failure::Preflight)?,
        ));
        Ok(())
    }

    fn derive_audit(rows: &mut GraphRows, plan: &Plan) -> Result<(), Failure> {
        for action in &plan.actions {
            let pre = super::reconciliation_sql::image_digest(action.before.as_ref())?;
            let post = super::reconciliation_sql::image_digest(action.after.as_ref())?;
            rows.insert((
                "audit".into(),
                format!("{}:{}", plan.operation_id, action.action_id),
                serde_json::to_string(&(
                    plan.operation_id.clone(),
                    action.action_id.clone(),
                    action.action_type.clone(),
                    action.table_name.clone(),
                    action.canonical_pk.clone(),
                    pre,
                    post,
                    action.evidence_digest.clone(),
                    action.original_digest.clone(),
                    action.original_revision.clone(),
                ))
                .map_err(|_| Failure::Preflight)?,
            ));
        }
        Ok(())
    }

    fn derive_anchors(rows: &mut GraphRows, plan: &Plan) -> Result<(), Failure> {
        let mut anchors = plan
            .anchors
            .iter()
            .map(|anchor| {
                (
                    &anchor.table_name,
                    &anchor.canonical_pk,
                    "metadata_anchor",
                    &plan.original_binding.db_generation,
                    &plan.original_binding.root_digest,
                    &plan.plan_digest,
                    &plan.operation_id,
                    &anchor.original_revision,
                    &anchor.original_digest,
                )
            })
            .collect::<Vec<_>>();
        anchors.sort();
        rows.insert((
            "anchors".into(),
            "set".into(),
            serde_json::to_string(&anchors).map_err(|_| Failure::Preflight)?,
        ));
        Ok(())
    }

    fn derive_receipts(rows: &mut GraphRows, plan: &Plan) -> Result<(), Failure> {
        let mut receipts = plan
            .workspaces
            .iter()
            .map(|workspace| {
                (
                    "workspace_claims".to_owned(),
                    workspace.canonical_pk.clone(),
                    workspace.receipt_digest.clone(),
                    workspace.runtime_owner.clone(),
                    workspace.task_id.clone(),
                    workspace.classification_json.clone(),
                    workspace.snapshot_json.clone(),
                    workspace.context_json.clone(),
                    workspace.authority_fence_json.clone(),
                    workspace.owner_fence,
                )
            })
            .collect::<Vec<_>>();
        receipts.sort();
        rows.insert((
            "receipts".into(),
            "set".into(),
            serde_json::to_string(&receipts).map_err(|_| Failure::Preflight)?,
        ));
        let mut deletions = plan
            .workspaces
            .iter()
            .map(|workspace| {
                (
                    "workspace_claims".to_owned(),
                    workspace.canonical_pk.clone(),
                )
            })
            .collect::<Vec<_>>();
        deletions.sort();
        rows.insert((
            "deletions".into(),
            "set".into(),
            serde_json::to_string(&deletions).map_err(|_| Failure::Preflight)?,
        ));
        Ok(())
    }

    fn derive_classifications(rows: &mut GraphRows, plan: &Plan) -> Result<(), Failure> {
        let mut classifications = plan
            .classifications
            .iter()
            .chain(&plan.inserted_classifications)
            .collect::<Vec<_>>();
        classifications.sort_by_key(|item| (&item.table_name, &item.canonical_pk));
        let cells = classifications
            .iter()
            .map(|item| -> Result<_, Failure> {
                Ok((
                    item.table_name.clone(),
                    item.canonical_pk.clone(),
                    item.origin_generation.clone(),
                    item.original_revision.clone(),
                    item.original_digest.clone(),
                    serde_json::to_string(&item.parent_relations)
                        .map_err(|_| Failure::Preflight)?,
                    item.disposition.clone(),
                    item.evidence_digest.clone(),
                    item.context_kind.clone(),
                    item.context_generation.clone(),
                    item.admin_operation_id.clone(),
                    Some(plan.plan_digest.clone()),
                    item.matrix_operation_id.clone(),
                ))
            })
            .collect::<Result<Vec<_>, Failure>>()?;
        rows.insert((
            "cells".into(),
            "set".into(),
            serde_json::to_string(&cells).map_err(|_| Failure::Preflight)?,
        ));
        let parents = classifications
            .iter()
            .map(|item| {
                (
                    item.table_name.clone(),
                    item.canonical_pk.clone(),
                    serde_json::to_string(&item.parent_relations).unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>();
        rows.insert((
            "parents".into(),
            "set".into(),
            serde_json::to_string(&parents).map_err(|_| Failure::Preflight)?,
        ));
        Ok(())
    }

    fn derive_images(
        rows: &mut GraphRows,
        pre: &PreTransitionGraph,
        plan: &Plan,
    ) -> Result<(), Failure> {
        for action in &plan.actions {
            validate_action_parents(pre, plan, action)?;
            let identity = inventory::RowIdentity {
                table: action.table_name.clone(),
                key: action.canonical_pk.clone(),
            };
            let key = serde_json::to_string(&identity).map_err(|_| Failure::Preflight)?;
            let before = action
                .before
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|_| Failure::Preflight)?;
            let actual_before = pre
                .rows
                .iter()
                .find(|row| row.0 == "image" && row.1 == key)
                .map(|row| row.2.clone());
            if actual_before != before {
                return Err(Failure::ManualRequired);
            }
            rows.retain(|row| row.0 != "image" || row.1 != key);
            rows.retain(|row| row.0 != "lineage" || row.1 != key);
            match action.action_type.as_str() {
                "delete" if action.after.is_none() => {}
                "insert" | "update" | "preserve" => {
                    let after = action.after.as_ref().ok_or(Failure::ManualRequired)?;
                    rows.insert((
                        "image".into(),
                        key,
                        serde_json::to_string(after).map_err(|_| Failure::Preflight)?,
                    ));
                    let mut parents = action
                        .parents
                        .iter()
                        .map(|parent| serde_json::to_string(&parent.identity))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| Failure::Preflight)?;
                    parents.sort();
                    rows.insert((
                        "lineage".into(),
                        serde_json::to_string(&identity).map_err(|_| Failure::Preflight)?,
                        serde_json::to_string(&parents).map_err(|_| Failure::Preflight)?,
                    ));
                }
                _ => return Err(Failure::ManualRequired),
            }
        }
        Ok(())
    }

    fn derive_native(
        rows: &mut GraphRows,
        pre: &PreTransitionGraph,
        plan: &Plan,
    ) -> Result<(), Failure> {
        let original = pre
            .rows
            .iter()
            .find(|row| row.0 == "compatibility")
            .ok_or(Failure::ManualRequired)?;
        let original: Vec<String> =
            serde_json::from_str(&original.2).map_err(|_| Failure::ManualRequired)?;
        let legacy = pre
            .rows
            .iter()
            .find(|row| row.0 == "legacy" && row.1 == plan.source_state_digest)
            .ok_or(Failure::ManualRequired)?;
        let legacy: Option<crate::storage::matrix_recovery_snapshot::LegacyCutover> =
            serde_json::from_str(&legacy.2).map_err(|_| Failure::ManualRequired)?;
        let native = match original.as_slice() {
            [original] => {
                let fields: serde_json::Value =
                    serde_json::from_str(original).map_err(|_| Failure::ManualRequired)?;
                if fields[1] != "held"
                    || fields[3] != plan.source_state_digest
                    || !fields[12].is_null()
                {
                    return Err(Failure::ManualRequired);
                }
                expected_compatibility(original, &plan.native_operation_id)?
            }
            [] => {
                let mut fields = serde_json::json!([
                    1,
                    "native",
                    24,
                    plan.source_state_digest,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    plan.native_operation_id,
                    "pristine-native",
                    plan.native_at
                ]);
                if let Some((
                    operation,
                    phase,
                    baseline,
                    digest,
                    watermark,
                    revision,
                    binding,
                    archive,
                )) = legacy
                {
                    let values = [
                        serde_json::json!(operation),
                        serde_json::json!(phase),
                        serde_json::json!(baseline),
                        serde_json::json!(digest),
                        serde_json::json!(watermark),
                        serde_json::json!(revision),
                        serde_json::json!(binding),
                        serde_json::json!(archive),
                    ];
                    for (index, value) in values.into_iter().enumerate() {
                        fields[index + 4] = value;
                    }
                }
                fields
            }
            _ => return Err(Failure::ManualRequired),
        };
        rows.retain(|row| row.0 != "compatibility");
        rows.insert((
            "compatibility".into(),
            "set".into(),
            serde_json::to_string(&vec![
                serde_json::to_string(&native).map_err(|_| Failure::Preflight)?,
            ])
            .map_err(|_| Failure::Preflight)?,
        ));
        Ok(())
    }

    pub(super) fn matches(&self, actual: &BTreeSet<(String, String, String)>) -> bool {
        &self.rows == actual
    }

    pub(super) fn digest(&self) -> Result<String, Failure> {
        let bytes = serde_json::to_vec(&self.rows).map_err(|_| Failure::Preflight)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    pub(super) fn commit_digest(&self, plan: &Plan) -> Result<String, Failure> {
        super::records::encoded_digest(&(
            2_u8,
            &plan.operation_id,
            &plan.root_digest,
            &self.intent_digest,
            &plan.grant_digest,
            &plan.plan_digest,
            &plan.action_digest,
            &plan.archive_digest,
            &plan.schema_complete_digest,
            &plan.identity_digest,
            plan.original_binding.revision + 1,
            self.digest()?,
        ))
    }

    fn completion(
        &self,
        plan: &Plan,
    ) -> Result<super::reconciliation_records::Completion, Failure> {
        Ok(super::reconciliation_records::Completion {
            protocol: 2,
            operation_id: plan.operation_id.clone(),
            intent_digest: self.intent_digest.clone(),
            plan_digest: plan.plan_digest.clone(),
            action_digest: plan.action_digest.clone(),
            classification_digest: plan.classification_digest.clone(),
            native_operation_id: plan.native_operation_id.clone(),
            committed_graph_digest: self.digest()?,
            committed_binding_revision: plan.original_binding.revision + 1,
        })
    }

    pub(super) fn finalized(&self, plan: &Plan) -> Result<Self, Failure> {
        let mut final_graph = self.clone();
        let completion_digest = super::records::encoded_digest(&self.completion(plan)?)?;
        let original = self
            .rows
            .iter()
            .find(|row| row.0 == "binding_cells")
            .ok_or(Failure::ManualRequired)?;
        let mut binding: super::reconciliation_records::Binding =
            serde_json::from_str(&original.2).map_err(|_| Failure::ManualRequired)?;
        binding.phase = "baseline_ready".into();
        binding.revision += 1;
        binding.transition_complete_digest = Some(completion_digest.clone());
        final_graph.rows.retain(|row| {
            !matches!(
                row.0.as_str(),
                "binding" | "binding_cells" | "marker_projection"
            )
        });
        final_graph.rows.insert((
            "binding_cells".into(),
            "1".into(),
            serde_json::to_string(&binding).map_err(|_| Failure::Preflight)?,
        ));
        final_graph.rows.insert((
            "binding".into(),
            "1".into(),
            serde_json::to_string(&(
                1_i64,
                "baseline_ready",
                &plan.operation_id,
                plan.original_binding.revision + 2,
                &plan.plan_digest,
                &self.intent_digest,
                &completion_digest,
            ))
            .map_err(|_| Failure::Preflight)?,
        ));
        final_graph.rows.insert((
            "marker_projection".into(),
            "1".into(),
            serde_json::json!([
                1,
                2,
                plan.operation_id,
                plan.root_digest,
                "finalized",
                self.intent_digest,
                plan.grant_digest,
                plan.plan_digest,
                plan.action_digest,
                plan.archive_digest,
                plan.schema_complete_digest,
                plan.identity_digest,
                self.digest()?,
                plan.original_binding.revision + 1,
                self.commit_digest(plan)?,
                completion_digest
            ])
            .to_string(),
        ));
        Ok(final_graph)
    }

    pub(super) fn final_cas_digest(&self, plan: &Plan) -> Result<String, Failure> {
        let final_graph = self.finalized(plan)?;
        let pending = self
            .rows
            .iter()
            .find(|row| row.0 == "binding_cells")
            .ok_or(Failure::ManualRequired)?;
        let ready = final_graph
            .rows
            .iter()
            .find(|row| row.0 == "binding_cells")
            .ok_or(Failure::ManualRequired)?;
        super::records::encoded_digest(&(
            &pending.2,
            &ready.2,
            self.commit_digest(plan)?,
            super::records::encoded_digest(&self.completion(plan)?)?,
            final_graph.digest()?,
        ))
    }
}

type ControlRow = (i64, String, String, Option<String>);
type BindingGraphRow = (
    i64,
    String,
    String,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
);

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

#[cfg(test)]
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
    let mut classifications = plan
        .classifications
        .iter()
        .chain(&plan.inserted_classifications)
        .collect::<Vec<_>>();
    classifications.sort_by_key(|item| (&item.table_name, &item.canonical_pk));
    let fields = classifications
        .iter()
        .map(|item| {
            (
                &item.table_name,
                &item.canonical_pk,
                &item.origin_generation,
                &item.original_revision,
                &item.original_digest,
                &item.parent_relations,
                &item.disposition,
                &item.evidence_digest,
                &item.matrix_operation_id,
            )
        })
        .collect::<Vec<_>>();
    if super::records::encoded_digest(&fields)? != plan.classification_digest {
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

fn validate_native_classification(plan: &Plan) -> Result<(), Failure> {
    if plan.inserted_classifications.len() != 1 {
        return Err(Failure::ManualRequired);
    }
    let native = &plan.inserted_classifications[0];
    if native.table_name != "matrix_recovery_control"
        || native.canonical_pk != "[\"integer:31\"]"
        || native.context_kind != "admin"
        || native.context_generation.is_some()
        || native.admin_operation_id.as_deref() != Some(plan.operation_id.as_str())
        || native.origin_generation != plan.native_operation_id
        || native.matrix_operation_id.as_deref() != Some(plan.native_operation_id.as_str())
        || native.disposition != "eligible_new"
        || native.parent_relations != serde_json::json!([])
        || !hex_digest(&native.evidence_digest)
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

fn validate_actions(plan: &Plan) -> Result<(), Failure> {
    validate_native_classification(plan)?;
    let mut seen = BTreeSet::new();
    let mut seen_keys = BTreeSet::new();
    if super::records::encoded_digest(&plan.actions)? != plan.action_digest {
        return Err(Failure::ManualRequired);
    }
    let identities = plan
        .classifications
        .iter()
        .chain(&plan.inserted_classifications)
        .map(|item| (item.table_name.as_str(), item.canonical_pk.as_str()))
        .collect::<BTreeSet<_>>();
    for action in &plan.actions {
        if action.action_id.is_empty()
            || !seen.insert(action.action_id.as_str())
            || !seen_keys.insert((action.table_name.as_str(), action.canonical_pk.as_str()))
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
        validate_action_cells(action)?;
        let source = plan
            .classifications
            .iter()
            .chain(&plan.inserted_classifications)
            .find(|item| {
                item.table_name == action.table_name && item.canonical_pk == action.canonical_pk
            })
            .ok_or(Failure::ManualRequired)?;
        let source_matches = if action.action_type == "insert" {
            let after = action.after.as_ref().ok_or(Failure::ManualRequired)?;
            let digest = super::records::encoded_digest(&(&after.names, &after.values))?;
            source.original_digest == digest && source.original_revision == digest
        } else {
            source.original_digest == action.original_digest
                && source.original_revision == action.original_revision
        };
        if !source_matches {
            return Err(Failure::ManualRequired);
        }
    }
    if seen.len() != plan.actions.len() || seen_keys != identities {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

fn validate_action_cells(action: &super::reconciliation_records::Action) -> Result<(), Failure> {
    validate_image(action.before.as_ref())?;
    validate_image(action.after.as_ref())?;
    let absent_digest = super::records::encoded_digest(&Option::<RowImage>::None)?;
    if let (Some(before), Some(after)) = (&action.before, &action.after) {
        if before.names != after.names {
            return Err(Failure::ManualRequired);
        }
        let declared = action
            .set_columns
            .iter()
            .chain(&action.unchanged_columns)
            .collect::<BTreeSet<_>>();
        if declared != before.names.iter().collect::<BTreeSet<_>>()
            || declared.len() != action.set_columns.len() + action.unchanged_columns.len()
        {
            return Err(Failure::ManualRequired);
        }
        for (index, name) in before.names.iter().enumerate() {
            if action.unchanged_columns.contains(name)
                && before.values[index] != after.values[index]
            {
                return Err(Failure::ManualRequired);
            }
        }
        if action.action_type == "preserve" && (before != after || !action.set_columns.is_empty()) {
            return Err(Failure::ManualRequired);
        }
    }
    let shape_valid = match action.action_type.as_str() {
        "insert" => {
            action.before.is_none()
                && action.after.is_some()
                && action.original_revision == "absent"
                && action.original_digest == absent_digest
        }
        "delete" => action.before.is_some() && action.after.is_none(),
        "update" | "preserve" => action.before.is_some() && action.after.is_some(),
        _ => false,
    };
    if !shape_valid {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

fn validate_image(image: Option<&RowImage>) -> Result<(), Failure> {
    if let Some(image) = image
        && (image.names.len() != image.values.len()
            || image.names.iter().collect::<BTreeSet<_>>().len() != image.names.len())
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

fn validate_action_parents(
    pre: &PreTransitionGraph,
    plan: &Plan,
    action: &super::reconciliation_records::Action,
) -> Result<(), Failure> {
    let mut seen = BTreeSet::new();
    for parent in &action.parents {
        let key = serde_json::to_string(&parent.identity).map_err(|_| Failure::Preflight)?;
        if !seen.insert(key.clone()) || action.table_name == "matrix_recovery_control" {
            return Err(Failure::ManualRequired);
        }
        let role: ParentRole = serde_json::from_value(serde_json::json!(parent.role))
            .map_err(|_| Failure::ManualRequired)?;
        if role == ParentRole::MetadataAnchor
            && !plan.anchors.iter().any(|anchor| {
                anchor.table_name == parent.identity.table
                    && anchor.canonical_pk == parent.identity.key
            })
        {
            return Err(Failure::ManualRequired);
        }
        let original = pre
            .rows
            .iter()
            .find(|row| row.0 == "image" && row.1 == key)
            .map(|row| serde_json::from_str::<RowImage>(&row.2))
            .transpose()
            .map_err(|_| Failure::ManualRequired)?;
        let after = plan
            .actions
            .iter()
            .find(|candidate| {
                candidate.table_name == parent.identity.table
                    && candidate.canonical_pk == parent.identity.key
            })
            .and_then(|candidate| candidate.after.as_ref());
        let mut matched = false;
        for image in original.as_ref().into_iter().chain(after) {
            let digest = super::records::encoded_digest(&(&image.names, &image.values))?;
            let revision = image
                .names
                .iter()
                .position(|name| matches!(name.as_str(), "revision" | "version" | "claim_revision"))
                .map(|index| image.values[index].clone())
                .unwrap_or_else(|| digest.clone());
            matched |= parent.digest == digest && parent.revision == revision;
        }
        if !matched {
            return Err(Failure::ManualRequired);
        }
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
    super::reconciliation_owner::validate(connection, plan).await?;
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

pub(super) async fn verify_expected_graph(
    connection: &mut SqliteConnection,
    plan: &Plan,
    expected: Option<&ExpectedPostGraph>,
) -> Result<(), Failure> {
    if expected.is_none() {
        return Err(Failure::ManualRequired);
    }
    if !sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?
        .is_empty()
    {
        return Err(Failure::ManualRequired);
    }
    if let Some(expected_graph) = expected {
        let current = PreTransitionGraph::capture(connection).await?;
        let mut target = if current
            .rows
            .iter()
            .any(|row| row.0 == "binding" && row.2.contains("\"baseline_ready\""))
        {
            expected_graph.finalized(plan)?
        } else {
            expected_graph.clone()
        };
        target.rows.retain(|row| row.0 != "marker_projection");
        if !target.matches(&current.rows) {
            return Err(Failure::ManualRequired);
        }
    }
    if plan.native_operation_id.is_empty() {
        return Err(Failure::ManualRequired);
    }
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
    for workspace in &plan.workspaces {
        allowed.remove(&(
            "workspace_claims".to_owned(),
            workspace.canonical_pk.clone(),
        ));
    }
    let mut expected = BTreeSet::new();
    for action in &plan.actions {
        let key = (action.table_name.clone(), action.canonical_pk.clone());
        let deleted = action.action_type == "delete"
            && plan.workspaces.iter().any(|workspace| {
                action.table_name == "workspace_claims"
                    && workspace.canonical_pk == action.canonical_pk
            });
        if (!allowed.contains(&key) && !deleted) || !expected.insert(key.clone()) {
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
        if classification.table_name == "workspace_claims"
            && plan
                .workspaces
                .iter()
                .any(|workspace| workspace.canonical_pk == classification.canonical_pk)
        {
            continue;
        }
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
            if parent.identity.table == "workspace_claims"
                && plan
                    .workspaces
                    .iter()
                    .any(|workspace| workspace.canonical_pk == parent.identity.key)
            {
                continue;
            }
            let parent_snapshot = inventory::snapshot(connection, &parent.identity)
                .await
                .map_err(|_| Failure::ManualRequired)?;
            let after = plan
                .actions
                .iter()
                .find(|action| {
                    action.table_name == parent.identity.table
                        && action.canonical_pk == parent.identity.key
                })
                .and_then(|action| action.after.as_ref());
            let digest = match after {
                Some(image) => super::records::encoded_digest(&(&image.names, &image.values))?,
                None => parent.digest.clone(),
            };
            let revision = after
                .and_then(|image| {
                    image
                        .names
                        .iter()
                        .position(|name| {
                            matches!(name.as_str(), "revision" | "version" | "claim_revision")
                        })
                        .map(|index| image.values[index].clone())
                })
                .unwrap_or_else(|| {
                    if after.is_some() {
                        digest.clone()
                    } else {
                        parent.revision.clone()
                    }
                });
            let role_valid = parent.role == ParentRole::Effectful
                || (parent.role == ParentRole::MetadataAnchor
                    && plan.anchors.iter().any(|anchor| {
                        anchor.table_name == parent.identity.table
                            && anchor.canonical_pk == parent.identity.key
                    }));
            if parent_snapshot.revision != revision
                || parent_snapshot.digest != digest
                || !role_valid
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

pub(super) async fn verify_committed_marker(
    connection: &mut SqliteConnection,
    plan: &Plan,
    intent: &super::reconciliation_records::Intent,
    expected: &ExpectedPostGraph,
) -> Result<(), Failure> {
    let intent_digest = super::records::encoded_digest(intent)?;
    let graph = expected.digest()?;
    let revision = plan.original_binding.revision + 1;
    let commit = expected.commit_digest(plan)?;
    let completion = super::reconciliation_records::Completion {
        protocol: 2,
        operation_id: plan.operation_id.clone(),
        intent_digest: intent_digest.clone(),
        plan_digest: plan.plan_digest.clone(),
        action_digest: plan.action_digest.clone(),
        classification_digest: plan.classification_digest.clone(),
        native_operation_id: plan.native_operation_id.clone(),
        committed_graph_digest: graph.clone(),
        committed_binding_revision: revision,
    };
    let completion_digest = super::records::encoded_digest(&completion)?;
    let final_graph = expected.finalized(plan)?.digest()?;
    let final_cas = expected.final_cas_digest(plan)?;
    let rows: Vec<String> = sqlx::query_scalar("SELECT json_array(singleton,protocol,operation_id,root_digest,state,intent_digest,grant_digest,plan_digest,action_digest,archive_digest,schema_digest,identity_digest,committed_graph_digest,committed_binding_revision,commit_digest,completion_digest,final_graph_digest,final_cas_digest) FROM r093_reconciliation_marker ORDER BY singleton").fetch_all(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
    let committed = serde_json::json!([
        1,
        2,
        plan.operation_id,
        plan.root_digest,
        "committed",
        intent_digest,
        plan.grant_digest,
        plan.plan_digest,
        plan.action_digest,
        plan.archive_digest,
        plan.schema_complete_digest,
        plan.identity_digest,
        graph,
        revision,
        commit,
        null,
        null,
        null
    ]);
    let finalized = serde_json::json!([
        1,
        2,
        plan.operation_id,
        plan.root_digest,
        "finalized",
        intent_digest,
        plan.grant_digest,
        plan.plan_digest,
        plan.action_digest,
        plan.archive_digest,
        plan.schema_complete_digest,
        plan.identity_digest,
        graph,
        revision,
        commit,
        completion_digest,
        final_graph,
        final_cas
    ]);
    if rows.len() != 1 {
        return Err(Failure::ManualRequired);
    }
    let actual: serde_json::Value =
        serde_json::from_str(&rows[0]).map_err(|_| Failure::ManualRequired)?;
    let phase: String =
        sqlx::query_scalar("SELECT phase FROM offline_recovery_binding WHERE singleton=1")
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| Failure::ManualRequired)?;
    let valid = match phase.as_str() {
        "transition_pending" => actual == committed,
        "baseline_ready" => actual == finalized,
        _ => false,
    };
    if !valid {
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
        if action.action_type == "delete" {
            let identities = inventory::identities(connection)
                .await
                .map_err(|_| Failure::ManualRequired)?;
            if identities.iter().any(|identity| {
                identity.table == action.table_name && identity.key == action.canonical_pk
            }) {
                return Err(Failure::ManualRequired);
            }
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
