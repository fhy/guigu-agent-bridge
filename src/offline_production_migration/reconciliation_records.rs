use super::{Failure, evidence, records};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub(super) const GRANT_ROOT: &str = "/etc/guigu-agent-bridge/recovery-successors";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Grant {
    pub version: u8,
    pub operation_id: String,
    pub root_digest: String,
    pub original_provenance_digest: String,
    pub original_executable_digest: String,
    pub archive_digest: String,
    pub schema_complete_digest: String,
    pub identity_digest: String,
    pub successor_executable_digest: String,
    pub successor_package_digest: String,
    pub successor_source: String,
    pub successor_review: String,
    pub protocol: u8,
    pub entry: String,
    pub allowed_modes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    pub db_generation: String,
    pub canonical_db_digest: String,
    pub root_digest: String,
    pub admin_operation_id: String,
    pub schema_intent_digest: String,
    pub schema_complete_digest: Option<String>,
    pub plan_digest: Option<String>,
    pub identity_digest: Option<String>,
    pub classification_digest: Option<String>,
    pub transition_intent_digest: Option<String>,
    pub transition_complete_digest: Option<String>,
    pub native_operation_id: Option<String>,
    pub phase: String,
    pub revision: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    pub protocol: u8,
    pub plan_digest: String,
    pub operation_id: String,
    pub root_digest: String,
    pub original_provenance_digest: String,
    pub original_executable_digest: String,
    pub archive_digest: String,
    pub schema_complete_digest: String,
    pub identity_digest: String,
    pub grant_digest: String,
    pub successor_executable_digest: String,
    pub successor_package_digest: String,
    pub successor_source: String,
    pub successor_review: String,
    pub native_operation_id: String,
    pub source_state_digest: String,
    pub original_binding: Binding,
    pub classifications: Vec<Classification>,
    #[serde(default)]
    pub inserted_classifications: Vec<Classification>,
    pub actions: Vec<Action>,
    #[serde(default)]
    pub d1_unknown_chain: Option<Decision>,
    #[serde(default)]
    pub d1_quarantined: Option<Decision>,
    #[serde(default)]
    pub d2_owner: Vec<OwnerProof>,
    #[serde(default)]
    pub anchors: Vec<Anchor>,
    #[serde(default)]
    pub workspaces: Vec<Workspace>,
    pub native_at: String,
    pub abandonment_at: String,
    pub action_digest: String,
    pub classification_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Classification {
    pub table_name: String,
    pub canonical_pk: String,
    pub origin_generation: String,
    pub original_revision: String,
    pub original_digest: String,
    pub parent_relations: serde_json::Value,
    pub disposition: String,
    pub evidence_digest: String,
    pub context_kind: String,
    pub context_generation: Option<String>,
    pub admin_operation_id: Option<String>,
    pub plan_digest: Option<String>,
    pub matrix_operation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RowImage {
    pub names: Vec<String>,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Parent {
    pub identity: super::super::storage::recovery_eligibility::RowIdentity,
    pub revision: String,
    pub digest: String,
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Action {
    pub action_id: String,
    pub action_type: String,
    pub table_name: String,
    pub canonical_pk: String,
    pub original_revision: String,
    pub original_digest: String,
    pub before: Option<RowImage>,
    pub after: Option<RowImage>,
    pub set_columns: Vec<String>,
    pub unchanged_columns: Vec<String>,
    pub parents: Vec<Parent>,
    pub evidence_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Decision {
    pub decision_id: String,
    pub evidence_digest: String,
    pub references: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OwnerProof {
    pub instance_token: String,
    pub runtime_generation: i64,
    pub evidence_revision: i64,
    pub evidence_digest: String,
    pub parent_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Anchor {
    pub table_name: String,
    pub canonical_pk: String,
    pub original_revision: String,
    pub original_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Workspace {
    pub canonical_pk: String,
    pub classification_json: String,
    pub snapshot_json: String,
    pub context_json: String,
    pub authority_fence_json: String,
    pub receipt_digest: String,
    pub runtime_owner: String,
    pub task_id: String,
    pub owner_fence: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub protocol: u8,
    pub operation_id: String,
    pub root_digest: String,
    pub grant_digest: String,
    pub plan_digest: String,
    pub action_digest: String,
    pub classification_digest: String,
    pub source_state_digest: String,
    pub native_operation_id: String,
    pub original_binding: Binding,
    pub pre_transition: Option<super::reconciliation_graph::PreTransitionGraph>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Completion {
    pub protocol: u8,
    pub operation_id: String,
    pub intent_digest: String,
    pub plan_digest: String,
    pub action_digest: String,
    pub classification_digest: String,
    pub native_operation_id: String,
    pub committed_graph_digest: String,
    pub committed_binding_revision: i64,
}

pub(super) fn grant_path(operation_id: &str) -> PathBuf {
    #[cfg(test)]
    if let Some(path) = std::env::var_os("R094_GRANT_PATH") {
        return PathBuf::from(path);
    }
    Path::new(GRANT_ROOT).join(format!("{operation_id}.json"))
}

pub(super) fn discriminator(path: &Path) -> Result<bool, Failure> {
    evidence::canonical(path).map_err(|_| Failure::Preflight)?;
    let bytes = fs::read(path).map_err(|_| Failure::ManualRequired)?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| Failure::ManualRequired)?;
    let Some(object) = value.as_object() else {
        return Err(Failure::ManualRequired);
    };
    let Some(protocol) = object.get("protocol") else {
        return Ok(false);
    };
    match protocol.as_u64() {
        Some(1) => Ok(false),
        Some(2) => Ok(true),
        _ => Err(Failure::ManualRequired),
    }
}

pub(super) use super::reconciliation_files::{load, load_grant, protected_bytes, publish};

pub(super) fn plan_digest(plan: &Plan) -> Result<String, Failure> {
    let mut copy = plan.clone();
    copy.plan_digest.clear();
    for classification in &mut copy.classifications {
        classification.plan_digest = None;
    }
    for classification in &mut copy.inserted_classifications {
        classification.plan_digest = None;
    }
    records::encoded_digest(&copy)
}

pub(super) fn intent(plan: &Plan) -> Intent {
    Intent {
        protocol: 2,
        operation_id: plan.operation_id.clone(),
        root_digest: plan.root_digest.clone(),
        grant_digest: plan.grant_digest.clone(),
        plan_digest: plan.plan_digest.clone(),
        action_digest: plan.action_digest.clone(),
        classification_digest: plan.classification_digest.clone(),
        source_state_digest: plan.source_state_digest.clone(),
        native_operation_id: plan.native_operation_id.clone(),
        original_binding: plan.original_binding.clone(),
        pre_transition: None,
    }
}
