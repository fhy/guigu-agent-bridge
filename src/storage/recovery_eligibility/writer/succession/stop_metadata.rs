//! Exact archived stop metadata qualification; never writes a runtime before-image.
use super::{RecoveryScope, State, lineage};
use crate::storage::recovery_eligibility::{inventory, writer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

fn refused() -> sqlx::Error {
    crate::native_succession::state::refuse("stop-metadata-witness")
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Image {
    names: Vec<String>,
    values: Vec<String>,
}
impl Image {
    pub(crate) async fn read(c: &mut SqliteConnection, token: &str) -> Result<Self, sqlx::Error> {
        let id = writer::text_identity("runtime_instances", token)?;
        let (names, values) = inventory::values(c, &id).await?;
        Ok(Self { names, values })
    }
    fn digest(&self) -> Result<String, sqlx::Error> {
        Ok(format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(&self.names, &self.values)).map_err(|_| refused())?
            )
        ))
    }
    fn text(&self, name: &str) -> Result<String, sqlx::Error> {
        let index = self
            .names
            .iter()
            .position(|n| n == name)
            .ok_or_else(refused)?;
        let hex = self
            .values
            .get(index)
            .and_then(|s| s.strip_prefix("text:"))
            .ok_or_else(refused)?;
        if !hex.len().is_multiple_of(2) || !hex.is_ascii() {
            return Err(refused());
        }
        let bytes = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| refused()))
            .collect::<Result<Vec<_>, _>>()?;
        String::from_utf8(bytes).map_err(|_| refused())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Witness {
    pub before: Image,
    pub current: Image,
    pub evidence_digest: String,
}
impl Witness {
    pub(crate) fn validate_delta(&self, state: &State) -> Result<(), sqlx::Error> {
        let b = &self.before;
        let c = &self.current;
        if b.names != c.names
            || b.values.len() != b.names.len()
            || c.values.len() != c.names.len()
            || b.text("instance_token")? != state.owner.token
            || b.text("process_fingerprint")? != state.owner.fingerprint
            || !matches!(b.text("state")?.as_str(), "active" | "stopping")
            || c.text("state")? != "stopped"
        {
            return Err(refused());
        }
        for (i, name) in b.names.iter().enumerate() {
            if !matches!(name.as_str(), "state" | "heartbeat_at") && b.values[i] != c.values[i] {
                return Err(refused());
            }
        }
        let parse = |s: String| chrono::DateTime::parse_from_rfc3339(&s).map_err(|_| refused());
        let start = parse(b.text("started_at")?)?;
        let before = parse(b.text("heartbeat_at")?)?;
        let stopped = parse(c.text("heartbeat_at")?)?;
        if start > before || before >= stopped || stopped > chrono::Utc::now() {
            return Err(refused());
        }
        Ok(())
    }
}

pub(crate) async fn evidence_digest(
    c: &mut SqliteConnection,
    state: &State,
) -> Result<String, sqlx::Error> {
    let owner = &state.owner;
    let key: String = sqlx::query_scalar("SELECT json_array('text:'||hex(CAST(instance_token AS BLOB)),'integer:'||hex(CAST(runtime_generation AS BLOB)),'integer:'||hex(CAST(evidence_revision AS BLOB))) FROM runtime_owner_evidence WHERE instance_token=? AND runtime_generation=? AND evidence_revision=? AND is_current=1")
        .bind(&owner.token).bind(owner.generation).bind(owner.revision).fetch_one(&mut *c).await?;
    Ok(inventory::snapshot(
        c,
        &crate::storage::recovery_eligibility::RowIdentity {
            table: "runtime_owner_evidence".into(),
            key,
        },
    )
    .await?
    .digest)
}

pub(super) async fn apply(
    c: &mut SqliteConnection,
    scope: &RecoveryScope,
    state: &State,
    witness: &Witness,
) -> Result<(), sqlx::Error> {
    witness.validate_delta(state)?;
    if Image::read(c, &state.owner.token).await? != witness.current
        || evidence_digest(c, state).await? != witness.evidence_digest
    {
        return Err(refused());
    }
    let id = writer::text_identity("runtime_instances", &state.owner.token)?;
    let row = sqlx::query("SELECT original_revision,original_digest,parent_relations,disposition,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id,origin_generation FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=? AND disposition IN ('eligible_new','eligible_preserved')")
        .bind(&id.table).bind(&id.key).fetch_one(&mut *c).await?;
    scope.context().matches_classification(&row)?;
    let digest = witness.before.digest()?;
    if row.get::<&str, _>("original_digest") != digest
        || row.get::<&str, _>("original_revision") != digest
    {
        return Err(refused());
    }
    // BEGIN IMMEDIATE plus exact current image/evidence/State comparison fences the
    // observed drift. Only classification changes, with existing reference CAS.
    let mutation = writer::Mutation {
        identity: id,
        revision: digest.clone(),
        digest,
        parents: row.get("parent_relations"),
        released_lease: None,
    };
    lineage::complete(c, scope, mutation).await
}

#[cfg(test)]
#[path = "stop_metadata_tests.rs"]
mod tests;
