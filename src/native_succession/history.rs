//! Validate completed per-instance histories without changing their selectors.
use super::state::{Owner, Result, refuse};
use crate::storage::recovery_eligibility::{
    self as eligibility, EffectDomain, EligibilityContext, inventory, writer,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection, sqlite::SqliteRow};
#[cfg(test)]
#[path = "history_record_tests.rs"]
mod tests;

#[derive(Deserialize)]
struct Stop {
    kind: String,
    owner: String,
    generation: i64,
    source_revision: i64,
    fingerprint: String,
    epoch: i64,
}
#[derive(Deserialize)]
struct Succession {
    kind: String,
    historical_joined: bool,
    source: Source,
    exclusion: Exclusion,
}
// Read only the identity contract; older R105 records lack later optional fields.
#[derive(Deserialize)]
struct Source {
    owner: Owner,
    native_generation: String,
    operation: String,
    revision: i64,
}
#[derive(Deserialize)]
struct Exclusion {
    kind: String,
    owner: Owner,
    #[serde(default)]
    boot: Option<super::process::boot::BootObservation>,
}
impl Exclusion {
    fn valid(&self) -> bool {
        match self.kind.as_str() {
            "current-scoped-exclusion-v1" => self.boot.is_none(),
            "current-scoped-exclusion-v2" => {
                self.boot.as_ref().is_some_and(|b| b.valid_for(&self.owner))
            }
            _ => false,
        }
    }
}

pub(super) async fn completed(
    c: &mut SqliteConnection,
    row: &SqliteRow,
    generation: &str,
    operation: &str,
    revision: i64,
) -> Result<Option<(String, String)>> {
    if row.get::<&str, _>("state") != "observing" {
        return Ok(None);
    }
    if row.get::<&str, _>("runtime_state") != "stopped"
        || row.get::<&str, _>("runtime_fingerprint") != row.get::<&str, _>("process_fingerprint")
    {
        return Err(refuse("history-owner-identity"));
    }
    let owner = Owner {
        token: row.get("instance_token"),
        generation: row.get("runtime_generation"),
        revision: row.get("evidence_revision"),
        pid: row.get("pid"),
        starttime: row.get("proc_starttime"),
        boot: row.get("boot_id"),
        fingerprint: row.get("process_fingerprint"),
    };
    if owner.pid <= 0 || owner.starttime <= 0 || owner.boot.is_empty() || owner.revision < 2 {
        return Err(refuse("history-owner-identity"));
    }
    match row.get::<&str, _>("barrier_state") {
        "closed_joined" => controlled(row, &owner)?,
        "open" => succeeded(row, &owner, generation, operation, revision)?,
        _ => return Err(refuse("history-barrier")),
    }
    let context = EligibilityContext::NonAdmin {
        generation: generation.into(),
        revision,
        matrix_enabled: true,
        matrix_operation: Some(operation.into()),
    };
    let digest = lineage(c, &owner, &context).await?;
    effects_clear(c, &owner.token).await?;
    Ok(Some((owner.token, digest)))
}

fn controlled(row: &SqliteRow, owner: &Owner) -> Result<()> {
    let record: Stop = serde_json::from_str(row.get("child_snapshot"))
        .map_err(|_| refuse("history-controlled-record"))?;
    if record.kind != "controlled-stop-v1"
        || record.owner != owner.token
        || record.generation != owner.generation
        || record.source_revision.checked_add(1) != Some(owner.revision)
        || record.fingerprint != owner.fingerprint
        || record.epoch <= 0
        || record.epoch != row.get::<i64, _>("barrier_epoch")
        || row.get::<i64, _>("in_flight_spawns") != 0
        || row.get::<&str, _>("child_scan_state") != "inaccessible"
    {
        return Err(refuse("history-controlled-identity"));
    }
    Ok(())
}

fn succeeded(
    row: &SqliteRow,
    owner: &Owner,
    generation: &str,
    operation: &str,
    revision: i64,
) -> Result<()> {
    let record: Succession = serde_json::from_str(row.get("child_snapshot"))
        .map_err(|_| refuse("history-succession-record"))?;
    let mut previous = owner.clone();
    previous.revision -= 1;
    if record.kind != "safe-succession-v1"
        || record.historical_joined
        || record.source.owner != previous
        || record.exclusion.owner != previous
        || !record.exclusion.valid()
        || record.source.native_generation != generation
        || record.source.operation != operation
        || record.source.revision < 0
        || record.source.revision >= revision
    {
        return Err(refuse("history-succession-identity"));
    }
    Ok(())
}

async fn lineage(
    c: &mut SqliteConnection,
    owner: &Owner,
    context: &EligibilityContext,
) -> Result<String> {
    let rows = sqlx::query(
        "SELECT * FROM runtime_owner_evidence WHERE instance_token=? ORDER BY evidence_revision",
    )
    .bind(&owner.token)
    .fetch_all(&mut *c)
    .await?;
    if i64::try_from(rows.len()).ok() != Some(owner.revision) {
        return Err(refuse("history-lineage"));
    }
    let mut digests = Vec::new();
    let parent = writer::text_identity("runtime_instances", &owner.token)?;
    eligibility::require_lineage(c, context, EffectDomain::Matrix, &parent)
        .await
        .map_err(|_| refuse("history-classification"))?;
    digests.push(inventory::snapshot(c, &parent).await?.digest);
    for (index, row) in rows.iter().enumerate() {
        let rev = i64::try_from(index).map_err(|_| refuse("history-lineage"))? + 1;
        if row.get::<i64, _>("evidence_revision") != rev
            || row.get::<i64, _>("runtime_generation") != owner.generation
            || row.get::<i64, _>("pid") != owner.pid
            || row.get::<i64, _>("proc_starttime") != owner.starttime
            || row.get::<&str, _>("boot_id") != owner.boot
            || row.get::<&str, _>("process_fingerprint") != owner.fingerprint
            || row.get::<bool, _>("is_current") != (rev == owner.revision)
        {
            return Err(refuse("history-lineage"));
        }
        let key: String = sqlx::query_scalar("SELECT json_array('text:'||hex(CAST(instance_token AS BLOB)),'integer:'||hex(CAST(runtime_generation AS BLOB)),'integer:'||hex(CAST(evidence_revision AS BLOB))) FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
            .bind(&owner.token).bind(rev).fetch_one(&mut *c).await?;
        let id = eligibility::RowIdentity {
            table: "runtime_owner_evidence".into(),
            key,
        };
        eligibility::require_lineage(c, context, EffectDomain::Matrix, &id)
            .await
            .map_err(|_| refuse("history-classification"))?;
        digests.push(inventory::snapshot(c, &id).await?.digest);
    }
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&digests).map_err(|_| refuse("history-encoding"))?)
    ))
}

async fn effects_clear(c: &mut SqliteConnection, token: &str) -> Result<()> {
    for query in [
        "SELECT count(*) FROM matrix_component_leases WHERE owner_token=? AND state!='expired'",
        "SELECT count(*) FROM task_admissions WHERE runtime_instance=? AND state!='terminal'",
        "SELECT count(*) FROM execution_leases WHERE state!='released' AND (owner_token=?1 OR task_id IN (SELECT task_id FROM task_admissions WHERE runtime_instance=?1))",
        "SELECT count(*) FROM matrix_recovery_batches WHERE owner_token=? AND state!='terminal'",
        "SELECT count(*) FROM session_owner_bindings WHERE runtime_instance=? AND state NOT IN ('reaped','released')",
        "SELECT count(*) FROM deliveries d JOIN task_admissions a USING(task_id) LEFT JOIN delivery_dispositions p USING(delivery_id) WHERE a.runtime_instance=? AND (d.acknowledged_at IS NULL OR p.state IS NULL OR p.state!='terminal')",
        "SELECT count(*) FROM transport_receipts r JOIN task_admissions a USING(task_id) WHERE a.runtime_instance=? AND r.state='reserved'",
    ] {
        if sqlx::query_scalar::<_, i64>(query)
            .bind(token)
            .fetch_one(&mut *c)
            .await?
            != 0
        {
            return Err(refuse("history-unresolved-effect"));
        }
    }
    Ok(())
}
