use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

use super::{EligibilityContext, LiveFence, RowIdentity, inventory, refused};

pub(super) struct ReleaseEvidence<'proof> {
    pub owner: &'proof str,
    pub task: &'proof str,
    pub fence: i64,
    pub authority: Option<&'proof LiveFence>,
}

async fn classification(
    connection: &mut SqliteConnection,
    identity: &RowIdentity,
) -> Result<String, sqlx::Error> {
    sqlx::query_scalar("SELECT json_array(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id) FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=?")
        .bind(&identity.table).bind(&identity.key).fetch_optional(connection).await?.ok_or_else(refused)
}

pub(super) async fn record(
    connection: &mut SqliteConnection,
    context: &EligibilityContext,
    identity: &RowIdentity,
    evidence: ReleaseEvidence<'_>,
) -> Result<(), sqlx::Error> {
    if identity.table != "workspace_claims" {
        return Err(refused());
    }
    let released: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_leases WHERE task_id=? AND owner_token=? AND fence=? AND state='released')")
        .bind(evidence.task).bind(evidence.owner).bind(evidence.fence).fetch_one(&mut *connection).await?;
    if !released {
        return Err(refused());
    }
    let snapshot = inventory::values(connection, identity).await?;
    let snapshot = serde_json::to_string(&snapshot).map_err(|_| refused())?;
    let classification = classification(connection, identity).await?;
    let context = serde_json::to_string(context).map_err(|_| refused())?;
    let authority = serde_json::to_string(&evidence.authority).map_err(|_| refused())?;
    let digest = proof_digest(&classification, &snapshot, &context, &authority, &evidence)?;
    sqlx::query("INSERT INTO offline_recovery_workspace_deletions(table_name,canonical_pk,classification_json,snapshot_json,context_json,authority_fence_json,receipt_digest,runtime_owner,task_id,owner_fence) VALUES(?,?,?,?,?,?,?,?,?,?)")
        .bind(&identity.table).bind(&identity.key).bind(classification).bind(snapshot)
        .bind(context).bind(authority).bind(digest).bind(evidence.owner).bind(evidence.task).bind(evidence.fence).execute(connection).await?;
    Ok(())
}

pub(super) async fn validate(
    connection: &mut SqliteConnection,
    context: &EligibilityContext,
    live: &[RowIdentity],
) -> Result<Vec<RowIdentity>, sqlx::Error> {
    let receipts = sqlx::query("SELECT * FROM offline_recovery_workspace_deletions")
        .fetch_all(&mut *connection)
        .await?;
    let mut deleted = Vec::new();
    for receipt in receipts {
        let identity = RowIdentity {
            table: receipt.try_get("table_name")?,
            key: receipt.try_get("canonical_pk")?,
        };
        if identity.table != "workspace_claims" || live.contains(&identity) {
            return Err(refused());
        }
        let current = classification(connection, &identity).await?;
        if receipt.try_get::<&str, _>("classification_json")? != current {
            return Err(refused());
        }
        validate_digest(&receipt)?;
        let historical: EligibilityContext =
            serde_json::from_str(receipt.try_get("context_json")?).map_err(|_| refused())?;
        validate_context(context, &historical)?;
        let classification: Vec<serde_json::Value> =
            serde_json::from_str(&current).map_err(|_| refused())?;
        validate_snapshot(&receipt, &identity, &classification)?;
        deleted.push(identity);
    }
    Ok(deleted)
}

fn proof_digest(
    classification: &str,
    snapshot: &str,
    context: &str,
    authority: &str,
    evidence: &ReleaseEvidence<'_>,
) -> Result<String, sqlx::Error> {
    let bytes = serde_json::to_vec(&(
        classification,
        snapshot,
        context,
        authority,
        evidence.owner,
        evidence.task,
        evidence.fence,
    ))
    .map_err(|_| refused())?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn validate_digest(receipt: &sqlx::sqlite::SqliteRow) -> Result<(), sqlx::Error> {
    let authority: Option<LiveFence> =
        serde_json::from_str(receipt.try_get("authority_fence_json")?).map_err(|_| refused())?;
    let evidence = ReleaseEvidence {
        owner: receipt.try_get("runtime_owner")?,
        task: receipt.try_get("task_id")?,
        fence: receipt.try_get("owner_fence")?,
        authority: authority.as_ref(),
    };
    let expected = proof_digest(
        receipt.try_get("classification_json")?,
        receipt.try_get("snapshot_json")?,
        receipt.try_get("context_json")?,
        receipt.try_get("authority_fence_json")?,
        &evidence,
    )?;
    if expected != receipt.try_get::<&str, _>("receipt_digest")? {
        return Err(refused());
    }
    Ok(())
}

fn validate_context(
    current: &EligibilityContext,
    historical: &EligibilityContext,
) -> Result<(), sqlx::Error> {
    let valid = match (current, historical) {
        (
            EligibilityContext::NonAdmin {
                generation,
                revision,
                ..
            },
            EligibilityContext::NonAdmin {
                generation: previous,
                revision: previous_revision,
                ..
            },
        ) => generation == previous && *previous_revision >= 0 && previous_revision <= revision,
        (
            EligibilityContext::Admin {
                generation,
                root,
                operation,
                plan,
                revision,
                matrix_operation,
                ..
            },
            EligibilityContext::Admin {
                generation: previous,
                root: previous_root,
                operation: previous_operation,
                plan: previous_plan,
                revision: previous_revision,
                matrix_operation: previous_matrix,
                ..
            },
        ) => {
            generation == previous
                && root == previous_root
                && operation == previous_operation
                && plan == previous_plan
                && matrix_operation == previous_matrix
                && *previous_revision >= 0
                && previous_revision <= revision
        }
        _ => false,
    };
    if valid { Ok(()) } else { Err(refused()) }
}

fn validate_snapshot(
    receipt: &sqlx::sqlite::SqliteRow,
    identity: &RowIdentity,
    classification: &[serde_json::Value],
) -> Result<(), sqlx::Error> {
    let snapshot: (Vec<String>, Vec<String>) =
        serde_json::from_str(receipt.try_get("snapshot_json")?).map_err(|_| refused())?;
    let (names, values) = &snapshot;
    if names.len() != values.len() || classification.len() != 13 {
        return Err(refused());
    }
    let value = |name: &str| -> Result<&str, sqlx::Error> {
        let index = names
            .iter()
            .position(|column| column == name)
            .ok_or_else(refused)?;
        Ok(values[index].as_str())
    };
    let digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&snapshot).map_err(|_| refused())?)
    );
    let owner: &str = receipt.try_get("runtime_owner")?;
    let task: &str = receipt.try_get("task_id")?;
    let fence: i64 = receipt.try_get("owner_fence")?;
    let encode = |text: &str| {
        format!(
            "text:{}",
            text.as_bytes()
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        )
    };
    let expected_key = serde_json::to_string(&[value("claim_id")?]).map_err(|_| refused())?;
    if identity.key != expected_key
        || classification[4].as_str() != Some(&digest)
        || classification[3].as_str() != Some(value("revision")?)
        || !matches!(
            classification[6].as_str(),
            Some("eligible_new" | "eligible_preserved")
        )
        || value("runtime_owner")? != encode(owner)
        || value("task_id")? != encode(task)
        || value("owner_fence")?
            != format!(
                "integer:{}",
                fence
                    .to_string()
                    .as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02X}"))
                    .collect::<String>()
            )
        || value("state")? != encode("active")
    {
        return Err(refused());
    }
    Ok(())
}
