mod lineage;
mod readback;

use sha2::{Digest, Sha256};
use sqlx::{SqliteConnection, SqlitePool};

use super::{EligibilityContext, ParentIdentity, ParentRole, RowIdentity, inventory, refused};

struct Entry {
    identity: RowIdentity,
    snapshot: inventory::Snapshot,
    matrix: bool,
    disposition: &'static str,
}

struct Native {
    operation: Option<String>,
    source: Option<String>,
    phase: String,
    watermark: Option<String>,
}

pub async fn bootstrap_nonadmin(
    pool: &SqlitePool,
    matrix_enabled: bool,
) -> Result<EligibilityContext, sqlx::Error> {
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let admin: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_binding")
        .fetch_one(&mut *transaction)
        .await?;
    if admin != 0 {
        return Err(refused());
    }
    let native = lineage::native(&mut transaction, matrix_enabled).await?;
    let state: (String, String) = sqlx::query_as(
        "SELECT state,bootstrap_nonce FROM runtime_eligibility_bootstrap WHERE singleton=1",
    )
    .fetch_one(&mut *transaction)
    .await?;
    let context = match state.0.as_str() {
        "pending" => create(&mut transaction, matrix_enabled, &native, &state.1).await?,
        "complete" => readback::context(&mut transaction, matrix_enabled, &native).await?,
        _ => return Err(refused()),
    };
    readback::inventory(&mut transaction, &context).await?;
    transaction.commit().await?;
    Ok(context)
}

async fn create(
    connection: &mut SqliteConnection,
    matrix_enabled: bool,
    native: &Native,
    nonce: &str,
) -> Result<EligibilityContext, sqlx::Error> {
    let existing: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM runtime_eligibility_context)+(SELECT count(*) FROM offline_recovery_rows)")
        .fetch_one(&mut *connection).await?;
    if existing != 0 {
        return Err(refused());
    }
    let entries = lineage::inventory(connection, native).await?;
    let committed = entries
        .iter()
        .map(|entry| {
            (
                &entry.identity,
                &entry.snapshot.revision,
                &entry.snapshot.digest,
                entry.matrix,
                entry.disposition,
            )
        })
        .collect::<Vec<_>>();
    let digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&committed).map_err(|_| refused())?)
    );
    let generation = uuid::Uuid::now_v7().to_string();
    insert_context(
        connection,
        native,
        matrix_enabled,
        nonce,
        &generation,
        &digest,
    )
    .await?;
    for entry in &entries {
        insert_entry(connection, native, &generation, &digest, entry, &entries).await?;
    }
    let changed = sqlx::query("UPDATE runtime_eligibility_bootstrap SET state='complete',generation=?,inventory_digest=? WHERE singleton=1 AND state='pending' AND bootstrap_nonce=? AND generation IS NULL AND inventory_digest IS NULL")
        .bind(&generation).bind(&digest).bind(nonce).execute(&mut *connection).await?;
    if changed.rows_affected() != 1 {
        return Err(refused());
    }
    Ok(EligibilityContext::NonAdmin {
        generation,
        revision: 0,
        matrix_enabled,
        matrix_operation: native.operation.clone(),
    })
}

async fn insert_context(
    connection: &mut SqliteConnection,
    native: &Native,
    enabled: bool,
    nonce: &str,
    generation: &str,
    digest: &str,
) -> Result<(), sqlx::Error> {
    let provenance = if native.operation.is_some() {
        "validated_native"
    } else {
        "pristine_local"
    };
    let watermark = native
        .watermark
        .as_ref()
        .map(|token| format!("{:x}", Sha256::digest(token.as_bytes())));
    sqlx::query("INSERT INTO runtime_eligibility_context(singleton,generation,bootstrap_nonce,provenance_kind,compatibility_operation_id,compatibility_source_digest,matrix_phase,matrix_enabled,expected_b_digest,state,revision,created_at,bootstrap_inventory_digest) VALUES(1,?,?,?,?,?,?,?,?,'ready',0,?,?)")
        .bind(generation).bind(nonce).bind(provenance).bind(&native.operation).bind(&native.source)
        .bind(&native.phase).bind(enabled).bind(watermark).bind(chrono::Utc::now().to_rfc3339())
        .bind(digest).execute(connection).await?;
    Ok(())
}

async fn insert_entry(
    connection: &mut SqliteConnection,
    native: &Native,
    generation: &str,
    digest: &str,
    entry: &Entry,
    entries: &[Entry],
) -> Result<(), sqlx::Error> {
    let mut parents = Vec::new();
    for identity in &entry.snapshot.parents {
        let parent = entries
            .iter()
            .find(|parent| parent.identity == *identity)
            .ok_or_else(refused)?;
        parents.push(ParentIdentity {
            identity: identity.clone(),
            revision: parent.snapshot.revision.clone(),
            digest: parent.snapshot.digest.clone(),
            role: ParentRole::Effectful,
        });
    }
    sqlx::query("INSERT INTO offline_recovery_rows(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,context_generation,matrix_operation_id) VALUES(?,?,?,?,?,?,?,?,'nonadmin',?,?)")
        .bind(&entry.identity.table).bind(&entry.identity.key).bind(generation)
        .bind(&entry.snapshot.revision).bind(&entry.snapshot.digest)
        .bind(serde_json::to_string(&parents).map_err(|_| refused())?).bind(entry.disposition)
        .bind(digest).bind(generation).bind(if entry.matrix { native.operation.as_deref() } else { None })
        .execute(connection).await?;
    Ok(())
}
