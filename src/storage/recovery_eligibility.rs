mod authority;
mod bootstrap;
#[cfg(test)]
mod bootstrap_tests;
#[cfg(test)]
mod contract_tests;
mod inventory;
mod metadata;
#[cfg(test)]
mod metadata_tests;
mod native_context;
mod semantic;
#[cfg(test)]
mod tests;
pub(crate) mod writer;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};

pub use authority::{EffectDomain, EligibilityContext, LiveFence};
pub use bootstrap::bootstrap_nonadmin;
pub use inventory::RowIdentity;
pub use writer::RecoveryScope;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParentIdentity {
    pub identity: RowIdentity,
    pub revision: String,
    pub digest: String,
    #[serde(default)]
    pub role: ParentRole,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParentRole {
    #[default]
    Effectful,
    MetadataAnchor,
}

pub async fn require_row(
    connection: &mut SqliteConnection,
    context: &EligibilityContext,
    domain: EffectDomain,
    fence: Option<&LiveFence>,
    identity: &RowIdentity,
) -> Result<(), sqlx::Error> {
    let transaction_active = {
        let mut handle = connection.lock_handle().await?;
        unsafe {
            libsqlite3_sys::sqlite3_get_autocommit(handle.as_raw_handle().as_ptr()) == 0
                && libsqlite3_sys::sqlite3_txn_state(
                    handle.as_raw_handle().as_ptr(),
                    c"main".as_ptr(),
                ) == libsqlite3_sys::SQLITE_TXN_WRITE
        }
    };
    if !transaction_active {
        return Err(refused());
    }
    authority::validate(connection, context, domain, fence).await?;
    let mut pending = vec![(identity.clone(), None::<ParentIdentity>)];
    let mut checked = BTreeMap::<(String, String), (String, String)>::new();
    while let Some((identity, expected)) = pending.pop() {
        if let Some(parent) = expected.as_ref()
            && parent.role == ParentRole::MetadataAnchor
        {
            metadata::validate(connection, context, domain, parent).await?;
            continue;
        }
        if checked.len() >= 10000 {
            return Err(refused());
        }
        let key = (identity.table.clone(), identity.key.clone());
        if let Some((revision, digest)) = checked.get(&key) {
            if let Some(expected) = expected
                && (expected.revision != *revision || expected.digest != *digest)
            {
                return Err(refused());
            }
            continue;
        }
        let (revision, digest, parents) =
            validate_node(connection, context, domain, &identity, expected).await?;
        checked.insert(key, (revision, digest));
        for parent in parents {
            pending.push((parent.identity.clone(), Some(parent)));
        }
    }
    Ok(())
}

async fn validate_node(
    connection: &mut SqliteConnection,
    context: &EligibilityContext,
    domain: EffectDomain,
    identity: &RowIdentity,
    expected: Option<ParentIdentity>,
) -> Result<(String, String, Vec<ParentIdentity>), sqlx::Error> {
    let row = sqlx::query("SELECT original_revision,original_digest,parent_relations,disposition,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id,origin_generation FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=?")
            .bind(&identity.table).bind(&identity.key).fetch_optional(&mut *connection).await?
            .ok_or_else(refused)?;
    let revision: String = row.try_get(0)?;
    let digest: String = row.try_get(1)?;
    let disposition: String = row.try_get(3)?;
    context.matches_classification(&row)?;
    if !matches!(disposition.as_str(), "eligible_new" | "eligible_preserved")
        || (matches!(context, EligibilityContext::Admin { .. }) && disposition != "eligible_new")
    {
        return Err(refused());
    }
    if let Some(expected) = expected
        && (expected.revision != revision || expected.digest != digest)
    {
        return Err(refused());
    }
    let snapshot = inventory::snapshot(connection, identity).await?;
    if snapshot.revision != revision || snapshot.digest != digest {
        return Err(refused());
    }
    let matrix_operation: Option<String> = row.try_get(8)?;
    if snapshot.matrix && matrix_operation.is_none() {
        return Err(refused());
    }
    if domain == EffectDomain::Local && (matrix_operation.is_some() || snapshot.matrix) {
        return Err(refused());
    }
    if matrix_operation.is_some() && matrix_operation.as_deref() != context.matrix_operation() {
        return Err(refused());
    }
    let parents: Vec<ParentIdentity> =
        serde_json::from_str(row.try_get::<&str, _>(2)?).map_err(|_| refused())?;
    for required in snapshot.parents {
        if !parents.iter().any(|parent| parent.identity == required) {
            return Err(refused());
        }
    }
    for parent in &parents {
        let inherited: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_rows WHERE table_name=? AND canonical_pk=? AND matrix_operation_id IS NOT NULL")
            .bind(&parent.identity.table).bind(&parent.identity.key).fetch_one(&mut *connection).await?;
        if inherited != 0 && matrix_operation.is_none() {
            return Err(refused());
        }
    }
    Ok((revision, digest, parents))
}

fn refused() -> sqlx::Error {
    sqlx::Error::Protocol("offline-recovery-eligibility-refused".into())
}
