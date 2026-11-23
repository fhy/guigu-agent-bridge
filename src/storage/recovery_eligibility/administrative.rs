use sqlx::{Row, SqliteConnection, SqlitePool};

use super::{EffectDomain, EligibilityContext, inventory, refused};

pub(crate) async fn context(
    pool: &SqlitePool,
    enabled: bool,
) -> Result<EligibilityContext, sqlx::Error> {
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let binding = sqlx::query("SELECT db_generation,root_digest,admin_operation_id,plan_digest,revision,native_operation_id FROM offline_recovery_binding WHERE singleton=1 AND phase='active'")
        .fetch_one(&mut *transaction).await?;
    let context = EligibilityContext::Admin {
        generation: binding.try_get(0)?,
        root: binding.try_get(1)?,
        operation: binding.try_get(2)?,
        plan: binding.try_get(3)?,
        revision: binding.try_get(4)?,
        matrix_enabled: enabled,
        matrix_operation: binding.try_get(5)?,
    };
    super::authority::validate(&mut transaction, &context, EffectDomain::Local, None).await?;
    super::bootstrap::readback::inventory(&mut transaction, &context).await?;
    transaction.commit().await?;
    Ok(context)
}

pub(crate) async fn seed_activation(connection: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let (operation, plan, native): (String, String, String) = sqlx::query_as("SELECT admin_operation_id,plan_digest,native_operation_id FROM offline_recovery_binding WHERE singleton=1 AND phase='active'")
        .fetch_one(&mut *connection).await?;
    let identities = [
        inventory::RowIdentity {
            table: "matrix_recovery_control".into(),
            key: serde_json::to_string(&["integer:31"]).map_err(|_| refused())?,
        },
        super::writer::text_identity("matrix_component_leases", "MatrixSync")?,
    ];
    for identity in identities {
        let snapshot = inventory::snapshot(connection, &identity).await?;
        if !snapshot.parents.is_empty() {
            return Err(refused());
        }
        sqlx::query("INSERT INTO offline_recovery_rows(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,admin_operation_id,plan_digest,matrix_operation_id) VALUES(?,?,?,?,?,'[]','eligible_new',?,'admin',?,?,?)")
            .bind(&identity.table).bind(&identity.key).bind(&native).bind(snapshot.revision).bind(&snapshot.digest)
            .bind(&snapshot.digest).bind(&operation).bind(&plan).bind(&native).execute(&mut *connection).await?;
    }
    Ok(())
}
