use sqlx::SqliteConnection;

use super::Failure;

pub(super) async fn original(connection: &mut SqliteConnection) -> Result<Option<String>, Failure> {
    sqlx::query_scalar("SELECT json_array(singleton,decision,source_schema_version,source_state_digest,legacy_operation_id,legacy_phase,legacy_baseline,legacy_baseline_digest,legacy_watermark,legacy_revision,legacy_binding,legacy_archive_digest,native_operation_id,reason_code,created_at) FROM matrix_recovery_compatibility WHERE singleton=1")
        .fetch_optional(connection).await.map_err(|_| Failure::ManualRequired)
}

pub(super) async fn apply(
    connection: &mut SqliteConnection,
    operation: &str,
    original_record: &Option<String>,
    source_digest: &str,
) -> Result<(), Failure> {
    if original(connection).await? != *original_record {
        return Err(Failure::ManualRequired);
    }
    let reserved: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_eligibility_bootstrap WHERE singleton=1 AND state='admin_reserved' AND NOT EXISTS(SELECT 1 FROM runtime_eligibility_context)")
        .fetch_one(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
    if reserved != 1 {
        return Err(Failure::ManualRequired);
    }
    let uncertain: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM matrix_recovery_control)+(SELECT count(*) FROM matrix_recovery_batches)+(SELECT count(*) FROM matrix_recovery_receipts)+(SELECT count(*) FROM matrix_component_leases)+(SELECT count(*) FROM matrix_activation_intent)")
        .fetch_one(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
    if uncertain != 0 {
        return Err(Failure::ManualRequired);
    }
    let legacy = crate::storage::matrix_recovery_snapshot::snapshot(connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
    if legacy.digest != source_digest {
        return Err(Failure::ManualRequired);
    }
    let now = chrono::Utc::now().to_rfc3339();
    if original_record.is_some() {
        let changed = sqlx::query("UPDATE matrix_recovery_compatibility SET decision='native',native_operation_id=?,reason_code='administrative-transition' WHERE singleton=1 AND decision='held' AND source_state_digest=? AND native_operation_id IS NULL")
            .bind(operation).bind(source_digest).execute(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
        if changed.rows_affected() != 1 {
            return Err(Failure::ManualRequired);
        }
    } else {
        crate::storage::MatrixCompatibilityStore::persist_record(
            connection,
            &legacy,
            Some(operation),
            None,
            &now,
        )
        .await
        .map_err(|_| Failure::ManualRequired)?;
    }
    sqlx::query("INSERT INTO matrix_recovery_control(control_id,cutover_operation_id,phase,watermark,updated_at) VALUES(1,?,'baseline',NULL,?)")
        .bind(operation).bind(now).execute(connection).await.map_err(|_| Failure::ManualRequired)?;
    Ok(())
}
