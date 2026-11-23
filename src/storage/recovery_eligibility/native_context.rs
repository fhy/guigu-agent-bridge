use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

use super::{EligibilityContext, refused};

pub(super) async fn validate(
    connection: &mut SqliteConnection,
    context: &EligibilityContext,
) -> Result<(), sqlx::Error> {
    match context {
        EligibilityContext::NonAdmin {
            generation,
            matrix_operation: None,
            ..
        } => {
            let valid: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_eligibility_context WHERE generation=? AND provenance_kind='pristine_local' AND matrix_phase='absent' AND expected_b_digest IS NULL AND NOT EXISTS(SELECT 1 FROM matrix_recovery_compatibility) AND NOT EXISTS(SELECT 1 FROM matrix_recovery_control)")
                .bind(generation).fetch_one(connection).await?;
            if valid != 1 {
                return Err(refused());
            }
        }
        EligibilityContext::NonAdmin {
            generation,
            matrix_operation: Some(operation),
            ..
        } => {
            let row = sqlx::query("SELECT context.matrix_phase,context.expected_b_digest,context.compatibility_source_digest,control.phase,control.watermark FROM runtime_eligibility_context AS context JOIN matrix_recovery_control AS control ON control.cutover_operation_id=context.compatibility_operation_id JOIN matrix_recovery_compatibility AS compatibility ON compatibility.native_operation_id=control.cutover_operation_id WHERE context.generation=? AND context.provenance_kind='validated_native' AND compatibility.decision='native' AND compatibility.native_operation_id=? AND compatibility.source_state_digest=context.compatibility_source_digest AND control.restart_hold_state IS NULL")
                .bind(generation).bind(operation).fetch_optional(&mut *connection).await?.ok_or_else(refused)?;
            let phase: &str = row.try_get(0)?;
            let expected: Option<&str> = row.try_get(1)?;
            let actual: Option<String> = row.try_get(4)?;
            let digest = actual
                .as_ref()
                .map(|token| format!("{:x}", Sha256::digest(token.as_bytes())));
            if phase != row.try_get::<&str, _>(3)?
                || expected != digest.as_deref()
                || !match phase {
                    "baseline" => actual.is_none(),
                    "active" => actual.as_ref().is_some_and(|token| !token.is_empty()),
                    _ => false,
                }
            {
                return Err(refused());
            }
            let legacy = crate::storage::matrix_recovery_snapshot::snapshot(connection).await?;
            if row.try_get::<&str, _>(2)? != legacy.digest {
                return Err(refused());
            }
        }
        EligibilityContext::Admin {
            matrix_operation, ..
        } => {
            let valid: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_control AS control JOIN matrix_recovery_compatibility AS compatibility ON compatibility.native_operation_id=control.cutover_operation_id WHERE control.control_id=1 AND control.cutover_operation_id=? AND control.phase='active' AND control.watermark IS NOT NULL AND length(CAST(control.watermark AS BLOB))>0 AND control.restart_hold_state IS NULL AND compatibility.decision='native'")
                .bind(matrix_operation).fetch_one(connection).await?;
            if valid != 1 {
                return Err(refused());
            }
        }
    }
    Ok(())
}
