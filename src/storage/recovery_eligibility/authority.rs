use sqlx::{Row, SqliteConnection, sqlite::SqliteRow};

use super::refused;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectDomain {
    Local,
    Matrix,
}

#[derive(Debug, Clone)]
pub enum EligibilityContext {
    NonAdmin {
        generation: String,
        revision: i64,
        matrix_enabled: bool,
        matrix_operation: Option<String>,
    },
    Admin {
        generation: String,
        root: String,
        operation: String,
        plan: String,
        revision: i64,
        matrix_enabled: bool,
        matrix_operation: String,
    },
}

#[derive(Debug, Clone)]
pub struct LiveFence {
    pub component: String,
    pub owner: String,
    pub epoch: i64,
}

impl EligibilityContext {
    pub(super) fn matrix_operation(&self) -> Option<&str> {
        match self {
            Self::NonAdmin {
                matrix_operation, ..
            } => matrix_operation.as_deref(),
            Self::Admin {
                matrix_operation, ..
            } => Some(matrix_operation),
        }
    }

    pub(super) fn matches_classification(&self, row: &SqliteRow) -> Result<(), sqlx::Error> {
        let kind: &str = row.try_get(4)?;
        let generation: Option<&str> = row.try_get(5)?;
        let operation: Option<&str> = row.try_get(6)?;
        let plan: Option<&str> = row.try_get(7)?;
        let origin: &str = row.try_get(9)?;
        let valid = match self {
            Self::NonAdmin {
                generation: expected,
                ..
            } => {
                kind == "nonadmin"
                    && generation == Some(expected.as_str())
                    && origin == expected
                    && operation.is_none()
                    && plan.is_none()
            }
            Self::Admin {
                operation: expected,
                plan: expected_plan,
                matrix_operation: expected_origin,
                ..
            } => {
                kind == "admin"
                    && generation.is_none()
                    && operation == Some(expected.as_str())
                    && plan == Some(expected_plan.as_str())
                    && origin == expected_origin
            }
        };
        if valid { Ok(()) } else { Err(refused()) }
    }
}

pub(super) async fn validate(
    connection: &mut SqliteConnection,
    context: &EligibilityContext,
    domain: EffectDomain,
    fence: Option<&LiveFence>,
) -> Result<(), sqlx::Error> {
    let (valid, enabled) = match context {
        EligibilityContext::NonAdmin {
            generation,
            revision,
            matrix_enabled,
            matrix_operation,
        } => {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_eligibility_context AS context JOIN runtime_eligibility_bootstrap AS bootstrap ON bootstrap.generation=context.generation AND bootstrap.bootstrap_nonce=context.bootstrap_nonce AND bootstrap.inventory_digest=context.bootstrap_inventory_digest WHERE context.singleton=1 AND bootstrap.state='complete' AND context.generation=? AND context.revision=? AND context.state='ready' AND context.compatibility_operation_id IS ? AND context.matrix_enabled=? AND NOT EXISTS(SELECT 1 FROM offline_recovery_binding)")
                .bind(generation).bind(revision).bind(matrix_operation).bind(matrix_enabled).fetch_one(&mut *connection).await?;
            (count == 1, *matrix_enabled)
        }
        EligibilityContext::Admin {
            generation,
            root,
            operation,
            plan,
            revision,
            matrix_enabled,
            matrix_operation,
        } => {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_binding WHERE singleton=1 AND phase='active' AND db_generation=? AND root_digest=? AND admin_operation_id=? AND plan_digest=? AND revision=? AND native_operation_id=? AND transition_complete_digest IS NOT NULL AND NOT EXISTS(SELECT 1 FROM runtime_eligibility_context)")
                .bind(generation).bind(root).bind(operation).bind(plan).bind(revision).bind(matrix_operation).fetch_one(&mut *connection).await?;
            (count == 1, *matrix_enabled)
        }
    };
    if !valid {
        return Err(refused());
    }
    let held: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_rows WHERE disposition IN ('uncertain_hold','refused_hold')")
        .fetch_one(&mut *connection).await?;
    if held != 0 {
        return Err(refused());
    }
    if domain == EffectDomain::Matrix {
        if !enabled || fence.is_none() {
            return Err(refused());
        }
        let operation = context.matrix_operation().ok_or_else(refused)?;
        let fence = fence.ok_or_else(refused)?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_control AS control JOIN matrix_recovery_compatibility AS compat ON compat.native_operation_id=control.cutover_operation_id WHERE control.control_id=1 AND control.phase='active' AND control.watermark IS NOT NULL AND control.cutover_operation_id=? AND control.restart_hold_state IS NULL AND compat.decision='native' AND NOT EXISTS(SELECT 1 FROM matrix_recovery_receipts WHERE state IN ('unknown','refused')) AND NOT EXISTS(SELECT 1 FROM matrix_recovery_batches WHERE state IN ('unknown','refused') OR (state IN ('pending','processing') AND NOT(component=? AND owner_token=? AND owner_epoch=?)))")
            .bind(operation).bind(&fence.component).bind(&fence.owner).bind(fence.epoch).fetch_one(&mut *connection).await?;
        if count != 1 {
            return Err(refused());
        }
        if matches!(context, EligibilityContext::NonAdmin { .. }) {
            let (expected, actual): (Option<String>, Option<String>) = sqlx::query_as("SELECT context.expected_b_digest,control.watermark FROM runtime_eligibility_context AS context JOIN matrix_recovery_control AS control ON control.cutover_operation_id=context.compatibility_operation_id WHERE context.matrix_phase='active' AND context.provenance_kind='validated_native'")
                .fetch_optional(&mut *connection).await?.ok_or_else(refused)?;
            use sha2::{Digest, Sha256};
            let actual = actual.map(|token| format!("{:x}", Sha256::digest(token.as_bytes())));
            if expected.is_none() || expected != actual {
                return Err(refused());
            }
        }
    }
    if let Some(fence) = fence {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_component_leases WHERE component=? AND owner_token=? AND epoch=? AND state='active' AND julianday(expires_at)>julianday('now')")
            .bind(&fence.component).bind(&fence.owner).bind(fence.epoch).fetch_one(connection).await?;
        if count != 1 {
            return Err(refused());
        }
    }
    Ok(())
}
