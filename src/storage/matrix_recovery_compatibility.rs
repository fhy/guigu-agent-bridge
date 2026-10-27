use std::path::Path;

use sqlx::{Row, SqlitePool};

use super::{
    MatrixRecoveryStore,
    matrix_recovery_snapshot::snapshot,
    matrix_store_evidence::{self, StoreEvidence},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatibilityGate {
    PristineDisabled,
    Native(String),
    Held(&'static str),
}

#[derive(Clone)]
pub struct MatrixCompatibilityStore {
    pool: SqlitePool,
}

impl MatrixCompatibilityStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn gate(
        &self,
        enabled: bool,
        root: Option<&Path>,
    ) -> Result<CompatibilityGate, sqlx::Error> {
        let recorded: i64 =
            sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_compatibility")
                .fetch_one(&self.pool)
                .await?;
        let evidence = if recorded == 0 {
            matrix_store_evidence::inspect(root)
        } else {
            StoreEvidence::Empty
        };
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let legacy = snapshot(&mut transaction).await?;
        let decision: Option<(String, String)> = sqlx::query_as("SELECT decision,source_state_digest FROM matrix_recovery_compatibility WHERE singleton=1")
            .fetch_optional(&mut *transaction).await?;
        let selected = if let Some((decision, digest)) = decision {
            if digest != legacy.digest {
                CompatibilityGate::Held("legacy-source-digest-mismatch")
            } else if decision == "held" {
                CompatibilityGate::Held("compatibility-held")
            } else {
                Self::native_readback(&mut transaction).await?
            }
        } else {
            Self::create_decision(&mut transaction, enabled, &legacy, evidence).await?
        };
        transaction.commit().await?;
        if let CompatibilityGate::Native(operation) = selected {
            if MatrixRecoveryStore::new(self.pool.clone())
                .reconcile_restart()
                .await
                .is_err()
            {
                return Ok(CompatibilityGate::Held("native-restart-hold"));
            }
            let mut connection = self.pool.acquire().await?;
            return match Self::native_readback(&mut connection).await? {
                CompatibilityGate::Native(readback) if readback == operation => {
                    Ok(CompatibilityGate::Native(operation))
                }
                _ => Ok(CompatibilityGate::Held("native-control-inconsistent")),
            };
        }
        Ok(selected)
    }

    async fn create_decision(
        connection: &mut sqlx::SqliteConnection,
        enabled: bool,
        legacy: &super::matrix_recovery_snapshot::LegacySnapshot,
        evidence: StoreEvidence,
    ) -> Result<CompatibilityGate, sqlx::Error> {
        let selected = {
            let control: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_control")
                .fetch_one(&mut *connection)
                .await?;
            let reason = if legacy.legacy_rows > 0 {
                Some("legacy-cutover-unproven")
            } else if control > 0 {
                Some("mixed-owner-provenance")
            } else if legacy.work_rows > 0 {
                Some("legacy-unbound-state")
            } else {
                match evidence {
                    StoreEvidence::Nonempty => Some("legacy-sdk-store-unbound"),
                    StoreEvidence::Indeterminate => Some("legacy-sdk-store-indeterminate"),
                    StoreEvidence::Empty => None,
                }
            };
            if reason.is_none() && !enabled {
                CompatibilityGate::PristineDisabled
            } else {
                let operation = reason.is_none().then(|| uuid::Uuid::now_v7().to_string());
                let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
                Self::persist_record(connection, legacy, operation.as_deref(), reason, &now)
                    .await?;
                if let Some(operation) = operation {
                    sqlx::query("INSERT INTO matrix_recovery_control(control_id,cutover_operation_id,phase,watermark,updated_at) VALUES(1,?,'baseline',NULL,?)")
                    .bind(&operation).bind(&now).execute(&mut *connection).await?;
                    Self::native_readback(&mut *connection).await?
                } else {
                    CompatibilityGate::Held(reason.unwrap_or("compatibility-held"))
                }
            }
        };
        Ok(selected)
    }

    async fn persist_record(
        connection: &mut sqlx::SqliteConnection,
        legacy: &super::matrix_recovery_snapshot::LegacySnapshot,
        operation: Option<&str>,
        reason: Option<&str>,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        let (
            legacy_operation,
            phase,
            baseline,
            baseline_digest,
            watermark,
            revision,
            binding,
            archive,
        ) = match &legacy.cutover {
            Some((operation, phase, baseline, digest, watermark, revision, binding, archive)) => (
                Some(operation),
                Some(phase),
                baseline.as_ref(),
                digest.as_ref(),
                watermark.as_ref(),
                Some(*revision),
                Some(binding),
                Some(archive),
            ),
            None => (None, None, None, None, None, None, None, None),
        };
        sqlx::query("INSERT INTO matrix_recovery_compatibility(singleton,decision,source_schema_version,source_state_digest,legacy_operation_id,legacy_phase,legacy_baseline,legacy_baseline_digest,legacy_watermark,legacy_revision,legacy_binding,legacy_archive_digest,native_operation_id,reason_code,created_at) VALUES(1,?,24,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(if reason.is_some() { "held" } else { "native" }).bind(&legacy.digest)
            .bind(legacy_operation).bind(phase).bind(baseline).bind(baseline_digest).bind(watermark)
            .bind(revision).bind(binding).bind(archive).bind(operation).bind(reason.unwrap_or("pristine-native")).bind(now)
            .execute(&mut *connection).await?;
        Ok(())
    }

    async fn native_readback(
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<CompatibilityGate, sqlx::Error> {
        let row = sqlx::query("SELECT compatibility.native_operation_id,control.cutover_operation_id,control.phase,control.watermark,control.restart_hold_state FROM matrix_recovery_compatibility compatibility LEFT JOIN matrix_recovery_control control ON control.control_id=1 WHERE compatibility.singleton=1 AND compatibility.decision='native'")
            .fetch_one(connection).await?;
        let operation: String = row.try_get("native_operation_id")?;
        let stored: Option<String> = row.try_get("cutover_operation_id")?;
        let phase: Option<String> = row.try_get("phase")?;
        let watermark: Option<String> = row.try_get("watermark")?;
        let hold: Option<String> = row.try_get("restart_hold_state")?;
        let valid_phase = match phase.as_deref() {
            Some("baseline") => watermark.is_none(),
            Some("active") => watermark.as_ref().is_some_and(|token| !token.is_empty()),
            _ => false,
        };
        if operation.trim().is_empty()
            || stored.as_deref() != Some(operation.as_str())
            || !valid_phase
            || hold.is_some()
        {
            return Ok(CompatibilityGate::Held("native-control-inconsistent"));
        }
        Ok(CompatibilityGate::Native(operation))
    }
}
