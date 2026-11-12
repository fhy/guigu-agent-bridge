use sqlx::Row;

use super::{PendingProjection, ReliabilityError, ReliabilityStore};
use crate::storage::recovery_eligibility::{RowIdentity, writer};

impl ReliabilityStore {
    pub(crate) async fn has_sending_projection(&self) -> Result<bool, ReliabilityError> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM projection_outbox WHERE state='sending')",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn claim_projections(
        &self,
        owner: &str,
        now: &str,
        _stale_before: &str,
        limit: u32,
    ) -> Result<Vec<PendingProjection>, ReliabilityError> {
        self.require_projection_component()?;
        let mut transaction = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let mut claimed = Vec::new();
        for _ in 0..limit {
            let row = sqlx::query("SELECT source_kind,source_id,projection FROM projection_outbox AS candidate WHERE state='pending' AND (?=0 OR NOT EXISTS(SELECT 1 FROM offline_recovery_rows AS eligibility WHERE eligibility.table_name='projection_outbox' AND eligibility.canonical_pk=json_array(typeof(candidate.source_kind)||':'||hex(CAST(candidate.source_kind AS BLOB)),typeof(candidate.source_id)||':'||hex(CAST(candidate.source_id AS BLOB)),typeof(candidate.projection)||':'||hex(CAST(candidate.projection AS BLOB))) AND eligibility.disposition IN ('terminal_evidenced','quarantined_no_effect'))) ORDER BY created_at,source_kind,source_id,projection LIMIT 1")
                .bind(self.recovery.is_some()).fetch_optional(&mut *transaction).await?;
            let Some(row) = row else { break };
            let source_kind: String = row.try_get("source_kind")?;
            let source_id: String = row.try_get("source_id")?;
            let projection: String = row.try_get("projection")?;
            let identity = identity(&source_kind, &source_id, &projection)?;
            let mutation =
                writer::prepare_mutation(&mut transaction, self.recovery.as_ref(), &identity)
                    .await?;
            let row = sqlx::query("UPDATE projection_outbox SET state='sending',claim_owner=?,claim_revision=COALESCE(claim_revision,0)+1,updated_at=? WHERE source_kind=? AND source_id=? AND projection=? AND state='pending' RETURNING stable_txn_id,room_id,thread_root,reply_event_id,body,claim_revision")
                .bind(owner).bind(now).bind(&source_kind).bind(&source_id).bind(&projection)
                .fetch_one(&mut *transaction).await?;
            writer::complete_mutation(&mut transaction, self.recovery.as_ref(), mutation).await?;
            claimed.push(PendingProjection {
                source_kind,
                source_id,
                projection,
                stable_txn_id: row.try_get("stable_txn_id")?,
                room_id: row.try_get("room_id")?,
                thread_root: row.try_get("thread_root")?,
                reply_event_id: row.try_get("reply_event_id")?,
                body: row.try_get("body")?,
                claim_owner: owner.into(),
                claim_revision: row.try_get("claim_revision")?,
            });
        }
        transaction.commit().await?;
        Ok(claimed)
    }

    pub async fn mark_projection_sent(
        &self,
        row: &PendingProjection,
    ) -> Result<bool, ReliabilityError> {
        self.finish_projection(row, "sent").await
    }

    pub async fn authorize_projection_send(
        &self,
        row: &PendingProjection,
    ) -> Result<bool, ReliabilityError> {
        self.require_projection_component()?;
        let mut transaction = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let identity = identity(&row.source_kind, &row.source_id, &row.projection)?;
        writer::prepare_mutation(&mut transaction, self.recovery.as_ref(), &identity).await?;
        let matches: i64 = sqlx::query_scalar("SELECT count(*) FROM projection_outbox WHERE source_kind=? AND source_id=? AND projection=? AND state='sending' AND claim_owner=? AND claim_revision=?")
            .bind(&row.source_kind).bind(&row.source_id).bind(&row.projection)
            .bind(&row.claim_owner).bind(row.claim_revision).fetch_one(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(matches == 1)
    }

    fn require_projection_component(&self) -> Result<(), ReliabilityError> {
        if let Some(scope) = self.recovery.as_ref() {
            scope.require_matrix_component("OutboxDrain")?;
        }
        Ok(())
    }

    pub async fn mark_projection_abandoned(
        &self,
        row: &PendingProjection,
    ) -> Result<bool, ReliabilityError> {
        self.finish_projection(row, "abandoned").await
    }

    async fn finish_projection(
        &self,
        row: &PendingProjection,
        state: &str,
    ) -> Result<bool, ReliabilityError> {
        self.require_projection_component()
            .map_err(|error| completion_failure("component", error))?;
        let mut transaction = writer::begin(&self.pool, self.recovery.as_ref())
            .await
            .map_err(|error| completion_failure("begin", error.into()))?;
        let identity = identity(&row.source_kind, &row.source_id, &row.projection)
            .map_err(|error| completion_failure("identity", error.into()))?;
        let mutation =
            writer::prepare_mutation(&mut transaction, self.recovery.as_ref(), &identity)
                .await
                .map_err(|error| completion_failure("prepare", error.into()))?;
        let result = sqlx::query("UPDATE projection_outbox SET state=?,attempt_count=attempt_count+1,updated_at=CASE WHEN ?='abandoned' THEN ? ELSE updated_at END WHERE source_kind=? AND source_id=? AND projection=? AND state='sending' AND claim_owner=? AND claim_revision=?")
            .bind(state).bind(state).bind(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
            .bind(&row.source_kind).bind(&row.source_id).bind(&row.projection)
            .bind(&row.claim_owner).bind(row.claim_revision).execute(&mut *transaction).await
            .map_err(|error| completion_failure("update", error.into()))?;
        if result.rows_affected() != 1 {
            tracing::error!(
                stage = "outbox-completion",
                step = "claim-cas",
                class = "cas-mismatch",
                outcome = "retained-without-resend"
            );
            return Ok(false);
        }
        writer::complete_mutation(&mut transaction, self.recovery.as_ref(), mutation)
            .await
            .map_err(|error| completion_failure("classification", error.into()))?;
        transaction
            .commit()
            .await
            .map_err(|error| completion_failure("commit", error.into()))?;
        Ok(true)
    }
}

fn identity(
    source_kind: &str,
    source_id: &str,
    projection: &str,
) -> Result<RowIdentity, sqlx::Error> {
    let keys = [source_kind, source_id, projection].map(|value| {
        format!(
            "text:{}",
            value
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        )
    });
    Ok(RowIdentity {
        table: "projection_outbox".into(),
        key: serde_json::to_string(&keys)
            .map_err(|_| sqlx::Error::Protocol("offline-recovery-eligibility-refused".into()))?,
    })
}

// Never render SQL errors: their messages may contain database values or paths.
fn completion_failure(step: &'static str, error: ReliabilityError) -> ReliabilityError {
    let class = match &error {
        ReliabilityError::Sql(error)
            if crate::storage::recovery_eligibility::matrix_effect_busy(error) =>
        {
            "sync-busy"
        }
        ReliabilityError::Sql(sqlx::Error::Protocol(_)) => "authority-or-classification",
        ReliabilityError::Sql(sqlx::Error::Database(_)) => "database",
        ReliabilityError::Sql(sqlx::Error::PoolTimedOut) => "pool-timeout",
        ReliabilityError::Sql(_) => "storage",
        _ => "component",
    };
    if class == "sync-busy" {
        tracing::debug!(
            stage = "outbox-completion",
            step,
            class,
            outcome = "retained-without-resend"
        );
    } else {
        tracing::error!(
            stage = "outbox-completion",
            step,
            class,
            outcome = "retained-without-resend"
        );
    }
    error
}
