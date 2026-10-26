use serde_json::Value;
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchAdmission {
    New,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchState {
    pub batch_id: String,
    pub response_token: String,
    pub expected_events: Vec<String>,
    pub covered_events: Vec<String>,
    pub state: String,
}

#[derive(Clone)]
pub struct MatrixRecoveryStore {
    pool: SqlitePool,
}

impl MatrixRecoveryStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn initialize_cutover(
        &self,
        operation_id: &str,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO matrix_recovery_control(control_id,cutover_operation_id,phase,watermark,updated_at) VALUES(1,?,'baseline',NULL,?) ON CONFLICT(control_id) DO NOTHING",
        )
        .bind(operation_id).bind(now).execute(&self.pool).await?;
        let stored: String = sqlx::query_scalar(
            "SELECT cutover_operation_id FROM matrix_recovery_control WHERE control_id=1",
        )
        .fetch_one(&self.pool)
        .await?;
        if stored != operation_id {
            return Err(sqlx::Error::Protocol("cutover provenance mismatch".into()));
        }
        Ok(())
    }

    pub async fn activate_cutover(&self, watermark: &str, now: &str) -> Result<(), sqlx::Error> {
        let result = sqlx::query("UPDATE matrix_recovery_control SET phase='active',watermark=?,updated_at=? WHERE control_id=1 AND phase='baseline'")
            .bind(watermark).bind(now).execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }

    pub async fn ensure_active(
        &self,
        operation_id: &str,
        initial_watermark: &str,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        self.initialize_cutover(operation_id, now).await?;
        let phase: String =
            sqlx::query_scalar("SELECT phase FROM matrix_recovery_control WHERE control_id=1")
                .fetch_one(&self.pool)
                .await?;
        if phase == "baseline" {
            self.activate_cutover(initial_watermark, now).await?;
        }
        Ok(())
    }

    pub async fn acquire_component_lease(
        &self,
        component: &str,
        owner_token: &str,
        now: &str,
        expires_at: &str,
    ) -> Result<i64, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query(
            "SELECT owner_token,epoch,state,expires_at FROM matrix_component_leases WHERE component=?",
        )
        .bind(component)
        .fetch_optional(&mut *tx)
        .await?;
        let epoch = match existing {
            None => 1,
            Some(row) => {
                let state: String = row.try_get("state")?;
                let current: String = row.try_get("owner_token")?;
                let expires: String = row.try_get("expires_at")?;
                if state == "active" && current != owner_token && expires.as_str() > now {
                    return Err(sqlx::Error::Protocol("component lease is owned".into()));
                }
                row.try_get::<i64, _>("epoch")? + 1
            }
        };
        sqlx::query("INSERT INTO matrix_component_leases(component,owner_token,epoch,state,expires_at,updated_at) VALUES(?,?,?,'active',?,?) ON CONFLICT(component) DO UPDATE SET owner_token=excluded.owner_token,epoch=excluded.epoch,state='active',expires_at=excluded.expires_at,updated_at=excluded.updated_at")
            .bind(component).bind(owner_token).bind(epoch).bind(expires_at).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(epoch)
    }

    pub async fn begin_batch(
        &self,
        batch_id: &str,
        request_token: Option<&str>,
        response_token: &str,
        response_digest: &str,
        previous_watermark: &str,
        owner_token: &str,
        owner_epoch: i64,
        expected_events: &[String],
        now: &str,
    ) -> Result<(), sqlx::Error> {
        let active: Option<String> = sqlx::query_scalar(
            "SELECT watermark FROM matrix_recovery_control WHERE control_id=1 AND phase='active'",
        )
        .fetch_optional(&self.pool)
        .await?;
        if active.as_deref() != Some(previous_watermark)
            || (request_token != Some(previous_watermark)
                && !(previous_watermark.is_empty() && request_token.is_none()))
        {
            return Err(sqlx::Error::Protocol(
                "batch previous watermark mismatch".into(),
            ));
        }
        let lease: Option<(String, i64)> = sqlx::query_as("SELECT owner_token,epoch FROM matrix_component_leases WHERE component='MatrixSync' AND state='active' AND expires_at>=?")
            .bind(now).fetch_optional(&self.pool).await?;
        if lease.as_ref() != Some(&(owner_token.to_owned(), owner_epoch)) {
            return Err(sqlx::Error::Protocol("stale component lease".into()));
        }
        if let Some(existing_digest) = sqlx::query_scalar::<_, String>(
            "SELECT response_digest FROM matrix_recovery_batches WHERE response_token=? AND previous_watermark=? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(response_token)
        .bind(previous_watermark)
        .fetch_optional(&self.pool)
        .await?
        {
            if existing_digest != response_digest {
                return Err(sqlx::Error::Protocol("response digest mismatch".into()));
            }
        }
        sqlx::query("INSERT INTO matrix_recovery_batches(batch_id,request_token,previous_watermark,response_token,response_digest,component,owner_token,owner_epoch,expected_events_json,covered_events_json,state,created_at,updated_at) VALUES(?,?,?,?,?,'MatrixSync',?,?,?,'[]','pending',?,?)")
            .bind(batch_id).bind(request_token).bind(previous_watermark).bind(response_token).bind(response_digest).bind(owner_token).bind(owner_epoch)
            .bind(serde_json::to_string(expected_events).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
            .bind(now).bind(now).execute(&self.pool).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn admit_event(
        &self,
        account_id: &str,
        room_id: &str,
        event_id: &str,
        operation_id: &str,
        payload_digest: &str,
        batch_id: &str,
        now: &str,
    ) -> Result<BatchAdmission, sqlx::Error> {
        let admission = self
            .reserve_event(
                account_id,
                room_id,
                event_id,
                operation_id,
                payload_digest,
                batch_id,
                now,
            )
            .await?;
        if admission == BatchAdmission::New {
            self.finish_event(
                account_id,
                room_id,
                event_id,
                operation_id,
                batch_id,
                operation_id,
                1,
                now,
            )
            .await?;
        }
        Ok(admission)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn reserve_event(
        &self,
        account_id: &str,
        room_id: &str,
        event_id: &str,
        operation_id: &str,
        payload_digest: &str,
        batch_id: &str,
        now: &str,
    ) -> Result<BatchAdmission, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query("SELECT payload_digest,state FROM matrix_recovery_receipts WHERE account_id=? AND room_id=? AND event_id=? AND cutover_operation_id=?")
            .bind(account_id).bind(room_id).bind(event_id).bind(operation_id).fetch_optional(&mut *tx).await?;
        if let Some(row) = existing {
            let digest: String = row.try_get("payload_digest")?;
            if digest != payload_digest {
                return Err(sqlx::Error::Protocol("receipt digest mismatch".into()));
            }
            let state: String = row.try_get("state")?;
            tx.commit().await?;
            if state == "dispositioned" {
                return Ok(BatchAdmission::Duplicate);
            }
            return Err(sqlx::Error::Protocol(
                "receipt requires recovery reconciliation".into(),
            ));
        }
        let expected: String = sqlx::query_scalar("SELECT expected_events_json FROM matrix_recovery_batches WHERE batch_id=? AND state IN ('pending','processing')")
            .bind(batch_id).fetch_one(&mut *tx).await?;
        let expected: Vec<String> = serde_json::from_str(&expected)
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        if !expected.iter().any(|value| value == event_id) {
            return Err(sqlx::Error::Protocol("event outside batch coverage".into()));
        }
        sqlx::query("INSERT INTO matrix_recovery_receipts(account_id,room_id,event_id,cutover_operation_id,payload_digest,state,batch_id,created_at,updated_at) VALUES(?,?,?,?,?,'pending',?,?,?)")
            .bind(account_id).bind(room_id).bind(event_id).bind(operation_id).bind(payload_digest).bind(batch_id).bind(now).bind(now).execute(&mut *tx).await?;
        let row = sqlx::query("SELECT covered_events_json FROM matrix_recovery_batches WHERE batch_id=? AND state IN ('pending','processing')")
            .bind(batch_id).fetch_one(&mut *tx).await?;
        let mut covered: Vec<String> =
            serde_json::from_str(row.try_get::<String, _>("covered_events_json")?.as_str())
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        if !covered.iter().any(|value| value == event_id) {
            covered.push(event_id.to_owned());
            sqlx::query("UPDATE matrix_recovery_batches SET covered_events_json=?,state='processing',updated_at=? WHERE batch_id=?")
                .bind(serde_json::to_string(&covered).map_err(|error| sqlx::Error::Protocol(error.to_string()))?)
                .bind(now).bind(batch_id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(BatchAdmission::New)
    }

    pub async fn finish_event(
        &self,
        account_id: &str,
        room_id: &str,
        event_id: &str,
        operation_id: &str,
        batch_id: &str,
        owner_token: &str,
        owner_epoch: i64,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query("UPDATE matrix_recovery_receipts SET state='dispositioned',updated_at=? WHERE account_id=? AND room_id=? AND event_id=? AND cutover_operation_id=? AND batch_id=? AND state IN ('pending','processing') AND EXISTS (SELECT 1 FROM matrix_recovery_batches WHERE batch_id=? AND owner_token=? AND owner_epoch=? AND state='processing')")
            .bind(now).bind(account_id).bind(room_id).bind(event_id).bind(operation_id).bind(batch_id).bind(batch_id).bind(owner_token).bind(owner_epoch).execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(
                "receipt disposition fence failed".into(),
            ));
        }
        Ok(())
    }

    pub async fn refuse_event(
        &self,
        account_id: &str,
        room_id: &str,
        event_id: &str,
        operation_id: &str,
        batch_id: &str,
        code: &str,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query("UPDATE matrix_recovery_receipts SET state='refused',refusal_code=?,updated_at=? WHERE account_id=? AND room_id=? AND event_id=? AND cutover_operation_id=? AND batch_id=? AND state IN ('pending','processing')")
            .bind(code).bind(now).bind(account_id).bind(room_id).bind(event_id).bind(operation_id).bind(batch_id).execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(
                "refusal disposition fence failed".into(),
            ));
        }
        Ok(())
    }

    pub async fn complete_batch(
        &self,
        batch_id: &str,
        checkpoint: &str,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT previous_watermark,response_token,expected_events_json,covered_events_json,state FROM matrix_recovery_batches WHERE batch_id=?")
            .bind(batch_id).fetch_one(&mut *tx).await?;
        let expected: Vec<String> =
            serde_json::from_str(row.try_get::<String, _>("expected_events_json")?.as_str())
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        let covered: Vec<String> =
            serde_json::from_str(row.try_get::<String, _>("covered_events_json")?.as_str())
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        if row.try_get::<String, _>("state")? == "refused"
            || expected.iter().any(|event| !covered.contains(event))
        {
            return Err(sqlx::Error::Protocol("batch coverage incomplete".into()));
        }
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM matrix_recovery_receipts WHERE batch_id=? AND state <> 'dispositioned'",
        )
        .bind(batch_id)
        .fetch_one(&mut *tx)
        .await?;
        if pending != 0 {
            return Err(sqlx::Error::Protocol(
                "batch effects are not terminal".into(),
            ));
        }
        let previous: String = row.try_get("previous_watermark")?;
        let token: String = row.try_get("response_token")?;
        if token != checkpoint {
            return Err(sqlx::Error::Protocol(
                "checkpoint is not batch response token".into(),
            ));
        }
        let batch_update = sqlx::query(
            "UPDATE matrix_recovery_batches SET state='terminal',updated_at=? WHERE batch_id=? AND previous_watermark=? AND state='processing'",
        )
        .bind(now)
        .bind(batch_id).bind(&previous).execute(&mut *tx).await?;
        if batch_update.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(
                "batch completion fence failed".into(),
            ));
        }
        let watermark_update = sqlx::query("UPDATE matrix_recovery_control SET watermark=?,updated_at=? WHERE control_id=1 AND phase='active' AND watermark=?")
            .bind(checkpoint).bind(now).bind(&previous).execute(&mut *tx).await?;
        if watermark_update.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol("watermark CAS failed".into()));
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn batch(&self, batch_id: &str) -> Result<Option<BatchState>, sqlx::Error> {
        let row = sqlx::query("SELECT response_token,expected_events_json,covered_events_json,state FROM matrix_recovery_batches WHERE batch_id=?").bind(batch_id).fetch_optional(&self.pool).await?;
        row.map(|row| {
            Ok(BatchState {
                batch_id: batch_id.to_owned(),
                response_token: row.try_get("response_token")?,
                expected_events: serde_json::from_str(
                    row.try_get::<String, _>("expected_events_json")?.as_str(),
                )
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?,
                covered_events: serde_json::from_str(
                    row.try_get::<String, _>("covered_events_json")?.as_str(),
                )
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))?,
                state: row.try_get("state")?,
            })
        })
        .transpose()
    }
}

pub fn event_set(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| {
            event
                .get("event_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect()
}
