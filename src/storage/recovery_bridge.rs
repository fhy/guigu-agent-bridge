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
            "INSERT INTO matrix_recovery_control(control_id,cutover_operation_id,phase,watermark,updated_at) VALUES(1,?,'baseline',NULL,?) ON CONFLICT(control_id) DO UPDATE SET cutover_operation_id=excluded.cutover_operation_id,phase='baseline',watermark=NULL,updated_at=excluded.updated_at",
        )
        .bind(operation_id).bind(now).execute(&self.pool).await?;
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

    pub async fn begin_batch(
        &self,
        batch_id: &str,
        request_token: Option<&str>,
        response_token: &str,
        response_digest: &str,
        expected_events: &[String],
        now: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO matrix_recovery_batches(batch_id,request_token,response_token,response_digest,expected_events_json,covered_events_json,state,created_at,updated_at) VALUES(?,?,?,?,?,'[]','pending',?,?)")
            .bind(batch_id).bind(request_token).bind(response_token).bind(response_digest)
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
        let mut tx = self.pool.begin().await?;
        let existing = sqlx::query("SELECT payload_digest FROM matrix_recovery_receipts WHERE account_id=? AND room_id=? AND event_id=? AND cutover_operation_id=?")
            .bind(account_id).bind(room_id).bind(event_id).bind(operation_id).fetch_optional(&mut *tx).await?;
        if let Some(row) = existing {
            let digest: String = row.try_get("payload_digest")?;
            if digest != payload_digest {
                return Err(sqlx::Error::Protocol("receipt digest mismatch".into()));
            }
            tx.commit().await?;
            return Ok(BatchAdmission::Duplicate);
        }
        sqlx::query("INSERT INTO matrix_recovery_receipts(account_id,room_id,event_id,cutover_operation_id,payload_digest,state,batch_id,created_at,updated_at) VALUES(?,?,?,?,?,'dispositioned',?,?,?)")
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

    pub async fn complete_batch(
        &self,
        batch_id: &str,
        checkpoint: &str,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT response_token,expected_events_json,covered_events_json,state FROM matrix_recovery_batches WHERE batch_id=?")
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
        let token: String = row.try_get("response_token")?;
        sqlx::query(
            "UPDATE matrix_recovery_batches SET state='terminal',updated_at=? WHERE batch_id=?",
        )
        .bind(now)
        .bind(batch_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE matrix_recovery_control SET watermark=?,updated_at=? WHERE control_id=1 AND phase='active'")
            .bind(checkpoint).bind(now).execute(&mut *tx).await?;
        if token != checkpoint {
            return Err(sqlx::Error::Protocol(
                "checkpoint is not batch response token".into(),
            ));
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
