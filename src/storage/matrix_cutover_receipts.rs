use sqlx::Row;

use super::{CutoverStore, cutover_digest};
use crate::matrix::MatrixError;

impl CutoverStore {
    pub async fn begin_event(
        &self,
        room: &str,
        event: &str,
        digest: &str,
    ) -> Result<bool, MatrixError> {
        self.state()
            .await?
            .baseline
            .ok_or(MatrixError::CutoverRefused)?;
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| MatrixError::Storage)?;
        let row = sqlx::query("SELECT content_digest,state FROM matrix_cutover_dispositions WHERE operation_id=? AND room_id=? AND event_id=?")
            .bind(&self.operation).bind(room).bind(event).fetch_optional(&mut *transaction)
            .await.map_err(|_| MatrixError::Storage)?;
        let Some(row) = row else {
            sqlx::query("INSERT INTO matrix_cutover_dispositions(operation_id,room_id,event_id,content_digest,state) VALUES(?,?,?,?,'processing')")
                .bind(&self.operation).bind(room).bind(event).bind(digest)
                .execute(&mut *transaction).await.map_err(|_| MatrixError::Storage)?;
            transaction
                .commit()
                .await
                .map_err(|_| MatrixError::Storage)?;
            return Ok(true);
        };
        let state: String = row.get(1);
        if row.get::<String, _>(0) == digest && state == "complete" {
            transaction
                .commit()
                .await
                .map_err(|_| MatrixError::Storage)?;
            return Ok(false);
        }
        let terminal = if row.get::<String, _>(0) == digest && state == "processing" {
            let admitted: i64 = sqlx::query_scalar("SELECT count(*) FROM transport_receipts WHERE transport='matrix' AND external_event_id=? AND room_id=? AND receipt_kind='ordinary' AND state='admitted' AND task_id IS NOT NULL")
                .bind(event).bind(room).fetch_one(&mut *transaction).await.map_err(|_| MatrixError::Storage)?;
            if admitted == 1 {
                "complete"
            } else {
                "manual_required"
            }
        } else {
            "manual_required"
        };
        sqlx::query("UPDATE matrix_cutover_dispositions SET state=? WHERE operation_id=? AND room_id=? AND event_id=?")
            .bind(terminal).bind(&self.operation).bind(room).bind(event)
            .execute(&mut *transaction).await.map_err(|_| MatrixError::Storage)?;
        transaction
            .commit()
            .await
            .map_err(|_| MatrixError::Storage)?;
        if terminal == "complete" {
            Ok(false)
        } else {
            Err(MatrixError::CutoverRefused)
        }
    }

    pub async fn finish_event(
        &self,
        room: &str,
        event: &str,
        digest: &str,
    ) -> Result<(), MatrixError> {
        let changed = sqlx::query("UPDATE matrix_cutover_dispositions SET state='complete' WHERE operation_id=? AND room_id=? AND event_id=? AND content_digest=? AND state='processing'")
            .bind(&self.operation).bind(room).bind(event).bind(digest).execute(&self.pool)
            .await.map_err(|_| MatrixError::Storage)?.rows_affected();
        if changed != 1 {
            return Err(MatrixError::CutoverRefused);
        }
        Ok(())
    }
}

pub fn cutover_event_identity(raw: &str) -> Result<(String, String), MatrixError> {
    let mut event: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| MatrixError::CutoverRefused)?;
    let identity = event
        .get("event_id")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or(MatrixError::CutoverRefused)?
        .to_owned();
    event
        .as_object_mut()
        .ok_or(MatrixError::CutoverRefused)?
        .remove("unsigned");
    let encoded = serde_json::to_vec(&event).map_err(|_| MatrixError::CutoverRefused)?;
    Ok((identity, cutover_digest(&encoded)))
}
