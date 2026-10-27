use super::*;

impl MatrixRecoveryStore {
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
            if state == "dispositioned" {
                sqlx::query("UPDATE matrix_recovery_batches SET covered_events_json=json_insert(covered_events_json,'$[#]',?),state='processing',updated_at=? WHERE batch_id=? AND state IN ('pending','processing') AND EXISTS (SELECT 1 FROM json_each(expected_events_json) WHERE value=?) AND NOT EXISTS (SELECT 1 FROM json_each(covered_events_json) WHERE value=?)")
                    .bind(event_id).bind(now).bind(batch_id).bind(event_id).bind(event_id).execute(&mut *tx).await?;
                tx.commit().await?;
                return Ok(BatchAdmission::Duplicate);
            }
            tx.commit().await?;
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

    #[allow(clippy::too_many_arguments)]
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
        let result = sqlx::query("UPDATE matrix_recovery_receipts SET state='dispositioned',updated_at=? WHERE account_id=? AND room_id=? AND event_id=? AND cutover_operation_id=? AND batch_id=? AND state IN ('pending','processing') AND EXISTS (SELECT 1 FROM matrix_recovery_batches b JOIN matrix_component_leases l ON l.component=b.component AND l.owner_token=b.owner_token AND l.epoch=b.owner_epoch WHERE b.batch_id=? AND b.owner_token=? AND b.owner_epoch=? AND b.state='processing' AND l.state='active' AND l.expires_at>=?)")
            .bind(now).bind(account_id).bind(room_id).bind(event_id).bind(operation_id).bind(batch_id).bind(batch_id).bind(owner_token).bind(owner_epoch).bind(now).execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(
                "receipt disposition fence failed".into(),
            ));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn refuse_event(
        &self,
        account_id: &str,
        room_id: &str,
        event_id: &str,
        operation_id: &str,
        batch_id: &str,
        owner_token: &str,
        owner_epoch: i64,
        code: &str,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query("UPDATE matrix_recovery_receipts SET state='refused',refusal_code=?,updated_at=? WHERE account_id=? AND room_id=? AND event_id=? AND cutover_operation_id=? AND batch_id=? AND state IN ('pending','processing') AND EXISTS (SELECT 1 FROM matrix_recovery_batches b JOIN matrix_component_leases l ON l.component=b.component AND l.owner_token=b.owner_token AND l.epoch=b.owner_epoch WHERE b.batch_id=? AND b.owner_token=? AND b.owner_epoch=? AND b.state='processing' AND l.state='active' AND l.expires_at>=?)")
            .bind(code).bind(now).bind(account_id).bind(room_id).bind(event_id).bind(operation_id).bind(batch_id).bind(batch_id).bind(owner_token).bind(owner_epoch).bind(now).execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(
                "refusal disposition fence failed".into(),
            ));
        }
        Ok(())
    }
}
