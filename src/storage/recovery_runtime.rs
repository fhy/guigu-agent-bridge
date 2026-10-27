use super::*;

impl MatrixRecoveryStore {
    #[allow(clippy::too_many_arguments)]
    pub async fn start_event(
        &self,
        account: &str,
        room: &str,
        event: &str,
        operation: &str,
        batch: &str,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query("UPDATE matrix_recovery_receipts SET state='processing',updated_at=? WHERE account_id=? AND room_id=? AND event_id=? AND cutover_operation_id=? AND batch_id=? AND state='pending' AND EXISTS (SELECT 1 FROM matrix_recovery_batches b JOIN matrix_component_leases l ON l.component=b.component AND l.owner_token=b.owner_token AND l.epoch=b.owner_epoch WHERE b.batch_id=? AND b.state='processing' AND l.state='active' AND l.expires_at>=?)")
            .bind(now).bind(account).bind(room).bind(event).bind(operation).bind(batch).bind(batch).bind(now).execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol("pre-effect fence failed".into()));
        }
        Ok(())
    }

    pub async fn refuse_response(
        &self,
        operation: &str,
        owner: &str,
        epoch: i64,
        reason: &str,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query("UPDATE matrix_recovery_control SET phase='refused',restart_hold_state='refused',restart_hold_reason=?,updated_at=? WHERE control_id=1 AND cutover_operation_id=? AND phase='active' AND EXISTS (SELECT 1 FROM matrix_component_leases WHERE component='MatrixSync' AND owner_token=? AND epoch=? AND state='active' AND expires_at>=?)")
            .bind(reason).bind(now).bind(operation).bind(owner).bind(epoch).bind(now).execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(
                "response refusal fence failed".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn unresolved_work(
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<Vec<(String, String)>, sqlx::Error> {
        let mut unresolved: Vec<(String,String)> = sqlx::query_as("SELECT event_id,state FROM matrix_recovery_receipts WHERE state <> 'dispositioned' ORDER BY created_at,event_id LIMIT 1")
            .fetch_all(&mut *connection).await?;
        if unresolved.is_empty() {
            unresolved = sqlx::query_as("SELECT 'batch:' || batch_id,state FROM matrix_recovery_batches WHERE state <> 'terminal' ORDER BY created_at,batch_id LIMIT 1").fetch_all(connection).await?;
        }
        Ok(unresolved)
    }
}
