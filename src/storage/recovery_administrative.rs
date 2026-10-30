use super::*;

fn refused() -> sqlx::Error {
    sqlx::Error::Protocol("administrative activation fenced".into())
}

async fn lease(
    connection: &mut sqlx::SqliteConnection,
    owner: &str,
    epoch: i64,
) -> Result<(), sqlx::Error> {
    let valid: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_component_leases WHERE component='MatrixSync' AND owner_token=? AND epoch=? AND state='active' AND julianday(expires_at)>julianday('now')")
        .bind(owner).bind(epoch).fetch_one(connection).await?;
    if valid != 1 {
        return Err(refused());
    }
    Ok(())
}

async fn binding(connection: &mut sqlx::SqliteConnection, phase: &str) -> Result<(), sqlx::Error> {
    let valid: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_binding b JOIN matrix_recovery_control c ON c.cutover_operation_id=b.native_operation_id JOIN matrix_recovery_compatibility m ON m.native_operation_id=c.cutover_operation_id WHERE b.singleton=1 AND b.protocol=2 AND b.phase=? AND b.transition_complete_digest IS NOT NULL AND b.identity_digest IS NOT NULL AND b.plan_digest IS NOT NULL AND c.control_id=1 AND c.phase='baseline' AND c.watermark IS NULL AND c.restart_hold_state IS NULL AND m.decision='native' AND EXISTS(SELECT 1 FROM runtime_eligibility_bootstrap WHERE singleton=1 AND state='admin_reserved') AND NOT EXISTS(SELECT 1 FROM runtime_eligibility_context) AND NOT EXISTS(SELECT 1 FROM matrix_recovery_batches) AND NOT EXISTS(SELECT 1 FROM matrix_recovery_receipts) AND NOT EXISTS(SELECT 1 FROM offline_recovery_rows WHERE disposition IN ('uncertain_hold','refused_hold'))")
        .bind(phase).fetch_one(connection).await?;
    if valid != 1 {
        return Err(refused());
    }
    Ok(())
}

impl MatrixRecoveryStore {
    pub(crate) async fn acquire_administrative_lease(
        &self,
        owner: &str,
        now: &str,
        expires: &str,
    ) -> Result<i64, sqlx::Error> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let phase: String =
            sqlx::query_scalar("SELECT phase FROM offline_recovery_binding WHERE singleton=1")
                .fetch_one(&mut *transaction)
                .await?;
        if !matches!(phase.as_str(), "baseline_ready" | "activation_prepared") {
            return Err(refused());
        }
        binding(&mut transaction, &phase).await?;
        let existing: Option<(String, i64, String, String)> = sqlx::query_as("SELECT owner_token,epoch,state,expires_at FROM matrix_component_leases WHERE component='MatrixSync'")
            .fetch_optional(&mut *transaction).await?;
        let epoch = match existing {
            None => 1,
            Some((current, epoch, state, expiry)) => {
                if state == "unknown"
                    || (state == "active"
                        && current != owner
                        && chrono::DateTime::parse_from_rfc3339(&expiry).map_err(|_| refused())?
                            > chrono::DateTime::parse_from_rfc3339(now).map_err(|_| refused())?)
                {
                    return Err(refused());
                }
                epoch.checked_add(1).ok_or_else(refused)?
            }
        };
        sqlx::query("INSERT INTO matrix_component_leases(component,owner_token,epoch,state,expires_at,updated_at) VALUES('MatrixSync',?,?,'active',?,?) ON CONFLICT(component) DO UPDATE SET owner_token=excluded.owner_token,epoch=excluded.epoch,state='active',expires_at=excluded.expires_at,updated_at=excluded.updated_at")
            .bind(owner).bind(epoch).bind(expires).bind(now).execute(&mut *transaction).await?;
        lease(&mut transaction, owner, epoch).await?;
        transaction.commit().await?;
        Ok(epoch)
    }

    pub async fn begin_activation_intent(
        &self,
        owner: &str,
        epoch: i64,
        attempt: &str,
        request: &str,
        now: &str,
    ) -> Result<bool, sqlx::Error> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_binding")
            .fetch_one(&mut *transaction)
            .await?;
        if count == 0 {
            transaction.commit().await?;
            return Ok(false);
        }
        if attempt.is_empty() || request.is_empty() {
            return Err(refused());
        }
        binding(&mut transaction, "baseline_ready").await?;
        lease(&mut transaction, owner, epoch).await?;
        sqlx::query("INSERT INTO matrix_activation_intent(native_operation_id,plan_digest,identity_digest,attempt_id,component_owner,component_epoch,request_digest,state,revision,created_at) SELECT native_operation_id,plan_digest,identity_digest,?,?,?,?,'inflight',0,? FROM offline_recovery_binding WHERE singleton=1 AND phase='baseline_ready'")
            .bind(attempt).bind(owner).bind(epoch).bind(request).bind(now).execute(&mut *transaction).await?;
        let changed = sqlx::query("UPDATE offline_recovery_binding SET phase='baseline_inflight',revision=revision+1 WHERE singleton=1 AND phase='baseline_ready'").execute(&mut *transaction).await?;
        if changed.rows_affected() != 1 {
            return Err(refused());
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn prepare_activation_intent(
        &self,
        owner: &str,
        epoch: i64,
        token: &str,
        response: &str,
        now: &str,
    ) -> Result<bool, sqlx::Error> {
        if token.is_empty() || response != crate::storage::cutover_digest(token.as_bytes()) {
            return Err(refused());
        }
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        binding(&mut transaction, "baseline_inflight").await?;
        lease(&mut transaction, owner, epoch).await?;
        let changed = sqlx::query("UPDATE matrix_activation_intent SET response_digest=?,response_token=?,response_at=?,state='prepared',revision=revision+1 WHERE component_owner=? AND component_epoch=? AND state='inflight' AND EXISTS(SELECT 1 FROM offline_recovery_binding b WHERE b.singleton=1 AND b.native_operation_id=matrix_activation_intent.native_operation_id AND b.plan_digest=matrix_activation_intent.plan_digest AND b.identity_digest=matrix_activation_intent.identity_digest AND b.phase='baseline_inflight')")
            .bind(response).bind(token).bind(now).bind(owner).bind(epoch).execute(&mut *transaction).await?;
        if changed.rows_affected() != 1 {
            return Err(refused());
        }
        let changed = sqlx::query("UPDATE offline_recovery_binding SET phase='activation_prepared',revision=revision+1 WHERE singleton=1 AND phase='baseline_inflight'").execute(&mut *transaction).await?;
        if changed.rows_affected() != 1 {
            return Err(refused());
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub(crate) async fn resume_prepared_activation(
        &self,
        owner: &str,
        epoch: i64,
    ) -> Result<String, sqlx::Error> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        binding(&mut transaction, "activation_prepared").await?;
        lease(&mut transaction, owner, epoch).await?;
        let token: String = sqlx::query_scalar("SELECT i.response_token FROM matrix_activation_intent i JOIN offline_recovery_binding b ON b.native_operation_id=i.native_operation_id AND b.plan_digest=i.plan_digest AND b.identity_digest=i.identity_digest WHERE b.singleton=1 AND i.state='prepared' AND i.response_digest IS NOT NULL AND i.response_at IS NOT NULL")
            .fetch_one(&mut *transaction).await?;
        let digest: String = sqlx::query_scalar(
            "SELECT response_digest FROM matrix_activation_intent WHERE state='prepared'",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if token.is_empty() || digest != crate::storage::cutover_digest(token.as_bytes()) {
            return Err(refused());
        }
        let changed = sqlx::query("UPDATE matrix_activation_intent SET component_owner=?,component_epoch=?,revision=revision+1 WHERE state='prepared' AND native_operation_id=(SELECT native_operation_id FROM offline_recovery_binding WHERE singleton=1 AND phase='activation_prepared')")
            .bind(owner).bind(epoch).execute(&mut *transaction).await?;
        if changed.rows_affected() != 1 {
            return Err(refused());
        }
        transaction.commit().await?;
        Ok(token)
    }

    pub async fn commit_activation_intent(
        &self,
        operation: &str,
        owner: &str,
        epoch: i64,
        token: &str,
        now: &str,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        lease(&mut transaction, owner, epoch).await?;
        let committed: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_activation_intent i JOIN offline_recovery_binding b ON b.native_operation_id=i.native_operation_id JOIN matrix_recovery_control c ON c.cutover_operation_id=i.native_operation_id WHERE b.singleton=1 AND b.phase='active' AND i.state='committed' AND b.plan_digest=i.plan_digest AND b.identity_digest=i.identity_digest AND c.phase='active' AND c.restart_hold_state IS NULL AND i.native_operation_id=? AND i.component_owner=? AND i.component_epoch=? AND i.response_token=? AND c.watermark=i.response_token")
            .bind(operation).bind(owner).bind(epoch).bind(token).fetch_one(&mut *transaction).await?;
        if committed == 1 {
            return transaction.commit().await;
        }
        binding(&mut transaction, "activation_prepared").await?;
        lease(&mut transaction, owner, epoch).await?;
        let digest = crate::storage::cutover_digest(token.as_bytes());
        let intent = sqlx::query("UPDATE matrix_activation_intent SET state='committed',revision=revision+1 WHERE native_operation_id=? AND component_owner=? AND component_epoch=? AND response_token=? AND response_digest=? AND state='prepared' AND EXISTS(SELECT 1 FROM offline_recovery_binding b WHERE b.native_operation_id=matrix_activation_intent.native_operation_id AND b.plan_digest=matrix_activation_intent.plan_digest AND b.identity_digest=matrix_activation_intent.identity_digest AND b.phase='activation_prepared')")
            .bind(operation).bind(owner).bind(epoch).bind(token).bind(digest).execute(&mut *transaction).await?;
        if intent.rows_affected() != 1 {
            return Err(refused());
        }
        let control = sqlx::query("UPDATE matrix_recovery_control SET phase='active',watermark=?,updated_at=? WHERE control_id=1 AND cutover_operation_id=? AND phase='baseline' AND watermark IS NULL AND restart_hold_state IS NULL")
            .bind(token).bind(now).bind(operation).execute(&mut *transaction).await?;
        let changed = sqlx::query("UPDATE offline_recovery_binding SET phase='active',revision=revision+1 WHERE singleton=1 AND phase='activation_prepared' AND native_operation_id=?").bind(operation).execute(&mut *transaction).await?;
        if control.rows_affected() != 1 || changed.rows_affected() != 1 {
            return Err(refused());
        }
        super::super::recovery_eligibility::administrative::seed_activation(&mut transaction)
            .await?;
        let readback: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_binding b JOIN matrix_activation_intent i ON i.native_operation_id=b.native_operation_id JOIN matrix_recovery_control c ON c.cutover_operation_id=i.native_operation_id WHERE b.phase='active' AND i.state='committed' AND c.phase='active' AND c.watermark=i.response_token AND b.plan_digest=i.plan_digest AND b.identity_digest=i.identity_digest AND c.watermark=?")
            .bind(token).fetch_one(&mut *transaction).await?;
        if readback != 1 {
            return Err(refused());
        }
        transaction.commit().await
    }
}
