use super::output_spool::{OutputSegment, OutputSpoolStore, OutputStoreError, decode_segment};

impl OutputSpoolStore {
    pub(crate) async fn abandon(
        &self,
        set_id: &str,
        now: &str,
    ) -> Result<Vec<OutputSegment>, OutputStoreError> {
        let mut tx = self.transaction().await?;
        let set_change = self
            .prepare_row(&mut tx, "acp_output_sets", set_id, None)
            .await?;
        let segment_changes = self.prepare_segments(&mut tx, set_id).await?;
        let rows=sqlx::query("SELECT set_id,segment_index,byte_start,byte_end,content_hash,relative_name,stable_txn_id,reservation_owner,state,revision FROM acp_output_segments WHERE set_id=? AND state IN ('reserved','landed') ORDER BY segment_index")
            .bind(set_id).fetch_all(&mut *tx).await?;
        let segments = rows
            .into_iter()
            .map(decode_segment)
            .collect::<Result<Vec<_>, _>>()?;
        let bytes:i64=sqlx::query_scalar("SELECT COALESCE(SUM(byte_length),0) FROM acp_output_segments WHERE set_id=? AND state IN ('reserved','landed')")
            .bind(set_id).fetch_one(&mut *tx).await?;
        let changed=sqlx::query("UPDATE acp_output_sets SET state='abandoned',total_bytes=?,total_hash='',segment_count=?,updated_at=? WHERE set_id=? AND state IN ('assembling','prepared')")
            .bind(bytes).bind(segments.len() as i64).bind(now).bind(set_id).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        }
        self.finish_segments(&mut tx, segment_changes).await?;
        self.finish_row(&mut tx, set_change).await?;
        tx.commit().await?;
        Ok(segments)
    }

    pub(crate) async fn abandoned_sets(&self) -> Result<Vec<String>, OutputStoreError> {
        Ok(sqlx::query_scalar("SELECT set_id FROM acp_output_sets WHERE state='abandoned' AND EXISTS(SELECT 1 FROM acp_output_segments WHERE acp_output_segments.set_id=acp_output_sets.set_id AND state IN ('reserved','landed')) ORDER BY created_at")
            .fetch_all(&self.pool).await?)
    }

    pub(crate) async fn abandoned_segments(
        &self,
        set_id: &str,
    ) -> Result<Vec<OutputSegment>, OutputStoreError> {
        sqlx::query("SELECT set_id,segment_index,byte_start,byte_end,content_hash,relative_name,stable_txn_id,reservation_owner,state,revision FROM acp_output_segments WHERE set_id=? AND state IN ('reserved','landed') ORDER BY segment_index")
            .bind(set_id).fetch_all(&self.pool).await?.into_iter().map(decode_segment).collect()
    }

    pub(crate) async fn finish_abandon(
        &self,
        set_id: &str,
        now: &str,
    ) -> Result<(), OutputStoreError> {
        let mut tx = self.transaction().await?;
        let set_change = self
            .prepare_row(&mut tx, "acp_output_sets", set_id, None)
            .await?;
        let segment_changes = self.prepare_segments(&mut tx, set_id).await?;
        let reserved:i64=sqlx::query_scalar("SELECT COALESCE(SUM(byte_length),0) FROM acp_output_segments WHERE set_id=? AND state='reserved'").bind(set_id).fetch_one(&mut *tx).await?;
        let landed:i64=sqlx::query_scalar("SELECT COALESCE(SUM(byte_length),0) FROM acp_output_segments WHERE set_id=? AND state='landed'").bind(set_id).fetch_one(&mut *tx).await?;
        let quota=sqlx::query("UPDATE acp_output_quota SET reserved_bytes=reserved_bytes-?,landed_bytes=landed_bytes-?,revision=revision+1 WHERE singleton=1 AND reserved_bytes>=? AND landed_bytes>=?")
            .bind(reserved).bind(landed).bind(reserved).bind(landed).execute(&mut *tx).await?;
        sqlx::query("UPDATE acp_output_segments SET state='cleaned',revision=revision+1,updated_at=? WHERE set_id=? AND state IN ('reserved','landed')")
            .bind(now).bind(set_id).execute(&mut *tx).await?;
        if quota.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        }
        self.finish_segments(&mut tx, segment_changes).await?;
        self.finish_row(&mut tx, set_change).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn cleanup_sets(&self) -> Result<Vec<String>, OutputStoreError> {
        Ok(sqlx::query_scalar("SELECT set_id FROM acp_output_sets WHERE state IN ('delivered','cleaning') ORDER BY created_at")
            .fetch_all(&self.pool).await?)
    }

    pub(crate) async fn begin_cleanup(
        &self,
        set_id: &str,
        now: &str,
    ) -> Result<Vec<OutputSegment>, OutputStoreError> {
        let mut tx = self.transaction().await?;
        let set_change = self
            .prepare_row(&mut tx, "acp_output_sets", set_id, None)
            .await?;
        let segment_changes = self.prepare_segments(&mut tx, set_id).await?;
        let changed=sqlx::query("UPDATE acp_output_sets SET state='cleaning',claim_revision=claim_revision+1,updated_at=? WHERE set_id=? AND state IN ('delivered','cleaning')")
            .bind(now).bind(set_id).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        }
        let rows=sqlx::query("SELECT set_id,segment_index,byte_start,byte_end,content_hash,relative_name,stable_txn_id,reservation_owner,state,revision FROM acp_output_segments WHERE set_id=? AND state='landed' ORDER BY segment_index")
            .bind(set_id).fetch_all(&mut *tx).await?;
        let result = rows
            .into_iter()
            .map(decode_segment)
            .collect::<Result<Vec<_>, _>>()?;
        self.finish_segments(&mut tx, segment_changes).await?;
        self.finish_row(&mut tx, set_change).await?;
        tx.commit().await?;
        Ok(result)
    }

    pub(crate) async fn finish_cleanup(
        &self,
        set_id: &str,
        now: &str,
    ) -> Result<(), OutputStoreError> {
        let mut tx = self.transaction().await?;
        let set_change = self
            .prepare_row(&mut tx, "acp_output_sets", set_id, None)
            .await?;
        let segment_changes = self.prepare_segments(&mut tx, set_id).await?;
        let bytes: i64=sqlx::query_scalar("SELECT COALESCE(SUM(byte_length),0) FROM acp_output_segments WHERE set_id=? AND state='landed'").bind(set_id).fetch_one(&mut *tx).await?;
        sqlx::query("UPDATE acp_output_segments SET state='cleaned',revision=revision+1,updated_at=? WHERE set_id=? AND state='landed'").bind(now).bind(set_id).execute(&mut *tx).await?;
        let quota=sqlx::query("UPDATE acp_output_quota SET landed_bytes=landed_bytes-?,revision=revision+1 WHERE singleton=1 AND landed_bytes>=?").bind(bytes).bind(bytes).execute(&mut *tx).await?;
        let set=sqlx::query("UPDATE acp_output_sets SET state='cleaned',claim_owner=NULL,updated_at=? WHERE set_id=? AND state='cleaning'").bind(now).bind(set_id).execute(&mut *tx).await?;
        if quota.rows_affected() != 1 || set.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        }
        self.finish_segments(&mut tx, segment_changes).await?;
        self.finish_row(&mut tx, set_change).await?;
        tx.commit().await?;
        Ok(())
    }
}
