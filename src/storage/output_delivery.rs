use sqlx::Row;

use super::output_spool::{
    ClaimedOutputSet, DeliveryPart, OutputSegment, OutputSpoolStore, OutputStoreError,
    PendingDelivery, decode_segment,
};

pub(crate) struct PreparedOutput {
    pub(crate) set_id: String,
}

impl OutputSpoolStore {
    pub(crate) async fn adopt_reserved(
        &self,
        set_id: &str,
        index: i64,
        owner: &str,
    ) -> Result<(), OutputStoreError> {
        let changed=sqlx::query("UPDATE acp_output_segments SET reservation_owner=?,revision=revision+1 WHERE set_id=? AND segment_index=? AND state='reserved' AND EXISTS(SELECT 1 FROM acp_output_sets WHERE set_id=? AND state='assembling')")
            .bind(owner).bind(set_id).bind(index).bind(set_id).execute(&self.pool).await?;
        if changed.rows_affected() != 1 {
            return Err(OutputStoreError::Conflict);
        }
        Ok(())
    }

    pub(crate) async fn verify_quota(&self) -> Result<(), OutputStoreError> {
        let row = sqlx::query("SELECT q.reserved_bytes,q.landed_bytes,(SELECT COALESCE(SUM(byte_length),0) FROM acp_output_segments WHERE state='reserved') expected_reserved,(SELECT COALESCE(SUM(byte_length),0) FROM acp_output_segments WHERE state IN ('landed','quarantined')) expected_landed FROM acp_output_quota q WHERE singleton=1")
            .fetch_one(&self.pool).await?;
        let reserved: i64 = row.try_get("reserved_bytes")?;
        let landed: i64 = row.try_get("landed_bytes")?;
        let expected_reserved: i64 = row.try_get("expected_reserved")?;
        let expected_landed: i64 = row.try_get("expected_landed")?;
        if reserved != expected_reserved
            || landed != expected_landed
            || reserved.saturating_add(landed) > self.quota_limit
        {
            return Err(OutputStoreError::Malformed);
        }
        Ok(())
    }

    pub(crate) async fn cleanup_sets(&self) -> Result<Vec<String>, OutputStoreError> {
        Ok(sqlx::query_scalar("SELECT set_id FROM acp_output_sets WHERE state IN ('delivered','cleaning') ORDER BY created_at")
            .fetch_all(&self.pool).await?)
    }

    pub(crate) async fn activate_prepared(
        &self,
        set_id: &str,
        now: &str,
    ) -> Result<bool, OutputStoreError> {
        let result=sqlx::query("UPDATE acp_output_sets SET state='sealed',updated_at=? WHERE set_id=? AND state='prepared' AND EXISTS(SELECT 1 FROM task_events e WHERE e.task_id=acp_output_sets.task_id AND e.status='completed' AND e.seq=(SELECT MAX(e2.seq) FROM task_events e2 WHERE e2.task_id=e.task_id))")
            .bind(now).bind(set_id).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub(crate) async fn claim_next(
        &self,
        owner: &str,
        now: &str,
        stale_before: &str,
    ) -> Result<Option<ClaimedOutputSet>, OutputStoreError> {
        let row = sqlx::query("UPDATE acp_output_sets SET state='delivering',claim_owner=?,claim_revision=claim_revision+1,updated_at=? WHERE rowid=(SELECT rowid FROM acp_output_sets WHERE state='sealed' OR (state='delivering' AND updated_at<=?) ORDER BY created_at LIMIT 1) RETURNING set_id,room_id,thread_root,reply_event_id,acknowledged_cursor,segment_count,claim_revision,attempt_count")
            .bind(owner).bind(now).bind(stale_before).fetch_optional(&self.pool).await?;
        row.map(|row| {
            Ok(ClaimedOutputSet {
                set_id: row.try_get("set_id")?,
                room_id: row.try_get("room_id")?,
                thread_root: row.try_get("thread_root")?,
                reply_event_id: row.try_get("reply_event_id")?,
                acknowledged_cursor: row.try_get("acknowledged_cursor")?,
                segment_count: row.try_get("segment_count")?,
                claim_owner: owner.into(),
                claim_revision: row.try_get("claim_revision")?,
                attempt_count: row.try_get("attempt_count")?,
            })
        })
        .transpose()
    }

    pub(crate) async fn next_segment(
        &self,
        set: &ClaimedOutputSet,
    ) -> Result<Option<PendingDelivery>, OutputStoreError> {
        let index = set.acknowledged_cursor + 1;
        let row = sqlx::query("SELECT d.delivery_index,d.spool_index,d.byte_start AS delivery_start,d.byte_end AS delivery_end,d.content_hash AS delivery_hash,d.stable_txn_id AS delivery_txn,s.set_id,s.segment_index,s.byte_start,s.byte_end,s.content_hash,s.relative_name,s.stable_txn_id,s.reservation_owner,s.state,s.revision,o.total_bytes,o.total_hash FROM acp_output_delivery_segments d JOIN acp_output_segments s ON s.set_id=d.set_id AND s.segment_index=d.spool_index JOIN acp_output_sets o ON o.set_id=d.set_id WHERE d.set_id=? AND d.delivery_index=? AND s.state='landed'")
            .bind(&set.set_id).bind(index).fetch_optional(&self.pool).await?;
        row.map(|row| {
            let total_bytes = row.try_get("total_bytes")?;
            let total_hash = row.try_get("total_hash")?;
            Ok(PendingDelivery {
                part: DeliveryPart {
                    index: row.try_get("delivery_index")?,
                    spool_index: row.try_get("spool_index")?,
                    byte_start: row.try_get("delivery_start")?,
                    byte_end: row.try_get("delivery_end")?,
                    content_hash: row.try_get("delivery_hash")?,
                    stable_txn_id: row.try_get("delivery_txn")?,
                },
                spool: decode_segment(row)?,
                total_bytes,
                total_hash,
            })
        })
        .transpose()
    }

    pub(crate) async fn acknowledge(
        &self,
        set: &ClaimedOutputSet,
        index: i64,
        now: &str,
    ) -> Result<bool, OutputStoreError> {
        let state = if index + 1 == set.segment_count {
            "delivered"
        } else {
            "delivering"
        };
        let result = sqlx::query("UPDATE acp_output_sets SET acknowledged_cursor=?,state=?,attempt_count=0,updated_at=? WHERE set_id=? AND state='delivering' AND claim_owner=? AND claim_revision=? AND acknowledged_cursor=?")
            .bind(index).bind(state).bind(now).bind(&set.set_id).bind(&set.claim_owner)
            .bind(set.claim_revision).bind(index - 1).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub(crate) async fn record_failure(
        &self,
        set: &ClaimedOutputSet,
        class: Option<&str>,
        limit: u32,
        now: &str,
    ) -> Result<bool, OutputStoreError> {
        let next = set.attempt_count + 1;
        let terminal = class == Some("permanent") || next >= i64::from(limit);
        let state = if terminal { "partial" } else { "sealed" };
        let failure = if class == Some("permanent") {
            Some("permanent")
        } else if terminal {
            Some("attempts_exhausted")
        } else {
            None
        };
        let result = sqlx::query("UPDATE acp_output_sets SET state=?,attempt_count=?,failure_class=?,claim_owner=NULL,updated_at=? WHERE set_id=? AND state='delivering' AND claim_owner=? AND claim_revision=?")
            .bind(state).bind(next).bind(failure).bind(now).bind(&set.set_id)
            .bind(&set.claim_owner).bind(set.claim_revision).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub(crate) async fn begin_cleanup(
        &self,
        set_id: &str,
        now: &str,
    ) -> Result<Vec<OutputSegment>, OutputStoreError> {
        let mut tx = self.pool.begin().await?;
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
        tx.commit().await?;
        Ok(result)
    }

    pub(crate) async fn finish_cleanup(
        &self,
        set_id: &str,
        now: &str,
    ) -> Result<(), OutputStoreError> {
        let mut tx = self.pool.begin().await?;
        let bytes: i64=sqlx::query_scalar("SELECT COALESCE(SUM(byte_length),0) FROM acp_output_segments WHERE set_id=? AND state='landed'").bind(set_id).fetch_one(&mut *tx).await?;
        sqlx::query("UPDATE acp_output_segments SET state='cleaned',revision=revision+1,updated_at=? WHERE set_id=? AND state='landed'").bind(now).bind(set_id).execute(&mut *tx).await?;
        let quota=sqlx::query("UPDATE acp_output_quota SET landed_bytes=landed_bytes-?,revision=revision+1 WHERE singleton=1 AND landed_bytes>=?").bind(bytes).bind(bytes).execute(&mut *tx).await?;
        let set=sqlx::query("UPDATE acp_output_sets SET state='cleaned',claim_owner=NULL,updated_at=? WHERE set_id=? AND state='cleaning'").bind(now).bind(set_id).execute(&mut *tx).await?;
        if quota.rows_affected() != 1 || set.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        }
        tx.commit().await?;
        Ok(())
    }
}
