use sqlx::{Row, SqlitePool};
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum OutputStoreError {
    #[error("durable output storage failed")]
    Sql(#[from] sqlx::Error),
    #[error("durable output quota exhausted")]
    Quota,
    #[error("durable output compare-and-swap failed")]
    Conflict,
    #[error("durable output row is malformed")]
    Malformed,
}
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputSetInput<'a> {
    pub set_id: &'a str,
    pub task_id: &'a str,
    pub attempt: i64,
    pub turn: i64,
    pub runtime_fence: &'a str,
    pub room_id: &'a str,
    pub thread_root: Option<&'a str>,
    pub reply_event_id: Option<&'a str>,
    pub now: &'a str,
}

pub(crate) struct OutputSegment {
    pub set_id: String,
    pub index: i64,
    pub byte_start: i64,
    pub byte_end: i64,
    pub content_hash: String,
    pub relative_name: String,
    pub reservation_owner: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryPart {
    pub(crate) index: i64,
    pub(crate) spool_index: i64,
    pub(crate) byte_start: i64,
    pub(crate) byte_end: i64,
    pub(crate) content_hash: String,
    pub(crate) stable_txn_id: String,
}

pub(crate) struct PendingDelivery {
    pub(crate) part: DeliveryPart,
    pub(crate) spool: OutputSegment,
    pub(crate) total_bytes: i64,
    pub(crate) total_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaimedOutputSet {
    pub set_id: String,
    pub room_id: String,
    pub thread_root: Option<String>,
    pub reply_event_id: Option<String>,
    pub acknowledged_cursor: i64,
    pub segment_count: i64,
    pub claim_owner: String,
    pub claim_revision: i64,
    pub attempt_count: i64,
}

pub(crate) struct AssemblingOutput {
    pub(crate) set_id: String,
    pub(crate) state: String,
    pub(crate) payload: String,
    pub(crate) created_at: String,
}

#[derive(Clone)]
pub(crate) struct OutputSpoolStore {
    pub(super) pool: SqlitePool,
    pub(super) quota_limit: i64,
    pub(super) recovery: Option<super::recovery_eligibility::RecoveryScope>,
}

impl OutputSpoolStore {
    #[cfg(test)]
    pub(crate) fn new(pool: SqlitePool, quota_limit: u64) -> Result<Self, OutputStoreError> {
        Self::new_scoped(pool, quota_limit, None)
    }

    pub(crate) fn new_scoped(
        pool: SqlitePool,
        quota_limit: u64,
        recovery: Option<super::recovery_eligibility::RecoveryScope>,
    ) -> Result<Self, OutputStoreError> {
        let quota_limit = i64::try_from(quota_limit).map_err(|_| OutputStoreError::Malformed)?;
        Ok(Self {
            pool,
            quota_limit,
            recovery,
        })
    }

    pub(crate) async fn assembling_outputs(
        &self,
    ) -> Result<Vec<AssemblingOutput>, OutputStoreError> {
        let rows=sqlx::query("SELECT s.set_id,s.state,e.payload,s.created_at FROM acp_output_sets s JOIN task_events e ON e.task_id=s.task_id WHERE s.state IN ('assembling','prepared') AND e.seq=(SELECT MAX(e2.seq) FROM task_events e2 WHERE e2.task_id=s.task_id) ORDER BY s.created_at")
            .fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok(AssemblingOutput {
                    set_id: row.try_get("set_id")?,
                    state: row.try_get("state")?,
                    payload: row.try_get("payload")?,
                    created_at: row.try_get("created_at")?,
                })
            })
            .collect()
    }

    pub(crate) async fn segment(
        &self,
        set_id: &str,
        index: i64,
    ) -> Result<Option<OutputSegment>, OutputStoreError> {
        let row=sqlx::query("SELECT set_id,segment_index,byte_start,byte_end,content_hash,relative_name,stable_txn_id,reservation_owner,state,revision FROM acp_output_segments WHERE set_id=? AND segment_index=?")
            .bind(set_id).bind(index).fetch_optional(&self.pool).await?;
        row.map(decode_segment).transpose()
    }

    pub(crate) async fn release_reserved(
        &self,
        set_id: &str,
        index: i64,
        owner: &str,
    ) -> Result<(), OutputStoreError> {
        let mut tx = self.transaction().await?;
        self.prepare_row(&mut tx, "acp_output_sets", set_id, None)
            .await?;
        let segment_change = self
            .prepare_row(&mut tx, "acp_output_segments", set_id, Some(index))
            .await?;
        let statement = if self.recovery.is_some() {
            "UPDATE acp_output_segments SET state='cleaned',revision=revision+1 WHERE set_id=? AND segment_index=? AND state='reserved' AND reservation_owner=? RETURNING byte_length"
        } else {
            "DELETE FROM acp_output_segments WHERE set_id=? AND segment_index=? AND state='reserved' AND reservation_owner=? RETURNING byte_length"
        };
        let row = sqlx::query(statement)
            .bind(set_id)
            .bind(index)
            .bind(owner)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        };
        let bytes: i64 = row.try_get("byte_length")?;
        let changed=sqlx::query("UPDATE acp_output_quota SET reserved_bytes=reserved_bytes-?,revision=revision+1 WHERE singleton=1 AND reserved_bytes>=?")
            .bind(bytes).bind(bytes).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        }
        self.finish_row(&mut tx, segment_change).await?;
        tx.commit().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn reserve_segment(
        &self,
        set_id: &str,
        index: i64,
        start: i64,
        bytes: i64,
        hash: &str,
        name: &str,
        owner: &str,
        now: &str,
    ) -> Result<(), OutputStoreError> {
        if bytes < 0 {
            return Err(OutputStoreError::Malformed);
        }
        let mut tx = self.transaction().await?;
        self.prepare_row(&mut tx, "acp_output_sets", set_id, None)
            .await?;
        let reused = self.prepare_reuse(&mut tx, set_id, index).await?;
        let quota=sqlx::query("UPDATE acp_output_quota SET reserved_bytes=reserved_bytes+?,revision=revision+1 WHERE singleton=1 AND reserved_bytes+landed_bytes+?<=? RETURNING revision")
            .bind(bytes).bind(bytes).bind(self.quota_limit).fetch_optional(&mut *tx).await?;
        if quota.is_none() {
            tx.rollback().await?;
            return Err(OutputStoreError::Quota);
        }
        let inserted=sqlx::query("INSERT INTO acp_output_segments(set_id,segment_index,byte_start,byte_end,byte_length,content_hash,relative_name,state,reservation_owner,created_at,updated_at) SELECT ?,?,?,?,?,?,?,'reserved',?,?,? WHERE EXISTS(SELECT 1 FROM acp_output_sets WHERE set_id=? AND state='assembling') ON CONFLICT(set_id,segment_index) DO UPDATE SET byte_start=excluded.byte_start,byte_end=excluded.byte_end,byte_length=excluded.byte_length,content_hash=excluded.content_hash,relative_name=excluded.relative_name,state='reserved',reservation_owner=excluded.reservation_owner,revision=acp_output_segments.revision+1,updated_at=excluded.updated_at WHERE acp_output_segments.state='cleaned' AND ?=1")
            .bind(set_id).bind(index).bind(start).bind(start + bytes).bind(bytes).bind(hash)
            .bind(name).bind(owner).bind(now).bind(now).bind(set_id).bind(i64::from(reused.is_some())).execute(&mut *tx).await?;
        if inserted.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        }
        if let Some(change) = reused {
            self.finish_row(&mut tx, Some(change)).await?;
        } else {
            self.insert_row(&mut tx, "acp_output_segments", set_id, Some(index))
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn land_segment(
        &self,
        set_id: &str,
        index: i64,
        owner: &str,
        now: &str,
    ) -> Result<(), OutputStoreError> {
        let mut tx = self.transaction().await?;
        self.prepare_row(&mut tx, "acp_output_sets", set_id, None)
            .await?;
        let change = self
            .prepare_row(&mut tx, "acp_output_segments", set_id, Some(index))
            .await?;
        let row=sqlx::query("UPDATE acp_output_segments SET state='landed',revision=revision+1,updated_at=? WHERE set_id=? AND segment_index=? AND state='reserved' AND reservation_owner=? RETURNING byte_length")
            .bind(now).bind(set_id).bind(index).bind(owner).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        };
        let bytes: i64 = row.try_get("byte_length")?;
        let changed=sqlx::query("UPDATE acp_output_quota SET reserved_bytes=reserved_bytes-?,landed_bytes=landed_bytes+?,revision=revision+1 WHERE singleton=1 AND reserved_bytes>=?")
            .bind(bytes).bind(bytes).bind(bytes).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        }
        self.finish_row(&mut tx, change).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn seal_set(
        &self,
        set_id: &str,
        total_bytes: i64,
        total_hash: &str,
        count: i64,
        deliveries: &[DeliveryPart],
        now: &str,
    ) -> Result<(), OutputStoreError> {
        if count < 0 || usize::try_from(count).ok() != Some(deliveries.len()) {
            return Err(OutputStoreError::Malformed);
        }
        let mut tx = self.transaction().await?;
        let change = self
            .prepare_row(&mut tx, "acp_output_sets", set_id, None)
            .await?;
        self.prepare_segments(&mut tx, set_id).await?;
        validate_segments(&mut tx, set_id, total_bytes).await?;
        let mut end = 0_i64;
        for (expected, part) in deliveries.iter().enumerate() {
            if part.index != expected as i64
                || part.byte_start != end
                || part.byte_end < part.byte_start
            {
                tx.rollback().await?;
                return Err(OutputStoreError::Malformed);
            }
            let covered=sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM acp_output_segments WHERE set_id=? AND segment_index=? AND byte_start<=? AND byte_end>=?")
                .bind(set_id).bind(part.spool_index).bind(part.byte_start).bind(part.byte_end).fetch_one(&mut *tx).await?;
            if covered != 1 {
                tx.rollback().await?;
                return Err(OutputStoreError::Malformed);
            }
            sqlx::query("INSERT INTO acp_output_delivery_segments(set_id,delivery_index,spool_index,byte_start,byte_end,content_hash,stable_txn_id) VALUES(?,?,?,?,?,?,?)")
                .bind(set_id).bind(part.index).bind(part.spool_index).bind(part.byte_start)
                .bind(part.byte_end).bind(&part.content_hash).bind(&part.stable_txn_id).execute(&mut *tx).await?;
            self.insert_row(
                &mut tx,
                "acp_output_delivery_segments",
                set_id,
                Some(part.index),
            )
            .await?;
            end = part.byte_end;
        }
        if end != total_bytes {
            tx.rollback().await?;
            return Err(OutputStoreError::Malformed);
        }
        let changed=sqlx::query("UPDATE acp_output_sets SET state='prepared',total_bytes=?,total_hash=?,segment_count=?,updated_at=? WHERE set_id=? AND state='assembling'")
            .bind(total_bytes).bind(total_hash).bind(count).bind(now).bind(set_id).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
        }
        self.finish_row(&mut tx, change).await?;
        tx.commit().await?;
        Ok(())
    }
}

async fn validate_segments(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    set_id: &str,
    total: i64,
) -> Result<(), OutputStoreError> {
    let rows=sqlx::query("SELECT segment_index,byte_start,byte_end,state FROM acp_output_segments WHERE set_id=? ORDER BY segment_index")
        .bind(set_id).fetch_all(&mut **tx).await?;
    let mut end = 0_i64;
    for (expected, row) in rows.iter().enumerate() {
        if row.try_get::<i64, _>("segment_index")? != expected as i64
            || row.try_get::<i64, _>("byte_start")? != end
            || row.try_get::<String, _>("state")? != "landed"
        {
            return Err(OutputStoreError::Malformed);
        }
        end = row.try_get("byte_end")?;
    }
    if end != total {
        return Err(OutputStoreError::Malformed);
    }
    Ok(())
}

pub(super) fn decode_segment(
    row: sqlx::sqlite::SqliteRow,
) -> Result<OutputSegment, OutputStoreError> {
    Ok(OutputSegment {
        set_id: row.try_get("set_id")?,
        index: row.try_get("segment_index")?,
        byte_start: row.try_get("byte_start")?,
        byte_end: row.try_get("byte_end")?,
        content_hash: row.try_get("content_hash")?,
        relative_name: row.try_get("relative_name")?,
        reservation_owner: row.try_get("reservation_owner")?,
        state: row.try_get("state")?,
    })
}
