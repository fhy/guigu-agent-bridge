use sqlx::{Row, SqlitePool};
use thiserror::Error;

use super::output_delivery::PreparedOutput;
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
}

impl OutputSpoolStore {
    pub(crate) fn new(pool: SqlitePool, quota_limit: u64) -> Result<Self, OutputStoreError> {
        let quota_limit = i64::try_from(quota_limit).map_err(|_| OutputStoreError::Malformed)?;
        Ok(Self { pool, quota_limit })
    }

    #[cfg(test)]
    pub(crate) async fn create_set(
        &self,
        input: &OutputSetInput<'_>,
    ) -> Result<(), OutputStoreError> {
        sqlx::query("INSERT INTO acp_output_sets(set_id,task_id,attempt,turn,runtime_fence,room_id,thread_root,reply_event_id,render_version,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'v1','assembling',?,?)")
            .bind(input.set_id).bind(input.task_id).bind(input.attempt).bind(input.turn)
            .bind(input.runtime_fence).bind(input.room_id).bind(input.thread_root)
            .bind(input.reply_event_id).bind(input.now).bind(input.now).execute(&self.pool).await?;
        Ok(())
    }

    pub(crate) async fn assembling_set(
        &self,
        task_id: &str,
    ) -> Result<Option<String>, OutputStoreError> {
        Ok(sqlx::query_scalar("SELECT set_id FROM acp_output_sets WHERE task_id=? AND state='assembling' ORDER BY turn DESC LIMIT 1")
            .bind(task_id).fetch_optional(&self.pool).await?)
    }

    pub(crate) async fn prepare_task(
        &self,
        task_id: &str,
        now: &str,
    ) -> Result<Option<PreparedOutput>, OutputStoreError> {
        let row=sqlx::query("SELECT d.attempt,c.completed_turns+1 AS turn,c.runtime_generation,a.reply_room,a.reply_thread_root,a.reply_event_id,COALESCE(a.render_version,'v1') render_version FROM task_continuations c JOIN deliveries d ON d.delivery_id=c.delivery_id JOIN task_admissions a ON a.task_id=c.task_id WHERE c.task_id=? AND c.state='in_flight'")
            .bind(task_id).fetch_optional(&self.pool).await?;
        let Some(row) = row else { return Ok(None) };
        let Some(room) = row.try_get::<Option<String>, _>("reply_room")? else {
            return Ok(None);
        };
        let attempt: i64 = row.try_get("attempt")?;
        let turn: i64 = row.try_get("turn")?;
        let set_id = uuid::Uuid::new_v5(
            &uuid::Uuid::from_u128(0x64c52b8d_26ef_57a0_a0c1_6d460bb30dca),
            format!("{task_id}\0{attempt}\0{turn}").as_bytes(),
        )
        .to_string();
        sqlx::query("INSERT INTO acp_output_sets(set_id,task_id,attempt,turn,runtime_fence,room_id,thread_root,reply_event_id,render_version,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,'assembling',?,?) ON CONFLICT(task_id,attempt,turn) DO NOTHING")
            .bind(&set_id).bind(task_id).bind(attempt).bind(turn)
            .bind(row.try_get::<String,_>("runtime_generation")?).bind(room)
            .bind(row.try_get::<Option<String>,_>("reply_thread_root")?)
            .bind(row.try_get::<Option<String>,_>("reply_event_id")?)
            .bind(row.try_get::<String,_>("render_version")?).bind(now).bind(now)
            .execute(&self.pool).await?;
        let winner:Option<String>=sqlx::query_scalar("SELECT set_id FROM acp_output_sets WHERE task_id=? AND attempt=? AND turn=? AND state='assembling'")
            .bind(task_id).bind(attempt).bind(turn).fetch_optional(&self.pool).await?;
        if winner.as_deref() != Some(&set_id) {
            return Err(OutputStoreError::Conflict);
        }
        Ok(Some(PreparedOutput { set_id }))
    }

    pub(crate) async fn abandon(
        &self,
        set_id: &str,
        now: &str,
    ) -> Result<Vec<OutputSegment>, OutputStoreError> {
        let mut tx = self.pool.begin().await?;
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
        let mut tx = self.pool.begin().await?;
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
        tx.commit().await?;
        Ok(())
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
        let mut tx = self.pool.begin().await?;
        let row=sqlx::query("DELETE FROM acp_output_segments WHERE set_id=? AND segment_index=? AND state='reserved' AND reservation_owner=? RETURNING byte_length")
            .bind(set_id).bind(index).bind(owner).fetch_optional(&mut *tx).await?;
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
        let mut tx = self.pool.begin().await?;
        let quota=sqlx::query("UPDATE acp_output_quota SET reserved_bytes=reserved_bytes+?,revision=revision+1 WHERE singleton=1 AND reserved_bytes+landed_bytes+?<=? RETURNING revision")
            .bind(bytes).bind(bytes).bind(self.quota_limit).fetch_optional(&mut *tx).await?;
        if quota.is_none() {
            tx.rollback().await?;
            return Err(OutputStoreError::Quota);
        }
        let inserted=sqlx::query("INSERT INTO acp_output_segments(set_id,segment_index,byte_start,byte_end,byte_length,content_hash,relative_name,state,reservation_owner,created_at,updated_at) SELECT ?,?,?,?,?,?,?,'reserved',?,?,? WHERE EXISTS(SELECT 1 FROM acp_output_sets WHERE set_id=? AND state='assembling')")
            .bind(set_id).bind(index).bind(start).bind(start + bytes).bind(bytes).bind(hash)
            .bind(name).bind(owner).bind(now).bind(now).bind(set_id).execute(&mut *tx).await?;
        if inserted.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(OutputStoreError::Conflict);
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
        let mut tx = self.pool.begin().await?;
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
        let mut tx = self.pool.begin().await?;
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
