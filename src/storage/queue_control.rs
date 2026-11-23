//! Existing queue controls share the classified transaction with their CAS.
use super::{QueueClaim, ReliabilityError, ReliabilityStore};
use crate::storage::recovery_eligibility::writer;
use sqlx::Row;

impl ReliabilityStore {
    async fn queue_mutation(
        &self,
        c: &mut sqlx::SqliteConnection,
        key: &str,
    ) -> Result<Option<writer::Mutation>, sqlx::Error> {
        writer::prepare_mutation(
            c,
            self.recovery.as_ref(),
            &writer::text_identity("agent_work_queue", key)?,
        )
        .await
    }
    /// Claims the oldest queued item while holding the SQLite immediate writer lock.
    pub async fn claim_next(
        &self,
        target_endpoint_id: &str,
        resource_key: &str,
        runtime_owner: &str,
        owner_fence: i64,
        now: &str,
    ) -> Result<Option<QueueClaim>, ReliabilityError> {
        let mut conn = writer::begin(&self.pool, None).await?;
        let result = async {
            let lease = sqlx::query("SELECT task_id FROM execution_leases WHERE resource_key=? AND owner_token=? AND fence=? AND state='active' AND expires_at>?")
                .bind(resource_key).bind(runtime_owner).bind(owner_fence).bind(now).fetch_optional(&mut *conn).await?;
            let Some(lease) = lease else { return Err(ReliabilityError::WorkflowConflict); };
            let lease_task: String = lease.try_get("task_id")?;
            let row = sqlx::query("SELECT queue_id,task_id,revision FROM agent_work_queue WHERE target_endpoint_id=? AND lane='ordinary' AND state='queued' ORDER BY sequence LIMIT 1")
                .bind(target_endpoint_id).fetch_optional(&mut *conn).await?;
            let Some(row) = row else { return Ok(None); };
            let queue_id: String = row.try_get("queue_id")?;
            let task_id: String = row.try_get("task_id")?;
            if task_id != lease_task { return Err(ReliabilityError::WorkflowConflict); }
            let revision: i64 = row.try_get("revision")?;
            let next = sqlx::query("UPDATE agent_work_queue SET state='claimed',revision=revision+1,claimed_at=?,runtime_owner=?,owner_fence=?,updated_at=? WHERE queue_id=? AND state='queued' AND revision=?")
                .bind(now).bind(runtime_owner).bind(owner_fence).bind(now).bind(&queue_id).bind(revision).execute(&mut *conn).await?;
            if next.rows_affected() != 1 { return Err(ReliabilityError::WorkflowConflict); }
            Ok(Some(QueueClaim { queue_id, task_id, revision: revision + 1, owner_fence }))
        }.await;
        match result {
            Ok(v) => {
                conn.commit().await?;
                Ok(v)
            }
            Err(e) => {
                conn.rollback().await?;
                Err(e)
            }
        }
    }

    pub async fn mark_send_started(
        &self,
        claim: &QueueClaim,
        resource_key: &str,
        runtime_owner: &str,
        now: &str,
    ) -> Result<(), ReliabilityError> {
        let mut tx = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let lease: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_leases WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND state='active' AND expires_at>?")
            .bind(resource_key).bind(&claim.task_id).bind(runtime_owner).bind(claim.owner_fence).bind(now).fetch_one(&mut *tx).await?;
        if lease != 1 {
            return Err(ReliabilityError::WorkflowConflict);
        }
        let mutation = self.queue_mutation(&mut tx, &claim.queue_id).await?;
        let changed = sqlx::query("UPDATE agent_work_queue SET state='running',send_started=1,revision=revision+1,updated_at=? WHERE queue_id=? AND task_id=? AND state='claimed' AND send_started=0 AND revision=? AND runtime_owner=? AND owner_fence=?")
            .bind(now).bind(&claim.queue_id).bind(&claim.task_id).bind(claim.revision).bind(runtime_owner).bind(claim.owner_fence).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(ReliabilityError::WorkflowConflict);
        }
        writer::complete_mutation(&mut tx, self.recovery.as_ref(), mutation).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn pause_queue(
        &self,
        queue_id: &str,
        revision: i64,
        owner: &str,
        fence: i64,
        now: &str,
    ) -> Result<(), ReliabilityError> {
        let mut tx = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let mutation = self.queue_mutation(&mut tx, queue_id).await?;
        let changed = sqlx::query("UPDATE agent_work_queue SET state='paused',revision=revision+1,updated_at=? WHERE queue_id=? AND revision=? AND state IN ('queued','claimed','running') AND send_started=0 AND runtime_owner=? AND owner_fence=?")
            .bind(now).bind(queue_id).bind(revision).bind(owner).bind(fence).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(ReliabilityError::WorkflowConflict);
        }
        writer::complete_mutation(&mut tx, self.recovery.as_ref(), mutation).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Coordinator control for a queued row, which has no owner/fence yet.
    pub async fn pause_task_queued(
        &self,
        task_id: &str,
        now: &str,
    ) -> Result<u64, ReliabilityError> {
        let mut tx = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let ids: Vec<String> = sqlx::query_scalar("SELECT queue_id FROM agent_work_queue WHERE task_id=? AND state='queued' AND send_started=0 AND runtime_owner IS NULL AND owner_fence IS NULL").bind(task_id).fetch_all(&mut *tx).await?;
        let mut mutations = Vec::new();
        for id in ids {
            mutations.push(self.queue_mutation(&mut tx, &id).await?);
        }
        let changed = sqlx::query("UPDATE agent_work_queue SET state='paused',revision=revision+1,updated_at=? WHERE task_id=? AND state='queued' AND send_started=0 AND runtime_owner IS NULL AND owner_fence IS NULL")
            .bind(now).bind(task_id).execute(&mut *tx).await?;
        for mutation in mutations {
            writer::complete_mutation(&mut tx, self.recovery.as_ref(), mutation).await?;
        }
        tx.commit().await?;
        Ok(changed.rows_affected())
    }

    /// Control boundary after ACP shutdown/reap. An active lease means reap is
    /// not proven and is classified as recovery_needed rather than requeued.
    pub(crate) async fn pause_task_after_reap(
        &self,
        task_id: &str,
        resource_key: &str,
        owner_token: &str,
        owner_fence: i64,
        now: &str,
    ) -> Result<&'static str, ReliabilityError> {
        let mut tx = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let row = sqlx::query("SELECT queue_id,revision,state,runtime_owner,owner_fence,send_started FROM agent_work_queue WHERE task_id=? AND state IN ('queued','claimed','running') ORDER BY sequence LIMIT 1")
            .bind(task_id).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok("absent");
        };
        let id: String = row.try_get("queue_id")?;
        let mutation = self.queue_mutation(&mut tx, &id).await?;
        let rev: i64 = row.try_get("revision")?;
        let state: String = row.try_get("state")?;
        let owner: Option<String> = row.try_get("runtime_owner")?;
        let fence: Option<i64> = row.try_get("owner_fence")?;
        if state == "queued" {
            sqlx::query("UPDATE agent_work_queue SET state='paused',revision=revision+1,updated_at=? WHERE queue_id=? AND revision=? AND state='queued' AND send_started=0")
                .bind(now).bind(id).bind(rev).execute(&mut *tx).await?;
            writer::complete_mutation(&mut tx, self.recovery.as_ref(), mutation).await?;
            tx.commit().await?;
            return Ok("paused");
        }
        if owner.as_deref() != Some(owner_token) || fence != Some(owner_fence) {
            tx.rollback().await?;
            return Err(ReliabilityError::WorkflowConflict);
        }
        let bound: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_leases WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=?")
            .bind(resource_key).bind(task_id).bind(owner_token).bind(owner_fence)
            .fetch_one(&mut *tx).await?;
        if bound != 1 {
            tx.rollback().await?;
            return Err(ReliabilityError::WorkflowConflict);
        }
        let live: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_leases WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND state='active' AND expires_at>?")
            .bind(resource_key).bind(task_id).bind(owner_token).bind(owner_fence).bind(now).fetch_one(&mut *tx).await?;
        if live != 0 {
            sqlx::query("UPDATE agent_work_queue SET state='recovery_needed',revision=revision+1,reason='pause reap not confirmed',updated_at=? WHERE queue_id=? AND revision=?").bind(now).bind(id).bind(rev).execute(&mut *tx).await?;
            writer::complete_mutation(&mut tx, self.recovery.as_ref(), mutation).await?;
            tx.commit().await?;
            return Ok("recovery_needed");
        }
        let changed = sqlx::query("UPDATE agent_work_queue SET state='paused',revision=revision+1,updated_at=? WHERE queue_id=? AND revision=? AND runtime_owner=? AND owner_fence=? AND state IN ('claimed','running')").bind(now).bind(id).bind(rev).bind(owner_token).bind(owner_fence).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(ReliabilityError::WorkflowConflict);
        }
        writer::complete_mutation(&mut tx, self.recovery.as_ref(), mutation).await?;
        tx.commit().await?;
        Ok("paused")
    }

    pub(crate) async fn mark_pause_recovery_needed(
        &self,
        task_id: &str,
        now: &str,
    ) -> Result<&'static str, ReliabilityError> {
        let mut tx = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let ids: Vec<String> = sqlx::query_scalar("SELECT queue_id FROM agent_work_queue WHERE task_id=? AND state IN ('claimed','running') AND send_started=1").bind(task_id).fetch_all(&mut *tx).await?;
        let mut mutations = Vec::new();
        for id in ids {
            mutations.push(self.queue_mutation(&mut tx, &id).await?);
        }
        let changed = sqlx::query("UPDATE agent_work_queue SET state='recovery_needed',revision=revision+1,reason='pause reap not confirmed',updated_at=? WHERE task_id=? AND state IN ('claimed','running') AND send_started=1")
            .bind(now).bind(task_id).execute(&mut *tx).await?;
        for mutation in mutations {
            writer::complete_mutation(&mut tx, self.recovery.as_ref(), mutation).await?;
        }
        tx.commit().await?;
        Ok(if changed.rows_affected() == 1 {
            "recovery_needed"
        } else {
            "absent"
        })
    }

    pub async fn resume_task(&self, task_id: &str, now: &str) -> Result<u64, ReliabilityError> {
        let mut tx = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let rows = sqlx::query("SELECT queue_id,revision,target_endpoint_id,runtime_owner,owner_fence FROM agent_work_queue WHERE task_id=? AND state='paused' ORDER BY sequence")
            .bind(task_id).fetch_all(&mut *tx).await?;
        let mut count = 0;
        for row in rows {
            let id: String = row.try_get("queue_id")?;
            let rev: i64 = row.try_get("revision")?;
            let target: String = row.try_get("target_endpoint_id")?;
            let owner: Option<String> = row.try_get("runtime_owner")?;
            let fence: Option<i64> = row.try_get("owner_fence")?;
            let mutation = self.queue_mutation(&mut tx, &id).await?;
            let counter = writer::prepare_mutation(
                &mut tx,
                self.recovery.as_ref(),
                &writer::text_identity("agent_queue_counters", &target)?,
            )
            .await?;
            let sequence: i64 = sqlx::query_scalar(
                "SELECT next_sequence FROM agent_queue_counters WHERE target_endpoint_id=?",
            )
            .bind(&target)
            .fetch_one(&mut *tx)
            .await?;
            sqlx::query("UPDATE agent_queue_counters SET next_sequence=next_sequence+1,updated_at=? WHERE target_endpoint_id=?").bind(now).bind(&target).execute(&mut *tx).await?;
            let changed = sqlx::query("UPDATE agent_work_queue SET state='queued',sequence=?,revision=revision+1,claimed_at=NULL,send_started=0,runtime_owner=NULL,owner_fence=NULL,updated_at=? WHERE queue_id=? AND revision=? AND state='paused' AND ((runtime_owner=? AND owner_fence=?) OR (runtime_owner IS NULL AND owner_fence IS NULL))")
                .bind(sequence).bind(now).bind(&id).bind(rev).bind(owner.as_deref()).bind(fence).execute(&mut *tx).await?;
            if changed.rows_affected() != 1 {
                tx.rollback().await?;
                return Err(ReliabilityError::WorkflowConflict);
            }
            writer::complete_mutation(&mut tx, self.recovery.as_ref(), counter).await?;
            writer::complete_mutation(&mut tx, self.recovery.as_ref(), mutation).await?;
            count += 1;
        }
        tx.commit().await?;
        Ok(count)
    }

    pub async fn resume_queue(
        &self,
        queue_id: &str,
        revision: i64,
        owner: &str,
        fence: i64,
        now: &str,
    ) -> Result<(), ReliabilityError> {
        let mut tx = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let target: String = sqlx::query_scalar("SELECT target_endpoint_id FROM agent_work_queue WHERE queue_id=? AND revision=? AND state='paused' AND ((runtime_owner=? AND owner_fence=?) OR (runtime_owner IS NULL AND owner_fence IS NULL))")
            .bind(queue_id).bind(revision).bind(owner).bind(fence).fetch_one(&mut *tx).await?;
        let mutation = self.queue_mutation(&mut tx, queue_id).await?;
        let counter = writer::prepare_mutation(
            &mut tx,
            self.recovery.as_ref(),
            &writer::text_identity("agent_queue_counters", &target)?,
        )
        .await?;
        let sequence: i64 = sqlx::query_scalar(
            "SELECT next_sequence FROM agent_queue_counters WHERE target_endpoint_id=?",
        )
        .bind(&target)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query("UPDATE agent_queue_counters SET next_sequence=next_sequence+1,updated_at=? WHERE target_endpoint_id=?").bind(now).bind(&target).execute(&mut *tx).await?;
        let changed = sqlx::query("UPDATE agent_work_queue SET state='queued',sequence=?,revision=revision+1,claimed_at=NULL,send_started=0,runtime_owner=NULL,owner_fence=NULL,updated_at=? WHERE queue_id=? AND revision=? AND state='paused' AND ((runtime_owner=? AND owner_fence=?) OR (runtime_owner IS NULL AND owner_fence IS NULL))")
            .bind(sequence).bind(now).bind(queue_id).bind(revision).bind(owner).bind(fence).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(ReliabilityError::WorkflowConflict);
        }
        writer::complete_mutation(&mut tx, self.recovery.as_ref(), counter).await?;
        writer::complete_mutation(&mut tx, self.recovery.as_ref(), mutation).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn recover_queue(
        &self,
        queue_id: &str,
        revision: i64,
        now: &str,
    ) -> Result<(), ReliabilityError> {
        let mut tx = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let mutation = self.queue_mutation(&mut tx, queue_id).await?;
        let changed = sqlx::query("UPDATE agent_work_queue SET state=CASE WHEN send_started=0 THEN 'queued' ELSE 'recovery_needed' END,revision=revision+1,claimed_at=CASE WHEN send_started=0 THEN NULL ELSE claimed_at END,runtime_owner=CASE WHEN send_started=0 THEN NULL ELSE runtime_owner END,owner_fence=CASE WHEN send_started=0 THEN NULL ELSE owner_fence END,updated_at=? WHERE queue_id=? AND revision=? AND state IN ('claimed','running') AND NOT EXISTS (SELECT 1 FROM execution_leases l WHERE l.task_id=agent_work_queue.task_id AND l.owner_token=agent_work_queue.runtime_owner AND l.fence=agent_work_queue.owner_fence AND l.state='active' AND l.expires_at>?)")
            .bind(now).bind(queue_id).bind(revision).bind(now).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(ReliabilityError::WorkflowConflict);
        }
        writer::complete_mutation(&mut tx, self.recovery.as_ref(), mutation).await?;
        tx.commit().await?;
        Ok(())
    }
}
