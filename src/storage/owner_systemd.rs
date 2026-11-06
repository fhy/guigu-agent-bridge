use super::owner_lifecycle::evidence_identity;
use super::*;
use crate::storage::owner_observation::fresh_timestamp;
use crate::storage::recovery_eligibility::writer;

impl ReliabilityStore {
    fn fresh_systemd_observation(&self, proof: &crate::systemd_owner::Proof) -> bool {
        fresh_timestamp(
            &proof.observed_at,
            &chrono::Utc::now().to_rfc3339(),
            self.owner_evidence_max_age,
        ) && proof.boottime >= proof.capsule.boottime
    }
    async fn stage_systemd_scan(
        &self,
        connection: &mut sqlx::SqliteConnection,
        proof: &crate::systemd_owner::Proof,
        revision: i64,
        evidence_state: &str,
        runtime_state: &str,
    ) -> Result<(i64, String), ReliabilityError> {
        if evidence_state == "scan_pending" && matches!(runtime_state, "stopping" | "stopped") {
            return Ok((revision, runtime_state.into()));
        }
        if evidence_state != "live" || runtime_state != "active" {
            return Err(ReliabilityError::Malformed);
        }
        let capsule = &proof.capsule;
        let next = revision.checked_add(1).ok_or(ReliabilityError::Malformed)?;
        let owner = writer::text_identity("runtime_instances", &capsule.owner)?;
        let owner_mutation =
            writer::prepare_mutation(connection, self.recovery.as_ref(), &owner).await?;
        let changed = sqlx::query("UPDATE runtime_instances SET state='stopping' WHERE instance_token=? AND process_fingerprint=? AND state='active'")
            .bind(&capsule.owner).bind(&capsule.fingerprint).execute(&mut *connection).await?;
        if changed.rows_affected() != 1 {
            return Err(ReliabilityError::Malformed);
        }
        writer::complete_mutation(connection, self.recovery.as_ref(), owner_mutation).await?;
        let identity = evidence_identity(connection, &capsule.owner, revision).await?;
        let mutation =
            writer::prepare_mutation(connection, self.recovery.as_ref(), &identity).await?;
        let changed = sqlx::query("UPDATE runtime_owner_evidence SET is_current=0 WHERE instance_token=? AND evidence_revision=? AND is_current=1 AND state='live'")
            .bind(&capsule.owner).bind(revision).execute(&mut *connection).await?;
        if changed.rows_affected() != 1 {
            return Err(ReliabilityError::Malformed);
        }
        writer::complete_mutation(connection, self.recovery.as_ref(), mutation).await?;
        sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) SELECT instance_token,runtime_generation,?,'scan_pending',pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,1,max_age_seconds FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
            .bind(next).bind(&capsule.owner).bind(revision).execute(&mut *connection).await?;
        let identity = evidence_identity(connection, &capsule.owner, next).await?;
        writer::inserted(connection, self.recovery.as_ref(), &identity).await?;
        Ok((next, "stopping".into()))
    }

    pub(crate) async fn finalize_systemd_owner(
        &self,
        proof: &crate::systemd_owner::Proof,
        expected_revision: i64,
    ) -> Result<bool, ReliabilityError> {
        let capsule = &proof.capsule;
        if !self.fresh_systemd_observation(proof) {
            return Ok(false);
        }
        let mut transaction = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        let row = sqlx::query("SELECT e.*,r.state AS runtime_state FROM runtime_owner_evidence e JOIN runtime_instances r USING(instance_token) WHERE e.instance_token=? AND e.is_current=1 AND r.process_fingerprint=e.process_fingerprint")
            .bind(&capsule.owner).fetch_optional(&mut *transaction).await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let snapshot = serde_json::json!({"reason":"systemd-terminal-v1","binding":proof.binding,"terminal":proof.terminal,"source_revision":expected_revision}).to_string();
        let state: String = row.get("state");
        let revision: i64 = row.get("evidence_revision");
        if state == "reaped" {
            return Ok((revision
                == expected_revision
                    .checked_add(1)
                    .ok_or(ReliabilityError::Malformed)?
                || revision
                    == expected_revision
                        .checked_add(2)
                        .ok_or(ReliabilityError::Malformed)?)
                && row.get::<String, _>("child_snapshot") == snapshot
                && row.get::<String, _>("runtime_state") == "stopped");
        }
        if revision != expected_revision
            || !matches!(state.as_str(), "live" | "scan_pending")
            || row.get::<i64, _>("runtime_generation") != capsule.generation
            || row.get::<String, _>("process_fingerprint") != capsule.fingerprint
            || row.get::<i64, _>("pid") != capsule.pid
            || row.get::<i64, _>("proc_starttime") != capsule.starttime
            || row.get::<String, _>("boot_id") != capsule.boot
            || row.get::<String, _>("child_snapshot") != proof.binding
            || !matches!(
                row.get::<String, _>("runtime_state").as_str(),
                "active" | "stopping" | "stopped"
            )
        {
            return Ok(false);
        }
        let chain: (i64,i64,i64) = sqlx::query_as("SELECT count(*),min(evidence_revision),max(evidence_revision) FROM runtime_owner_evidence WHERE instance_token=? AND runtime_generation=?")
            .bind(&capsule.owner).bind(capsule.generation).fetch_one(&mut *transaction).await?;
        if chain != (revision, 1, revision) {
            return Ok(false);
        }
        let conflicts: i64 = sqlx::query_scalar("SELECT
            (SELECT count(*) FROM offline_recovery_binding) +
            (SELECT count(*) FROM runtime_instances WHERE state IN ('active','stopping') AND instance_token != ?) +
            (SELECT count(*) FROM execution_leases WHERE state NOT IN ('released','terminal')) +
            (SELECT count(*) FROM task_continuations WHERE state NOT IN ('terminal','released')) +
            (SELECT count(*) FROM agent_work_queue WHERE state NOT IN ('completed','failed','cancelled','expired')) +
            (SELECT count(*) FROM workspace_claims WHERE runtime_owner=? AND state != 'released') +
            (SELECT count(*) FROM session_owner_bindings WHERE runtime_instance=?) +
            (SELECT count(*) FROM task_admissions WHERE runtime_instance=? AND state != 'terminal') +
            (SELECT count(*) FROM deliveries d JOIN task_admissions a USING(task_id) WHERE a.runtime_instance=?)")
            .bind(&capsule.owner).bind(&capsule.owner).bind(&capsule.owner).bind(&capsule.owner).bind(&capsule.owner)
            .fetch_one(&mut *transaction).await?;
        if conflicts != 0 {
            return Ok(false);
        }
        let initial: Option<String> = sqlx::query_scalar("SELECT child_snapshot FROM runtime_owner_evidence WHERE instance_token=? AND runtime_generation=? AND evidence_revision=?")
            .bind(&capsule.owner).bind(capsule.generation).bind(capsule.initial_revision).fetch_optional(&mut *transaction).await?;
        if initial.as_deref() != Some(&proof.binding) {
            return Ok(false);
        }
        let runtime_state: String = row.get("runtime_state");
        let (revision, runtime_state) = self
            .stage_systemd_scan(&mut transaction, proof, revision, &state, &runtime_state)
            .await?;
        self.publish_systemd_owner(transaction, proof, revision, snapshot, runtime_state)
            .await
    }

    async fn publish_systemd_owner(
        &self,
        mut transaction: writer::ScopedTransaction<'_>,
        proof: &crate::systemd_owner::Proof,
        revision: i64,
        snapshot: String,
        runtime_state: String,
    ) -> Result<bool, ReliabilityError> {
        let capsule = &proof.capsule;
        let next = revision.checked_add(1).ok_or(ReliabilityError::Malformed)?;
        let owner = writer::text_identity("runtime_instances", &capsule.owner)?;
        let owner_mutation =
            writer::prepare_mutation(&mut transaction, self.recovery.as_ref(), &owner).await?;
        let identity = evidence_identity(&mut transaction, &capsule.owner, revision).await?;
        let mutation =
            writer::prepare_mutation(&mut transaction, self.recovery.as_ref(), &identity).await?;
        let changed = sqlx::query("UPDATE runtime_owner_evidence SET is_current=0 WHERE instance_token=? AND evidence_revision=? AND is_current=1")
            .bind(&capsule.owner).bind(revision).execute(&mut *transaction).await?;
        if changed.rows_affected() != 1 {
            return Ok(false);
        }
        writer::complete_mutation(&mut transaction, self.recovery.as_ref(), mutation).await?;
        sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) SELECT instance_token,runtime_generation,?,'reaped',pid,proc_starttime,process_fingerprint,boot_id,?,?,barrier_epoch+1,'closed_joined',0,'complete_zero_escaped',?,1,max_age_seconds FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
            .bind(next).bind(proof.boottime).bind(&proof.observed_at).bind(snapshot).bind(&capsule.owner).bind(revision).execute(&mut *transaction).await?;
        let identity = evidence_identity(&mut transaction, &capsule.owner, next).await?;
        writer::inserted(&mut transaction, self.recovery.as_ref(), &identity).await?;
        let changed = sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token=? AND process_fingerprint=? AND state=?")
            .bind(&proof.observed_at).bind(&capsule.owner).bind(&capsule.fingerprint).bind(runtime_state).execute(&mut *transaction).await?;
        if changed.rows_affected() != 1 {
            return Ok(false);
        }
        writer::complete_mutation(&mut transaction, self.recovery.as_ref(), owner_mutation).await?;
        transaction.commit().await?;
        Ok(true)
    }
}
