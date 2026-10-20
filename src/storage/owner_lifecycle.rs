use super::*;
use crate::storage::owner_observation::{FinalObservation, fresh_timestamp};

#[cfg(test)]
#[path = "owner_lifecycle_tests.rs"]
mod tests;

impl ReliabilityStore {
    pub async fn begin_runtime_with_evidence(
        &self,
        token: &str,
        fingerprint: &str,
        now: &str,
        evidence: &RuntimeOwnerEvidence,
    ) -> Result<bool, ReliabilityError> {
        if !valid_owner_clock(evidence, now) || fingerprint.trim().is_empty() {
            return Ok(false);
        }
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let inserted = sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES(?,?,?,'active',?) ON CONFLICT DO NOTHING")
            .bind(token).bind(now).bind(now).bind(fingerprint).execute(&mut *transaction).await?;
        if inserted.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) VALUES(?,1,1,'live',?,?,?,?,?,?,0,'open',0,'inaccessible','{}',1,?)")
            .bind(token).bind(evidence.pid).bind(evidence.proc_starttime).bind(fingerprint)
            .bind(&evidence.boot_id).bind(evidence.boottime_ns).bind(now).bind(self.owner_evidence_max_age)
            .execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn stop_runtime(&self, token: &str, now: &str) -> Result<bool, ReliabilityError> {
        let result = sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token=? AND state IN ('active','stopping')")
            .bind(now).bind(token).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn publish_owner_heartbeat(
        &self,
        token: &str,
        fingerprint: &str,
        now: &str,
        evidence: &RuntimeOwnerEvidence,
    ) -> Result<bool, ReliabilityError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let current = sqlx::query("SELECT e.* FROM runtime_owner_evidence e JOIN runtime_instances r USING(instance_token) WHERE e.instance_token=? AND e.is_current=1 AND e.state='live' AND r.state='active'")
            .bind(token).fetch_optional(&mut *transaction).await?;
        let Some(current) = current else {
            return Ok(false);
        };
        let same_capture = chrono::DateTime::parse_from_rfc3339(now)
            .ok()
            .zip(
                chrono::DateTime::parse_from_rfc3339(&current.get::<String, _>("captured_at")).ok(),
            )
            .is_some_and(|(now, previous)| now == previous);
        let valid = self.matches_clock(&current, fingerprint, now, evidence) && !same_capture;
        let revision = current
            .get::<i64, _>("evidence_revision")
            .checked_add(1)
            .ok_or(ReliabilityError::Malformed)?;
        sqlx::query("UPDATE runtime_owner_evidence SET is_current=0 WHERE instance_token=? AND is_current=1")
            .bind(token).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) SELECT instance_token,runtime_generation,?,?,pid,proc_starttime,process_fingerprint,boot_id,?, ?,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,1,max_age_seconds FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
            .bind(revision).bind(if valid { "live" } else { "unknown" })
            .bind(if valid { evidence.boottime_ns } else { current.get("boottime_ns") })
            .bind(now).bind(token).bind(revision - 1).execute(&mut *transaction).await?;
        if valid {
            sqlx::query("UPDATE runtime_instances SET heartbeat_at=? WHERE instance_token=? AND state='active'")
                .bind(now).bind(token).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(valid)
    }

    fn matches_clock(
        &self,
        row: &sqlx::sqlite::SqliteRow,
        fingerprint: &str,
        now: &str,
        evidence: &RuntimeOwnerEvidence,
    ) -> bool {
        let previous: String = row.get("captured_at");
        valid_owner_clock(evidence, now)
            && fresh_timestamp(&previous, now, self.owner_evidence_max_age)
            && boot_identity_matches(&row.get::<String, _>("boot_id"), &evidence.boot_id)
            && boottime_monotonic(row.get("boottime_ns"), evidence.boottime_ns)
            && evidence.boottime_ns - row.get::<i64, _>("boottime_ns")
                <= self.owner_evidence_max_age * 1_000_000_000
            && row.get::<i64, _>("pid") == evidence.pid
            && row.get::<i64, _>("proc_starttime") == evidence.proc_starttime
            && row.get::<String, _>("process_fingerprint") == fingerprint
            && row.get::<i64, _>("max_age_seconds") == self.owner_evidence_max_age
    }

    pub async fn publish_runtime_reaped(
        &self,
        _token: &str,
        _now: &str,
        _barrier_epoch: i64,
        _child_snapshot: &str,
    ) -> Result<bool, ReliabilityError> {
        Ok(false)
    }

    pub(crate) async fn finalize_owner(
        &self,
        token: &str,
        now: &str,
        evidence: &RuntimeOwnerEvidence,
        observation: &FinalObservation,
    ) -> Result<bool, ReliabilityError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query("SELECT e.* FROM runtime_owner_evidence e JOIN runtime_instances r USING(instance_token) WHERE e.instance_token=? AND e.is_current=1 AND e.state='scan_pending' AND r.state='stopping'")
            .bind(token).fetch_optional(&mut *transaction).await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let fingerprint: String = row.get("process_fingerprint");
        let revision: i64 = row.get("evidence_revision");
        let chain: (i64, i64, i64) = sqlx::query_as("SELECT count(*),min(evidence_revision),max(evidence_revision) FROM runtime_owner_evidence WHERE instance_token=? AND runtime_generation=?")
            .bind(token).bind(row.get::<i64, _>("runtime_generation")).fetch_one(&mut *transaction).await?;
        if !observation.qualifies(observation.epoch)
            || !observation.bound_to(token)
            || !self.matches_clock(&row, &fingerprint, now, evidence)
            || chain != (revision, 1, revision)
        {
            return Ok(false);
        }
        let next_revision = revision.checked_add(1).ok_or(ReliabilityError::Malformed)?;
        let snapshot =
            serde_json::to_string(observation).map_err(|_| ReliabilityError::Malformed)?;
        sqlx::query("UPDATE runtime_owner_evidence SET is_current=0 WHERE instance_token=? AND is_current=1")
            .bind(token).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) SELECT instance_token,runtime_generation,?,'reaped',pid,proc_starttime,process_fingerprint,boot_id,?,?,?,'closed_joined',0,'complete_zero_escaped',?,1,max_age_seconds FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
            .bind(next_revision).bind(evidence.boottime_ns).bind(now).bind(observation.epoch).bind(snapshot).bind(token).bind(revision)
            .execute(&mut *transaction).await?;
        let stopped = sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token=? AND state='stopping'")
            .bind(now).bind(token).execute(&mut *transaction).await?;
        if stopped.rows_affected() != 1 {
            return Ok(false);
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub(crate) async fn verify_final_owner(
        &self,
        token: &str,
        now: &str,
        evidence: &RuntimeOwnerEvidence,
        observation: &FinalObservation,
    ) -> Result<bool, ReliabilityError> {
        if !observation.qualifies(observation.epoch) || !observation.bound_to(token) {
            return Ok(false);
        }
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT e.* FROM runtime_owner_evidence e JOIN runtime_instances r USING(instance_token) WHERE e.instance_token=? AND e.is_current=1 AND e.state='reaped' AND r.state='stopped' AND e.barrier_state='closed_joined' AND e.in_flight_spawns=0 AND e.child_scan_state='complete_zero_escaped'")
            .bind(token).fetch_optional(&mut *transaction).await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let fingerprint: String = row.get("process_fingerprint");
        let revision: i64 = row.get("evidence_revision");
        let chain: (i64, i64, i64) = sqlx::query_as("SELECT count(*),min(evidence_revision),max(evidence_revision) FROM runtime_owner_evidence WHERE instance_token=? AND runtime_generation=?")
            .bind(token).bind(row.get::<i64, _>("runtime_generation")).fetch_one(&mut *transaction).await?;
        Ok(self.matches_clock(&row, &fingerprint, now, evidence)
            && chain == (revision, 1, revision)
            && row.get::<i64, _>("barrier_epoch") == observation.epoch
            && row.get::<String, _>("child_snapshot")
                == serde_json::to_string(observation).map_err(|_| ReliabilityError::Malformed)?)
    }
}
