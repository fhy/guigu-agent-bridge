use super::*;
use crate::storage::recovery_eligibility::writer;
use serde::{Deserialize, Serialize};

#[path = "owner_restart_guards.rs"]
pub(crate) mod guards;
#[cfg(test)]
#[path = "owner_restart_tests.rs"]
mod tests;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControlledStop {
    kind: String,
    owner: String,
    generation: i64,
    pub(crate) source_revision: i64,
    pub(crate) fingerprint: String,
    pub(crate) epoch: i64,
}

pub(crate) async fn restart_eligible(
    connection: &mut sqlx::SqliteConnection,
    token: &str,
) -> Result<bool, sqlx::Error> {
    if sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1",
    )
    .bind(token)
    .fetch_one(&mut *connection)
    .await?
        != 1
    {
        return Ok(false);
    }
    let row = sqlx::query("SELECT e.* FROM runtime_owner_evidence e JOIN runtime_instances r USING(instance_token) WHERE e.instance_token=? AND e.is_current=1 AND r.state='stopped' AND e.state='observing' AND e.barrier_state='closed_joined' AND e.in_flight_spawns=0 AND e.child_scan_state='inaccessible' AND e.process_fingerprint=r.process_fingerprint")
        .bind(token).fetch_optional(&mut *connection).await?;
    let Some(row) = row else { return Ok(false) };
    let Ok(record) =
        serde_json::from_str::<ControlledStop>(&row.get::<String, _>("child_snapshot"))
    else {
        return Ok(false);
    };
    let generation: i64 = row.get("runtime_generation");
    let revision: i64 = row.get("evidence_revision");
    if record.kind != "controlled-stop-v1"
        || record.owner != token
        || record.generation != generation
        || record.source_revision.checked_add(1) != Some(revision)
        || record.fingerprint != row.get::<String, _>("process_fingerprint")
        || record.epoch <= 0
        || record.epoch != row.get::<i64, _>("barrier_epoch")
    {
        return Ok(false);
    }
    let chain: (i64, i64, i64, i64) = sqlx::query_as("SELECT count(*),min(evidence_revision),max(evidence_revision),sum(CASE WHEN process_fingerprint!=? OR pid!=? OR proc_starttime!=? OR boot_id!=? OR (evidence_revision<? AND state NOT IN ('live','scan_pending')) THEN 1 ELSE 0 END) FROM runtime_owner_evidence WHERE instance_token=? AND runtime_generation=?")
        .bind(&record.fingerprint).bind(row.get::<i64, _>("pid")).bind(row.get::<i64, _>("proc_starttime"))
        .bind(row.get::<String, _>("boot_id")).bind(revision).bind(token).bind(generation)
        .fetch_one(&mut *connection).await?;
    let total: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence WHERE instance_token=?")
            .bind(token)
            .fetch_one(&mut *connection)
            .await?;
    Ok(total == revision
        && chain == (revision, 1, revision, 0)
        && guards::work_clear(connection).await?)
}

pub(crate) async fn bootstrap_evidence(
    connection: &mut sqlx::SqliteConnection,
    key: &str,
) -> Result<bool, sqlx::Error> {
    let token: Option<String> = sqlx::query_scalar("SELECT instance_token FROM runtime_owner_evidence WHERE json_array('text:'||hex(CAST(instance_token AS BLOB)),'integer:'||hex(CAST(runtime_generation AS BLOB)),'integer:'||hex(CAST(evidence_revision AS BLOB)))=?")
        .bind(key).fetch_optional(&mut *connection).await?;
    match token {
        Some(token) => restart_eligible(connection, &token).await,
        None => Ok(false),
    }
}

impl ReliabilityStore {
    pub(crate) fn with_runtime_executable(
        mut self,
        executable: crate::storage::owner_executable::RuntimeExecutable,
    ) -> Result<Self, ReliabilityError> {
        if let Some(scope) = &self.recovery {
            scope.install_runtime_executable(executable.clone())?;
        }
        self.executable = Some(executable);
        Ok(self)
    }

    pub(crate) async fn runtime_fingerprint(&self) -> Result<String, ReliabilityError> {
        let value = match &self.executable {
            Some(executable) => executable.validate().await,
            None => match &self.recovery {
                Some(scope) => return Ok(scope.runtime_fingerprint().await?),
                None => RuntimeOwnerEvidence::fingerprint(),
            },
        };
        value.ok_or(ReliabilityError::Malformed)
    }

    pub(crate) async fn finish_controlled_stop(
        &self,
        token: &str,
        now: &str,
        evidence: &RuntimeOwnerEvidence,
        joined: &crate::acp::JoinedChildren,
    ) -> Result<bool, ReliabilityError> {
        if writer::teardown::is_refused(&self.pool).await? {
            return writer::teardown::finish_controlled_stop(
                &self.pool,
                self.recovery.as_ref(),
                self,
                token,
                now,
                evidence,
                joined,
            )
            .await;
        }
        let fingerprint = self.runtime_fingerprint().await?;
        let mut transaction = writer::begin(&self.pool, self.recovery.as_ref()).await?;
        if restart_eligible(&mut transaction, token).await? {
            return Ok(true);
        }
        let Some(record) = self
            .prepare_controlled_stop(&mut transaction, token, now, evidence, joined, &fingerprint)
            .await?
        else {
            return Ok(false);
        };
        let snapshot = serde_json::to_string(&record).map_err(|_| ReliabilityError::Malformed)?;
        self.publish_controlled_stop(
            &mut transaction,
            token,
            now,
            record.source_revision,
            &snapshot,
            evidence,
            record.epoch,
            &record.fingerprint,
        )
        .await?;
        transaction.commit().await?;
        tracing::info!(
            phase = "owner-controlled-stop",
            owner = token,
            outcome = "restart-eligible",
            containment = false,
            "controlled owner joins completed"
        );
        Ok(true)
    }

    pub(crate) async fn prepare_controlled_stop(
        &self,
        connection: &mut sqlx::SqliteConnection,
        token: &str,
        now: &str,
        evidence: &RuntimeOwnerEvidence,
        joined: &crate::acp::JoinedChildren,
        fingerprint: &str,
    ) -> Result<Option<ControlledStop>, ReliabilityError> {
        let (generation, epoch) = joined.identity();
        if generation != token || epoch <= 0 || evidence.pid != i64::from(std::process::id()) {
            return Ok(None);
        }
        let row = sqlx::query("SELECT e.* FROM runtime_owner_evidence e JOIN runtime_instances r USING(instance_token) WHERE e.instance_token=? AND e.is_current=1 AND e.state='scan_pending' AND r.state='stopping' AND e.process_fingerprint=r.process_fingerprint")
            .bind(token).fetch_optional(&mut *connection).await?;
        let Some(row) = row else { return Ok(None) };
        let refusal = if sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1")
            .bind(token).fetch_one(&mut *connection).await? != 1 {
            Some("current-evidence-count")
        } else if !self.matches_clock(&row, fingerprint, now, evidence) {
            Some("owner-identity-or-clock")
        } else if !guards::work_clear(connection).await? {
            Some("unresolved-work")
        } else if sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime_instances WHERE instance_token!=? AND state IN ('active','stopping')")
            .bind(token).fetch_one(&mut *connection).await? != 0 {
            Some("competing-owner")
        } else {
            None
        };
        if let Some(reason) = refusal {
            tracing::warn!(
                phase = "owner-controlled-stop",
                reason,
                owner = token,
                "controlled stop remains unresolved"
            );
            return Ok(None);
        }
        let revision: i64 = row.get("evidence_revision");
        let chain: (i64, i64, i64, i64) = sqlx::query_as("SELECT count(*),min(evidence_revision),max(evidence_revision),sum(CASE WHEN state NOT IN ('live','scan_pending') OR process_fingerprint!=? OR pid!=? OR proc_starttime!=? OR boot_id!=? THEN 1 ELSE 0 END) FROM runtime_owner_evidence WHERE instance_token=? AND runtime_generation=?")
            .bind(fingerprint).bind(evidence.pid).bind(evidence.proc_starttime).bind(&evidence.boot_id)
            .bind(token).bind(row.get::<i64, _>("runtime_generation")).fetch_one(&mut *connection).await?;
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runtime_owner_evidence WHERE instance_token=?",
        )
        .bind(token)
        .fetch_one(&mut *connection)
        .await?;
        if total != revision || chain != (revision, 1, revision, 0) {
            return Ok(None);
        }
        Ok(Some(ControlledStop {
            kind: "controlled-stop-v1".into(),
            owner: token.into(),
            generation: row.get("runtime_generation"),
            source_revision: revision,
            fingerprint: fingerprint.to_owned(),
            epoch,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_controlled_stop(
        &self,
        connection: &mut sqlx::SqliteConnection,
        token: &str,
        now: &str,
        revision: i64,
        snapshot: &str,
        evidence: &RuntimeOwnerEvidence,
        epoch: i64,
        fingerprint: &str,
    ) -> Result<(), ReliabilityError> {
        let next = revision.checked_add(1).ok_or(ReliabilityError::Malformed)?;
        let owner = writer::text_identity("runtime_instances", token)?;
        let owner_mutation =
            writer::prepare_mutation(connection, self.recovery.as_ref(), &owner).await?;
        let identity =
            super::owner_lifecycle::evidence_identity(connection, token, revision).await?;
        let mutation =
            writer::prepare_mutation(connection, self.recovery.as_ref(), &identity).await?;
        let changed = sqlx::query("UPDATE runtime_owner_evidence SET is_current=0 WHERE instance_token=? AND evidence_revision=? AND is_current=1 AND state='scan_pending'")
            .bind(token).bind(revision).execute(&mut *connection).await?;
        if changed.rows_affected() != 1 {
            return Err(ReliabilityError::Malformed);
        }
        writer::complete_mutation(connection, self.recovery.as_ref(), mutation).await?;
        sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) SELECT instance_token,runtime_generation,?,'observing',pid,proc_starttime,process_fingerprint,boot_id,?,?,?,'closed_joined',0,'inaccessible',?,1,max_age_seconds FROM runtime_owner_evidence WHERE instance_token=? AND evidence_revision=?")
            .bind(next).bind(evidence.boottime_ns).bind(now).bind(epoch).bind(snapshot).bind(token).bind(revision).execute(&mut *connection).await?;
        let identity = super::owner_lifecycle::evidence_identity(connection, token, next).await?;
        writer::inserted(connection, self.recovery.as_ref(), &identity).await?;
        let changed=sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token=? AND state='stopping' AND process_fingerprint=?")
            .bind(now).bind(token).bind(fingerprint).execute(&mut *connection).await?;
        if changed.rows_affected() != 1 {
            return Err(ReliabilityError::Malformed);
        }
        writer::complete_mutation(connection, self.recovery.as_ref(), owner_mutation).await?;
        Ok(())
    }
}
