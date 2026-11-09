//! Startup owns both futures; neither renewal nor network work is detached.
use super::*;
use crate::matrix::rebuild_gap::{self, Failure};
use matrix_sdk::sync::SyncResponse;

impl MatrixSync {
    pub(crate) async fn rebuild_state(&self) -> Result<(), MatrixError> {
        let recovery = self.recovery.as_ref().ok_or(MatrixError::Configuration)?;
        // SDK sync mutates its local response cache even when Bridge refuses the
        // response. Reusing that client can elide an identical next_batch. A
        // failed/cancelled attempt must be retried with a fresh startup owner.
        if self.rebuild_started.swap(true, Ordering::AcqRel) {
            return Err(Failure::new("rebuild-retry-refused").reported());
        }
        let token = self
            .tokens
            .load()
            .await?
            .ok_or(MatrixError::CutoverRefused)?;
        let result = guarded(recovery, self.rebuild_response(&token)).await;
        let result = match result {
            Guarded::Lost(failure) => return Err(failure.reported()),
            Guarded::Finished(result) => result,
        };
        if let Err(failure) = &result {
            failure.report();
        }
        // Completion racing a lease change must not hand over a response. This
        // also establishes a fresh fence before the refusal/teardown transition.
        if renew(recovery).await.is_err() {
            tracing::error!(
                stage = "rebuild-state",
                predicate = "final-lease-renewal",
                committed = false,
                "rebuild result not admitted; lease confirmation failed"
            );
            return Err(result.err().map_or(MatrixError::Storage, |f| f.error));
        }
        match result {
            Ok(response) => {
                *self
                    .initial_token
                    .lock()
                    .map_err(|_| MatrixError::Storage)? = Some(token);
                *self
                    .initial_response
                    .lock()
                    .map_err(|_| MatrixError::Storage)? = Some(response);
                Ok(())
            }
            Err(failure) => {
                finish_failure(recovery, &failure).await;
                Err(failure.error)
            }
        }
    }

    async fn rebuild_response(&self, token: &str) -> Result<SyncResponse, Failure> {
        let recovery = self.recovery.as_ref().expect("checked recovery binding");
        let mut response = self
            .client
            .inner
            .sync_once(
                SyncSettings::new()
                    .token(token)
                    .full_state(true)
                    .timeout(self.server_timeout),
            )
            .await
            .map_err(|error| {
                let mut failure = Failure::new("sync-response");
                failure.error = classify_sdk_error(error);
                failure
            })?;
        coverage(rebuild_gap::fill(
            &self.client,
            recovery.store.pool(),
            &recovery.operation_id,
            &mut response,
        ))
        .await?;
        if response.next_batch.is_empty() {
            return Err(Failure::new("response-token-absent"));
        }
        if response
            .rooms
            .joined
            .values()
            .any(|room| room.timeline.limited)
        {
            return Err(Failure::new("coverage-incomplete"));
        }
        Ok(response)
    }
}

async fn coverage<T>(work: impl Future<Output = Result<T, Failure>>) -> Result<T, Failure> {
    tokio::time::timeout(Duration::from_secs(90), work)
        .await
        .map_err(|_| Failure::new("coverage-deadline"))?
}

enum Guarded<T> {
    Finished(Result<T, Failure>),
    Lost(Failure),
}

async fn renew(recovery: &RecoveryBinding) -> Result<(), MatrixError> {
    recovery
        .store
        .renew_component_lease(
            "MatrixSync",
            &recovery.owner_token,
            recovery.owner_epoch,
            &timestamp(),
        )
        .await
        .map_err(|_| MatrixError::Storage)
}

async fn guarded<T>(
    recovery: &RecoveryBinding,
    work: impl Future<Output = Result<T, Failure>>,
) -> Guarded<T> {
    if renew(recovery).await.is_err() {
        return Guarded::Lost(Failure::storage("initial-lease-renewal"));
    }
    let maintain = async {
        let period = Duration::from_secs(15);
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if renew(recovery).await.is_err() {
                return Failure::storage("lease-renewal");
            }
        }
    };
    // Keep both futures polled when either waits for a writer. A timer arm
    // awaiting renewal inline could deadlock work holding that same writer.
    tokio::select! {
        biased;
        failure = maintain => Guarded::Lost(failure),
        result = work => Guarded::Finished(result),
    }
}

async fn finish_failure(recovery: &RecoveryBinding, failure: &Failure) {
    // An initial SDK failure historically did not publish a history refusal.
    // Fill failures still use the same durable hold reason and coverage boundary.
    if failure.reason != "sync-response" {
        let recorded = recovery
            .store
            .refuse_response(
                &recovery.operation_id,
                &recovery.owner_token,
                recovery.owner_epoch,
                "rebuild-history-gap",
                &timestamp(),
            )
            .await
            .is_ok();
        tracing::error!(
            stage = "rebuild-refusal",
            predicate = failure.reason,
            committed = recorded,
            "first rebuild failure retained"
        );
        if !recorded {
            return;
        }
    }
    // No MatrixSync task has been spawned: the local network future has ended.
    // Reuse classified teardown; never clear a hold or release a competing owner.
    let released = MatrixSyncLease::new(recovery.clone())
        .release()
        .await
        .is_ok();
    tracing::info!(
        stage = "rebuild-cleanup",
        lease_released = released,
        joined = true,
        "startup sync work ended before exact lease teardown"
    );
}

#[cfg(test)]
#[path = "sync_rebuild_tests.rs"]
mod tests;
