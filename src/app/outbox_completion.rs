//! Retry only the durable completion of a known in-process network outcome.
use crate::storage::{PendingProjection, ReliabilityError, ReliabilityStore};
use std::time::Duration;
use tokio::sync::watch;

// Independent completion budget; changing HTTP send timeouts must not change it.
pub(super) const WAIT_BUDGET: Duration = Duration::from_secs(30);

pub(crate) async fn finish(
    store: &ReliabilityStore,
    row: &PendingProjection,
    success: bool,
    mut shutdown: Option<watch::Receiver<bool>>,
    budget: Duration,
) -> Result<bool, ReliabilityError> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let attempt = async {
            if success {
                store.mark_projection_sent(row).await
            } else {
                store.mark_projection_abandoned(row).await
            }
        };
        let result = tokio::time::timeout_at(deadline, attempt)
            .await
            .map_err(|_| budget_exhausted())?;
        let Err(ReliabilityError::Sql(ref error)) = result else {
            return result;
        };
        if !crate::storage::recovery_eligibility::matrix_effect_busy(error) {
            return result;
        }
        // The failed transaction has been dropped. Never hold SQL/writer locks
        // while waiting for sync; never re-enter authorization or network send.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() || shutdown.as_ref().is_some_and(|r| *r.borrow()) {
            return result;
        }
        tokio::select! {
            biased;
            _ = cancelled(&mut shutdown) => return result,
            _ = tokio::time::sleep(remaining.min(Duration::from_millis(100))) => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return result;
        }
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        tokio::time::timeout_at(deadline, store.renew_outbox_component(&now))
            .await
            .map_err(|_| budget_exhausted())??;
        // Every attempt rechecks generation, live fence, lineage and exact claim.
        // SQL/CAS/hold/unknown/lease loss never turn into a retry permission.
    }
}

async fn cancelled(shutdown: &mut Option<watch::Receiver<bool>>) {
    match shutdown {
        Some(receiver) => {
            let _ = receiver.changed().await;
        }
        None => std::future::pending().await,
    }
}

fn budget_exhausted() -> ReliabilityError {
    // Cancellation does not establish whether SQL committed. Keep the claim and
    // never infer permission to resend from this timeout.
    tracing::error!(
        stage = "outbox-completion",
        step = "budget",
        class = "completion-timeout",
        outcome = "retained-without-resend"
    );
    ReliabilityError::Malformed
}
