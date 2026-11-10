use std::{collections::VecDeque, sync::Arc};

use crate::{matrix::MatrixOutboxSender, storage::ReliabilityStore};
use tokio::sync::{Mutex, watch};
const SEND_TIMEOUT: std::time::Duration = crate::acp::output_policy::MATRIX_SEND_TIMEOUT;

#[derive(Clone)]
pub struct OutboxDrain {
    store: ReliabilityStore,
    sender: Arc<dyn MatrixOutboxSender>,
    batch: u32,
    pending: Arc<Mutex<VecDeque<PendingSend>>>,
}

struct PendingSend {
    row: crate::storage::PendingProjection,
    // Some records the completed network call. A refused persistence write
    // must never cause that call to be repeated, including through a clone.
    sent: SendState,
}

enum SendState {
    Prepared,
    Started,
    Complete(bool),
}

#[derive(Debug, Clone, Copy)]
enum DrainFailure {
    Storage,
    Authority,
    UnknownSend,
    CompletionBlocked,
    SyncBusy,
}
impl DrainFailure {
    fn class(self) -> &'static str {
        match self {
            Self::Storage => "storage",
            Self::Authority => "authority-lost",
            Self::UnknownSend => "send-unknown",
            Self::CompletionBlocked => "completion-blocked",
            Self::SyncBusy => "sync-busy-budget",
        }
    }
}

impl OutboxDrain {
    pub fn new(store: ReliabilityStore, sender: Arc<dyn MatrixOutboxSender>, batch: u32) -> Self {
        assert!(batch > 0);
        Self {
            store,
            sender,
            batch,
            pending: Default::default(),
        }
    }

    #[allow(clippy::result_unit_err)]
    pub async fn drain_once(&self) -> Result<usize, ()> {
        self.drain_checked().await.map_err(|_| ())
    }

    async fn require_clear_send_state(&self) -> Result<(), DrainFailure> {
        if self
            .store
            .has_sending_projection()
            .await
            .map_err(|_| DrainFailure::Storage)?
        {
            return Err(DrainFailure::CompletionBlocked);
        }
        Ok(())
    }

    async fn drain_checked(&self) -> Result<usize, DrainFailure> {
        let mut pending = self.pending.lock().await;
        let now = chrono::Utc::now();
        let stale = (now - chrono::Duration::minutes(5))
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let now = now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        self.store
            .renew_outbox_component(&now)
            .await
            .map_err(|_| DrainFailure::Storage)?;
        if pending.is_empty() {
            self.require_clear_send_state().await?;
            let owner = uuid::Uuid::now_v7().to_string();
            let rows = self
                .store
                .claim_projections(&owner, &now, &stale, self.batch)
                .await
                .map_err(before_send_failure)?;
            pending.extend(rows.into_iter().map(|row| PendingSend {
                row,
                sent: SendState::Prepared,
            }));
        }
        let mut sent = 0;
        while let Some(next) = pending.front_mut() {
            let row = &next.row;
            if matches!(next.sent, SendState::Started) {
                // A cancelled network future has no known outcome. Retain the
                // sending claim; neither this instance nor a clone may retry it.
                return Err(DrainFailure::UnknownSend);
            }
            if matches!(next.sent, SendState::Prepared) {
                if !self
                    .store
                    .authorize_projection_send(row)
                    .await
                    .map_err(before_send_failure)?
                {
                    return Err(DrainFailure::Authority);
                }
                next.sent = SendState::Started;
                let send = self.sender.send_stable(
                    &row.room_id,
                    row.thread_root.as_deref(),
                    row.reply_event_id.as_deref(),
                    &row.body,
                    &row.stable_txn_id,
                );
                let Ok(outcome) = tokio::time::timeout(SEND_TIMEOUT, send).await else {
                    // Leave Started/sending: timeout does not establish a failed send.
                    return Err(DrainFailure::UnknownSend);
                };
                next.sent = SendState::Complete(outcome.is_ok());
            }
            let success = matches!(next.sent, SendState::Complete(true));
            let completed = if success {
                self.store.mark_projection_sent(row).await
            } else {
                self.store.mark_projection_abandoned(row).await
            };
            match completed {
                Ok(true) => {
                    sent += usize::from(success);
                    pending.pop_front();
                }
                Ok(false) | Err(_) => {
                    tracing::debug!(stage="outbox-completion", source_id=%row.source_id,
                        projection=%row.projection, send_succeeded=success,
                        outcome="retained-without-resend",
                        "projection completion deferred by durable authority");
                    return Err(DrainFailure::CompletionBlocked);
                }
            }
        }
        Ok(sent)
    }

    pub fn start(self, interval: std::time::Duration) -> OutboxDrainHandle {
        self.start_supervised(interval, None)
    }

    pub(crate) fn start_supervised(
        self,
        interval: std::time::Duration,
        owner: Option<super::required_owner::RequiredOwner>,
    ) -> OutboxDrainHandle {
        self.start_bounded(interval, owner, SEND_TIMEOUT)
    }

    fn start_bounded(
        self,
        interval: std::time::Duration,
        owner: Option<super::required_owner::RequiredOwner>,
        busy_budget: std::time::Duration,
    ) -> OutboxDrainHandle {
        let (shutdown, mut receiver) = watch::channel(false);
        let join = tokio::spawn(async move {
            let mut owner = owner;
            let mut busy_since: Option<tokio::time::Instant> = None;
            loop {
                if *receiver.borrow() {
                    return self.store.release_outbox_component().await;
                }
                let result = if busy_since.is_some_and(|at| at.elapsed() >= busy_budget) {
                    Err(DrainFailure::SyncBusy)
                } else {
                    self.drain_checked().await
                };
                let mut next_poll = interval;
                if matches!(result, Err(DrainFailure::SyncBusy))
                    && busy_since
                        .get_or_insert_with(tokio::time::Instant::now)
                        .elapsed()
                        < busy_budget
                {
                    next_poll =
                        interval.min(busy_budget.saturating_sub(busy_since.unwrap().elapsed()));
                } else if let Err(error) = result {
                    if let Some(owner) = owner.as_mut() {
                        owner.fail(error.class());
                    }
                    // Keep the original failure visible even if cleanup also fails.
                    let cleanup = self.store.release_outbox_component().await;
                    tracing::error!(
                        stage = "outbox-completion-or-storage",
                        class = error.class(),
                        cleanup_failed = cleanup.is_err(),
                        outcome = "retained-without-resend"
                    );
                    return Err(crate::storage::ReliabilityError::Malformed);
                } else {
                    busy_since = None;
                }
                tokio::select! {
                    biased;
                    changed = receiver.changed() => {
                        let _ = changed;
                        let result = self.store.release_outbox_component().await;
                        if result.is_err() {
                            tracing::error!(
                                stage = "outbox-cleanup",
                                reason = "component-lease-release-refused",
                                joined = true,
                                committed = false,
                                "outbox joined but its exact lease could not be released"
                            );
                        }
                        return result;
                    }
                    _ = tokio::time::sleep(next_poll) => {}
                }
            }
        });
        OutboxDrainHandle { shutdown, join }
    }
}

fn before_send_failure(error: crate::storage::ReliabilityError) -> DrainFailure {
    match error {
        crate::storage::ReliabilityError::Sql(error)
            if crate::storage::recovery_eligibility::matrix_effect_busy(&error) =>
        {
            DrainFailure::SyncBusy
        }
        _ => DrainFailure::Storage,
    }
}

#[cfg(test)]
#[path = "outbox_sync_tests.rs"]
mod sync_tests;

pub struct OutboxDrainHandle {
    shutdown: watch::Sender<bool>,
    join: tokio::task::JoinHandle<Result<(), crate::storage::ReliabilityError>>,
}

impl OutboxDrainHandle {
    pub async fn shutdown(
        self,
    ) -> Result<Result<(), crate::storage::ReliabilityError>, tokio::task::JoinError> {
        let _ = self.shutdown.send(true);
        let mut join = self.join;
        match tokio::time::timeout(SEND_TIMEOUT + std::time::Duration::from_secs(1), &mut join)
            .await
        {
            Ok(result) => result,
            Err(_) => {
                join.abort();
                join.await
            }
        }
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    #[tokio::test]
    async fn release_refusal_is_not_a_successful_cleanup() {
        let (shutdown, _) = watch::channel(false);
        let handle = OutboxDrainHandle {
            shutdown,
            join: tokio::spawn(async { Err(crate::storage::ReliabilityError::Malformed) }),
        };
        assert!(matches!(
            handle.shutdown().await,
            Ok(Err(crate::storage::ReliabilityError::Malformed))
        ));
    }
}
