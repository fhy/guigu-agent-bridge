use std::{collections::VecDeque, sync::Arc};

use crate::{matrix::MatrixOutboxSender, storage::ReliabilityStore};
use tokio::sync::{Mutex, watch};

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
        let mut pending = self.pending.lock().await;
        let now = chrono::Utc::now();
        let stale = (now - chrono::Duration::minutes(5))
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let now = now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        self.store
            .renew_outbox_component(&now)
            .await
            .map_err(|_| ())?;
        if pending.is_empty() {
            let owner = uuid::Uuid::now_v7().to_string();
            let rows = self
                .store
                .claim_projections(&owner, &now, &stale, self.batch)
                .await
                .map_err(|_| ())?;
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
                return Err(());
            }
            if matches!(next.sent, SendState::Prepared) {
                if !self
                    .store
                    .authorize_projection_send(row)
                    .await
                    .map_err(|_| ())?
                {
                    return Err(());
                }
                next.sent = SendState::Started;
                let outcome = self
                    .sender
                    .send_stable(
                        &row.room_id,
                        row.thread_root.as_deref(),
                        row.reply_event_id.as_deref(),
                        &row.body,
                        &row.stable_txn_id,
                    )
                    .await;
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
                    return Err(());
                }
            }
        }
        Ok(sent)
    }

    pub fn start(self, interval: std::time::Duration) -> OutboxDrainHandle {
        let (shutdown, mut receiver) = watch::channel(false);
        let join = tokio::spawn(async move {
            loop {
                let _ = self.drain_once().await;
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
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });
        OutboxDrainHandle { shutdown, join }
    }
}

pub struct OutboxDrainHandle {
    shutdown: watch::Sender<bool>,
    join: tokio::task::JoinHandle<Result<(), crate::storage::ReliabilityError>>,
}

impl OutboxDrainHandle {
    pub async fn shutdown(
        self,
    ) -> Result<Result<(), crate::storage::ReliabilityError>, tokio::task::JoinError> {
        let _ = self.shutdown.send(true);
        self.join.await
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
