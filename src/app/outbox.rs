use std::sync::Arc;

use crate::{matrix::MatrixOutboxSender, storage::ReliabilityStore};
use tokio::sync::watch;

#[derive(Clone)]
pub struct OutboxDrain {
    store: ReliabilityStore,
    sender: Arc<dyn MatrixOutboxSender>,
    batch: u32,
}

impl OutboxDrain {
    pub fn new(store: ReliabilityStore, sender: Arc<dyn MatrixOutboxSender>, batch: u32) -> Self {
        assert!(batch > 0);
        Self {
            store,
            sender,
            batch,
        }
    }

    #[allow(clippy::result_unit_err)]
    pub async fn drain_once(&self) -> Result<usize, ()> {
        let now = chrono::Utc::now();
        let stale = now - chrono::Duration::minutes(5);
        let owner = uuid::Uuid::now_v7().to_string();
        let now = now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        self.store
            .renew_outbox_component(&now)
            .await
            .map_err(|_| ())?;
        let stale = stale.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let rows = self
            .store
            .claim_projections(&owner, &now, &stale, self.batch)
            .await
            .map_err(|_| ())?;
        let mut sent = 0;
        for row in rows {
            if !self
                .store
                .authorize_projection_send(&row)
                .await
                .map_err(|_| ())?
            {
                return Err(());
            }
            let send_result = self
                .sender
                .send_stable(
                    &row.room_id,
                    row.thread_root.as_deref(),
                    row.reply_event_id.as_deref(),
                    &row.body,
                    &row.stable_txn_id,
                )
                .await;
            if send_result.is_ok() {
                match self.store.mark_projection_sent(&row).await {
                    Ok(true) => sent += 1,
                    Ok(false) | Err(_) => {
                        if !self
                            .store
                            .mark_projection_abandoned(&row)
                            .await
                            .map_err(|_| ())?
                        {
                            return Err(());
                        }
                    }
                }
            } else {
                if !self
                    .store
                    .mark_projection_abandoned(&row)
                    .await
                    .map_err(|_| ())?
                {
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
