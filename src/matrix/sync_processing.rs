use super::{Disposition, InboundMatrixEvent, MatrixError, MatrixSync, ProcessResult};
use crate::{matrix::event::decode_event, storage::cutover_event_identity};
use matrix_sdk::sync::SyncResponse;
use tokio::sync::{mpsc, watch};

impl MatrixSync {
    pub(super) async fn process_response(
        &self,
        events: &mpsc::Sender<InboundMatrixEvent>,
        dispositions: &mut Option<mpsc::Receiver<Disposition>>,
        mut shutdown: Option<&mut watch::Receiver<bool>>,
        response: SyncResponse,
    ) -> Result<ProcessResult, MatrixError> {
        let state = if let Some(cutover) = &self.cutover {
            Some(cutover.store.state().await?)
        } else {
            None
        };
        let mut delivered = 0;
        for (room_id, update) in response.rooms.joined {
            for timeline in update.timeline.events {
                let raw = timeline.kind.raw().json().get();
                let receipt = if let Some(cutover) = &self.cutover {
                    let (identity, digest) = cutover_event_identity(raw)?;
                    if !cutover
                        .store
                        .begin_event(room_id.as_str(), &identity, &digest)
                        .await?
                    {
                        continue;
                    }
                    Some((identity, digest))
                } else {
                    None
                };
                match self
                    .process_timeline(raw, room_id.as_str(), events, dispositions, &mut shutdown)
                    .await?
                {
                    ProcessResult::Cancelled => return Ok(ProcessResult::Cancelled),
                    ProcessResult::Complete(count) => delivered += count,
                }
                if let (Some(cutover), Some((identity, digest))) = (&self.cutover, receipt) {
                    cutover
                        .store
                        .finish_event(room_id.as_str(), &identity, &digest)
                        .await?;
                }
            }
        }
        if let (Some(cutover), Some(state)) = (&self.cutover, state) {
            cutover
                .store
                .commit_batch(&state, &response.next_batch)
                .await?;
        } else {
            self.tokens.save(&response.next_batch).await?;
        }
        Ok(ProcessResult::Complete(delivered))
    }

    async fn process_timeline(
        &self,
        raw: &str,
        room: &str,
        events: &mpsc::Sender<InboundMatrixEvent>,
        dispositions: &mut Option<mpsc::Receiver<Disposition>>,
        shutdown: &mut Option<&mut watch::Receiver<bool>>,
    ) -> Result<ProcessResult, MatrixError> {
        if let Some(consumer) = &self.raw_consumer
            && consumer.consume(raw, room).await?
        {
            return Ok(ProcessResult::Complete(1));
        }
        let mut delivered = 0;
        match decode_event(raw, room, &self.client.user_id) {
            Ok(Some(event)) => {
                events.try_send(event).map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => MatrixError::Backpressure,
                    mpsc::error::TrySendError::Closed(_) => MatrixError::ConsumerClosed,
                })?;
                if let Some(dispositions) = dispositions {
                    let disposition = if let Some(shutdown) = shutdown.as_deref_mut() {
                        tokio::select! {
                            biased;
                            changed = shutdown.changed() => {
                                let _ = changed;
                                return Ok(ProcessResult::Cancelled);
                            }
                            disposition = dispositions.recv() => disposition,
                        }
                    } else {
                        dispositions.recv().await
                    };
                    disposition.ok_or(MatrixError::ConsumerClosed)??;
                }
                delivered = 1;
            }
            Ok(None) | Err(MatrixError::Protocol { .. }) => {}
            Err(error) => return Err(error),
        }
        let encrypted = serde_json::from_str::<serde_json::Value>(raw)
            .ok()
            .is_some_and(|event| {
                event.get("type").and_then(|value| value.as_str()) == Some("m.room.encrypted")
            });
        if encrypted {
            if let Some(observer) = &self.missing_key_observer {
                observer.record_missing_room_key();
            }
            return Err(MatrixError::CryptoInitialization);
        }
        Ok(ProcessResult::Complete(delivered))
    }
}
