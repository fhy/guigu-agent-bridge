use super::*;
use matrix_sdk::sync::SyncResponse;
use serde_json::Value;

pub(super) fn canonical(raw: &str) -> Result<Value, MatrixError> {
    let mut value: Value = serde_json::from_str(raw).map_err(|_| MatrixError::Protocol {
        detail: "malformed timeline event",
    })?;
    if let Some(object) = value.as_object_mut() {
        object.remove("unsigned");
    }
    Ok(value)
}

impl MatrixSync {
    pub(super) async fn process_batch(
        &self,
        response: SyncResponse,
        token: Option<&str>,
        events: &mpsc::Sender<InboundMatrixEvent>,
        dispositions: &mut Option<mpsc::Receiver<Disposition>>,
    ) -> Result<usize, MatrixError> {
        if response.next_batch.is_empty()
            || response
                .rooms
                .joined
                .values()
                .any(|room| room.timeline.limited)
        {
            return Err(MatrixError::CutoverRefused);
        }
        let batch_id = Uuid::now_v7().to_string();
        if let Some(recovery) = &self.recovery {
            self.begin_response(recovery, &batch_id, token, &response)
                .await?;
        }
        let mut delivered = 0;
        for (room_id, update) in response.rooms.joined {
            for timeline in update.timeline.events {
                let raw = timeline.kind.raw().json().get();
                delivered += self
                    .process_recovery_event(raw, room_id.as_str(), &batch_id, events, dispositions)
                    .await?;
            }
        }
        if let Some(recovery) = &self.recovery {
            recovery
                .store
                .complete_batch(
                    &batch_id,
                    &response.next_batch,
                    &recovery.owner_token,
                    recovery.owner_epoch,
                    &timestamp(),
                )
                .await
                .map_err(|_| MatrixError::Storage)?;
        } else {
            self.tokens.save(&response.next_batch).await?;
        }
        self.first_sync.store(false, Ordering::Release);
        Ok(delivered)
    }

    async fn begin_response(
        &self,
        recovery: &RecoveryBinding,
        batch_id: &str,
        token: Option<&str>,
        response: &SyncResponse,
    ) -> Result<(), MatrixError> {
        let mut expected = Vec::new();
        let mut payloads = Vec::new();
        for (room_id, update) in &response.rooms.joined {
            for event in &update.timeline.events {
                let value = canonical(event.kind.raw().json().get())?;
                let identity = value
                    .get("event_id")
                    .and_then(Value::as_str)
                    .filter(|identity| !identity.is_empty())
                    .ok_or(MatrixError::Protocol {
                        detail: "missing timeline identity",
                    })?;
                if expected.iter().any(|existing| existing == identity) {
                    return Err(MatrixError::Protocol {
                        detail: "duplicate response identity",
                    });
                }
                expected.push(identity.to_owned());
                payloads.push((room_id.as_str().to_owned(), value));
            }
        }
        expected.sort();
        payloads.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.to_string().cmp(&right.1.to_string()))
        });
        let bytes = serde_json::to_vec(&(token, &response.next_batch, payloads))
            .map_err(|_| MatrixError::Storage)?;
        let digest = format!("{:x}", Sha256::digest(bytes));
        recovery
            .store
            .begin_batch(
                batch_id,
                token,
                &response.next_batch,
                &digest,
                token.unwrap_or_default(),
                &recovery.owner_token,
                recovery.owner_epoch,
                &expected,
                &timestamp(),
            )
            .await
            .map_err(|_| MatrixError::Storage)
    }

    async fn process_recovery_event(
        &self,
        raw: &str,
        room: &str,
        batch_id: &str,
        events: &mpsc::Sender<InboundMatrixEvent>,
        dispositions: &mut Option<mpsc::Receiver<Disposition>>,
    ) -> Result<usize, MatrixError> {
        let value = canonical(raw)?;
        let identity = value.get("event_id").and_then(Value::as_str);
        if let Some(recovery) = &self.recovery {
            let identity = identity.ok_or(MatrixError::Protocol {
                detail: "missing event identity",
            })?;
            let digest = format!("{:x}", Sha256::digest(value.to_string().as_bytes()));
            if recovery
                .store
                .reserve_event(
                    &recovery.account_id,
                    room,
                    identity,
                    &recovery.operation_id,
                    &digest,
                    batch_id,
                    &timestamp(),
                )
                .await
                .map_err(|_| MatrixError::Storage)?
                == BatchAdmission::Duplicate
            {
                return Ok(0);
            }
        }
        self.check_decryption(&value, room, identity, batch_id)
            .await?;
        if let (Some(recovery), Some(identity)) = (&self.recovery, identity) {
            recovery
                .store
                .start_event(
                    &recovery.account_id,
                    room,
                    identity,
                    &recovery.operation_id,
                    batch_id,
                    &timestamp(),
                )
                .await
                .map_err(|_| MatrixError::Storage)?;
        }
        let delivered = self.deliver(raw, room, events, dispositions).await?;
        if let (Some(recovery), Some(identity)) = (&self.recovery, identity) {
            recovery
                .store
                .finish_event(
                    &recovery.account_id,
                    room,
                    identity,
                    &recovery.operation_id,
                    batch_id,
                    &recovery.owner_token,
                    recovery.owner_epoch,
                    &timestamp(),
                )
                .await
                .map_err(|_| MatrixError::Storage)?;
        }
        Ok(delivered)
    }

    async fn check_decryption(
        &self,
        value: &Value,
        room: &str,
        identity: Option<&str>,
        batch_id: &str,
    ) -> Result<(), MatrixError> {
        if value.get("type").and_then(Value::as_str) == Some("m.room.encrypted") {
            if let Some(observer) = &self.missing_key_observer {
                observer.record_missing_room_key();
            }
            if let (Some(recovery), Some(identity)) = (&self.recovery, identity) {
                recovery
                    .store
                    .refuse_event(
                        &recovery.account_id,
                        room,
                        identity,
                        &recovery.operation_id,
                        batch_id,
                        &recovery.owner_token,
                        recovery.owner_epoch,
                        "missing-room-key-unrecoverable",
                        &timestamp(),
                    )
                    .await
                    .map_err(|_| MatrixError::Storage)?;
            }
            return Err(MatrixError::CryptoInitialization);
        }
        Ok(())
    }

    async fn deliver(
        &self,
        raw: &str,
        room: &str,
        events: &mpsc::Sender<InboundMatrixEvent>,
        dispositions: &mut Option<mpsc::Receiver<Disposition>>,
    ) -> Result<usize, MatrixError> {
        if let Some(consumer) = &self.raw_consumer
            && consumer.consume(raw, room).await?
        {
            consumer.admit(raw, room).await?;
            return Ok(1);
        }
        if let Some(event) = decode_event(raw, room, &self.client.user_id)? {
            events.try_send(event).map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => MatrixError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => MatrixError::ConsumerClosed,
            })?;
            if let Some(dispositions) = dispositions {
                dispositions
                    .recv()
                    .await
                    .ok_or(MatrixError::ConsumerClosed)??;
            }
            return Ok(1);
        }
        Ok(0)
    }
}
