use matrix_sdk::ruma::{OwnedEventId, OwnedRoomId, TransactionId};
use serde_json::{Value, json};

use crate::observer::{MAX_BODY_BYTES, MonitorSender, ObserverFuture, ObserverMessage};

use super::{MatrixClient, MatrixSender, ReplyContext, ReplyFuture};

pub trait MatrixOutboxSender: Send + Sync {
    fn send_stable<'a>(
        &'a self,
        room_id: &'a str,
        thread_root: Option<&'a str>,
        reply_event_id: Option<&'a str>,
        body: &'a str,
        txn_id: &'a str,
    ) -> ReplyFuture<'a>;
}

/// Concrete matrix-sdk sender. SDK types remain private to this adapter module.
#[derive(Clone)]
pub struct SdkMatrixSender {
    client: MatrixClient,
}

impl SdkMatrixSender {
    pub fn new(client: MatrixClient) -> Self {
        Self { client }
    }

    async fn send(&self, room_id: &str, content: Value) -> Result<(), ()> {
        let room_id: OwnedRoomId = room_id.parse().map_err(|_| ())?;
        let room = self.client.inner.get_room(&room_id).ok_or(())?;
        room.send_raw("m.room.message", content)
            .await
            .map_err(|_| ())?;
        Ok(())
    }
}

impl MatrixSender for SdkMatrixSender {
    fn send_reply<'a>(&'a self, context: &'a ReplyContext, body: &'a str) -> ReplyFuture<'a> {
        Box::pin(async move {
            let event_id: OwnedEventId = context.event_id.parse().map_err(|_| super::ReplyError)?;
            let relation = match &context.thread_root {
                Some(root) => {
                    let root: OwnedEventId = root.parse().map_err(|_| super::ReplyError)?;
                    json!({
                        "rel_type": "m.thread",
                        "event_id": root,
                        "is_falling_back": false,
                        "m.in_reply_to": {"event_id": event_id}
                    })
                }
                None => json!({"m.in_reply_to": {"event_id": event_id}}),
            };
            let content = json!({
                "msgtype": "m.text",
                "body": cap(body),
                "m.relates_to": relation
            });
            self.send(&context.room_id, content)
                .await
                .map_err(|_| super::ReplyError)
        })
    }
}

impl MatrixOutboxSender for SdkMatrixSender {
    fn send_stable<'a>(
        &'a self,
        room_id: &'a str,
        thread_root: Option<&'a str>,
        reply_event_id: Option<&'a str>,
        body: &'a str,
        txn_id: &'a str,
    ) -> ReplyFuture<'a> {
        Box::pin(async move {
            let room_id: OwnedRoomId = room_id.parse().map_err(|_| super::ReplyError)?;
            let txn_id: &TransactionId = txn_id.into();
            let room = self
                .client
                .inner
                .get_room(&room_id)
                .ok_or(super::ReplyError)?;
            let relation = match (thread_root, reply_event_id) {
                (Some(root), Some(reply)) => Some(
                    json!({"rel_type":"m.thread","event_id":root,"is_falling_back":false,"m.in_reply_to":{"event_id":reply}}),
                ),
                (None, Some(reply)) => Some(json!({"m.in_reply_to":{"event_id":reply}})),
                _ => None,
            };
            let mut content = json!({"msgtype":"m.text","body":cap(body)});
            if let Some(relation) = relation {
                content["m.relates_to"] = relation;
            }
            room.send_raw("m.room.message", content)
                .with_transaction_id(txn_id)
                .await
                .map_err(|_| super::ReplyError)?;
            Ok(())
        })
    }
}

impl MonitorSender for SdkMatrixSender {
    fn send<'a>(&'a self, room_id: &'a str, message: &'a ObserverMessage) -> ObserverFuture<'a> {
        Box::pin(async move {
            self.send(
                room_id,
                json!({"msgtype": "m.text", "body": cap(&message.body)}),
            )
            .await
            .map_err(|_| crate::observer::ObserverSendError)
        })
    }
}

fn cap(value: &str) -> &str {
    if value.len() <= MAX_BODY_BYTES {
        return value;
    }
    let mut end = MAX_BODY_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_is_bounded_and_utf8_safe() {
        let body = "x".repeat(MAX_BODY_BYTES + 1);
        assert_eq!(cap(&body).len(), MAX_BODY_BYTES);
        let unicode = "界".repeat(MAX_BODY_BYTES);
        assert!(cap(&unicode).len() <= MAX_BODY_BYTES);
    }
}
