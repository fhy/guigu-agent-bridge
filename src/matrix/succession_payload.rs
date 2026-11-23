use serde_json::Value;
use std::time::Duration;

use super::{MatrixClient, MatrixError, event::decode_event, response_identity};
use crate::native_succession::state::Batch;

/// Only events that the normal adapter ignores can be completed without ingress.
/// Gateway envelopes and ciphertext never qualify even if ordinary decoding ignores them.
pub(crate) fn ignored(raw: &Value, room: &str, user: &str) -> Result<bool, MatrixError> {
    if raw.get("type").and_then(Value::as_str) == Some("m.room.encrypted")
        || raw.pointer("/content/msgtype").and_then(Value::as_str)
            == Some("com.guigu.bridge.a2a.v1")
    {
        return Ok(false);
    }
    Ok(decode_event(&raw.to_string(), room, user)?.is_none())
}

pub(crate) fn verify(batch: &Batch, payloads: &[(String, Value)]) -> Result<(), MatrixError> {
    let mut ids = Vec::new();
    for (_, value) in payloads {
        if value.get("type").and_then(Value::as_str) == Some("m.room.encrypted") {
            return Err(MatrixError::CutoverRefused);
        }
        let id = value
            .get("event_id")
            .and_then(Value::as_str)
            .ok_or(MatrixError::CutoverRefused)?;
        if ids.iter().any(|old| old == id) {
            return Err(MatrixError::CutoverRefused);
        }
        ids.push(id.to_owned());
    }
    ids.sort();
    let mut expected = batch.expected.clone();
    expected.sort();
    if ids != expected
        || response_identity::digest(batch.request.as_deref(), &batch.response, payloads.to_vec())?
            != batch.digest
    {
        return Err(MatrixError::CutoverRefused);
    }
    Ok(())
}

pub(crate) async fn fetch(
    client: &MatrixClient,
    batch: &Batch,
) -> Result<Vec<(String, Value)>, MatrixError> {
    let rooms = client.inner.joined_rooms();
    if rooms.len() > 32 {
        return Err(MatrixError::CutoverRefused);
    }
    let mut payloads = Vec::new();
    let mut bytes = 0usize;
    for id in &batch.expected {
        let id: matrix_sdk::ruma::OwnedEventId =
            id.parse().map_err(|_| MatrixError::CutoverRefused)?;
        let mut found = None;
        for room in &rooms {
            let request = matrix_sdk::config::RequestConfig::new()
                .retry_limit(0)
                .timeout(Duration::from_secs(5));
            if let Ok(Ok(event)) =
                tokio::time::timeout(Duration::from_secs(6), room.event(&id, Some(request))).await
            {
                if found.is_some() {
                    return Err(MatrixError::CutoverRefused);
                }
                let value = response_identity::canonical(event.kind.raw().json().get())?;
                found = Some((room.room_id().to_string(), value));
            }
        }
        let found = found.ok_or(MatrixError::CutoverRefused)?;
        bytes = bytes
            .checked_add(found.1.to_string().len())
            .ok_or(MatrixError::CutoverRefused)?;
        if bytes > 16 * 1024 * 1024 {
            return Err(MatrixError::CutoverRefused);
        }
        payloads.push(found);
    }
    verify(batch, &payloads)?;
    Ok(payloads)
}
