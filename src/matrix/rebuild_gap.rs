//! Bounded recovery of a limited timeline to an authenticated committed overlap.
use super::{MatrixClient, MatrixError, response_identity};
use crate::{
    config::Config,
    native_succession::state::{Batch, State},
};
use matrix_sdk::{
    config::SyncSettings, deserialized_responses::TimelineEvent, room::MessagesOptions,
    sync::SyncResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use std::{collections::BTreeSet, time::Duration};

mod diagnostics;
pub(crate) use diagnostics::Failure;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Anchor {
    pub room: String,
    pub event: String,
    pub digest: String,
    pub batch: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Rebuild {
    pub batch: Batch,
    pub anchors: Vec<Anchor>,
    pub unrouted: Vec<String>,
}

pub(crate) async fn fill(
    client: &MatrixClient,
    pool: &SqlitePool,
    operation: &str,
    response: &mut SyncResponse,
) -> Result<Vec<Anchor>, Failure> {
    let mut anchors = Vec::new();
    for (id, update) in &mut response.rooms.joined {
        if !update.timeline.limited {
            continue;
        }
        if anchors.len() >= 8 {
            return Err(Failure::new("room-limit"));
        }
        let row: Option<(String,String,String)> = sqlx::query_as("SELECT r.event_id,r.payload_digest,r.batch_id FROM matrix_recovery_receipts r JOIN matrix_recovery_batches b ON b.batch_id=r.batch_id WHERE r.account_id=? AND r.room_id=? AND r.cutover_operation_id=? AND r.state='dispositioned' AND b.state='terminal' ORDER BY b.updated_at DESC,r.created_at DESC,r.event_id DESC LIMIT 1")
            .bind(&client.user_id).bind(id.as_str()).bind(operation).fetch_optional(pool).await
            .map_err(|_| Failure::storage("anchor-read"))?;
        let (event, digest, batch) = row.ok_or_else(|| Failure::new("anchor-absent"))?;
        let anchor = Anchor {
            room: id.to_string(),
            event,
            digest,
            batch,
        };
        let room = client
            .inner
            .get_room(id)
            .ok_or_else(|| Failure::new("room-absent"))?;
        let cursor = update
            .timeline
            .prev_batch
            .clone()
            .ok_or_else(|| Failure::new("cursor-absent"))?;
        let mut earlier = pages(&room, &anchor, cursor).await?;
        earlier.append(&mut update.timeline.events.clone());
        validate_events(&earlier)?;
        update.timeline.events = earlier;
        update.timeline.limited = false;
        anchors.push(anchor);
    }
    Ok(anchors)
}

async fn pages(
    room: &matrix_sdk::Room,
    anchor: &Anchor,
    mut cursor: String,
) -> Result<Vec<TimelineEvent>, Failure> {
    let mut earlier = Vec::new();
    let mut cursors = BTreeSet::new();
    for page_number in 1..=16 {
        let context = |failure: Failure| failure.at(page_number, earlier.len());
        if !cursors.insert(cursor.clone()) {
            return Err(context(Failure::new("cursor-repeat")));
        }
        let mut options = MessagesOptions::backward().from(cursor.as_str());
        options.limit = 100u16.into();
        let page = tokio::time::timeout(Duration::from_secs(6), room.messages(options))
            .await
            .map_err(|_| context(Failure::transport("page-timeout")))?
            .map_err(|_| context(Failure::transport("page-transport")))?;
        if page.start != cursor {
            return Err(context(Failure::new("page-start-mismatch")));
        }
        if append_page(anchor, &mut earlier, page.chunk)
            .map_err(|failure| failure.at(page_number, earlier.len()))?
        {
            earlier.reverse();
            return Ok(earlier);
        }
        cursor = page
            .end
            .ok_or_else(|| Failure::new("page-end-absent").at(page_number, earlier.len()))?;
    }
    Err(Failure::new("page-limit").at(16, earlier.len()))
}

fn append_page(
    anchor: &Anchor,
    events: &mut Vec<TimelineEvent>,
    page: Vec<TimelineEvent>,
) -> Result<bool, Failure> {
    for event in page {
        let value = response_identity::canonical(event.kind.raw().json().get())
            .map_err(|_| Failure::new("event-malformed"))?;
        if value.get("event_id").and_then(Value::as_str) == Some(&anchor.event) {
            if format!("{:x}", Sha256::digest(value.to_string().as_bytes())) != anchor.digest {
                return Err(Failure::new("anchor-digest-mismatch"));
            }
            return Ok(true);
        }
        if events.len() >= 1600 {
            return Err(Failure::new("event-limit"));
        }
        events.push(event);
    }
    Ok(false)
}

fn validate_events(events: &[TimelineEvent]) -> Result<(), Failure> {
    let mut ids = BTreeSet::new();
    let mut bytes = 0usize;
    for event in events {
        let raw = event.kind.raw().json().get();
        bytes = bytes
            .checked_add(raw.len())
            .ok_or_else(|| Failure::new("byte-overflow"))?;
        let value =
            response_identity::canonical(raw).map_err(|_| Failure::new("event-malformed"))?;
        let id = value
            .get("event_id")
            .and_then(Value::as_str)
            .ok_or_else(|| Failure::new("event-id-absent"))?;
        if bytes > 16 * 1024 * 1024 {
            return Err(Failure::new("byte-limit").bytes(bytes));
        }
        if !ids.insert(id.to_owned()) {
            return Err(Failure::new("event-duplicate").bytes(bytes));
        }
    }
    Ok(())
}

pub(crate) async fn fetch(
    client: &MatrixClient,
    pool: &SqlitePool,
    config: &Config,
    state: &State,
) -> Result<(Rebuild, Vec<(String, Value)>), MatrixError> {
    let mut response = client
        .inner
        .sync_once(
            SyncSettings::new()
                .token(&state.watermark)
                .full_state(true)
                .timeout(Duration::from_secs(1)),
        )
        .await
        .map_err(|_| MatrixError::Transport)?;
    if response.next_batch.is_empty() {
        return Err(MatrixError::CutoverRefused);
    }
    let anchors = fill(client, pool, &state.operation, &mut response)
        .await
        .map_err(Failure::reported)?;
    let mut payloads = Vec::new();
    let mut expected = Vec::new();
    let mut unrouted = Vec::new();
    for (room, update) in response.rooms.joined {
        validate_events(&update.timeline.events).map_err(Failure::reported)?;
        for event in update.timeline.events {
            let value = response_identity::canonical(event.kind.raw().json().get())?;
            let id = value
                .get("event_id")
                .and_then(Value::as_str)
                .ok_or(MatrixError::CutoverRefused)?
                .to_owned();
            if !super::succession_payload::ignored(&value, room.as_str(), &client.user_id)? {
                if !unrouted_no_effect(config, &value, room.as_str()) {
                    return Err(MatrixError::CutoverRefused);
                }
                unrouted.push(id.clone());
            }
            expected.push(id);
            payloads.push((room.to_string(), value));
        }
    }
    expected.sort();
    if expected.len() > 1600 || expected.windows(2).any(|w| w[0] == w[1]) {
        return Err(MatrixError::CutoverRefused);
    }
    let epoch = state
        .leases
        .iter()
        .find(|(c, o, _, _, _)| c == "MatrixSync" && o == &state.owner.token)
        .ok_or(MatrixError::CutoverRefused)?
        .2;
    let batch = Batch {
        id: uuid::Uuid::now_v7().to_string(),
        request: Some(state.watermark.clone()),
        previous: state.watermark.clone(),
        response: response.next_batch.clone(),
        digest: response_identity::digest(
            Some(&state.watermark),
            &response.next_batch,
            payloads.clone(),
        )?,
        owner: state.owner.token.clone(),
        epoch,
        expected,
        covered: Vec::new(),
    };
    Ok((
        Rebuild {
            batch,
            anchors,
            unrouted,
        },
        payloads,
    ))
}

pub(crate) fn unrouted_no_effect(config: &Config, raw: &Value, room: &str) -> bool {
    let matrix = &config.transports.matrix;
    let Some(body) = raw.pointer("/content/body").and_then(Value::as_str) else {
        return false;
    };
    raw.get("type").and_then(Value::as_str) == Some("m.room.message")
        && raw.pointer("/content/msgtype").and_then(Value::as_str) == Some("m.text")
        && !body.trim_start().starts_with('/')
        && !body
            .split_once(':')
            .is_some_and(|(p, _)| p.trim().starts_with('@'))
        && !matrix.routes.rooms.contains_key(room)
        && !matrix.routes.direct.contains_key(room)
        && matrix.monitor_room != room
        && !matrix.admin_rooms.iter().any(|r| r == room)
}

#[cfg(test)]
mod tests;
