use super::*;
use crate::matrix::{MatrixClient, response_identity};
use matrix_sdk::{config::SyncSettings, room::MessagesOptions};
use sqlx::SqlitePool;
use std::time::Duration;

pub(super) async fn target(
    client: &MatrixClient,
    pool: &SqlitePool,
    state: &State,
) -> Result<Target> {
    let response = client
        .inner
        .sync_once(
            SyncSettings::new()
                .token(&state.watermark)
                .full_state(true)
                .timeout(Duration::from_secs(1)),
        )
        .await
        .map_err(|_| refuse("coverage-sync-transport"))?;
    let mut rooms = Vec::new();
    for (id, update) in response.rooms.joined {
        let anchor = if update.timeline.limited {
            let row:Option<(String,String,String)>=sqlx::query_as("SELECT r.event_id,r.payload_digest,r.batch_id FROM matrix_recovery_receipts r JOIN matrix_recovery_batches b ON b.batch_id=r.batch_id WHERE r.account_id=? AND r.room_id=? AND r.cutover_operation_id=? AND r.state='dispositioned' AND b.state='terminal' ORDER BY b.updated_at DESC,r.created_at DESC,r.event_id DESC LIMIT 1")
                .bind(&client.user_id).bind(id.as_str()).bind(&state.operation).fetch_optional(pool).await?;
            let (event, digest, batch) = row.ok_or_else(|| refuse("coverage-anchor-absent"))?;
            Some(Anchor {
                room: id.to_string(),
                event,
                digest,
                batch,
            })
        } else {
            None
        };
        let cursor = if anchor.is_some() {
            Some(
                update
                    .timeline
                    .prev_batch
                    .ok_or_else(|| refuse("coverage-cursor-absent"))?,
            )
        } else {
            None
        };
        let initial = update
            .timeline
            .events
            .iter()
            .map(|e| {
                response_identity::canonical(e.kind.raw().json().get())
                    .map_err(|_| refuse("coverage-event-malformed"))
            })
            .collect::<Result<Vec<_>>>()?;
        rooms.push(Room {
            id: id.to_string(),
            initial,
            complete: anchor.is_none(),
            anchor,
            origin: cursor.clone(),
            cursor,
        });
    }
    rooms.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(Target {
        next: response.next_batch,
        rooms,
    })
}
pub(super) async fn page(client: &MatrixClient, room: &str, cursor: &str) -> Result<Page> {
    let id: matrix_sdk::ruma::OwnedRoomId = room.parse().map_err(|_| refuse("coverage-room-id"))?;
    let room = client
        .inner
        .get_room(&id)
        .ok_or_else(|| refuse("coverage-room-absent"))?;
    let mut options = MessagesOptions::backward().from(cursor);
    options.limit = 100u16.into();
    let page = tokio::time::timeout(Duration::from_secs(6), room.messages(options))
        .await
        .map_err(|_| refuse("coverage-page-timeout"))?
        .map_err(|_| refuse("coverage-page-transport"))?;
    if page.start != cursor {
        return Err(refuse("coverage-page-start"));
    }
    let events = page
        .chunk
        .iter()
        .map(|e| {
            response_identity::canonical(e.kind.raw().json().get())
                .map_err(|_| refuse("coverage-event-malformed"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Page {
        room: id.to_string(),
        start: page.start,
        end: page.end,
        events,
        anchor: None,
    })
}
