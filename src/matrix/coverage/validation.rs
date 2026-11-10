use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
pub(super) fn digest(value: &Value) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(|_| refuse("coverage-json"))?)
    ))
}
impl Coverage {
    pub fn validate(&self) -> Result<()> {
        self.bounds()?;
        if self.status == "published" {
            return if self.target.is_none()
                && self.pages.is_empty()
                && self.final_response.as_ref().is_some_and(|s| !s.is_empty())
                && self.response_digest.is_some()
            {
                Ok(())
            } else {
                Err(refuse("coverage-tombstone"))
            };
        }
        let Some(target) = &self.target else {
            return if matches!(self.status.as_str(), "initializing" | "refused")
                && self.pages.is_empty()
            {
                Ok(())
            } else {
                Err(refuse("coverage-target"))
            };
        };
        if target.next.is_empty() || target.rooms.iter().filter(|r| r.anchor.is_some()).count() > 8
        {
            return Err(refuse("coverage-room-limit"));
        }
        let mut rooms = BTreeSet::new();
        let mut ids = BTreeSet::new();
        for room in &target.rooms {
            if room.id.is_empty()
                || room.anchor.as_ref().is_some_and(|a| {
                    a.room != room.id
                        || a.event.is_empty()
                        || a.digest.is_empty()
                        || a.batch.is_empty()
                })
            {
                return Err(refuse("coverage-room-anchor"));
            }
            if !rooms.insert(&room.id) {
                return Err(refuse("coverage-duplicate-room"));
            }
            validate_room(room, &self.pages, &mut ids)?;
        }
        if self.pages.iter().any(|p| !rooms.contains(&p.room)) || ids.len() > MAX_EVENTS {
            return Err(refuse("coverage-event-limit"));
        }
        if matches!(self.status.as_str(), "ready" | "published")
            && target.rooms.iter().any(|r| !r.complete)
        {
            return Err(refuse("coverage-incomplete"));
        }
        Ok(())
    }
    fn bounds(&self) -> Result<()> {
        if !matches!(
            self.status.as_str(),
            "initializing" | "collecting" | "ready" | "refused" | "published"
        ) {
            return Err(refuse("coverage-status"));
        }
        if self.format != 1
            || self.revision < 0
            || self.deadline_ms.checked_sub(self.created_ms) != Some(1_800_000)
            || !(0..=1_800_000).contains(&self.elapsed_ms)
            || self.clock.utc_ms < self.created_ms
            || self.segments == 0
            || self.segments > 16
            || self.attempts > 180
            || self.sync_attempts > 2
            || self.publications > 3
            || self.segment_pages > 12
            || self.segment_events > 1200
            || self.segment_bytes > 4 * 1024 * 1024
            || self.segment_attempts > 15
            || self.pages.len() > 128
            || self.pages.len() > self.attempts
            || self.segment_pages > self.segment_attempts
        {
            return Err(refuse("coverage-bounds"));
        }
        self.encoded()?;
        Ok(())
    }
}
fn validate_room(room: &Room, pages: &[Page], ids: &mut BTreeSet<String>) -> Result<()> {
    let mut cursor = room.origin.clone();
    let mut complete = room.anchor.is_none();
    let mut cursors = BTreeSet::new();
    for page in pages.iter().filter(|p| p.room == room.id) {
        if complete
            || cursor.as_ref() != Some(&page.start)
            || !cursors.insert(&page.start)
            || page.events.len() > 100
            || page.end.as_ref() == Some(&page.start)
            || page.end.as_ref().is_some_and(|e| cursors.contains(e))
        {
            return Err(refuse("coverage-page-chain"));
        }
        if let Some(value) = &page.anchor {
            let a = room
                .anchor
                .as_ref()
                .ok_or_else(|| refuse("coverage-anchor"))?;
            if value.get("event_id").and_then(Value::as_str) != Some(&a.event)
                || digest(value)? != a.digest
            {
                return Err(refuse("coverage-anchor-digest"));
            }
            complete = true;
        } else if page.end.as_deref().is_none_or(str::is_empty) {
            return Err(refuse("coverage-page-end"));
        }
        cursor = page.end.clone();
        for value in &page.events {
            insert(ids, value)?;
        }
    }
    if complete != room.complete || cursor != room.cursor {
        return Err(refuse("coverage-frontier"));
    }
    for value in &room.initial {
        insert(ids, value)?;
    }
    Ok(())
}

fn insert(ids: &mut BTreeSet<String>, value: &Value) -> Result<()> {
    let id = value
        .get("event_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| refuse("coverage-event-id"))?;
    if !ids.insert(id.into()) {
        return Err(refuse("coverage-duplicate-event"));
    }
    Ok(())
}
