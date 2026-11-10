//! Fixed-target coverage evidence; never task execution authority.
use super::rebuild_gap::Anchor;
use crate::native_succession::state::{State, refuse};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) mod budget;
mod run;
mod sdk;
#[cfg(test)]
mod tests;
mod validation;
pub(crate) use budget::Clock;
pub(crate) use run::recover;
pub(crate) type Result<T> = std::result::Result<T, sqlx::Error>;
pub(crate) const MAX_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_EVENTS: usize = 12_800;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Room {
    pub id: String,
    pub initial: Vec<Value>,
    pub anchor: Option<Anchor>,
    pub origin: Option<String>,
    pub cursor: Option<String>,
    pub complete: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Target {
    pub next: String,
    pub rooms: Vec<Room>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Page {
    pub room: String,
    pub start: String,
    pub end: Option<String>,
    pub events: Vec<Value>,
    pub anchor: Option<Value>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Coverage {
    pub format: u32,
    pub revision: i64,
    pub state: State,
    pub pairing: String,
    pub status: String,
    pub target: Option<Target>,
    pub pages: Vec<Page>,
    pub batch_id: String,
    pub response_digest: Option<String>,
    pub final_response: Option<String>,
    pub clock: Clock,
    pub created_ms: i64,
    pub elapsed_ms: i64,
    pub deadline_ms: i64,
    pub segment_start: Clock,
    pub segment_pages: usize,
    pub segment_events: usize,
    pub segment_bytes: usize,
    pub segment_attempts: usize,
    pub segments: usize,
    pub attempts: usize,
    pub sync_attempts: usize,
    pub publications: usize,
    pub reservation: Option<(String, String)>,
    pub refusal: Option<String>,
}

impl Coverage {
    pub fn new(state: &State, pairing: String, now: Clock) -> Result<Self> {
        let deadline_ms = now
            .utc_ms
            .checked_add(1_800_000)
            .ok_or_else(|| refuse("coverage-clock"))?;
        Ok(Self {
            format: 1,
            revision: 0,
            state: state.clone(),
            pairing,
            status: "initializing".into(),
            target: None,
            pages: vec![],
            batch_id: uuid::Uuid::now_v7().to_string(),
            response_digest: None,
            final_response: None,
            created_ms: now.utc_ms,
            elapsed_ms: 0,
            deadline_ms,
            clock: now.clone(),
            segment_start: now,
            segment_pages: 0,
            segment_events: 0,
            segment_bytes: 0,
            segment_attempts: 0,
            segments: 1,
            attempts: 0,
            sync_attempts: 0,
            publications: 0,
            reservation: None,
            refusal: None,
        })
    }
    /// Count every encoded column, including JSON string escaping and row metadata.
    pub fn encoded(&self) -> Result<String> {
        let document = serde_json::to_string(self).map_err(|_| refuse("coverage-encoding"))?;
        let row = serde_json::to_vec(&(1, self.revision, &self.state.owner.token, &document))
            .map_err(|_| refuse("coverage-encoding"))?;
        if row.len() > MAX_BYTES {
            return Err(refuse("coverage-row-bytes"));
        }
        Ok(document)
    }
    pub fn freeze(&mut self, target: Target) -> Result<()> {
        if self.status != "initializing" || self.target.is_some() || self.sync_attempts == 0 {
            return Err(refuse("coverage-target-replacement"));
        }
        self.segment_events = target.rooms.iter().map(|r| r.initial.len()).sum();
        self.segment_bytes = serde_json::to_vec(&target)
            .map_err(|_| refuse("coverage-encoding"))?
            .len();
        self.target = Some(target);
        self.status = "collecting".into();
        self.validate()?;
        self.finish_if_covered();
        Ok(())
    }
    pub fn frontier(&self) -> Result<Option<(String, String)>> {
        let target = self
            .target
            .as_ref()
            .ok_or_else(|| refuse("coverage-target-absent"))?;
        target
            .rooms
            .iter()
            .find(|r| !r.complete)
            .map(|room| {
                Ok((
                    room.id.clone(),
                    room.cursor
                        .clone()
                        .ok_or_else(|| refuse("coverage-cursor"))?,
                ))
            })
            .transpose()
    }
    pub fn accept(&mut self, mut page: Page) -> Result<()> {
        if self.status != "collecting"
            || self.reservation.as_ref() != Some(&(page.room.clone(), page.start.clone()))
        {
            return Err(refuse("coverage-page-reservation"));
        }
        if page.events.len() > 100 {
            return Err(refuse("coverage-page-size"));
        }
        let room = self
            .target
            .as_mut()
            .and_then(|t| t.rooms.iter_mut().find(|r| r.id == page.room))
            .ok_or_else(|| refuse("coverage-room"))?;
        let anchor = room
            .anchor
            .as_ref()
            .ok_or_else(|| refuse("coverage-anchor"))?;
        if let Some(index) = page
            .events
            .iter()
            .position(|v| v.get("event_id").and_then(Value::as_str) == Some(&anchor.event))
        {
            let value = page.events[index].clone();
            if validation::digest(&value)? != anchor.digest {
                return Err(refuse("coverage-anchor-digest"));
            }
            page.events.truncate(index);
            page.anchor = Some(value);
            room.complete = true;
        } else if page.end.as_deref().is_none_or(str::is_empty) {
            return Err(refuse("coverage-page-end"));
        }
        if page.end.as_ref() == Some(&page.start)
            || self
                .pages
                .iter()
                .any(|p| p.room == page.room && page.end.as_ref() == Some(&p.start))
        {
            return Err(refuse("coverage-cursor-cycle"));
        }
        room.cursor = page.end.clone();
        self.segment_pages += 1;
        self.segment_events += page.events.len();
        self.segment_bytes = self
            .segment_bytes
            .checked_add(
                serde_json::to_vec(&page)
                    .map_err(|_| refuse("coverage-encoding"))?
                    .len(),
            )
            .ok_or_else(|| refuse("coverage-byte-overflow"))?;
        self.pages.push(page);
        self.reservation = None;
        self.validate()?;
        self.finish_if_covered();
        Ok(())
    }
    fn finish_if_covered(&mut self) {
        if self
            .target
            .as_ref()
            .is_some_and(|t| t.rooms.iter().all(|r| r.complete))
        {
            self.status = "ready".into();
        }
    }
    pub fn payloads(&self) -> Result<Vec<(String, Value)>> {
        if !matches!(self.status.as_str(), "ready" | "published") {
            return Err(refuse("coverage-incomplete"));
        }
        self.validate()?;
        let target = self
            .target
            .as_ref()
            .ok_or_else(|| refuse("coverage-target"))?;
        let mut result = Vec::new();
        for room in &target.rooms {
            for page in self.pages.iter().filter(|p| p.room == room.id).rev() {
                result.extend(
                    page.events
                        .iter()
                        .rev()
                        .cloned()
                        .map(|v| (room.id.clone(), v)),
                );
            }
            result.extend(room.initial.iter().cloned().map(|v| (room.id.clone(), v)));
        }
        Ok(result)
    }
}
