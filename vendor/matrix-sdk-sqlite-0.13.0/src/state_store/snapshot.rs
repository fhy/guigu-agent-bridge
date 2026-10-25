use super::*;
use matrix_sdk_base::store::StateSnapshot;
use ruma::events::AnyStrippedStateEvent;

pub(super) fn invalid() -> Error {
    refused("incomplete or unsupported fields")
}

pub(super) fn refused(stage: &str) -> Error {
    Error::InvalidData {
        details: format!("state snapshot refused: {stage}"),
    }
}

pub(super) fn room_for(
    store: &SqliteStateStore,
    snapshot: &StateSnapshot,
    table: &str,
    encoded: &[u8],
) -> Result<OwnedRoomId> {
    snapshot
        .changes
        .room_infos
        .keys()
        .find(|room| store.encode_key(table, room).as_ref() == encoded)
        .cloned()
        .ok_or_else(invalid)
}

impl SqliteStateStore {
    pub(super) async fn export_snapshot(&self) -> Result<StateSnapshot> {
        let store = self.clone();
        self.acquire()
            .await?
            .with_transaction(move |txn| {
                let version: Vec<u8> =
                    txn.query_row("SELECT value FROM kv WHERE key='version'", [], |row| {
                        row.get(0)
                    })?;
                if version != [DATABASE_VERSION] {
                    return Err(invalid());
                }
                let mut snapshot = StateSnapshot::default();
                store
                    .snapshot_rooms(txn, &mut snapshot)
                    .map_err(|_| refused("room info"))?;
                store
                    .snapshot_state(txn, &mut snapshot)
                    .map_err(|_| refused("room state"))?;
                store
                    .snapshot_account_data(txn, &mut snapshot)
                    .map_err(|_| refused("account data"))?;
                store.snapshot_auxiliary(txn, &mut snapshot)?;
                if snapshot
                    .changes
                    .sync_token
                    .as_deref()
                    .is_none_or(str::is_empty)
                {
                    return Err(invalid());
                }
                Ok(snapshot)
            })
            .await
    }

    fn snapshot_rooms(&self, txn: &Transaction<'_>, snapshot: &mut StateSnapshot) -> Result<()> {
        let mut statement = txn.prepare("SELECT room_id, state, data FROM room_info")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let room: RoomInfo = self.deserialize_json(&row.get::<_, Vec<u8>>(2)?)?;
            let encoded_room: Vec<u8> = row.get(0)?;
            let encoded_state: Vec<u8> = row.get(1)?;
            if self.encode_key(keys::ROOM_INFO, room.room_id()).as_ref() != encoded_room
                || self
                    .encode_key(keys::ROOM_INFO, serde_json::to_string(&room.state())?)
                    .as_ref()
                    != encoded_state
            {
                return Err(invalid());
            }
            if snapshot
                .changes
                .room_infos
                .insert(room.room_id().to_owned(), room)
                .is_some()
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    fn snapshot_state(&self, txn: &Transaction<'_>, snapshot: &mut StateSnapshot) -> Result<()> {
        let mut statement =
            txn.prepare("SELECT room_id, event_type, state_key, stripped, data FROM state_event")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let room = room_for(
                self,
                snapshot,
                keys::STATE_EVENT,
                &row.get::<_, Vec<u8>>(0)?,
            )?;
            let raw: Raw<serde_json::Value> = self.deserialize_json(&row.get::<_, Vec<u8>>(4)?)?;
            let event_type: StateEventType = raw.get_field("type")?.ok_or_else(invalid)?;
            let state_key: String = raw.get_field("state_key")?.ok_or_else(invalid)?;
            if self
                .encode_key(keys::STATE_EVENT, event_type.to_string())
                .as_ref()
                != row.get::<_, Vec<u8>>(1)?
                || self.encode_key(keys::STATE_EVENT, &state_key).as_ref()
                    != row.get::<_, Vec<u8>>(2)?
            {
                return Err(invalid());
            }
            if row.get::<_, bool>(3)? {
                let raw = raw.cast::<AnyStrippedStateEvent>();
                raw.deserialize()?;
                snapshot
                    .changes
                    .stripped_state
                    .entry(room)
                    .or_default()
                    .entry(event_type)
                    .or_default()
                    .insert(state_key, raw);
            } else {
                let raw = raw.cast::<AnySyncStateEvent>();
                raw.deserialize()?;
                snapshot
                    .changes
                    .state
                    .entry(room)
                    .or_default()
                    .entry(event_type)
                    .or_default()
                    .insert(state_key, raw);
            }
        }
        Ok(())
    }

    fn snapshot_account_data(
        &self,
        txn: &Transaction<'_>,
        snapshot: &mut StateSnapshot,
    ) -> Result<()> {
        let mut statement = txn.prepare("SELECT event_type, data FROM global_account_data")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let raw: Raw<AnyGlobalAccountDataEvent> =
                self.deserialize_json(&row.get::<_, Vec<u8>>(1)?)?;
            let event_type = raw.deserialize()?.event_type();
            if self
                .encode_key(keys::GLOBAL_ACCOUNT_DATA, event_type.to_string())
                .as_ref()
                != row.get::<_, Vec<u8>>(0)?
            {
                return Err(invalid());
            }
            snapshot.changes.account_data.insert(event_type, raw);
        }
        let mut statement =
            txn.prepare("SELECT room_id, event_type, data FROM room_account_data")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let room = room_for(
                self,
                snapshot,
                keys::ROOM_ACCOUNT_DATA,
                &row.get::<_, Vec<u8>>(0)?,
            )?;
            let raw: Raw<AnyRoomAccountDataEvent> =
                self.deserialize_json(&row.get::<_, Vec<u8>>(2)?)?;
            let event_type = raw.deserialize()?.event_type();
            if self
                .encode_key(keys::ROOM_ACCOUNT_DATA, event_type.to_string())
                .as_ref()
                != row.get::<_, Vec<u8>>(1)?
            {
                return Err(invalid());
            }
            snapshot
                .changes
                .room_account_data
                .entry(room)
                .or_default()
                .insert(event_type, raw);
        }
        Ok(())
    }
}
