use super::snapshot::{invalid, refused, room_for};
use super::*;
use matrix_sdk_base::store::{SnapshotKey, StateSnapshot};

impl SqliteStateStore {
    pub(super) fn snapshot_auxiliary(
        &self,
        txn: &Transaction<'_>,
        snapshot: &mut StateSnapshot,
    ) -> Result<()> {
        let users = self
            .snapshot_members(txn, snapshot)
            .map_err(|_| refused("membership"))?;
        self.snapshot_profiles(txn, snapshot, &users)
            .map_err(|_| refused("profiles"))?;
        self.snapshot_receipts(txn, snapshot)
            .map_err(|_| refused("receipts"))?;
        self.snapshot_names(txn, snapshot)
            .map_err(|_| refused("display names"))?;
        self.snapshot_kv(txn, snapshot, users)
            .map_err(|_| refused("key/value"))
    }

    fn snapshot_members(
        &self,
        txn: &Transaction<'_>,
        snapshot: &StateSnapshot,
    ) -> Result<BTreeSet<OwnedUserId>> {
        let mut users = BTreeSet::new();
        let mut statement =
            txn.prepare("SELECT room_id, user_id, membership, stripped, data FROM member")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let room = room_for(self, snapshot, keys::MEMBER, &row.get::<_, Vec<u8>>(0)?)?;
            let user: OwnedUserId = self.deserialize_value(&row.get::<_, Vec<u8>>(4)?)?;
            let membership = if row.get::<_, bool>(3)? {
                let raw = snapshot
                    .changes
                    .stripped_state
                    .get(&room)
                    .and_then(|types| types.get(&StateEventType::RoomMember))
                    .and_then(|members| members.get(user.as_str()))
                    .ok_or_else(invalid)?;
                raw.deserialize_as::<StrippedRoomMemberEvent>()?
                    .content
                    .membership
            } else {
                let raw = snapshot
                    .changes
                    .state
                    .get(&room)
                    .and_then(|types| types.get(&StateEventType::RoomMember))
                    .and_then(|members| members.get(user.as_str()))
                    .ok_or_else(invalid)?;
                raw.deserialize_as::<SyncRoomMemberEvent>()?
                    .membership()
                    .clone()
            };
            if self.encode_key(keys::MEMBER, &user).as_ref() != row.get::<_, Vec<u8>>(1)?
                || self.encode_key(keys::MEMBER, membership.as_str()).as_ref()
                    != row.get::<_, Vec<u8>>(2)?
            {
                return Err(invalid());
            }
            users.insert(user);
        }
        Ok(users)
    }

    fn snapshot_profiles(
        &self,
        txn: &Transaction<'_>,
        snapshot: &mut StateSnapshot,
        users: &BTreeSet<OwnedUserId>,
    ) -> Result<()> {
        let mut statement = txn.prepare("SELECT room_id, user_id, data FROM profile")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let room = room_for(self, snapshot, keys::PROFILE, &row.get::<_, Vec<u8>>(0)?)?;
            let encoded: Vec<u8> = row.get(1)?;
            let user = users
                .iter()
                .find(|user| self.encode_key(keys::PROFILE, user).as_ref() == encoded)
                .ok_or_else(invalid)?
                .clone();
            let profile: MinimalRoomMemberEvent =
                self.deserialize_json(&row.get::<_, Vec<u8>>(2)?)?;
            snapshot
                .changes
                .profiles
                .entry(room)
                .or_default()
                .insert(user, profile);
        }
        Ok(())
    }

    fn snapshot_receipts(&self, txn: &Transaction<'_>, snapshot: &mut StateSnapshot) -> Result<()> {
        let mut statement = txn.prepare(
            "SELECT room_id, user_id, receipt_type, thread, event_id, data FROM receipt",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let room = room_for(self, snapshot, keys::RECEIPT, &row.get::<_, Vec<u8>>(0)?)?;
            let data: ReceiptData = self.deserialize_json(&row.get::<_, Vec<u8>>(5)?)?;
            let encoded_type: Vec<u8> = row.get(2)?;
            let receipt_type = [ReceiptType::Read, ReceiptType::ReadPrivate]
                .into_iter()
                .find(|kind| self.encode_key(keys::RECEIPT, kind.as_str()).as_ref() == encoded_type)
                .ok_or_else(invalid)?;
            if self.encode_key(keys::RECEIPT, &data.user_id).as_ref() != row.get::<_, Vec<u8>>(1)?
                || self
                    .encode_key(
                        keys::RECEIPT,
                        rmp_serde::to_vec_named(&data.receipt.thread)?,
                    )
                    .as_ref()
                    != row.get::<_, Vec<u8>>(3)?
                || self.encode_key(keys::RECEIPT, &data.event_id).as_ref()
                    != row.get::<_, Vec<u8>>(4)?
            {
                return Err(invalid());
            }
            snapshot
                .changes
                .receipts
                .entry(room)
                .or_insert_with(|| ruma::events::receipt::ReceiptEventContent(BTreeMap::new()))
                .entry(data.event_id)
                .or_default()
                .entry(receipt_type)
                .or_default()
                .insert(data.user_id, data.receipt);
        }
        Ok(())
    }

    fn snapshot_names(&self, txn: &Transaction<'_>, snapshot: &mut StateSnapshot) -> Result<()> {
        let mut statement = txn.prepare("SELECT room_id, name, data FROM display_name")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let room = room_for(
                self,
                snapshot,
                keys::DISPLAY_NAME,
                &row.get::<_, Vec<u8>>(0)?,
            )?;
            let encoded: Vec<u8> = row.get(1)?;
            let users: BTreeSet<OwnedUserId> = self.deserialize_json(&row.get::<_, Vec<u8>>(2)?)?;
            let mut names: Vec<_> = snapshot
                .changes
                .profiles
                .get(&room)
                .into_iter()
                .flat_map(|profiles| profiles.values())
                .filter_map(|profile| {
                    profile
                        .as_original()
                        .and_then(|event| event.content.displayname.as_ref())
                })
                .map(|name| DisplayName::new(name))
                .collect();
            names.extend(member_names(snapshot, &room)?);
            let name = names
                .into_iter()
                .find(|name| {
                    [
                        name.as_raw_str(),
                        name.as_normalized_str()
                            .unwrap_or_else(|| name.as_raw_str()),
                    ]
                    .iter()
                    .any(|value| self.encode_key(keys::DISPLAY_NAME, value).as_ref() == encoded)
                })
                .ok_or_else(invalid)?;
            snapshot
                .changes
                .ambiguity_maps
                .entry(room)
                .or_default()
                .entry(name)
                .or_default()
                .extend(users);
        }
        Ok(())
    }

    fn snapshot_kv(
        &self,
        txn: &Transaction<'_>,
        snapshot: &mut StateSnapshot,
        users: BTreeSet<OwnedUserId>,
    ) -> Result<()> {
        let mut candidates = vec![
            SnapshotKey::SyncToken,
            SnapshotKey::ServerInfo,
            SnapshotKey::UtdHookManagerData,
        ];
        for user in &users {
            candidates.push(SnapshotKey::UserAvatarUrl(user.clone()));
            candidates.push(SnapshotKey::RecentlyVisitedRooms(user.clone()));
        }
        for room in snapshot.changes.room_infos.keys() {
            candidates.push(SnapshotKey::ComposerDraft(room.clone(), None));
            candidates.push(SnapshotKey::SeenKnockRequests(room.clone()));
        }
        let mut statement = txn.prepare("SELECT key, value FROM kv_blob")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let encoded: Vec<u8> = row.get(0)?;
            let data: Vec<u8> = row.get(1)?;
            if let Some(user) = users
                .iter()
                .find(|user| self.encode_presence_key(user).as_ref() == encoded)
            {
                let event: Raw<PresenceEvent> = self.deserialize_json(&data)?;
                if event.deserialize()?.sender != *user {
                    return Err(invalid());
                }
                snapshot.changes.presence.insert(user.clone(), event);
                continue;
            }
            let position = candidates
                .iter()
                .position(|key| self.encode_state_store_data_key(key.as_key()).as_ref() == encoded)
                .ok_or_else(invalid)?;
            let key = candidates.remove(position);
            let value = self.snapshot_kv_value(key.as_key(), &data)?;
            if let StateStoreDataValue::SyncToken(token) = &value {
                snapshot.changes.sync_token = Some(token.clone());
            }
            snapshot.kv.push((key, value));
        }
        Ok(())
    }

    fn snapshot_kv_value(
        &self,
        key: StateStoreDataKey<'_>,
        data: &[u8],
    ) -> Result<StateStoreDataValue> {
        Ok(match key {
            StateStoreDataKey::SyncToken => {
                StateStoreDataValue::SyncToken(self.deserialize_value(data)?)
            }
            StateStoreDataKey::ServerInfo => {
                StateStoreDataValue::ServerInfo(self.deserialize_value(data)?)
            }
            StateStoreDataKey::UtdHookManagerData => {
                StateStoreDataValue::UtdHookManagerData(self.deserialize_value(data)?)
            }
            StateStoreDataKey::UserAvatarUrl(_) => {
                StateStoreDataValue::UserAvatarUrl(self.deserialize_value(data)?)
            }
            StateStoreDataKey::RecentlyVisitedRooms(_) => {
                StateStoreDataValue::RecentlyVisitedRooms(self.deserialize_value(data)?)
            }
            StateStoreDataKey::ComposerDraft(_, _) => {
                StateStoreDataValue::ComposerDraft(self.deserialize_value(data)?)
            }
            StateStoreDataKey::SeenKnockRequests(_) => {
                StateStoreDataValue::SeenKnockRequests(self.deserialize_value(data)?)
            }
            StateStoreDataKey::Filter(_) => {
                StateStoreDataValue::Filter(self.deserialize_value(data)?)
            }
        })
    }
}

fn member_names(snapshot: &StateSnapshot, room: &RoomId) -> Result<Vec<DisplayName>> {
    let mut names = Vec::new();
    if let Some(members) = snapshot
        .changes
        .state
        .get(room)
        .and_then(|events| events.get(&StateEventType::RoomMember))
    {
        for (user, event) in members {
            let user = UserId::parse(user).map_err(|_| invalid())?;
            let content: serde_json::Value = event.get_field("content")?.ok_or_else(invalid)?;
            names.push(DisplayName::new(
                content
                    .get("displayname")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_else(|| user.localpart()),
            ));
        }
    }
    if let Some(members) = snapshot
        .changes
        .stripped_state
        .get(room)
        .and_then(|events| events.get(&StateEventType::RoomMember))
    {
        for (user, event) in members {
            let user = UserId::parse(user).map_err(|_| invalid())?;
            let content: serde_json::Value = event.get_field("content")?.ok_or_else(invalid)?;
            names.push(DisplayName::new(
                content
                    .get("displayname")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_else(|| user.localpart()),
            ));
        }
    }
    Ok(names)
}
