use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};

use super::{
    MemoryStore, Result, StateChanges, StateStore, StateStoreDataKey, StateStoreDataValue,
};

#[derive(Debug)]
#[doc(hidden)]
pub enum SnapshotKey {
    SyncToken,
    ServerInfo,
    UtdHookManagerData,
    UserAvatarUrl(OwnedUserId),
    RecentlyVisitedRooms(OwnedUserId),
    ComposerDraft(OwnedRoomId, Option<OwnedEventId>),
    SeenKnockRequests(OwnedRoomId),
    Filter(String),
}

impl SnapshotKey {
    pub fn as_key(&self) -> StateStoreDataKey<'_> {
        match self {
            Self::SyncToken => StateStoreDataKey::SyncToken,
            Self::ServerInfo => StateStoreDataKey::ServerInfo,
            Self::UtdHookManagerData => StateStoreDataKey::UtdHookManagerData,
            Self::UserAvatarUrl(user) => StateStoreDataKey::UserAvatarUrl(user),
            Self::RecentlyVisitedRooms(user) => StateStoreDataKey::RecentlyVisitedRooms(user),
            Self::ComposerDraft(room, thread) => {
                StateStoreDataKey::ComposerDraft(room, thread.as_deref())
            }
            Self::SeenKnockRequests(room) => StateStoreDataKey::SeenKnockRequests(room),
            Self::Filter(name) => StateStoreDataKey::Filter(name),
        }
    }
}

#[derive(Debug, Default)]
#[doc(hidden)]
pub struct StateSnapshot {
    pub changes: StateChanges,
    pub kv: Vec<(SnapshotKey, StateStoreDataValue)>,
}

impl StateSnapshot {
    pub async fn into_memory_store(self) -> Result<MemoryStore> {
        let memory = MemoryStore::new();
        memory.save_changes(&self.changes).await?;
        for (key, value) in self.kv {
            memory.set_kv_data(key.as_key(), value).await?;
        }
        Ok(memory)
    }
}
