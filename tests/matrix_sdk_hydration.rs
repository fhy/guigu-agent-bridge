use matrix_sdk::{
    config::SyncSettings,
    ruma::{OwnedRoomId, OwnedUserId, events::StateEventType},
    store::StateStoreDataKey,
};
use serde_json::json;
#[path = "support/matrix_hydration.rs"]
mod support;

async fn prove_rebuild(encrypted_store: bool) {
    let (_root, server, anchor) = support::fixture(encrypted_store).await;
    let active = anchor
        .notification_client("r084-full-state".into())
        .await
        .unwrap();
    let room: OwnedRoomId = "!room:example.test".parse().unwrap();
    let user: OwnedUserId = "@alice:example.test".parse().unwrap();
    assert!(active.get_room(&room).is_none());
    let response = active
        .sync_once(SyncSettings::new().token("B").full_state(true))
        .await
        .unwrap();
    assert_eq!(response.next_batch, "A");
    let restored = active.get_room(&room).unwrap();
    assert_eq!(restored.name().as_deref(), Some("Recovery room"));
    assert_eq!(restored.state(), matrix_sdk::RoomState::Joined);
    assert!(restored.encryption_settings().is_some());
    assert_eq!(active.user_id(), anchor.user_id());
    assert_eq!(active.device_id(), anchor.device_id());
    let agreement_key = anchor
        .encryption()
        .curve25519_key()
        .await
        .expect("device agreement key");
    assert_eq!(
        active.encryption().curve25519_key().await,
        Some(agreement_key)
    );
    let event = active
        .state_store()
        .get_state_event(
            &room,
            StateEventType::from("org.example.custom.state"),
            "arbitrary",
        )
        .await
        .unwrap()
        .unwrap();
    let matrix_sdk::deserialized_responses::RawAnySyncOrStrippedState::Sync(event) = event else {
        panic!("sync state")
    };
    assert_eq!(
        event
            .get_field::<serde_json::Value>("content")
            .unwrap()
            .unwrap(),
        json!({"value":"anchor"})
    );
    assert!(
        active
            .state_store()
            .get_profile(&room, &user)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        active
            .state_store()
            .get_account_data_event("org.example.global".into())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        active
            .state_store()
            .get_room_account_data_event(&room, "org.example.room".into())
            .await
            .unwrap()
            .is_some()
    );
    let signing_key = anchor
        .encryption()
        .ed25519_key()
        .await
        .expect("device signing key");
    assert_eq!(active.encryption().ed25519_key().await, Some(signing_key));
    active.state_store().remove_room(&room).await.unwrap();
    assert!(anchor.get_room(&room).is_some());
    assert_eq!(
        anchor
            .state_store()
            .get_kv_data(StateStoreDataKey::SyncToken)
            .await
            .unwrap()
            .and_then(|value| value.into_sync_token())
            .as_deref(),
        Some("A")
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request
                .url
                .query_pairs()
                .any(|pair| pair.0 == "since" && pair.1 == "B")
                && request
                    .url
                    .query_pairs()
                    .any(|pair| pair.0 == "full_state" && pair.1 == "true"))
    );
}

#[tokio::test]
async fn encrypted_sqlite_rebuild_recovers_required_state_and_isolates_writes() {
    prove_rebuild(true).await;
}
#[tokio::test]
async fn plain_sqlite_rebuild_recovers_required_state_and_isolates_writes() {
    prove_rebuild(false).await;
}
