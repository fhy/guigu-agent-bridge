use matrix_sdk::ruma::{OwnedRoomId, OwnedUserId, events::StateEventType, serde::Raw};
use matrix_sdk::{
    Client, StateChanges, StateStore,
    store::{StateStoreDataKey, StateStoreDataValue},
};
use serde_json::json;

#[path = "support/matrix_hydration.rs"]
mod support;

async fn prove_hydration(encrypted: bool) {
    let (_root, _server, anchor) = support::fixture(encrypted).await;
    let active = anchor
        .notification_client("r076-hydrated".into())
        .await
        .unwrap();
    let room: OwnedRoomId = "!room:example.test".parse().unwrap();
    let user: OwnedUserId = "@alice:example.test".parse().unwrap();
    assert!(active.get_room(&room).is_some());
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
        panic!("expected sync state");
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
    assert_eq!(
        active.encryption().ed25519_key().await,
        anchor.encryption().ed25519_key().await
    );
    prove_write_isolation(&anchor, &active, &room, &user).await;
}

async fn prove_write_isolation(
    anchor: &Client,
    active: &Client,
    room: &OwnedRoomId,
    user: &OwnedUserId,
) {
    let mut changes = StateChanges::new("B".into());
    changes
        .room_account_data
        .entry(room.clone())
        .or_default()
        .insert(
            "org.example.room".into(),
            Raw::new(&json!({"type":"org.example.room","content":{"value":"active"}}))
                .unwrap()
                .cast(),
        );
    active.state_store().save_changes(&changes).await.unwrap();
    active.state_store().remove_room(&room).await.unwrap();
    assert_eq!(token(anchor).await.as_deref(), Some("A"));
    assert_eq!(token(active).await.as_deref(), Some("B"));
    assert!(
        anchor
            .state_store()
            .get_profile(&room, &user)
            .await
            .unwrap()
            .is_some()
    );
    let data = anchor
        .state_store()
        .get_room_account_data_event(&room, "org.example.room".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        data.get_field::<serde_json::Value>("content")
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
            .is_none()
    );
}

async fn token(client: &Client) -> Option<String> {
    client
        .state_store()
        .get_kv_data(StateStoreDataKey::SyncToken)
        .await
        .unwrap()
        .and_then(|value| value.into_sync_token())
}

#[tokio::test]
async fn encrypted_sqlite_arbitrary_state_profiles_account_data_and_write_isolation() {
    prove_hydration(true).await;
}

#[tokio::test]
async fn plain_sqlite_arbitrary_state_profiles_account_data_and_write_isolation() {
    prove_hydration(false).await;
}

#[tokio::test]
async fn unknown_provider_refuses_without_an_empty_clone() {
    assert!(matrix_sdk::MemoryStore::new().snapshot().await.is_err());
}

#[tokio::test]
async fn missing_session_refuses_even_when_the_store_has_a_cursor() {
    let root = tempfile::tempdir().unwrap();
    let client = Client::builder()
        .homeserver_url("http://127.0.0.1:1")
        .sqlite_store(root.path(), None)
        .build()
        .await
        .unwrap();
    client
        .state_store()
        .set_kv_data(
            StateStoreDataKey::SyncToken,
            StateStoreDataValue::SyncToken("A".into()),
        )
        .await
        .unwrap();
    assert!(
        client
            .notification_client("r076-no-session".into())
            .await
            .is_err()
    );
    assert_eq!(token(&client).await.as_deref(), Some("A"));
}

#[tokio::test]
async fn unknown_encrypted_kv_refuses_without_partial_hydration() {
    let (_root, _server, anchor) = support::fixture(true).await;
    anchor
        .state_store()
        .set_custom_value(b"unrecognized", vec![1, 2, 3])
        .await
        .unwrap();
    assert!(
        anchor
            .notification_client("r076-refusal".into())
            .await
            .is_err()
    );
    assert_eq!(token(&anchor).await.as_deref(), Some("A"));
}

#[tokio::test]
async fn malformed_state_refuses_and_preserves_anchor() {
    let (_root, _server, anchor) = support::fixture(true).await;
    let room: OwnedRoomId = "!room:example.test".parse().unwrap();
    let mut changes = StateChanges::default();
    changes
        .state
        .entry(room)
        .or_default()
        .entry("org.example.bad".into())
        .or_default()
        .insert(
            "".into(),
            Raw::new(&json!({"type":"org.example.bad","state_key":42,"content":{}}))
                .unwrap()
                .cast(),
        );
    anchor.state_store().save_changes(&changes).await.unwrap();
    assert!(
        anchor
            .notification_client("r076-malformed".into())
            .await
            .is_err()
    );
    assert_eq!(token(&anchor).await.as_deref(), Some("A"));
}

#[tokio::test]
async fn missing_or_empty_cursor_refuses_before_clone_exposure() {
    let (_root, _server, anchor) = support::fixture(false).await;
    anchor
        .state_store()
        .remove_kv_data(StateStoreDataKey::SyncToken)
        .await
        .unwrap();
    assert!(
        anchor
            .notification_client("r076-missing".into())
            .await
            .is_err()
    );
    anchor
        .state_store()
        .set_kv_data(
            StateStoreDataKey::SyncToken,
            StateStoreDataValue::SyncToken(String::new()),
        )
        .await
        .unwrap();
    assert!(
        anchor
            .notification_client("r076-empty".into())
            .await
            .is_err()
    );
}

fn epoch_changes(epoch: u32) -> StateChanges {
    let mut changes = StateChanges::new(format!("epoch-{epoch}"));
    changes.account_data.insert(
        "org.example.epoch".into(),
        Raw::new(&json!({"type":"org.example.epoch","content":{"epoch":epoch}}))
            .unwrap()
            .cast(),
    );
    changes
}

#[tokio::test]
async fn encrypted_snapshot_keeps_cursor_and_payload_in_one_transaction() {
    let (_root, _server, anchor) = support::fixture(true).await;
    anchor
        .state_store()
        .save_changes(&epoch_changes(0))
        .await
        .unwrap();
    let writer = anchor.clone();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let writer_barrier = barrier.clone();
    let task = tokio::spawn(async move {
        writer_barrier.wait().await;
        for epoch in 1..=30 {
            writer
                .state_store()
                .save_changes(&epoch_changes(epoch))
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
    });
    barrier.wait().await;
    for _ in 0..30 {
        let snapshot = anchor.state_store().snapshot().await.unwrap();
        let content: serde_json::Value = snapshot
            .changes
            .account_data
            .get(&"org.example.epoch".into())
            .unwrap()
            .get_field("content")
            .unwrap()
            .unwrap();
        let epoch = content["epoch"].as_u64().unwrap();
        assert_eq!(snapshot.changes.sync_token, Some(format!("epoch-{epoch}")));
    }
    task.await.unwrap();
}
