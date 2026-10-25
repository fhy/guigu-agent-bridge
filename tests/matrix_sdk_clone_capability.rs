use std::time::Duration;

use matrix_sdk::{
    Client, SessionTokens,
    authentication::matrix::MatrixSession,
    config::{RequestConfig, SyncSettings},
    ruma::OwnedRoomId,
    store::StateStoreDataKey,
};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex},
};

async fn token(client: &Client) -> Option<String> {
    client
        .state_store()
        .get_kv_data(StateStoreDataKey::SyncToken)
        .await
        .unwrap()
        .and_then(|value| value.into_sync_token())
}

async fn server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/versions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"versions":["v1.1"],"unstable_features":{}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex("/_matrix/client/.*/keys/upload"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"one_time_key_counts":{"signed_curve25519":50}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "next_batch":"A", "rooms":{"join":{"!room:example.test":{
                "timeline":{"events":[],"limited":false},
                "state":{"events":[]},"ephemeral":{"events":[]},"account_data":{"events":[]},
                "unread_notifications":{}
            }},"invite":{},"leave":{},"knock":{}},
            "presence":{"events":[]},"account_data":{"events":[]},"to_device":{"events":[]},
            "device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{}
        })))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn pinned_notification_client_does_not_copy_anchor_cursor_or_room_state() {
    let server = server().await;
    let temporary = tempfile::tempdir().unwrap();
    let anchor = Client::builder()
        .homeserver_url(server.uri())
        .request_config(
            RequestConfig::new()
                .timeout(Duration::from_secs(2))
                .retry_limit(0),
        )
        .sqlite_store(temporary.path(), None)
        .build()
        .await
        .unwrap();
    anchor
        .restore_session(MatrixSession {
            meta: matrix_sdk::SessionMeta {
                user_id: "@bridge:example.test".parse().unwrap(),
                device_id: "BRIDGE".into(),
            },
            tokens: SessionTokens {
                access_token: "synthetic-local-fixture".into(),
                refresh_token: None,
            },
        })
        .await
        .unwrap();
    let response = anchor
        .sync_once(SyncSettings::new().timeout(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(response.next_batch, "A");
    let room: OwnedRoomId = "!room:example.test".parse().unwrap();
    assert_eq!(token(&anchor).await.as_deref(), Some("A"));
    assert!(anchor.get_room(&room).is_some());
    let active = anchor
        .notification_client("r076-disposable-proof".into())
        .await
        .unwrap();
    assert_eq!(active.user_id(), anchor.user_id());
    assert_eq!(active.device_id(), anchor.device_id());
    assert_eq!(
        active.encryption().ed25519_key().await,
        anchor.encryption().ed25519_key().await
    );
    assert_eq!(token(&active).await, None);
    assert!(active.get_room(&room).is_none());
    assert_eq!(token(&anchor).await.as_deref(), Some("A"));
    println!(
        "anchor: token=A, room=present; notification clone: token=None, room=absent; authenticated device/crypto identity retained"
    );
}
