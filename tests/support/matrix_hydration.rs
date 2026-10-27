use std::time::Duration;

use matrix_sdk::{
    Client, SessionMeta, SessionTokens,
    authentication::matrix::MatrixSession,
    config::{RequestConfig, SyncSettings},
};
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex},
};

fn state(event_type: &str, state_key: &str, content: Value) -> Value {
    let sender = if event_type == "m.room.member" {
        state_key
    } else {
        "@bridge:example.test"
    };
    json!({"type":event_type,"state_key":state_key,"content":content,
        "sender":sender,"event_id":format!("${event_type}{state_key}"),
        "origin_server_ts":1})
}

pub async fn fixture(encrypted: bool) -> (tempfile::TempDir, MockServer, Client) {
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
    Mock::given(method("GET")).and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "next_batch":"A", "rooms":{"join":{"!room:example.test":{
                "timeline":{"events":[],"limited":false},
                "state":{"events":[
                    state("m.room.create", "", json!({"creator":"@bridge:example.test","room_version":"10"})),
                    state("m.room.member", "@bridge:example.test", json!({"membership":"join","displayname":"Bridge"})),
                    state("m.room.member", "@alice:example.test", json!({"membership":"join","displayname":"Alice"})),
                    state("m.room.name", "", json!({"name":"Recovery room"})),
                    state("m.room.encryption", "", json!({"algorithm":"m.megolm.v1.aes-sha2"})),
                    state("org.example.custom.state", "arbitrary", json!({"value":"anchor"}))
                ]},
                "account_data":{"events":[{"type":"org.example.room","content":{"value":"anchor"}}]},
                "ephemeral":{"events":[]},"unread_notifications":{}
            }},"invite":{},"leave":{},"knock":{}},
            "presence":{"events":[]},"account_data":{"events":[{"type":"org.example.global","content":{"value":"anchor"}}]},
            "to_device":{"events":[]},"device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{}
        }))).mount(&server).await;
    let root = tempfile::tempdir().unwrap();
    let client = Client::builder()
        .homeserver_url(server.uri())
        .request_config(
            RequestConfig::new()
                .timeout(Duration::from_secs(2))
                .retry_limit(0),
        )
        .sqlite_store(
            root.path(),
            encrypted.then_some("synthetic-fixture-passphrase"),
        )
        .build()
        .await
        .unwrap();
    client
        .restore_session(MatrixSession {
            meta: SessionMeta {
                user_id: "@bridge:example.test".parse().unwrap(),
                device_id: "BRIDGE".into(),
            },
            tokens: SessionTokens {
                access_token: "synthetic-loopback-only".into(),
                refresh_token: None,
            },
        })
        .await
        .unwrap();
    client
        .sync_once(SyncSettings::new().timeout(Duration::from_secs(1)))
        .await
        .unwrap();
    (root, server, client)
}
