use super::*;
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex},
};

#[tokio::test]
async fn first_baseline_accepts_limited_history_without_loading_its_timeline() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/versions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"versions":["v1.1"],"unstable_features":{}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "next_batch": "fresh-cutover",
            "rooms": {"join": {"!room:example.test": {
                "timeline": {"limited": true, "prev_batch": "old", "events": [{
                    "type": "m.room.message", "event_id": "$old",
                    "sender": "@alice:example.test", "origin_server_ts": 1,
                    "content": {"msgtype": "m.text", "body": "historical command"}
                }]}
            }}}
        })))
        .mount(&server)
        .await;
    let inner = Client::builder()
        .homeserver_url(server.uri())
        .build()
        .await
        .unwrap();
    inner
        .restore_session(MatrixSession {
            meta: matrix_sdk::SessionMeta {
                user_id: "@bridge:example.test".parse().unwrap(),
                device_id: "SYNTHETIC".into(),
            },
            tokens: SessionTokens {
                access_token: "loopback-only".into(),
                refresh_token: None,
            },
        })
        .await
        .unwrap();
    let client = MatrixClient {
        inner,
        user_id: "@bridge:example.test".into(),
        device_id: "SYNTHETIC".into(),
    };
    assert_eq!(client.baseline_sync_token().await.unwrap(), "fresh-cutover");
    assert!(client.inner.rooms().is_empty());
}
