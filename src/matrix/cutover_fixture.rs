use std::time::Duration;

use matrix_sdk::{
    Client, SessionTokens,
    authentication::matrix::MatrixSession,
    config::{RequestConfig, SyncSettings},
    ruma::OwnedDeviceId,
};
use serde_json::{Value, json};
use sqlx::SqlitePool;
use wiremock::{
    Mock, MockGuard, MockServer, ResponseTemplate,
    matchers::{method, path_regex},
};

use crate::{
    matrix::MatrixClient,
    storage::{CutoverStore, connect, cutover_digest, migrate},
};

pub(super) struct Fixture {
    pub server: MockServer,
    pub root: tempfile::TempDir,
    pub pool: SqlitePool,
    pub store: CutoverStore,
    pub anchor: MatrixClient,
}

pub(super) fn response(token: &str, identities: &[&str], limited: bool) -> Value {
    let events: Vec<_> = identities
        .iter()
        .map(|identity| {
            json!({
                "type":"m.room.message", "event_id": identity, "sender":"@alice:example.test",
                "origin_server_ts":1, "content":{"msgtype":"m.text","body":"fixture"}
            })
        })
        .collect();
    json!({
        "next_batch":token,
        "rooms":{"join":{"!room:example.test":{
            "timeline":{"events":events,"limited":limited,"prev_batch":"previous"},
            "state":{"events":[]},"ephemeral":{"events":[]},"account_data":{"events":[]},
            "unread_notifications":{}
        }},"invite":{},"leave":{},"knock":{}},
        "presence":{"events":[]},"account_data":{"events":[]},"to_device":{"events":[]},
        "device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{}
    })
}

pub(super) async fn client(server: &MockServer, root: &std::path::Path) -> MatrixClient {
    let inner = Client::builder()
        .homeserver_url(server.uri())
        .request_config(
            RequestConfig::new()
                .timeout(Duration::from_secs(2))
                .retry_limit(0),
        )
        .sqlite_store(root, None)
        .build()
        .await
        .unwrap();
    inner
        .restore_session(MatrixSession {
            meta: matrix_sdk::SessionMeta {
                user_id: "@bridge:example.test".parse().unwrap(),
                device_id: OwnedDeviceId::from("BRIDGE"),
            },
            tokens: SessionTokens {
                access_token: "synthetic-fixture-token".into(),
                refresh_token: None,
            },
        })
        .await
        .unwrap();
    MatrixClient {
        inner,
        user_id: "@bridge:example.test".into(),
        device_id: "BRIDGE".into(),
        cutover_baseline: None,
    }
}

impl Fixture {
    pub async fn new() -> Self {
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
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("crypto")).unwrap();
        let anchor = client(&server, &root.path().join("crypto")).await;
        anchor
            .inner
            .encryption()
            .wait_for_e2ee_initialization_tasks()
            .await;
        let key = anchor.inner.encryption().ed25519_key().await.unwrap();
        Mock::given(method("POST"))
            .and(path_regex("/_matrix/client/.*/keys/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "failures":{}, "device_keys":{"@bridge:example.test":{"BRIDGE":{
                    "user_id":"@bridge:example.test", "device_id":"BRIDGE",
                    "algorithms":["m.olm.v1.curve25519-aes-sha2","m.megolm.v1.aes-sha2"],
                    "keys":{"ed25519:BRIDGE":key},"signatures":{}
                }}}
            })))
            .mount(&server)
            .await;
        let pool = connect(&root.path().join("state.db")).await.unwrap();
        migrate(&pool).await.unwrap();
        let store = CutoverStore::prepare(
            pool.clone(),
            "operation",
            &cutover_digest(b"fixture-binding"),
            &cutover_digest(b"fixture-archive"),
        )
        .await
        .unwrap();
        Self {
            server,
            root,
            pool,
            store,
            anchor,
        }
    }

    pub async fn sync_mock(&self, token: &str, events: &[&str], limited: bool) -> MockGuard {
        Mock::given(method("GET"))
            .and(path_regex("/_matrix/client/.*/sync"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(response(token, events, limited)),
            )
            .mount_as_scoped(&self.server)
            .await
    }

    pub async fn baseline(&self) -> MatrixClient {
        let _mock = self.sync_mock("A", &["$historical"], false).await;
        self.anchor.prepare_cutover(&self.store).await.unwrap()
    }

    pub async fn sdk_ahead_without_disposition(&self, active: &MatrixClient) {
        let _mock = self.sync_mock("B", &["$new"], false).await;
        let result = active
            .inner
            .sync_once(
                SyncSettings::new()
                    .token("A")
                    .timeout(Duration::from_secs(1)),
            )
            .await
            .unwrap();
        assert_eq!(result.next_batch, "B");
        assert_eq!(
            result
                .rooms
                .joined
                .values()
                .next()
                .unwrap()
                .timeline
                .events
                .len(),
            1
        );
    }
}
