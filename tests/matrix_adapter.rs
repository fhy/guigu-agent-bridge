use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use guigu_agent_bridge::{
    config::{MatrixTransportConfig, SecretString},
    matrix::{
        MatrixClient, MatrixError, MatrixSender, MatrixSync, MemorySyncTokenStore, ReplyContext,
        SdkMatrixSender, SyncTokenStore, resolve_conversation,
    },
    observer::{MessageCategory, MonitorSender, ObserverMessage, Severity},
    storage::{SqliteRepository, connect, migrate},
};
use serde_json::json;
use tokio::sync::mpsc;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex},
};

fn config(homeserver: &str) -> MatrixTransportConfig {
    MatrixTransportConfig {
        enabled: true,
        homeserver: homeserver.to_owned(),
        user_id: "@bridge:example.test".to_owned(),
        access_token: SecretString::new("planted-access-token".to_owned()),
        monitor_room: String::new(),
        sync_capacity: 4,
        allowed_users: Vec::new(),
        routes: Default::default(),
        admin_users: Vec::new(),
        admin_rooms: Vec::new(),
    }
}

fn sync_response(token: &str, event_id: &str) -> serde_json::Value {
    json!({
        "next_batch": token,
        "rooms": {
            "join": {
                "!room:example.test": {
                    "timeline": {"events": [{
                        "type": "m.room.message",
                        "event_id": event_id,
                        "sender": "@alice:example.test",
                        "origin_server_ts": 1,
                        "content": {"msgtype": "m.text", "body": "hello"}
                    }], "limited": false, "prev_batch": null},
                    "state": {"events": []},
                    "ephemeral": {"events": []},
                    "account_data": {"events": []},
                    "unread_notifications": {}
                }
            },
            "invite": {}, "leave": {}, "knock": {}
        },
        "presence": {"events": []},
        "account_data": {"events": []},
        "to_device": {"events": []},
        "device_lists": {"changed": [], "left": []},
        "device_one_time_keys_count": {}
    })
}

async fn mount_versions(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/versions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"versions": ["v1.1"], "unstable_features": {}})),
        )
        .mount(server)
        .await;
}

#[tokio::test]
async fn concrete_sender_preserves_thread_reply_and_monitor_envelopes() {
    let server = MockServer::start().await;
    mount_versions(&server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(sync_response("s1", "$one:x")))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path_regex(
            r"/_matrix/client/.*/rooms/.*/send/m.room.message/.*",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"event_id": "$sent:x"})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex(r"/_matrix/client/.*/keys/upload"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"one_time_key_counts": {}})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex(r"/_matrix/client/.*/keys/query"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"failures": {}, "device_keys": {}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(
            r"/_matrix/client/.*/rooms/.*/state/m.room.encryption/",
        ))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(json!({"errcode": "M_NOT_FOUND", "error": "not encrypted"})),
        )
        .mount(&server)
        .await;

    let client = MatrixClient::restore(&config(&server.uri())).await.unwrap();
    let sync =
        MatrixSync::new(client.clone(), Arc::new(MemorySyncTokenStore::default()), 4).unwrap();
    let (tx, mut rx) = mpsc::channel(4);
    sync.sync_once(&tx).await.unwrap();
    rx.recv().await.unwrap();

    let sender = SdkMatrixSender::new(client);
    sender
        .send_reply(
            &ReplyContext {
                room_id: "!room:example.test".into(),
                thread_root: Some("$root:example.test".into()),
                event_id: "$one:x".into(),
            },
            &"x".repeat(3000),
        )
        .await
        .unwrap();
    sender
        .send(
            "!room:example.test",
            &ObserverMessage {
                category: MessageCategory::Summary,
                severity: Severity::Info,
                body: "monitor".into(),
            },
        )
        .await
        .unwrap();

    let requests = server.received_requests().await.unwrap();
    let sent: Vec<_> = requests
        .iter()
        .filter(|request| request.method.as_str() == "PUT")
        .collect();
    assert_eq!(sent.len(), 2);
    let reply: serde_json::Value = serde_json::from_slice(&sent[0].body).unwrap();
    assert_eq!(reply["m.relates_to"]["rel_type"], "m.thread");
    assert_eq!(reply["m.relates_to"]["event_id"], "$root:example.test");
    assert_eq!(reply["m.relates_to"]["m.in_reply_to"]["event_id"], "$one:x");
    assert!(reply["body"].as_str().unwrap().len() <= 2048);
    let monitor: serde_json::Value = serde_json::from_slice(&sent[1].body).unwrap();
    assert_eq!(monitor["body"], "monitor");
    assert!(monitor.get("m.relates_to").is_none());
    assert!(!String::from_utf8_lossy(&sent[0].body).contains("planted-access-token"));
}

#[tokio::test]
async fn rebuilt_sync_uses_the_last_committed_since_token() {
    let server = MockServer::start().await;
    mount_versions(&server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(sync_response("s1", "$one:x")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    let tokens: Arc<dyn SyncTokenStore> = Arc::new(MemorySyncTokenStore::default());
    let client = MatrixClient::restore(&config(&server.uri())).await.unwrap();
    let sync = MatrixSync::new(client, Arc::clone(&tokens), 4).unwrap();
    let (tx, mut rx) = mpsc::channel(4);
    assert_eq!(sync.sync_once(&tx).await.unwrap(), 1);
    assert_eq!(rx.recv().await.unwrap().event_id, "$one:x");
    assert_eq!(tokens.load().await.unwrap().as_deref(), Some("s1"));

    server.reset().await;
    mount_versions(&server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(sync_response("s2", "$two:x")))
        .mount(&server)
        .await;
    let rebuilt = MatrixSync::new(
        MatrixClient::restore(&config(&server.uri())).await.unwrap(),
        Arc::clone(&tokens),
        4,
    )
    .unwrap();
    assert_eq!(rebuilt.sync_once(&tx).await.unwrap(), 1);
    let requests = server.received_requests().await.unwrap();
    let sync_requests: Vec<_> = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/sync"))
        .collect();
    assert_eq!(sync_requests.len(), 1);
    assert!(
        sync_requests[0]
            .url
            .query_pairs()
            .any(|(key, value)| key == "since" && value == "s1")
    );
    assert_eq!(tokens.load().await.unwrap().as_deref(), Some("s2"));
}

#[tokio::test]
async fn state_and_redacted_events_do_not_block_delivery_or_checkpoint() {
    let server = MockServer::start().await;
    mount_versions(&server).await;
    let mut response = sync_response("after-mixed", "$accepted:x");
    response["rooms"]["join"]["!room:example.test"]["timeline"]["events"] = json!([
        {
            "type": "m.room.message",
            "state_key": "topic",
            "event_id": "$state:x",
            "sender": "@alice:example.test",
            "origin_server_ts": 1,
            "content": {"msgtype": "m.text", "body": "state-shaped"}
        },
        {
            "type": "m.room.message",
            "event_id": "$redacted:x",
            "sender": "@alice:example.test",
            "origin_server_ts": 2,
            "content": {},
            "unsigned": {
                "redacted_because": {
                    "type": "m.room.redaction",
                    "event_id": "$redaction:x",
                    "sender": "@moderator:example.test",
                    "origin_server_ts": 3,
                    "content": {"redacts": "$redacted:x"},
                    "redacts": "$redacted:x"
                }
            }
        },
        {
            "type": "m.room.message",
            "event_id": "$accepted:x",
            "sender": "@alice:example.test",
            "origin_server_ts": 4,
            "content": {"msgtype": "m.text", "body": "accepted"}
        }
    ]);
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .mount(&server)
        .await;

    let tokens: Arc<dyn SyncTokenStore> = Arc::new(MemorySyncTokenStore::default());
    let sync = MatrixSync::new(
        MatrixClient::restore(&config(&server.uri())).await.unwrap(),
        Arc::clone(&tokens),
        3,
    )
    .unwrap();
    let (tx, mut rx) = mpsc::channel(3);

    assert_eq!(sync.sync_once(&tx).await.unwrap(), 1);
    let accepted = rx.try_recv().unwrap();
    assert_eq!(accepted.event_id, "$accepted:x");
    assert_eq!(accepted.body, "accepted");
    assert!(rx.try_recv().is_err());
    assert_eq!(tokens.load().await.unwrap().as_deref(), Some("after-mixed"));
}

#[tokio::test]
async fn backpressure_does_not_advance_the_checkpoint() {
    let server = MockServer::start().await;
    mount_versions(&server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(sync_response("lost", "$event:x")))
        .mount(&server)
        .await;
    let tokens: Arc<dyn SyncTokenStore> = Arc::new(MemorySyncTokenStore::default());
    tokens.save("before").await.unwrap();
    let sync = MatrixSync::new(
        MatrixClient::restore(&config(&server.uri())).await.unwrap(),
        Arc::clone(&tokens),
        1,
    )
    .unwrap();
    let (tx, _rx) = mpsc::channel(1);
    tx.try_send(guigu_agent_bridge::matrix::InboundMatrixEvent {
        event_id: "$existing:x".to_owned(),
        room_id: "!room:x".to_owned(),
        thread_root: None,
        sender: "@alice:x".to_owned(),
        body: "existing".to_owned(),
    })
    .unwrap();
    assert!(matches!(
        sync.sync_once(&tx).await,
        Err(MatrixError::Backpressure)
    ));
    assert_eq!(tokens.load().await.unwrap().as_deref(), Some("before"));
}

fn remove_db_files(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut candidate = path.as_os_str().to_owned();
        candidate.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(candidate));
    }
}

#[tokio::test]
async fn room_and_thread_conversations_are_distinct_and_idempotent() {
    let path = std::env::temp_dir().join(format!("guigu-matrix-{}.db", uuid::Uuid::now_v7()));
    let pool = connect(&path).await.unwrap();
    migrate(&pool).await.unwrap();
    let repository = SqliteRepository::new(pool.clone());
    let room = resolve_conversation(&repository, "!room:x", None)
        .await
        .unwrap();
    let room_replay = resolve_conversation(&repository, "!room:x", None)
        .await
        .unwrap();
    let thread = resolve_conversation(&repository, "!room:x", Some("$root:x"))
        .await
        .unwrap();
    let (concurrent_a, concurrent_b) = tokio::join!(
        resolve_conversation(&repository, "!race:x", None),
        resolve_conversation(&repository, "!race:x", None),
    );
    assert_eq!(room.id, room_replay.id);
    assert_ne!(room.id, thread.id);
    assert_eq!(concurrent_a.unwrap().id, concurrent_b.unwrap().id);
    assert!(room.participants.is_empty());
    assert!(thread.participants.is_empty());
    pool.close().await;
    drop(repository);
    drop(pool);
    remove_db_files(&path);
}

#[tokio::test]
async fn unknown_token_is_authentication_and_the_owner_shuts_down_cleanly() {
    let auth_server = MockServer::start().await;
    mount_versions(&auth_server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "errcode": "M_UNKNOWN_TOKEN",
            "error": "planted-server-secret"
        })))
        .mount(&auth_server)
        .await;
    let tokens: Arc<dyn SyncTokenStore> = Arc::new(MemorySyncTokenStore::default());
    let sync = MatrixSync::new(
        MatrixClient::restore(&config(&auth_server.uri()))
            .await
            .unwrap(),
        tokens,
        1,
    )
    .unwrap();
    let (tx, _rx) = mpsc::channel(1);
    let error = sync.sync_once(&tx).await.unwrap_err();
    assert!(matches!(error, MatrixError::Authentication));
    assert!(!error.to_string().contains("planted-server-secret"));

    let idle_server = MockServer::start().await;
    mount_versions(&idle_server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "next_batch": "idle",
            "rooms": {"join": {}, "invite": {}, "leave": {}, "knock": {}},
            "presence": {"events": []}, "account_data": {"events": []},
            "to_device": {"events": []},
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {}
        })))
        .mount(&idle_server)
        .await;
    let owner = MatrixSync::new(
        MatrixClient::restore(&config(&idle_server.uri()))
            .await
            .unwrap(),
        Arc::new(MemorySyncTokenStore::default()),
        1,
    )
    .unwrap();
    let (handle, receiver) = owner.start();
    handle.shutdown().await.unwrap();
    drop(receiver);
}

#[tokio::test]
async fn sync_owner_reconnects_after_a_transient_failure() {
    let server = MockServer::start().await;
    mount_versions(&server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "errcode": "M_UNKNOWN",
            "error": "temporary"
        })))
        .with_priority(1)
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(sync_response("recovered", "$ok:x")))
        .with_priority(2)
        .up_to_n_times(1)
        .mount(&server)
        .await;
    let tokens: Arc<dyn SyncTokenStore> = Arc::new(MemorySyncTokenStore::default());
    let owner = MatrixSync::new(
        MatrixClient::restore(&config(&server.uri())).await.unwrap(),
        Arc::clone(&tokens),
        2,
    )
    .unwrap();
    let (handle, mut events) = owner.start();
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
        .await
        .expect("sync owner deadlock")
        .expect("event stream closed");
    assert_eq!(event.event_id, "$ok:x");
    handle.shutdown().await.unwrap();
    assert_eq!(tokens.load().await.unwrap().as_deref(), Some("recovered"));
    let requests = server.received_requests().await.unwrap();
    assert!(
        requests
            .iter()
            .filter(|request| request.url.path().ends_with("/sync"))
            .count()
            >= 2
    );
}

#[tokio::test]
async fn rendered_errors_and_debug_values_never_contain_credentials() {
    let secret = "planted-access-token";
    let config = config("not a URL containing planted-query-secret");
    let error = MatrixClient::restore(&config).await.unwrap_err();
    for rendered in [error.to_string(), format!("{error:?}")] {
        assert!(!rendered.contains(secret));
        assert!(!rendered.contains("@bridge:example.test"));
        assert!(!rendered.contains("planted-query-secret"));
    }
}
