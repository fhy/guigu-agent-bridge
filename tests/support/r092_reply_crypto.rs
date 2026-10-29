use matrix_sdk::{
    Client, SessionMeta, SessionTokens,
    authentication::matrix::MatrixSession,
    ruma::{device_id, room_id, serde::Raw, user_id},
};
use matrix_sdk_crypto::{
    DecryptionSettings, OlmMachine, TrustRequirement, decrypt_room_key_export,
};
use serde_json::json;
use std::path::Path;
use wiremock::MockServer;

pub async fn assert_reply_decrypts(
    root: &Path,
    store: &Path,
    server: &MockServer,
    seed_requests: usize,
) {
    let client = Client::builder()
        .homeserver_url(server.uri())
        .sqlite_store(store, None)
        .build()
        .await
        .unwrap();
    client
        .restore_session(MatrixSession {
            meta: SessionMeta {
                user_id: user_id!("@bridge:example.test").to_owned(),
                device_id: device_id!("R084").to_owned(),
            },
            tokens: SessionTokens {
                access_token: "synthetic-no-network-token".into(),
                refresh_token: None,
            },
        })
        .await
        .unwrap();
    let export = root.join("synthetic-reply-key-export");
    client
        .encryption()
        .export_room_keys(export.clone(), "fixture", |session| {
            session.room_id() == room_id!("!room:example.test")
        })
        .await
        .unwrap();
    let keys = decrypt_room_key_export(std::fs::File::open(&export).unwrap(), "fixture").unwrap();
    std::fs::remove_file(&export).unwrap();
    drop(client);
    let receiver =
        OlmMachine::new(user_id!("@alice:example.test"), device_id!("ALICE-REPLY")).await;
    receiver
        .store()
        .import_exported_room_keys(keys, |_, _| {})
        .await
        .unwrap();
    let requests = server.received_requests().await.unwrap();
    let sends = requests
        .iter()
        .skip(seed_requests)
        .filter(|request| request.url.path().contains("/send/m.room.encrypted/"))
        .collect::<Vec<_>>();
    assert!(!sends.is_empty());
    let mut plaintext = String::new();
    for (index, send) in sends.into_iter().enumerate() {
        let content: serde_json::Value = serde_json::from_slice(&send.body).unwrap();
        assert!(content.get("body").is_none());
        let event = json!({"event_id":format!("$reply-{index}"),"origin_server_ts":3,"sender":"@bridge:example.test","type":"m.room.encrypted","content":content});
        let raw = Raw::from_json(serde_json::value::to_raw_value(&event).unwrap());
        let settings = DecryptionSettings {
            sender_device_trust_requirement: TrustRequirement::Untrusted,
        };
        let decrypted = receiver
            .decrypt_room_event(&raw, room_id!("!room:example.test"), &settings)
            .await
            .unwrap();
        let event: serde_json::Value = serde_json::from_str(decrypted.event.json().get()).unwrap();
        assert_eq!(event["type"], "m.room.message");
        plaintext.push_str(event["content"]["body"].as_str().unwrap());
    }
    assert!(
        plaintext.contains("Hello, world"),
        "synthetic reply content mismatch"
    );
}
