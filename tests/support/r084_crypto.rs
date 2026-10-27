use guigu_agent_bridge::matrix::MatrixClient;
use matrix_sdk::{
    Client, SessionMeta, SessionTokens,
    authentication::matrix::MatrixSession,
    ruma::{device_id, room_id, user_id},
};
use matrix_sdk_crypto::{EncryptionSettings, OlmMachine, encrypt_room_key_export};
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex},
};

pub fn response(token: &str, event: Value) -> Value {
    let state = [
        ("m.room.create","",json!({"creator":"@bridge:example.test","room_version":"10"})),
        ("m.room.member","@bridge:example.test",json!({"membership":"join"})),
        ("m.room.member","@alice:example.test",json!({"membership":"join"})),
        ("m.room.encryption","",json!({"algorithm":"m.megolm.v1.aes-sha2"})),
    ].into_iter().enumerate().map(|(index,(kind,key,content))| json!({"type":kind,"state_key":key,"content":content,"sender":"@bridge:example.test","event_id":format!("$state-{index}"),"origin_server_ts":1})).collect::<Vec<_>>();
    json!({"next_batch":token,"rooms":{"join":{"!room:example.test":{"timeline":{"events":[event],"limited":false},"state":{"events":state}}}}})
}

pub async fn mount_identity(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"versions":["v1.1"]})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/account/whoami"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"user_id":"@bridge:example.test","device_id":"R084","is_guest":false}),
        ))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex("/_matrix/client/.*/keys/upload"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"one_time_key_counts":{}})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex("/_matrix/client/.*/keys/query"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"device_keys":{},"failures":{}})),
        )
        .mount(server)
        .await;
}

pub async fn seed_key(fixture: &super::support::Fixture, server: &MockServer) -> (Value, String) {
    let config = fixture.config(true, &server.uri());
    drop(
        MatrixClient::restore(&config.transports.matrix)
            .await
            .unwrap(),
    );
    let client = Client::builder()
        .homeserver_url(server.uri())
        .sqlite_store(fixture.store(), None)
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
    let sender = OlmMachine::new(user_id!("@alice:example.test"), device_id!("ALICE")).await;
    sender
        .share_room_key(
            room_id!("!room:example.test"),
            std::iter::empty(),
            EncryptionSettings::default(),
        )
        .await
        .unwrap();
    let plaintext = json!({"msgtype":"m.text","body":"R084 real encrypted admission"});
    let encrypted = sender
        .encrypt_room_event_raw(
            room_id!("!room:example.test"),
            "m.room.message",
            &matrix_sdk::ruma::serde::Raw::from_json(
                serde_json::value::to_raw_value(&plaintext).unwrap(),
            ),
        )
        .await
        .unwrap();
    let keys = sender
        .store()
        .export_room_keys(|session| session.room_id() == room_id!("!room:example.test"))
        .await
        .unwrap();
    let file = fixture.root.path().join("synthetic-export");
    std::fs::write(&file, encrypt_room_key_export(&keys, "fixture", 1).unwrap()).unwrap();
    client
        .encryption()
        .import_room_keys(file.clone(), "fixture")
        .await
        .unwrap();
    std::fs::remove_file(file).unwrap();
    let public_key = client.encryption().ed25519_key().await.unwrap();
    drop(client);
    let event = json!({"type":"m.room.encrypted","content":serde_json::to_value(encrypted).unwrap(),"event_id":"$cipher","sender":"@alice:example.test","origin_server_ts":2,"unsigned":{}});
    (event, public_key)
}
