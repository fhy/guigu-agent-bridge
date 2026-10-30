use super::*;
use matrix_sdk::config::SyncSettings;
use serde_json::json;
use wiremock::matchers::path_regex;

pub(super) async fn prepare(fixture: &Fixture) -> (tempfile::TempDir, MockServer) {
    let store_root = tempfile::Builder::new()
        .prefix("r092-cli-sdk-")
        .tempdir_in(std::env::var_os("HOME").unwrap())
        .unwrap();
    let store = store_root.path().join("sdk");
    std::fs::set_permissions(store_root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::create_dir(&store).unwrap();
    std::fs::set_permissions(&store, std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = MockServer::start().await;
    initial(&server).await;
    let device_keys = seed(&server, &store).await;
    identity(&server, device_keys).await;
    configure(fixture, &store, &server).await;
    (store_root, server)
}

async fn initial(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"versions":["v1.1"]})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/account/whoami"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "user_id":"@fixture:example.test","device_id":"FIXTURE","is_guest":false
        })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex(r"/_matrix/client/.*/keys/upload"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"one_time_key_counts":{}})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "next_batch":"B","rooms":{"join":{},"invite":{},"leave":{}},
            "presence":{"events":[]},"account_data":{"events":[]},
            "to_device":{"events":[]},"device_lists":{"changed":[],"left":[]},
            "device_one_time_keys_count":{}
        })))
        .mount(server)
        .await;
}

async fn seed(server: &MockServer, store: &std::path::Path) -> serde_json::Value {
    let config = crate::config::MatrixTransportConfig {
        enabled: true,
        homeserver: server.uri(),
        user_id: "@fixture:example.test".into(),
        device_id: "FIXTURE".into(),
        access_token: crate::config::SecretString::new("synthetic-secret".into()),
        monitor_room: String::new(),
        sync_capacity: 4,
        allowed_users: Vec::new(),
        routes: Default::default(),
        admin_users: Vec::new(),
        admin_rooms: Vec::new(),
        crypto_store_path: Some(store.into()),
        device_trusted: true,
    };
    let client = crate::matrix::MatrixClient::restore(&config).await.unwrap();
    client.inner.sync_once(SyncSettings::new()).await.unwrap();
    let fingerprint = client.inner.encryption().ed25519_key().await.unwrap();
    let device_keys = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.url.path().ends_with("/keys/upload"))
        .filter_map(|request| serde_json::from_slice::<serde_json::Value>(&request.body).ok())
        .map(|upload| upload["device_keys"].clone())
        .find(|keys| keys["keys"]["ed25519:FIXTURE"].as_str() == Some(&fingerprint))
        .unwrap();
    drop(client);
    device_keys
}

async fn identity(server: &MockServer, device_keys: serde_json::Value) {
    server.reset().await;
    initial(server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/_matrix/client/.*/account/whoami"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "user_id":"@fixture:example.test","device_id":"FIXTURE","is_guest":false
        })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path_regex(r"/_matrix/client/.*/keys/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "failures":{},"device_keys":{"@fixture:example.test":{"FIXTURE":device_keys}}
        })))
        .mount(server)
        .await;
}

async fn configure(fixture: &Fixture, store: &std::path::Path, server: &MockServer) {
    std::fs::write(
        &fixture.request.config,
        format!(
            "[bridge]\ndatabase = {:?}\nsession_root = {:?}\nhealth_bind = \"127.0.0.1:0\"\n[transports.matrix]\nenabled = true\nhomeserver = {:?}\nuser_id = \"@fixture:example.test\"\ndevice_id = \"FIXTURE\"\naccess_token = \"{{env:R092_IDENTITY_TOKEN}}\"\ncrypto_store_path = {:?}\ndevice_trusted = true\n",
            fixture.request.database,
            fixture.root.path().join("sessions"),
            server.uri(),
            store,
        ),
    )
    .unwrap();
    crate::config::load_from_str_with_env(
        &std::fs::read_to_string(&fixture.request.config).unwrap(),
        &std::collections::BTreeMap::from([(
            "R092_IDENTITY_TOKEN".into(),
            "synthetic-secret".into(),
        )]),
    )
    .unwrap();
    let store_closed = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if crate::offline_identity::store_generation(store).is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < store_closed,
            "SDK fixture store did not settle after client drop"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    for entry in std::fs::read_dir(store).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::set_permissions(entry.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
}
