use guigu_agent_bridge::offline_identity::{
    CollectorRequest, CollectorResult, IsolationEvidence, observe_server,
};
use matrix_sdk::crypto::olm::Account;
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method, path},
};

fn signed_keys() -> Value {
    let user: matrix_sdk::ruma::OwnedUserId = "@observer:example.test".parse().unwrap();
    let device = "FIXTURE".into();
    serde_json::to_value(Account::with_device_id(&user, device).device_keys()).unwrap()
}

fn request(server: &MockServer) -> CollectorRequest {
    CollectorRequest {
        homeserver: server.uri(),
        user_id: "@observer:example.test".into(),
        device_id: "FIXTURE".into(),
        access_token: "synthetic-observer-only".into(),
        store: "/tmp/r092-not-opened-by-observer".into(),
        passphrase: None,
    }
}

fn whoami() -> Value {
    json!({"user_id":"@observer:example.test", "device_id":"FIXTURE", "is_guest":false})
}

fn response(keys: Value) -> Value {
    json!({"device_keys":{"@observer:example.test":{"FIXTURE":keys}}, "failures":{}})
}

async fn mount(server: &MockServer, whoami: Value, keys: Value, count: u64) {
    Mock::given(method("GET"))
        .and(path("/_matrix/client/v3/account/whoami"))
        .and(header("authorization", "Bearer synthetic-observer-only"))
        .respond_with(ResponseTemplate::new(200).set_body_json(whoami))
        .expect(count)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_matrix/client/v3/keys/query"))
        .and(header("authorization", "Bearer synthetic-observer-only"))
        .and(body_json(
            json!({"device_keys":{"@observer:example.test":["FIXTURE"]}}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(keys))
        .expect(count)
        .mount(server)
        .await;
}

fn local(observation: &guigu_agent_bridge::offline_identity::ServerObservation) -> CollectorResult {
    CollectorResult {
        ed25519: observation.ed25519.clone(),
        curve25519: observation.curve25519.clone(),
        isolation: IsolationEvidence {
            policy: "synthetic-comparison-only".into(),
            executable: "not-an-isolation-claim".into(),
            process: 0,
            generation: "synthetic".into(),
            no_new_privs: 1,
            seccomp: 2,
            denied_probes: [libc::EPERM; 6],
        },
    }
}

#[tokio::test]
async fn two_observations_validate_signed_identity_and_only_four_authenticated_requests() {
    let server = MockServer::start().await;
    mount(&server, whoami(), response(signed_keys()), 2).await;
    let before = observe_server(&request(&server)).await.unwrap();
    let after = observe_server(&request(&server)).await.unwrap();
    assert_eq!(before, after);
    before.require_continuity(&after, &local(&before)).unwrap();
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[1].method, "POST");
    assert_eq!(requests[2].method, "GET");
    assert_eq!(requests[3].method, "POST");
}

#[tokio::test]
async fn guest_or_wrong_user_device_stops_before_key_query() {
    for identity in [
        json!({"user_id":"@other:example.test", "device_id":"FIXTURE"}),
        json!({"user_id":"@observer:example.test", "device_id":"OTHER"}),
        json!({"user_id":"@observer:example.test", "is_guest":false}),
        json!({"user_id":"@observer:example.test", "device_id":"FIXTURE", "is_guest":true}),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_matrix/client/v3/account/whoami"))
            .respond_with(ResponseTemplate::new(200).set_body_json(identity))
            .expect(1)
            .mount(&server)
            .await;
        assert!(observe_server(&request(&server)).await.is_err());
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn missing_tampered_or_unsigned_device_keys_refuse() {
    let mut unsigned = signed_keys();
    unsigned["signatures"] = json!({});
    let mut tampered = signed_keys();
    tampered["keys"]["curve25519:FIXTURE"] = signed_keys()["keys"]["curve25519:FIXTURE"].clone();
    let mut wrong_device = signed_keys();
    wrong_device["device_id"] = json!("OTHER");
    for payload in [
        json!({"device_keys":{}, "failures":{}}),
        response(unsigned),
        response(tampered),
        response(wrong_device),
    ] {
        let server = MockServer::start().await;
        mount(&server, whoami(), payload, 1).await;
        assert!(observe_server(&request(&server)).await.is_err());
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }
}

#[tokio::test]
async fn valid_replacement_key_and_local_mismatch_do_not_establish_continuity() {
    let server = MockServer::start().await;
    mount(&server, whoami(), response(signed_keys()), 1).await;
    let before = observe_server(&request(&server)).await.unwrap();
    server.verify().await;
    server.reset().await;
    mount(&server, whoami(), response(signed_keys()), 1).await;
    let after = observe_server(&request(&server)).await.unwrap();
    assert!(before.require_continuity(&after, &local(&before)).is_err());
    let mut changed = local(&before);
    changed.curve25519 = after.curve25519;
    assert!(before.require_continuity(&before, &changed).is_err());
}

#[tokio::test]
async fn redirect_never_forwards_credentials_or_follows_location() {
    let destination = MockServer::start().await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(307).insert_header("location", destination.uri()))
        .expect(1)
        .mount(&server)
        .await;
    assert!(observe_server(&request(&server)).await.is_err());
    assert!(destination.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn malformed_base_is_refused_before_any_authenticated_request() {
    let server = MockServer::start().await;
    for base in [
        format!("{}/unexpected", server.uri()),
        format!("{}?query=1", server.uri()),
        format!("{}#fragment", server.uri()),
        server.uri().replace("http://", "http://user:secret@"),
        "http://example.test".into(),
        "file:///tmp/r092-observer".into(),
    ] {
        let mut request = request(&server);
        request.homeserver = base;
        assert!(observe_server(&request).await.is_err());
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn oversized_response_refuses_without_key_query() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(65537)))
        .expect(1)
        .mount(&server)
        .await;
    assert!(observe_server(&request(&server)).await.is_err());
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn server_failure_does_not_retry_or_open_other_routes() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    assert!(observe_server(&request(&server)).await.is_err());
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
