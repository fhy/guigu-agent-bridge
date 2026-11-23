#![cfg(feature = "test-support")]

#[allow(dead_code)]
#[path = "support/r084.rs"]
mod support;

#[path = "support/r092_runtime_route.rs"]
mod route;

use guigu_agent_bridge::{
    matrix::MatrixClient,
    storage::{CompatibilityGate, MatrixCompatibilityStore},
};
use matrix_sdk::{
    Client, SessionMeta, SessionTokens, authentication::matrix::MatrixSession, config::SyncSettings,
};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex, query_param},
};

async fn mount_server(server: &MockServer) {
    for (endpoint, body) in [
        ("versions", json!({"versions":["v1.1"]})),
        (
            "v3/account/whoami",
            json!({"user_id":"@bridge:example.test","device_id":"R084","is_guest":false}),
        ),
    ] {
        Mock::given(method("GET"))
            .and(path_regex(format!("/_matrix/client/{endpoint}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path_regex("/_matrix/client/.*/keys/upload"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"one_time_key_counts":{}})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"next_batch":"B"})))
        .with_priority(10)
        .mount(server)
        .await;
    Mock::given(method("GET")).and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since","B")).and(query_param("full_state","true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"next_batch":"C","rooms":{"join":{"!room:example.test":{"timeline":{"events":[],"limited":false},"state":{"events":[
            {"type":"m.room.create","state_key":"","content":{"creator":"@bridge:example.test","room_version":"10"},"sender":"@bridge:example.test","event_id":"$create","origin_server_ts":1},
            {"type":"m.room.member","state_key":"@bridge:example.test","content":{"membership":"join"},"sender":"@bridge:example.test","event_id":"$member","origin_server_ts":1},
            {"type":"m.room.name","state_key":"","content":{"name":"Recovery room"},"sender":"@bridge:example.test","event_id":"$name","origin_server_ts":1}
        ]}}}}}))).with_priority(1).mount(server).await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since", "C"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"next_batch":"C"}))
                .set_delay(std::time::Duration::from_secs(1)),
        )
        .with_priority(1)
        .mount(server)
        .await;
}

async fn seed_device(fixture: &support::Fixture, server: &MockServer) {
    drop(
        MatrixClient::restore(&fixture.config(true, &server.uri()).transports.matrix)
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
                user_id: "@bridge:example.test".parse().unwrap(),
                device_id: "R084".into(),
            },
            tokens: SessionTokens {
                access_token: "synthetic-no-network-token".into(),
                refresh_token: None,
            },
        })
        .await
        .unwrap();
    client.sync_once(SyncSettings::new()).await.unwrap();
    client
        .encryption()
        .wait_for_e2ee_initialization_tasks()
        .await;
    let upload = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .find(|request| request.url.path().ends_with("/keys/upload"))
        .unwrap();
    let upload: serde_json::Value = serde_json::from_slice(&upload.body).unwrap();
    Mock::given(method("POST")).and(path_regex("/_matrix/client/.*/keys/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"failures":{},"device_keys":{"@bridge:example.test":{"R084":upload["device_keys"]}}})))
        .with_priority(1).mount(server).await;
}

#[tokio::test]
async fn real_app_runtime_activates_bound_native_operation_and_processes_full_state_from_b() {
    let fixture = support::Fixture::new().await;
    let server = MockServer::start().await;
    mount_server(&server).await;
    let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(fixture.pool.clone())
        .gate(true, Some(&fixture.store()))
        .await
        .unwrap()
    else {
        panic!("native")
    };
    let hold: Option<String> = sqlx::query_scalar(
        "SELECT restart_hold_state FROM matrix_recovery_control WHERE control_id=1",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(hold, None);
    seed_device(&fixture, &server).await;
    let mut config = fixture.config(true, &server.uri());
    config.agents.get_mut("worker").unwrap().enabled = false;
    let runtime = fixture.start(config).await;
    let bootstrap: String =
        sqlx::query_scalar("SELECT state FROM runtime_eligibility_bootstrap WHERE singleton=1")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(bootstrap, "complete");
    assert_eq!(fixture.count("runtime_eligibility_context").await, 1);
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let state: (String, String, Option<String>) = sqlx::query_as(
                "SELECT cutover_operation_id,phase,watermark FROM matrix_recovery_control",
            )
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
            assert_eq!(state.0, operation);
            if state.2.as_deref() == Some("C") {
                assert_eq!(state.1, "active");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fixture.count("matrix_recovery_receipts").await, 0);
    let leases: Vec<(String, i64)> =
        sqlx::query_as("SELECT component,epoch FROM matrix_component_leases ORDER BY component")
            .fetch_all(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(
        leases,
        vec![("MatrixSync".into(), 1), ("OutboxDrain".into(), 1)]
    );
    runtime.shutdown().await.unwrap();
    let requests = server.received_requests().await.unwrap();
    assert!(requests.iter().any(|request| {
        request
            .url
            .query_pairs()
            .any(|pair| pair.0 == "since" && pair.1 == "B")
            && request
                .url
                .query_pairs()
                .any(|pair| pair.0 == "full_state" && pair.1 == "true")
    }));
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path().contains("login")
                || request.url.path().ends_with("/messages"))
    );
}

// Exhaust the SAME pool used by metrics, the owner heartbeat and MatrixSync.
// This exercises cancellation of pool acquisition, not executing SQLite SQL.
#[tokio::test]
async fn slow_metrics_pool_wait_preserves_runtime_heartbeat_and_matrix_lease() {
    use std::time::{Duration, Instant};

    let fixture = support::Fixture::new().await;
    let server = MockServer::start().await;
    mount_server(&server).await;
    assert!(matches!(
        MatrixCompatibilityStore::new(fixture.pool.clone())
            .gate(true, Some(&fixture.store()))
            .await
            .unwrap(),
        CompatibilityGate::Native(_)
    ));
    seed_device(&fixture, &server).await;
    let mut config = fixture.config(true, &server.uri());
    config.agents.get_mut("worker").unwrap().enabled = false;
    let runtime = fixture.start(config).await;
    let pool = runtime.test_storage_pool();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let url = format!("http://{}/metrics", runtime.health_addr().unwrap());
    let before: (String, i64) = sqlx::query_as(
        "SELECT instance_token,evidence_revision FROM runtime_owner_evidence WHERE is_current=1",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    let initial: (String, i64, String) = sqlx::query_as(
        "SELECT owner_token,epoch,updated_at FROM matrix_component_leases WHERE component='MatrixSync'",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();

    let mut timings = Vec::new();
    for _ in 0..3 {
        let mut held = Vec::new();
        for _ in 0..guigu_agent_bridge::storage::MAX_CONNECTIONS {
            held.push(pool.acquire().await.unwrap());
        }
        let started = Instant::now();
        let response = client.get(&url).send().await.unwrap();
        assert!(response.status().is_success());
        let metrics = response.text().await.unwrap();
        timings.push(started.elapsed());
        assert!(metrics.contains("readiness_storage_available 0"));
        assert!(metrics.contains("predicate=\"storage-unavailable\""));
        drop(held);
        // Leave renewal work a normal scheduling opportunity between bursts.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let (after_revision, after_update) = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let owner: (String, i64, String) = sqlx::query_as(
                "SELECT instance_token,evidence_revision,state FROM runtime_owner_evidence WHERE is_current=1",
            )
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
            let lease: (String, i64, String, String, bool) = sqlx::query_as(
                "SELECT owner_token,epoch,updated_at,state,julianday(expires_at)>julianday('now') FROM matrix_component_leases WHERE component='MatrixSync'",
            )
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
            assert_eq!(owner.0, before.0);
            assert_eq!((&lease.0, lease.1), (&initial.0, initial.1));
            assert_eq!(lease.3, "active");
            assert!(lease.4, "the real component lease must not expire");
            assert_ne!(owner.2, "unknown");
            if owner.1 > before.1 && lease.2 > initial.2 {
                break (owner.1, lease.2);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("real heartbeat and MatrixSync renewal must both advance");
    let recovered = client.get(&url).send().await.unwrap().text().await.unwrap();
    assert!(recovered.contains("readiness_storage_available 1"));
    assert_eq!(
        runtime.health_state().snapshot().await.readiness,
        guigu_agent_bridge::runtime::Readiness::Ready
    );
    runtime.shutdown().await.unwrap();
    for elapsed in &timings {
        assert!(*elapsed >= Duration::from_millis(900), "{elapsed:?}");
        assert!(*elapsed < Duration::from_secs(3), "{elapsed:?}");
    }
    eprintln!(
        "pool_wait_ms={:?}; owner_revision={}->{}; sync_epoch={}; sync_updated={}->{}",
        timings.iter().map(Duration::as_millis).collect::<Vec<_>>(),
        before.1,
        after_revision,
        initial.1,
        initial.2,
        after_update
    );
}
