#![cfg(feature = "test-support")]

use guigu_agent_bridge::{
    matrix::{MatrixClient, MatrixSync, SqliteSyncTokenStore},
    storage::{CompatibilityGate, MatrixCompatibilityStore, MatrixRecoveryStore},
};
use serde_json::json;
use std::sync::Arc;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex, query_param},
};

#[path = "support/r084_crypto.rs"]
mod crypto;
#[allow(dead_code)]
#[path = "support/r084.rs"]
mod support;
use crypto::{mount_identity, response, seed_key};

async fn wait_token(pool: &sqlx::SqlitePool, expected: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let token: Option<String> =
                sqlx::query_scalar("SELECT watermark FROM matrix_recovery_control")
                    .fetch_one(pool)
                    .await
                    .unwrap();
            if token.as_deref() == Some(expected) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn real_ciphertext_drives_receipt_then_disposition_checkpoint_and_reopen_dedup() {
    let fixture = support::Fixture::new().await;
    let server = MockServer::start().await;
    mount_identity(&server).await;
    let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(fixture.pool.clone())
        .gate(true, Some(&fixture.store()))
        .await
        .unwrap()
    else {
        panic!("native")
    };
    let (event, _public_key) = seed_key(&fixture, &server).await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since", "B"))
        .and(query_param("full_state", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response("C", event.clone())))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since", "C"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(response("D", event))
                .set_delay(std::time::Duration::from_millis(500)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since", "D"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"next_batch":"D"}))
                .set_delay(std::time::Duration::from_secs(1)),
        )
        .mount(&server)
        .await;
    let recovery = MatrixRecoveryStore::new(fixture.pool.clone());
    let now = chrono::Utc::now();
    let now_text = now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    recovery
        .activate_cutover(&operation, "B", &now_text)
        .await
        .unwrap();
    let epoch = recovery
        .acquire_component_lease(
            "MatrixSync",
            "receiver",
            &now_text,
            &(now + chrono::Duration::seconds(60))
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
        .await
        .unwrap();
    let client = MatrixClient::restore(&fixture.config(true, &server.uri()).transports.matrix)
        .await
        .unwrap();
    let sync = MatrixSync::new(
        client,
        Arc::new(SqliteSyncTokenStore::new(fixture.pool.clone())),
        4,
    )
    .unwrap()
    .with_recovery(recovery, "@bridge:example.test", &operation, epoch)
    .with_component_owner("receiver".into());
    let (handle, mut events, dispositions) = sync.start_with_dispositions();
    let event = tokio::time::timeout(std::time::Duration::from_secs(10), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.event_id, "$cipher");
    assert_eq!(event.sender, "@alice:example.test");
    assert_eq!(event.body, "R084 real encrypted admission");
    assert_eq!(fixture.count("matrix_recovery_receipts").await, 1);
    let before: String = sqlx::query_scalar("SELECT watermark FROM matrix_recovery_control")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(before, "B");
    dispositions.send(Ok(())).await.unwrap();
    wait_token(&fixture.pool, "C").await;
    handle.shutdown().await.unwrap();
    let now = chrono::Utc::now();
    let now_text = now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    sqlx::query("UPDATE matrix_component_leases SET state='expired'")
        .execute(&fixture.pool)
        .await
        .unwrap();
    let recovery = MatrixRecoveryStore::new(fixture.pool.clone());
    let epoch = recovery
        .acquire_component_lease(
            "MatrixSync",
            "receiver-reopened",
            &now_text,
            &(now + chrono::Duration::seconds(60))
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
        .await
        .unwrap();
    let reopened = MatrixClient::restore(&fixture.config(true, &server.uri()).transports.matrix)
        .await
        .unwrap();
    assert_eq!(reopened.device_id(), "R084");
    let sync = MatrixSync::new(
        reopened,
        Arc::new(SqliteSyncTokenStore::new(fixture.pool.clone())),
        4,
    )
    .unwrap()
    .with_recovery(recovery, "@bridge:example.test", &operation, epoch)
    .with_component_owner("receiver-reopened".into());
    let (handle, mut events, _dispositions) = sync.start_with_dispositions();
    wait_token(&fixture.pool, "D").await;
    assert!(events.try_recv().is_err());
    assert_eq!(fixture.count("matrix_recovery_receipts").await, 1);
    handle.shutdown().await.unwrap();
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| !request.url.path().contains("/messages")
                && !request.url.path().ends_with("/login"))
    );
}
