//! Actual paired-store startup inside the explicitly provided mount/network sandbox.
use crate::{app::AppRuntime, config::Config};
use serde_json::json;
use std::{path::Path, sync::Arc, time::Duration};
use tracing::instrument::WithSubscriber;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex},
};

#[tokio::test]
#[ignore = "explicit frozen paired copy, network namespace and original-path mounts required"]
async fn frozen_pair_ready_and_normal_restart() {
    let path = std::path::PathBuf::from(std::env::var_os("R115_COPY_CONFIG").expect("copy only"));
    let mut config = crate::config::load(&path).unwrap();
    // Keep the configured identity and physical paths. Only external process execution
    // and network transport are replaced by bounded test endpoints.
    config.bridge.health_bind = Some("127.0.0.1:0".parse().unwrap());
    for agent in config.agents.values_mut() {
        if agent.command.is_some() {
            agent.command = Some("/usr/bin/false".into());
        }
    }
    let server = MockServer::start().await;
    crate::matrix::COPY_LOOPBACK.set(server.uri()).unwrap();
    mount(&server, &config).await;
    seed_keys(&server, &config).await;
    let pool = crate::storage::connect(&config.bridge.database)
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let tasks: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks")
        .fetch_one(&pool)
        .await
        .unwrap();
    let notices = super::history_fixture::notices(&pool).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM projection_outbox WHERE state='sending'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        5,
        "Observer uncertain notices; full outbox snapshot also remains unchanged"
    );
    for cycle in 0..2 {
        ready(&path, &config, cycle).await;
        assert_eq!(super::history_fixture::notices(&pool).await, notices);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
                .fetch_one(&pool)
                .await
                .unwrap(),
            tasks
        );
        crate::storage::recovery_eligibility::bootstrap_nonadmin(&pool, true)
            .await
            .unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workspace_claims")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert!(
        !server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.url.path().contains("/send/") || r.url.path().contains("/login"))
    );
    println!(
        "frozen_pair ready_cycles=2 normal_shutdowns=2 tasks_unchanged=true notifications_unchanged=true sends=0"
    );
    pool.close().await;
}

async fn ready(path: &Path, config: &Config, cycle: usize) {
    crate::offline_production_migration::check_runtime_discovery(&config.bridge.database)
        .expect("copy discovery");
    drop(
        crate::offline_production_migration::acquire_runtime_lock(&config.bridge.database)
            .expect("copy maintenance lock"),
    );
    let captured = crate::test_diagnostics::Capture::default();
    let started = AppRuntime::start_with_config_and_a2a_faults(
        path,
        config.clone(),
        Arc::new(crate::a2a::NoFault),
    )
    .with_subscriber(captured.subscriber())
    .await;
    for line in captured.output().lines().filter(|line| {
        line.contains("native-succession")
            || line.contains("bootstrap_nonadmin")
            || line.contains("startup-eligibility")
    }) {
        println!("{line}");
    }
    let runtime = started.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let state = runtime.health_state().snapshot().await.readiness;
            if state == crate::runtime::Readiness::Ready {
                break;
            }
            assert_ne!(
                state,
                crate::runtime::Readiness::RecoveryBlocked,
                "cycle {cycle}: {:?}",
                runtime.health_state().recovery_diagnostic_line()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    let diagnostic = runtime.health_state().recovery_diagnostic_line();
    runtime.shutdown().await.unwrap();
    assert!(result.is_ok(), "cycle {cycle}: {diagnostic:?}");
    println!("paired_copy_cycle={cycle} ready=true shutdown=true");
}

async fn mount(server: &MockServer, config: &Config) {
    for (endpoint, body) in [
        ("versions", json!({"versions":["v1.1"]})),
        (
            "v3/account/whoami",
            json!({"user_id":config.transports.matrix.user_id,"device_id":config.transports.matrix.device_id,"is_guest":false}),
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
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"one_time_key_counts":{"signed_curve25519":100}})),
        )
        .mount(server)
        .await;
    // An unchanged checkpoint and empty response prevent any historical event replay.
    let pool = crate::storage::connect(&config.bridge.database)
        .await
        .unwrap();
    let watermark: String = sqlx::query_scalar("SELECT watermark FROM matrix_recovery_control")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"next_batch":watermark}))
                .set_delay(Duration::from_millis(250)),
        )
        .mount(server)
        .await;
}

async fn seed_keys(server: &MockServer, config: &Config) {
    let client = crate::matrix::MatrixClient::restore(&config.transports.matrix)
        .await
        .unwrap();
    client
        .inner
        .encryption()
        .wait_for_e2ee_initialization_tasks()
        .await;
    let device = client
        .inner
        .encryption()
        .get_own_device()
        .await
        .unwrap()
        .unwrap();
    let public_keys = std::collections::BTreeMap::from([
        (
            format!("ed25519:{}", device.device_id()),
            device.ed25519_key().unwrap().to_base64(),
        ),
        (
            format!("curve25519:{}", device.device_id()),
            device.curve25519_key().unwrap().to_base64(),
        ),
    ]);
    let keys = json!({"user_id":device.user_id(),"device_id":device.device_id(),"algorithms":device.algorithms(),"keys":public_keys,"signatures":device.signatures()});
    let body = json!({"failures":{},"device_keys":{config.transports.matrix.user_id.clone():{config.transports.matrix.device_id.clone():keys}}});
    Mock::given(method("POST"))
        .and(path_regex("/_matrix/client/.*/keys/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}
