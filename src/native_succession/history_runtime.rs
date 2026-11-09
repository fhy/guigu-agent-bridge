//! Disposable loopback runtime and separately scoped producer process.
#[path = "history_effects_tests.rs"]
mod effects;
use super::history_fixture;
use crate::{
    app::AppRuntime,
    config::{Config, load_from_str_with_env},
    matrix::MatrixClient,
};
use matrix_sdk::{
    Client, SessionMeta, SessionTokens, authentication::matrix::MatrixSession, config::SyncSettings,
};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex, query_param},
};

struct Fixture {
    root: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("r114-runtime-")
            .tempdir_in(std::env::var_os("HOME").expect("HOME"))
            .unwrap();
        std::fs::set_permissions(
            root.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        std::fs::write(
            root.path().join("bridge.toml"),
            "fixture immutable config identity",
        )
        .unwrap();
        for sub in ["crypto", "sessions", "workspace"] {
            std::fs::create_dir(root.path().join(sub)).unwrap();
            std::fs::set_permissions(
                root.path().join(sub),
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
        }
        Self { root }
    }
    fn store(&self) -> std::path::PathBuf {
        self.root.path().join("crypto")
    }
    fn config(&self, enabled: bool, server: &str) -> Config {
        let env = BTreeMap::from([
            ("HOME".into(), self.root.path().display().to_string()),
            ("R114_TOKEN".into(), "synthetic-no-network-token".into()),
        ]);
        load_from_str_with_env(
            &format!(
                r#"
[bridge]
database = {:?}
session_root = {:?}
health_bind = "127.0.0.1:0"
shutdown_timeout_seconds = 5
[agents.worker]
transport = "acp"
command = "must-not-start"
workspace = {:?}
enabled = false
[transports.matrix]
enabled = {enabled}
homeserver = {server:?}
user_id = "@bridge:example.test"
device_id = "R084"
device_trusted = true
access_token = "{{env:R114_TOKEN}}"
crypto_store_path = {:?}
"#,
                self.root.path().join("state.db"),
                self.root.path().join("sessions"),
                self.root.path().join("workspace"),
                self.store()
            ),
            &env,
        )
        .unwrap()
    }
}

#[tokio::test]
async fn producer_child() {
    let Ok(root) = std::env::var("R114_FIXTURE_ROOT") else {
        return;
    };
    let pool = history_fixture::seed(&std::path::Path::new(&root).join("state.db")).await;
    pool.close().await;
}

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

async fn seed_device(fixture: &Fixture, server: &MockServer) {
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
#[ignore = "requires private user systemd scope; run explicitly for R114 runtime acceptance"]
async fn mixed_history_real_startup_and_three_clean_cycles() {
    run_mixed_history(false).await;
}

#[tokio::test]
#[ignore = "requires private user systemd scope; run explicitly for R115"]
async fn previous_boot_mixed_history_real_startup_and_restart() {
    run_mixed_history(true).await;
}

async fn run_mixed_history(previous_boot: bool) {
    let f = Fixture::new();
    let server = MockServer::start().await;
    mount_server(&server).await;
    produce(&f).await;
    let pool = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    if previous_boot {
        super::history_boot_tests::previous_boot(&pool).await;
    }
    let before = history_fixture::notices(&pool).await;
    assert_eq!(before.len(), 5);
    refuses_history_drift(&f, &server, &pool).await;
    seed_device(&f, &server).await;
    for cycle in 0..3 {
        let runtime = AppRuntime::start_with_config_and_a2a_faults(
            f.root.path().join("bridge.toml"),
            f.config(true, &server.uri()),
            Arc::new(crate::a2a::NoFault),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                if runtime.health_state().snapshot().await.readiness
                    == crate::runtime::Readiness::Ready
                {
                    break;
                }
                assert_ne!(
                    runtime.health_state().snapshot().await.readiness,
                    crate::runtime::Readiness::RecoveryBlocked,
                    "cycle {cycle}: {:?}",
                    runtime.health_state().recovery_diagnostic_line()
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        runtime.shutdown().await.unwrap();
        if previous_boot {
            super::history_boot_tests::check_record(&pool).await;
        }
        if cycle == 0 {
            effects::refuse_completed_history_effects(&server, &pool).await;
        }
        assert_eq!(history_fixture::notices(&pool).await, before);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
                .fetch_one(&pool)
                .await
                .unwrap(),
            14
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM runtime_owner_evidence WHERE is_current=1"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            13 + cycle
        );
    }
    let requests = server.received_requests().await.unwrap();
    assert!(
        !requests
            .iter()
            .any(|r| r.url.path().contains("/send/") || r.url.path().contains("/login"))
    );
    pool.close().await;
}

async fn produce(f: &Fixture) {
    let unit = format!("r114-fixture-{}", uuid::Uuid::now_v7());
    let output = tokio::process::Command::new("systemd-run")
        .args(["--user", "--wait", "--pipe", "--collect", "--unit", &unit])
        .arg(format!(
            "--setenv=R114_FIXTURE_ROOT={}",
            f.root.path().display()
        ))
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "native_succession::history_runtime::producer_child",
            "--nocapture",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "producer {:?}: {} {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn refuses_history_drift(f: &Fixture, server: &MockServer, pool: &sqlx::SqlitePool) {
    let saved: String = sqlx::query_scalar("SELECT child_snapshot FROM runtime_owner_evidence WHERE instance_token='controlled-0' AND is_current=1").fetch_one(pool).await.unwrap();
    sqlx::query("UPDATE runtime_owner_evidence SET child_snapshot=json_set(child_snapshot,'$.epoch',0) WHERE instance_token='controlled-0' AND is_current=1").execute(pool).await.unwrap();
    let before = history_fixture::snapshot(pool).await;
    let requests = server.received_requests().await.unwrap().len();
    let runtime = AppRuntime::start_with_config_and_a2a_faults(
        f.root.path().join("bridge.toml"),
        f.config(true, &server.uri()),
        Arc::new(crate::a2a::NoFault),
    )
    .await
    .unwrap();
    assert_eq!(
        runtime.health_state().snapshot().await.readiness,
        crate::runtime::Readiness::RecoveryBlocked
    );
    runtime.shutdown().await.unwrap();
    assert_eq!(history_fixture::snapshot(pool).await, before);
    assert_eq!(server.received_requests().await.unwrap().len(), requests);
    sqlx::query("UPDATE runtime_owner_evidence SET child_snapshot=? WHERE instance_token='controlled-0' AND is_current=1").bind(saved).execute(pool).await.unwrap();
}
