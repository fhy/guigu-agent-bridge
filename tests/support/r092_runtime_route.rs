use super::{seed_device, support};
use guigu_agent_bridge::{
    app::AppRuntime,
    config::Config,
    storage::{CompatibilityGate, MatrixCompatibilityStore},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path_regex, query_param},
};

#[path = "r084_crypto.rs"]
mod crypto;

#[path = "r092_reply_crypto.rs"]
mod receiver;

async fn snapshot(fixture: &support::Fixture, stage: &str) -> String {
    let authority: (String, i64, String, Option<String>) = sqlx::query_as(
        "SELECT matrix_phase,revision,state,expected_b_digest FROM runtime_eligibility_context",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap_or(("absent".into(), -1, "absent".into(), None));
    let control: Option<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT phase,watermark,restart_hold_state FROM matrix_recovery_control")
            .fetch_optional(&fixture.pool)
            .await
            .unwrap();
    let mut counts = Vec::new();
    for table in [
        "runtime_instances",
        "conversations",
        "matrix_component_leases",
        "matrix_recovery_receipts",
        "transport_receipts",
        "tasks",
        "task_events",
        "acp_output_sets",
        "offline_recovery_rows",
    ] {
        counts.push((table, fixture.count(table).await));
    }
    let receipts: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT state,refusal_code FROM matrix_recovery_receipts")
            .fetch_all(&fixture.pool)
            .await
            .unwrap();
    let task_events: Vec<(String, String)> =
        sqlx::query_as("SELECT status,payload FROM task_events ORDER BY timestamp,seq")
            .fetch_all(&fixture.pool)
            .await
            .unwrap();
    let batches: Vec<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT batch_id,response_token,state,expected_events_json,covered_events_json FROM matrix_recovery_batches ORDER BY created_at,batch_id",
    )
    .fetch_all(&fixture.pool)
    .await
    .unwrap();
    format!(
        "stage={stage} authority={authority:?} control={control:?} batches={batches:?} receipts={receipts:?} counts={counts:?} task_events={task_events:?}"
    )
}

async fn terminal_coverage(fixture: &support::Fixture, watermark: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM matrix_recovery_control WHERE phase='active' AND watermark=? AND restart_hold_state IS NULL) AND EXISTS(SELECT 1 FROM matrix_recovery_batches WHERE response_token=? AND state='terminal') AND NOT EXISTS(SELECT 1 FROM matrix_recovery_batches WHERE state<>'terminal') AND EXISTS(SELECT 1 FROM matrix_recovery_receipts WHERE state='dispositioned') AND NOT EXISTS(SELECT 1 FROM matrix_recovery_receipts WHERE state<>'dispositioned')",
    )
    .bind(watermark)
    .bind(watermark)
    .fetch_one(&fixture.pool)
    .await
    .unwrap()
}

async fn encrypted_sends(server: &MockServer, seed_requests: usize) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .skip(seed_requests)
        .filter(|request| request.url.path().contains("/send/m.room.encrypted/"))
        .count()
}

async fn wait_reply_and_coverage(
    fixture: &support::Fixture,
    server: &MockServer,
    seed_requests: usize,
    runtime: &AppRuntime,
) {
    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            let terminal: i64 =
                sqlx::query_scalar("SELECT count(*) FROM task_admissions WHERE state='terminal'")
                    .fetch_one(&fixture.pool)
                    .await
                    .unwrap();
            if terminal == 1
                && encrypted_sends(server, seed_requests).await > 0
                && terminal_coverage(fixture, "C").await
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "{} health={:?}",
        snapshot(fixture, "message-reply-terminal-coverage").await,
        runtime.health_state().snapshot().await
    );
}

fn config(fixture: &support::Fixture, server: &MockServer) -> Config {
    let mut config = fixture.config(true, &server.uri());
    config.transports.matrix.allowed_users = vec!["@alice:example.test".into()];
    config
        .transports
        .matrix
        .routes
        .rooms
        .insert("!room:example.test".into(), "worker".into());
    let backend =
        std::path::PathBuf::from(std::env::var_os("CARGO_TARGET_DIR").expect("shared target"))
            .join("debug/examples/acp-mock-backend");
    assert!(
        backend.is_file(),
        "build the existing ACP mock before this target"
    );
    let worker = config.agents.get_mut("worker").unwrap();
    worker.command = Some(backend.to_str().unwrap().into());
    worker.args = vec!["happy".into()];
    config
}

async fn mount_route(fixture: &support::Fixture, server: &MockServer) -> Arc<AtomicBool> {
    crypto::mount_identity(server).await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"next_batch":"B"})))
        .with_priority(10)
        .mount(server)
        .await;
    let (ciphertext, _) = crypto::seed_key(fixture, server).await;
    seed_device(fixture, server).await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since", "B"))
        .and(query_param("full_state", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(crypto::response("C", ciphertext.clone())),
        )
        .with_priority(1)
        .mount(server)
        .await;
    let restarted = Arc::new(AtomicBool::new(false));
    let replay = restarted.clone();
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since", "C"))
        .respond_with(move |_: &Request| {
            let body = if replay.load(Ordering::Acquire) {
                crypto::response("D", ciphertext.clone())
            } else {
                json!({"next_batch":"C"})
            };
            ResponseTemplate::new(200)
                .set_body_json(body)
                .set_delay(std::time::Duration::from_millis(500))
        })
        .with_priority(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since", "D"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"next_batch":"D"}))
                .set_delay(std::time::Duration::from_millis(500)),
        )
        .with_priority(1)
        .mount(server)
        .await;
    Mock::given(method("GET")).and(path_regex("/_matrix/client/.*/rooms/.*/members"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"chunk":[{"type":"m.room.member","state_key":"@bridge:example.test","sender":"@bridge:example.test","event_id":"$bridge-member","content":{"membership":"join"}}]})))
        .mount(server).await;
    Mock::given(method("PUT"))
        .and(path_regex(
            "/_matrix/client/.*/rooms/.*/send/m.room.encrypted/.*",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"event_id":"$synthetic-reply"})),
        )
        .mount(server)
        .await;
    restarted
}

async fn start(fixture: &support::Fixture, config: Config, stage: &str) -> AppRuntime {
    let result = AppRuntime::start_with_config_and_a2a_faults(
        fixture.root.path().join("bridge.toml"),
        config,
        Arc::new(guigu_agent_bridge::a2a::NoFault),
    )
    .await;
    match result {
        Ok(runtime) => runtime,
        Err(error) => panic!("{error:?}; {}", snapshot(fixture, stage).await),
    }
}

async fn protected_fixture() -> support::Fixture {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("guigu_agent_bridge=warn")
        .with_test_writer()
        .try_init();
    let root = tempfile::Builder::new()
        .prefix("r092-native-route-")
        .tempdir_in(std::env::var_os("HOME").expect("HOME"))
        .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    for ancestor in root.path().ancestors() {
        let metadata = std::fs::symlink_metadata(ancestor).unwrap();
        assert!(metadata.is_dir() && !metadata.file_type().is_symlink());
        let forbidden = if ancestor == root.path() {
            0o077
        } else {
            0o022
        };
        assert_eq!(metadata.permissions().mode() & forbidden, 0);
    }
    std::fs::write(
        root.path().join("bridge.toml"),
        "synthetic R092 startup identity",
    )
    .unwrap();
    let pool = guigu_agent_bridge::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    guigu_agent_bridge::storage::migrate(&pool).await.unwrap();
    support::Fixture { root, pool }
}

#[tokio::test]
async fn encrypted_message_reply_and_restart_share_native_authority_without_replay() {
    let fixture = protected_fixture().await;
    eprintln!("R092 phase=synthetic-db-ready");
    let server = MockServer::start().await;
    assert!(matches!(
        MatrixCompatibilityStore::new(fixture.pool.clone())
            .gate(true, Some(&fixture.store()))
            .await
            .unwrap(),
        CompatibilityGate::Native(_)
    ));
    let restarted = mount_route(&fixture, &server).await;
    eprintln!("R092 phase=synthetic-device-seeded");
    let seed_requests = server.received_requests().await.unwrap().len();
    let config = config(&fixture, &server);
    sqlx::query("CREATE TRIGGER r092_interrupt_cleanup BEFORE UPDATE OF state ON acp_output_sets WHEN NEW.state='cleaned' BEGIN SELECT RAISE(ABORT,'synthetic-cleanup-interruption'); END")
        .execute(&fixture.pool).await.unwrap();
    let runtime = start(&fixture, config.clone(), "startup").await;
    eprintln!("R092 phase=runtime-started");
    wait_reply_and_coverage(&fixture, &server, seed_requests, &runtime).await;
    eprintln!("{}", snapshot(&fixture, "first-shutdown-begin").await);
    runtime.shutdown().await.unwrap();
    eprintln!("{}", snapshot(&fixture, "first-shutdown-complete").await);
    assert!(terminal_coverage(&fixture, "C").await);
    assert_eq!(cleanup_state(&fixture).await, "cleaning");
    sqlx::query("DROP TRIGGER r092_interrupt_cleanup")
        .execute(&fixture.pool)
        .await
        .unwrap();
    receiver::assert_reply_decrypts(
        fixture.root.path(),
        &fixture.store(),
        &server,
        seed_requests,
    )
    .await;
    eprintln!("R092 phase=synthetic-peer-decrypted");
    let sends = encrypted_sends(&server, seed_requests).await;
    assert_eq!(fixture.count("tasks").await, 1);
    disabled_restart(&fixture, &server, config.clone()).await;
    restarted.store(true, Ordering::Release);
    assert_replay(&fixture, &server, config, seed_requests, sends).await;
}

async fn cleanup_state(fixture: &support::Fixture) -> String {
    sqlx::query_scalar("SELECT state FROM acp_output_sets")
        .fetch_one(&fixture.pool)
        .await
        .unwrap()
}

async fn disabled_restart(fixture: &support::Fixture, server: &MockServer, mut config: Config) {
    let requests = server.received_requests().await.unwrap().len();
    config.transports.matrix.enabled = false;
    let runtime = start(fixture, config, "disabled-restart").await;
    assert_eq!(cleanup_state(fixture).await, "cleaning");
    let control: (String, String) =
        sqlx::query_as("SELECT phase,watermark FROM matrix_recovery_control")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(control, ("active".into(), "C".into()));
    runtime.shutdown().await.unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), requests);
    eprintln!("R092 phase=disabled-restart-no-network");
}

async fn assert_replay(
    fixture: &support::Fixture,
    server: &MockServer,
    config: Config,
    seed_requests: usize,
    sends: usize,
) {
    let runtime = start(fixture, config, "restart").await;
    eprintln!("R092 phase=runtime-restarted");
    let advanced = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if terminal_coverage(fixture, "D").await {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        advanced.is_ok(),
        "{}",
        snapshot(fixture, "restart-replay").await
    );
    eprintln!("{}", snapshot(fixture, "restart-shutdown-begin").await);
    runtime.shutdown().await.unwrap();
    eprintln!("{}", snapshot(fixture, "restart-shutdown-complete").await);
    assert!(terminal_coverage(fixture, "D").await);
    assert_eq!(cleanup_state(fixture).await, "cleaned");
    assert_eq!(fixture.count("tasks").await, 1);
    assert_eq!(fixture.count("matrix_recovery_receipts").await, 1);
    assert_eq!(encrypted_sends(server, seed_requests).await, sends);
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .skip(seed_requests)
            .all(|request| !request.url.path().contains("/send/m.room.message/"))
    );
}
