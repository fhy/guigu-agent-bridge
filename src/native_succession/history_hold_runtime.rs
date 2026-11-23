//! The production refusal/teardown path must leave a recoverable stream hold.
use super::*;

#[tokio::test]
async fn refused_child() {
    let Ok(root) = std::env::var("R112_HOLD_ROOT") else {
        return;
    };
    let server = std::env::var("R112_HOLD_SERVER").unwrap();
    let root = std::path::Path::new(&root);
    let result = AppRuntime::start_with_config_and_a2a_faults(
        root.join("bridge.toml"),
        Fixture::config_at(root, true, &server),
        Arc::new(crate::a2a::NoFault),
    )
    .await;
    assert!(
        matches!(result, Err(crate::app::AppError::Matrix)),
        "expected bounded fill startup failure"
    );
    // AppRuntime's failed assembly path performs owned teardown before returning.
    let pool = crate::storage::connect(root.join("state.db"))
        .await
        .unwrap();
    let held: (String, String) = sqlx::query_as(
        "SELECT restart_hold_state,restart_hold_reason FROM matrix_recovery_control",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(held, ("refused".into(), "rebuild-history-gap".into()));
    let mut c = pool.acquire().await.unwrap();
    assert!(
        super::super::state::read(&mut c)
            .await
            .unwrap()
            .unwrap()
            .history_hold_owner
    );
    drop(c);
    pool.close().await;
    println!("production_refusal=true controlled_shutdown=true");
}

#[tokio::test]
#[ignore = "explicit private systemd producer; only synthetic paired store"]
async fn history_hold_actual_refusal_ready_and_normal_restart() {
    let f = Fixture::new();
    let server = MockServer::start().await;
    mount_server(&server).await;
    initialize(&f, &server).await;
    Mock::given(method("GET")).and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("full_state", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"next_batch":"refused-next","rooms":{"join":{"!room:example.test":{"timeline":{"events":[],"limited":true,"prev_batch":"missing-anchor"}}}}})))
        .with_priority(1).up_to_n_times(1).expect(1).mount(&server).await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"errcode":"M_UNKNOWN","error":"synthetic coverage refusal"})),
        )
        .with_priority(1)
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    produce_refusal(&f, &server).await;
    recovered_server(&server).await;
    let pool = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    let mut c = pool.acquire().await.unwrap();
    let selected = super::super::state::read(&mut c).await.unwrap().unwrap();
    assert!(selected.history_hold_owner);
    assert_eq!(selected.hold.as_deref(), Some("refused"));
    drop(c);
    for _ in 0..2 {
        lifecycle::start(f.root.path(), &server.uri())
            .await
            .shutdown()
            .await
            .unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert!(history_fixture::notices(&pool).await.is_empty());
    let mut c = pool.acquire().await.unwrap();
    assert!(super::super::state::read(&mut c).await.unwrap().is_none());
    drop(c);
    assert!(
        !server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.url.path().contains("/send/") || r.url.path().contains("/login"))
    );
    pool.close().await;
    println!("refusal_then_ready=true same_db_store_restart=true no_replay=true");
}

pub(super) async fn initialize(f: &Fixture, server: &MockServer) {
    initialize_response(f, server, response("C", false, vec![anchor()])).await;
}
pub(super) async fn initialize_response(
    f: &Fixture,
    server: &MockServer,
    initial: serde_json::Value,
) {
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since", "B"))
        .and(query_param("full_state", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(initial))
        .with_priority(1)
        .up_to_n_times(1)
        .expect(1)
        .mount(server)
        .await;
    let pool = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let crate::storage::CompatibilityGate::Native(operation) =
        crate::storage::MatrixCompatibilityStore::new(pool.clone())
            .gate(true, None)
            .await
            .unwrap()
    else {
        panic!("native");
    };
    let scope = history_fixture::scope(&pool).await;
    crate::storage::MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope))
        .activate_cutover(&operation, "B", &chrono::Utc::now().to_rfc3339())
        .await
        .unwrap();
    pool.close().await;
    seed_device(f, server).await;
    let runtime = lifecycle::start(f.root.path(), &server.uri()).await;
    let pool = runtime.test_storage_pool();
    // Match the retained scenario: a fully dispositioned response precedes the
    // later failed startup. Readiness alone need not have committed that batch.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let terminal: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM matrix_recovery_receipts r JOIN matrix_recovery_batches b USING(batch_id) WHERE b.state='terminal' AND r.state='dispositioned' AND r.event_id='$anchor'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if terminal > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    drop(pool);
    runtime.shutdown().await.unwrap();
}

pub(super) fn anchor() -> serde_json::Value {
    json!({"type":"m.room.message","event_id":"$anchor","sender":"@bridge:example.test",
        "origin_server_ts":2,"content":{"msgtype":"m.notice","body":"synthetic own event"}})
}

pub(super) fn response(
    next: &str,
    limited: bool,
    events: Vec<serde_json::Value>,
) -> serde_json::Value {
    json!({"next_batch":next,"rooms":{"join":{"!room:example.test":{
    "timeline":{"events":events,"limited":limited,"prev_batch":"recover-page"},
    "state":{"events":[
        {"type":"m.room.create","state_key":"","content":{"creator":"@bridge:example.test","room_version":"10"},"sender":"@bridge:example.test","event_id":"$create","origin_server_ts":1},
        {"type":"m.room.member","state_key":"@bridge:example.test","content":{"membership":"join"},"sender":"@bridge:example.test","event_id":"$member","origin_server_ts":1}
    ]}}}}})
}

async fn recovered_server(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("full_state", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response("D", true, vec![])))
        .with_priority(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since", "D"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"next_batch":"D"}))
                .set_delay(Duration::from_secs(1)),
        )
        .with_priority(2)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"start":"recover-page","end":"done","chunk":[anchor()]})),
        )
        .mount(server)
        .await;
}

pub(super) async fn produce_refusal(f: &Fixture, server: &MockServer) {
    let unit = format!("r112-held-{}", uuid::Uuid::now_v7());
    let output = tokio::time::timeout(
        Duration::from_secs(90),
        tokio::process::Command::new("systemd-run")
            .args(["--user", "--wait", "--pipe", "--collect", "--unit", &unit])
            .arg("--property=RuntimeMaxSec=80")
            .arg(format!(
                "--setenv=R112_HOLD_ROOT={}",
                f.root.path().display()
            ))
            .arg(format!("--setenv=R112_HOLD_SERVER={}", server.uri()))
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_succession::history_runtime::hold::refused_child",
                "--nocapture",
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("production_refusal=true"));
}
