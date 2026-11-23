use super::*;
use std::sync::atomic::Ordering;

pub(super) async fn execute(
    scenario: &str,
    fixture: &Fixture,
    executable: &std::path::Path,
    manager: &PrivateManager,
    server: &MockServer,
) {
    let baseline = online::mount(fixture, server, scenario == "ambiguous").await;
    if scenario == "identity" {
        Mock::given(method("GET"))
            .and(path("/_matrix/client/v3/account/whoami"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "user_id":"@fixture:example.test","device_id":"FIXTURE","is_guest":true
            })))
            .with_priority(1)
            .mount(server)
            .await;
    }
    let crash = match scenario {
        "prepared" => Some("administrative-after-prepared"),
        "committed" => Some("administrative-after-commit"),
        "ambiguous" | "identity" => None,
        _ => panic!("unknown rooted scenario"),
    };
    let output = startup(fixture, executable, manager, crash).await;
    assert_eq!(
        output.status.code(),
        Some(if crash.is_some() { 86 } else { 1 }),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let expected = match scenario {
        "prepared" => ("activation_prepared", "baseline", None),
        "committed" => ("active", "active", Some("administrative-baseline")),
        "ambiguous" => ("baseline_inflight", "baseline", None),
        "identity" => ("baseline_ready", "baseline", None),
        _ => unreachable!(),
    };
    assert_state(fixture, expected).await;
    assert_eq!(
        baseline.load(Ordering::SeqCst),
        usize::from(scenario != "identity")
    );
    if crash.is_none() {
        let request_count = server.received_requests().await.unwrap().len();
        let retry = startup(fixture, executable, manager, None).await;
        assert_eq!(retry.status.code(), Some(1));
        assert_state(fixture, expected).await;
        if scenario == "ambiguous" {
            assert_eq!(
                server.received_requests().await.unwrap().len(),
                request_count
            );
        }
        return;
    }
    wait_expiry(fixture).await;
    let first = online::run(fixture, executable, manager, server, false).await;
    let second = online::run(fixture, executable, manager, server, true).await;
    assert_eq!(first, second);
    assert_eq!(baseline.load(Ordering::SeqCst), 1);
    online::verify_clean(fixture).await;
}

async fn startup(
    fixture: &Fixture,
    executable: &std::path::Path,
    manager: &PrivateManager,
    crash: Option<&str>,
) -> std::process::Output {
    let child_executable = crash.map(|_| std::env::current_exe().unwrap());
    let mut command =
        tokio::process::Command::new(child_executable.as_deref().unwrap_or(executable));
    if let Some(stage) = crash {
        command.args(["--exact", "offline_production_migration::identity_capture::tests::failures::administrative_runtime_child_entry", "--test-threads=1"])
            .env("R092_ACTIVE_CHILD_CONFIG", &fixture.request.config)
            .env("R071_TEST_CRASH", stage);
    } else {
        command
            .arg(&fixture.request.config)
            .env_remove("R071_TEST_CRASH");
    }
    command
        .env("R092_IDENTITY_TOKEN", "synthetic-secret")
        .env("XDG_RUNTIME_DIR", &manager.runtime)
        .env("DBUS_SESSION_BUS_ADDRESS", &manager.bus)
        .kill_on_drop(true);
    tokio::time::timeout(std::time::Duration::from_secs(90), command.output())
        .await
        .unwrap()
        .unwrap()
}

async fn assert_state(fixture: &Fixture, expected: (&str, &str, Option<&str>)) {
    let mut connection = verification::open(&fixture.request.database, true)
        .await
        .unwrap();
    let state: (String, String, Option<String>) = sqlx::query_as(
        "SELECT b.phase,c.phase,c.watermark FROM offline_recovery_binding b CROSS JOIN matrix_recovery_control c")
        .fetch_one(&mut connection).await.unwrap();
    assert_eq!(
        (state.0.as_str(), state.1.as_str(), state.2.as_deref()),
        expected
    );
    let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_instances")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(owners, 0);
    connection.close().await.unwrap();
}

async fn wait_expiry(fixture: &Fixture) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(75);
    loop {
        let mut connection = verification::open(&fixture.request.database, true)
            .await
            .unwrap();
        let live: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_component_leases WHERE state='active' AND julianday(expires_at)>julianday('now')")
            .fetch_one(&mut connection).await.unwrap();
        connection.close().await.unwrap();
        if live == 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "crashed lease failed to expire"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
