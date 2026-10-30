use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use wiremock::matchers::path_regex;

#[derive(Clone)]
struct Baseline {
    database: std::path::PathBuf,
    inflight_requests: Arc<AtomicUsize>,
    ambiguous: bool,
}

impl wiremock::Respond for Baseline {
    fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
        let connection =
            crate::offline_production_migration::discovery_sqlite::ReadOnly::open(&self.database)
                .unwrap();
        let phase = connection
            .rows(c"SELECT phase FROM offline_recovery_binding WHERE singleton=1")
            .unwrap();
        let token = if phase == vec![vec!["baseline_inflight".to_owned()]] {
            assert_eq!(
                connection
                    .rows(c"SELECT state FROM matrix_activation_intent")
                    .unwrap(),
                vec![vec!["inflight".to_owned()]]
            );
            assert_eq!(
                connection
                    .rows(c"SELECT CAST(count(*) AS TEXT) FROM runtime_instances")
                    .unwrap(),
                vec![vec!["0".to_owned()]]
            );
            self.inflight_requests.fetch_add(1, Ordering::SeqCst);
            if self.ambiguous {
                return ResponseTemplate::new(500);
            }
            "administrative-baseline"
        } else {
            assert_eq!(phase, vec![vec!["active".to_owned()]]);
            "native-C"
        };
        ResponseTemplate::new(200)
            .set_body_json(serde_json::json!({
                "next_batch":token,"rooms":{"join":{},"invite":{},"leave":{}},
                "presence":{"events":[]},"account_data":{"events":[]},"to_device":{"events":[]},
                "device_lists":{"changed":[],"left":[]},"device_one_time_keys_count":{}
            }))
            .set_delay(std::time::Duration::from_millis(100))
    }
}

pub(super) async fn activate_and_restart(
    fixture: &Fixture,
    executable: &std::path::Path,
    manager: &PrivateManager,
    server: &MockServer,
) {
    crate::offline_production_migration::administrative_startup::files(&fixture.request.database)
        .expect("rooted files must validate before production runtime entry");
    if let Err(error) =
        crate::offline_production_migration::check_runtime_discovery(&fixture.request.database)
    {
        let diagnostic = snapshot(fixture, server, 0).await;
        let mut connection = verification::open(&fixture.request.database, true)
            .await
            .unwrap();
        let sentinel: Vec<String> =
            sqlx::query_scalar("SELECT state FROM runtime_eligibility_bootstrap")
                .fetch_all(&mut connection)
                .await
                .unwrap();
        connection.close().await.unwrap();
        panic!(
            "read-only administrative startup discovery: {error}; sentinel={sentinel:?}; {diagnostic}"
        );
    }
    let baseline = mount(fixture, server, false).await;
    let first = run(fixture, executable, manager, server, false).await;
    assert_eq!(baseline.load(Ordering::SeqCst), 1);
    let second = run(fixture, executable, manager, server, true).await;
    assert_eq!(
        first, second,
        "restart must retain exact activation attempt"
    );
    assert_eq!(
        baseline.load(Ordering::SeqCst),
        1,
        "restart must not rebaseline"
    );
    verify_clean(fixture).await;
    super::binding::resume(fixture, executable, manager).await;
}

pub(super) async fn mount(
    fixture: &Fixture,
    server: &MockServer,
    ambiguous: bool,
) -> Arc<AtomicUsize> {
    let baseline = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(Baseline {
            database: fixture.request.database.clone(),
            inflight_requests: baseline.clone(),
            ambiguous,
        })
        .with_priority(1)
        .mount(server)
        .await;
    baseline
}

pub(super) async fn verify_clean(fixture: &Fixture) {
    let mut connection = verification::open(&fixture.request.database, true)
        .await
        .unwrap();
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_eligibility_context")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(rows, 0, "administrative path must never bootstrap nonadmin");
    let uncertain: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM matrix_recovery_receipts)+(SELECT count(*) FROM matrix_recovery_batches WHERE state!='terminal')+(SELECT count(*) FROM tasks)")
        .fetch_one(&mut connection).await.unwrap();
    assert_eq!(uncertain, 0);
    connection.close().await.unwrap();
}

pub(super) async fn run(
    fixture: &Fixture,
    executable: &std::path::Path,
    manager: &PrivateManager,
    server: &MockServer,
    restart: bool,
) -> String {
    let requests = server.received_requests().await.unwrap().len();
    let mut child = tokio::process::Command::new(executable)
        .arg(&fixture.request.config)
        .env("R092_IDENTITY_TOKEN", "synthetic-secret")
        .env("XDG_RUNTIME_DIR", &manager.runtime)
        .env("DBUS_SESSION_BUS_ADDRESS", &manager.bus)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let expected = if restart {
        "native-C"
    } else {
        "administrative-baseline"
    };
    if !await_native(&mut child, fixture, server, requests, expected).await {
        let output = child.wait_with_output().await.unwrap();
        let diagnostic = snapshot(fixture, server, requests).await;
        panic!(
            "runtime {:?}: {} {} {diagnostic}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let pid = child.id().unwrap();
    assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGTERM) }, 0);
    let output = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "runtime shutdown {:?}: {} {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let mut connection = verification::open(&fixture.request.database, true)
        .await
        .unwrap();
    let attempt: String = sqlx::query_scalar(
        "SELECT attempt_id FROM matrix_activation_intent WHERE state='committed'",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();
    attempt
}

async fn await_native(
    child: &mut tokio::process::Child,
    fixture: &Fixture,
    server: &MockServer,
    requests: usize,
    expected: &str,
) -> bool {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(90);
    loop {
        if child.try_wait().unwrap().is_some() {
            return false;
        }
        if full_state(server, requests, expected).await {
            let mut connection = verification::open(&fixture.request.database, true)
                .await
                .unwrap();
            let watermark: Option<String> = sqlx::query_scalar(
                "SELECT watermark FROM matrix_recovery_control WHERE phase='active'",
            )
            .fetch_optional(&mut connection)
            .await
            .unwrap();
            connection.close().await.unwrap();
            if watermark.as_deref() == Some("native-C") && signal_ready(child.id().unwrap()) {
                return true;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "runtime did not complete actual full-state activation/restart"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn full_state(server: &MockServer, start: usize, token: &str) -> bool {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .skip(start)
        .any(|request| {
            request.url.path().ends_with("/sync")
                && request
                    .url
                    .query_pairs()
                    .any(|(name, value)| name == "full_state" && value == "true")
                && request
                    .url
                    .query_pairs()
                    .any(|(name, value)| name == "since" && value == token)
        })
}

fn signal_ready(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("SigCgt:")
                    .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
            })
        })
        .is_some_and(|signals| signals & (1 << (libc::SIGTERM - 1)) != 0)
}

async fn snapshot(fixture: &Fixture, server: &MockServer, start: usize) -> String {
    let mut connection = verification::open(&fixture.request.database, true)
        .await
        .unwrap();
    let binding: (String, i64) =
        sqlx::query_as("SELECT phase,revision FROM offline_recovery_binding")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    let intent: Option<(String, String, i64)> = sqlx::query_as(
        "SELECT state,component_owner,component_epoch FROM matrix_activation_intent",
    )
    .fetch_optional(&mut connection)
    .await
    .unwrap();
    let control: (String, Option<String>, Option<String>) =
        sqlx::query_as("SELECT phase,watermark,restart_hold_state FROM matrix_recovery_control")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    let leases: Vec<(String, String, i64, String)> =
        sqlx::query_as("SELECT component,owner_token,epoch,state FROM matrix_component_leases")
            .fetch_all(&mut connection)
            .await
            .unwrap();
    connection.close().await.unwrap();
    let requests = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .skip(start)
        .map(|request| (request.method.to_string(), request.url.path().to_owned()))
        .collect::<Vec<_>>();
    format!(
        "binding={binding:?} intent={intent:?} control={control:?} leases={leases:?} requests={requests:?}"
    )
}
