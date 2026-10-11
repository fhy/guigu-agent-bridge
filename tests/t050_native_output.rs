#![cfg(feature = "test-support")]
//! Real root HTTP/ACP process fixtures; not labelled live native/Matrix E2E.
use guigu_agent_bridge::{
    a2a::NoFault, app::AppRuntime, config::load_from_str_with_env, storage::connect,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

struct Fixture {
    root: tempfile::TempDir,
    legacy: bool,
}

impl Fixture {
    async fn start(&self) -> AppRuntime {
        self.start_scenario("legacy-end-turn-optin", 120).await
    }

    async fn start_scenario(&self, scenario: &str, timeout: u64) -> AppRuntime {
        let root = self.root.path();
        let executable = std::env::current_exe().unwrap();
        let mock = executable
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/acp-mock-backend");
        let source = format!(
            r#"
[bridge]
database = {:?}
session_root = {:?}
health_bind = "127.0.0.1:0"
shutdown_timeout_seconds = 3
default_timeout_seconds = {timeout}
[runtime]
legacy_acp_results = {}
[transports.a2a]
enabled = true
listen = "tcp:127.0.0.1:0"
allow_private_plaintext = true
exposed_endpoints = ["worker"]
[transports.a2a.peers.fixture]
url = "http://127.0.0.1:9/a2a/worker/rpc"
expected_peer_id = "fixture"
token = "{{env:T050_FIXTURE_TOKEN}}"
allowed_targets = ["worker"]
[agents.worker]
transport = "acp"
command = {:?}
args = [{scenario:?}]
workspace = {:?}
enabled = true
"#,
            root.join("state.db"),
            root.join("sessions"),
            self.legacy,
            mock,
            root
        );
        let env = BTreeMap::from([("T050_FIXTURE_TOKEN".into(), "fixture-sentinel".into())]);
        let config = load_from_str_with_env(&source, &env).unwrap();
        let path = root.join("bridge.toml");
        std::fs::write(&path, source).unwrap();
        AppRuntime::start_with_config_and_a2a_faults(&path, config, Arc::new(NoFault))
            .await
            .unwrap()
    }
}

async fn rpc(runtime: &AppRuntime, method: &str, params: Value) -> Value {
    rpc_id(runtime, method, params, "fixture-rpc").await
}

async fn rpc_id(runtime: &AppRuntime, method: &str, params: Value, id: &str) -> Value {
    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/a2a/worker/rpc",
            runtime.a2a_addr().unwrap()
        ))
        .bearer_auth("fixture-sentinel")
        .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.json::<Value>().await.unwrap()["result"].clone()
}

fn send() -> Value {
    json!({"message":{"messageId":"fixture-message","contextId":"fixture-context",
        "role":"user","parts":[{"kind":"text","text":"fixture text"}]}})
}

async fn terminal(runtime: &AppRuntime, task: &Value) -> Value {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let result = rpc(runtime, "tasks/get", json!({"id":task["id"]})).await;
            if !matches!(result["status"].as_str(), Some("submitted" | "working")) {
                return result;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn legacy_stream_output_replay_restart_and_expired_suppression() {
    let fixture = Fixture {
        root: tempfile::tempdir().unwrap(),
        legacy: true,
    };
    let runtime = fixture.start().await;
    let accepted = rpc(&runtime, "message/send", send()).await;
    let completed = terminal(&runtime, &accepted).await;
    assert_eq!(completed["status"], "completed");
    assert_eq!(
        completed["artifacts"][0]["parts"][0]["text"],
        "Hello, world"
    );
    assert_eq!(rpc(&runtime, "message/send", send()).await, completed);
    runtime.shutdown().await.unwrap();
    let runtime = fixture.start().await;
    assert_eq!(
        rpc(&runtime, "tasks/get", json!({"id":accepted["id"]})).await,
        completed
    );
    assert_eq!(rpc(&runtime, "message/send", send()).await, completed);
    let pool = connect(fixture.root.path().join("state.db")).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM projection_outbox WHERE transport='matrix'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    sqlx::query("UPDATE a2a_exchanges SET cleanup_state='cleaned', content_bytes=0")
        .execute(&pool)
        .await
        .unwrap();
    let expired = rpc(&runtime, "tasks/get", json!({"id":accepted["id"]})).await;
    assert_eq!(expired["status"], "completed");
    assert_eq!(expired["metadata"]["contentExpired"], true);
    assert!(expired.get("artifacts").is_none());
    pool.close().await;
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn default_strict_stream_does_not_infer_task_completion() {
    let fixture = Fixture {
        root: tempfile::tempdir().unwrap(),
        legacy: false,
    };
    let runtime = fixture.start().await;
    let accepted = rpc(&runtime, "message/send", send()).await;
    let failed = terminal(&runtime, &accepted).await;
    assert_eq!(failed["status"], "failed");
    assert!(failed.get("artifacts").is_none());
    runtime.shutdown().await.unwrap();
}

#[test]
fn contradictory_end_turn_policies_refuse_and_default_stays_strict() {
    let env = BTreeMap::from([("HOME".into(), "/tmp/t050-config-only".into())]);
    assert!(
        !load_from_str_with_env("", &env)
            .unwrap()
            .runtime
            .legacy_acp_results
    );
    let text = "[runtime]\nlegacy_acp_results=true\nallow_nonterminal_end_turn=true\n";
    assert!(load_from_str_with_env(text, &env).is_err());
}

#[tokio::test]
async fn streaming_deadline_commits_once_and_shutdown_reaps() {
    let fixture = Fixture {
        root: tempfile::tempdir().unwrap(),
        legacy: true,
    };
    let runtime = fixture.start_scenario("chunks-then-hang", 2).await;
    let accepted = rpc(&runtime, "message/send", send()).await;
    assert_eq!(terminal(&runtime, &accepted).await["status"], "failed");
    let pool = connect(fixture.root.path().join("state.db").to_str().unwrap())
        .await
        .unwrap();
    let terminal_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_events WHERE status='timed_out'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(terminal_count, 1);
    let disposition: String = sqlx::query_scalar("SELECT state FROM delivery_dispositions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(disposition, "terminal");
    pool.close().await;
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn immediate_cancel_after_restart_keeps_worker_available() {
    let fixture = Fixture {
        root: tempfile::tempdir().unwrap(),
        legacy: true,
    };
    let runtime = fixture.start().await;
    let accepted = rpc(&runtime, "message/send", send()).await;
    assert_eq!(terminal(&runtime, &accepted).await["status"], "completed");
    runtime.shutdown().await.unwrap();
    let runtime = fixture.start_scenario("chunks-then-hang", 2).await;
    let mut next = send();
    next["message"]["messageId"] = json!("cancel-message");
    let accepted = rpc_id(&runtime, "message/send", next, "cancel-send").await;
    rpc(&runtime, "tasks/cancel", json!({"id":accepted["id"]})).await;
    assert_eq!(terminal(&runtime, &accepted).await["status"], "canceled");
    let mut next = send();
    next["message"]["messageId"] = json!("after-cancel");
    let accepted = rpc_id(&runtime, "message/send", next, "after-cancel").await;
    assert_eq!(terminal(&runtime, &accepted).await["status"], "failed");
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepared_cancel_waits_for_competing_writer() {
    let fixture = Fixture {
        root: tempfile::tempdir().unwrap(),
        legacy: true,
    };
    let runtime = fixture.start_scenario("chunks-then-hang", 120).await;
    let accepted = rpc(&runtime, "message/send", send()).await;
    let pool = connect(fixture.root.path().join("state.db")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let running: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_continuations WHERE state='in_flight'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if running == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let mut writer = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE tasks SET version=version")
        .execute(&mut *writer)
        .await
        .unwrap();
    rpc(&runtime, "tasks/cancel", json!({"id":accepted["id"]})).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    writer.commit().await.unwrap();
    assert_eq!(terminal(&runtime, &accepted).await["status"], "canceled");
    let rows: (i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM task_events WHERE status='cancelled'),(SELECT COUNT(*) FROM execution_leases WHERE state='released')")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(rows, (1, 1));
    pool.close().await;
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_during_delivery_closes_lease_before_restart() {
    interrupted_delivery_closes_before_restart(true).await;
}

#[tokio::test]
async fn deadline_during_delivery_closes_lease_before_restart() {
    interrupted_delivery_closes_before_restart(false).await;
}

async fn interrupted_delivery_closes_before_restart(cancel: bool) {
    let fixture = Fixture {
        root: tempfile::tempdir().unwrap(),
        legacy: true,
    };
    let runtime = fixture
        .start_scenario("delayed-initialize", if cancel { 120 } else { 1 })
        .await;
    let accepted = rpc(&runtime, "message/send", send()).await;
    let pool = connect(fixture.root.path().join("state.db")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let prepared: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM delivery_dispositions WHERE state='prepared'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if prepared == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if cancel {
        rpc(&runtime, "tasks/cancel", json!({"id":accepted["id"]})).await;
    }
    assert_eq!(
        terminal(&runtime, &accepted).await["status"],
        if cancel { "canceled" } else { "failed" }
    );
    let rows: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM execution_leases WHERE state IN ('active','recovery_needed')),(SELECT COUNT(*) FROM delivery_dispositions WHERE state!='terminal'),(SELECT COUNT(*) FROM deliveries WHERE acknowledged_at IS NULL)")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(rows, (0, 0, 0));
    pool.close().await;
    runtime.shutdown().await.unwrap();
    let runtime = fixture.start().await;
    runtime.shutdown().await.unwrap();
}
