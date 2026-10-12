#![cfg(feature = "test-support")]
use guigu_agent_bridge::{
    a2a::{FaultPlan, FaultPoint, IngressFaultInjector, NoFault},
    app::{AppError, AppRuntime},
    config::{Config, load_from_str_with_env},
    storage::connect,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
struct Fixture {
    root: PathBuf,
    config_path: PathBuf,
    database: PathBuf,
    config: Config,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("t049-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let config_path = root.join("bridge.toml");
        let database = root.join("state.db");
        let sessions = root.join("sessions");
        let mock = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples")
            .join(format!("acp-mock-backend{}", std::env::consts::EXE_SUFFIX));
        std::fs::write(&config_path, "# test-support assembly source\n").unwrap();
        let source = format!(
            r#"
[bridge]
database = {:?}
session_root = {:?}
health_bind = "127.0.0.1:0"
shutdown_timeout_seconds = 1

[transports.a2a]
enabled = true
listen = "tcp:127.0.0.1:0"
exposed_endpoints = ["worker"]

[transports.a2a.peers.test]
url = "http://127.0.0.1:9/a2a/worker/rpc"
expected_peer_id = "peer-a"
token = "{{env:T049_TOKEN}}"
allowed_targets = ["worker"]

[transports.a2a.peers.other]
url = "http://127.0.0.1:9/a2a/worker/rpc"
expected_peer_id = "peer-b"
token = "{{env:T049_TOKEN_B}}"
allowed_targets = ["worker"]

[agents.worker]
transport = "acp"
command = {:?}
args = ["happy"]
workspace = {:?}
enabled = true
"#,
            database, sessions, mock, root
        );
        let env = BTreeMap::from([
            ("HOME".into(), root.to_string_lossy().into_owned()),
            ("T049_TOKEN".into(), "t049-secret-sentinel".into()),
            ("T049_TOKEN_B".into(), "t049-peer-b-sentinel".into()),
        ]);
        let config = load_from_str_with_env(&source, &env).unwrap();
        Self {
            root,
            config_path,
            database,
            config,
        }
    }

    async fn start(&self, point: FaultPoint) -> AppRuntime {
        self.start_with(Arc::new(FaultPlan::once(point))).await
    }

    async fn start_clean(&self) -> AppRuntime {
        self.start_with(Arc::new(NoFault)).await
    }

    async fn start_with(&self, faults: Arc<dyn IngressFaultInjector>) -> AppRuntime {
        let runtime = AppRuntime::start_with_config_and_a2a_faults(
            &self.config_path,
            self.config.clone(),
            faults,
        )
        .await
        .unwrap();
        let pool = connect(&self.database).await.unwrap();
        sqlx::raw_sql("BEGIN IMMEDIATE; COMMIT;")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        runtime
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn standard(request: &str, text: &str) -> Value {
    json!({
        "jsonrpc": "2.0", "id": request, "method": "message/send",
        "params": {"message": {
            "messageId": "message-a", "contextId": "context-a", "role": "user",
            "parts": [{"kind": "text", "text": text}]
        }}
    })
}

fn legacy(request: &str) -> Value {
    json!({
        "jsonrpc": "2.0", "id": format!("rpc-{request}"), "method": "message/send",
        "params": {"protocolVersion": "0.3.0", "requestId": request,
            "contextId": format!("context-{request}"), "message": {
                "messageId": format!("message-{request}"), "role": "user",
                "parts": [{"kind": "text", "text": "legacy"}]
            }}
    })
}

async fn post(runtime: &AppRuntime, token: &str, body: &Value) -> (u16, Value) {
    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/a2a/worker/rpc",
            runtime.a2a_addr().unwrap()
        ))
        .bearer_auth(token)
        .json(body)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = response.json().await.unwrap();
    (status, body)
}

async fn counts(path: &PathBuf) -> (i64, i64, i64) {
    let pool = connect(path).await.unwrap();
    let exchanges = sqlx::query_scalar("SELECT COUNT(*) FROM a2a_exchanges")
        .fetch_one(&pool)
        .await
        .unwrap();
    let receipts =
        sqlx::query_scalar("SELECT COUNT(*) FROM transport_receipts WHERE transport='a2a'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let tasks = sqlx::query_scalar("SELECT COUNT(*) FROM tasks")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    (exchanges, receipts, tasks)
}

async fn wait_completed(runtime: &AppRuntime, task_id: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let query = json!({
                "jsonrpc": "2.0",
                "id": "wait",
                "method": "tasks/get",
                "params": {"id": task_id}
            });
            let (status, value) = post(runtime, "t049-secret-sentinel", &query).await;
            assert_eq!(status, 200);
            match value["result"]["status"].as_str() {
                Some("completed") => break,
                Some("failed" | "cancelled" | "timed_out") => {
                    panic!("task ended before completion: {value}")
                }
                _ => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .expect("task completion deadline");
}

async fn shutdown_after_admission(runtime: AppRuntime) {
    let result = runtime.shutdown().await;
    assert!(
        result.is_ok()
            || matches!(
                result,
                Err(AppError::RequiredOwner {
                    component: "task-worker",
                    class: "failed"
                }) | Err(AppError::RequiredOwner {
                    component: "task-worker",
                    class: "timeout"
                }) | Err(AppError::Assembly("task-worker/timeout"))
                    | Err(AppError::Assembly("event drain timed out"))
            ),
        "{result:?}"
    );
}

async fn shutdown_without_admission(runtime: AppRuntime) {
    let result = runtime.shutdown().await;
    assert!(
        result.is_ok() || matches!(result, Err(AppError::Assembly("event drain timed out"))),
        "{result:?}"
    );
}

#[tokio::test]
async fn standard_legacy_replay_conflict_and_auth_are_real_http() {
    let fixture = Fixture::new();
    let runtime = fixture.start(FaultPoint::BeforeSnapshot).await;
    let request = standard("request-a", "first");
    let (status, failed) = post(&runtime, "t049-secret-sentinel", &request).await;
    assert_eq!(
        (status, failed["error"]["code"].as_i64()),
        (500, Some(-32603))
    );
    let (status, first) = post(&runtime, "t049-secret-sentinel", &request).await;
    assert_eq!(status, 200);
    let (status, replay) = post(&runtime, "t049-secret-sentinel", &request).await;
    assert_eq!(status, 200);
    assert_eq!(first["result"]["id"], replay["result"]["id"]);
    let task_id = first["result"]["id"].as_str().unwrap();
    for method in ["tasks/get", "tasks/cancel"] {
        let query = json!({"jsonrpc":"2.0","id":method,"method":method,"params":{"id":task_id}});
        let (status, value) = post(&runtime, "t049-secret-sentinel", &query).await;
        assert_eq!(status, 200);
        assert_eq!(value["result"]["id"], task_id);
    }
    let (status, conflict) = post(
        &runtime,
        "t049-secret-sentinel",
        &standard("request-a", "other"),
    )
    .await;
    assert_eq!(
        (status, conflict["error"]["code"].as_i64()),
        (409, Some(-32009))
    );
    let (status, denied) = post(&runtime, "wrong", &request).await;
    assert_eq!(
        (status, denied["error"]["message"].as_str()),
        (401, Some("unauthorized"))
    );
    assert_eq!(counts(&fixture.database).await, (1, 1, 1));
    shutdown_after_admission(runtime).await;

    let legacy_fixture = Fixture::new();
    let legacy_runtime = legacy_fixture.start_clean().await;
    let (status, legacy_value) =
        post(&legacy_runtime, "t049-secret-sentinel", &legacy("legacy-a")).await;
    assert_eq!(status, 200, "{legacy_value}");
    let legacy_id = legacy_value["result"]["id"].as_str().unwrap();
    let legacy_get = json!({"jsonrpc":"2.0","id":"legacy-get","method":"tasks/get","params":{"protocolVersion":"0.3.0","taskId":legacy_id}});
    assert_eq!(
        post(&legacy_runtime, "t049-secret-sentinel", &legacy_get)
            .await
            .0,
        200
    );
    assert_eq!(counts(&legacy_fixture.database).await, (1, 1, 1));
    shutdown_after_admission(legacy_runtime).await;
}

#[tokio::test]
async fn every_fault_boundary_is_process_visible_and_reconciles_without_duplicates() {
    let cases = [
        (
            FaultPoint::BeforeReservation,
            500,
            -32010,
            "storage unavailable",
        ),
        (
            FaultPoint::AfterReservation,
            500,
            -32010,
            "storage unavailable",
        ),
        (
            FaultPoint::BeforeContent,
            500,
            -32010,
            "storage unavailable",
        ),
        (FaultPoint::AfterContent, 500, -32010, "storage unavailable"),
        (
            FaultPoint::BeforePrerequisites,
            500,
            -32010,
            "storage unavailable",
        ),
        (
            FaultPoint::AfterPrerequisites,
            500,
            -32010,
            "storage unavailable",
        ),
        (FaultPoint::AdmissionBusy, 503, -32011, "admission busy"),
        (
            FaultPoint::BeforeAdmission,
            500,
            -32010,
            "storage unavailable",
        ),
        (
            FaultPoint::AfterAdmission,
            500,
            -32010,
            "storage unavailable",
        ),
        (FaultPoint::BeforeBinding, 503, -32012, "recovery required"),
        (FaultPoint::AfterBinding, 503, -32012, "recovery required"),
        (FaultPoint::BeforeSnapshot, 500, -32603, "internal error"),
    ];
    for (index, (point, expected_status, expected_code, message)) in cases.into_iter().enumerate() {
        for repeat in 0..2 {
            let fixture = Fixture::new();
            let runtime = fixture.start(point).await;
            let request = standard(&format!("request-{index}-{repeat}"), "bounded");
            let (status, first) = post(&runtime, "t049-secret-sentinel", &request).await;
            assert_eq!(status, expected_status, "{point:?}");
            assert_eq!(first["error"]["code"], expected_code, "{point:?}");
            assert_eq!(first["error"]["message"], message, "{point:?}");
            let rendered = first.to_string();
            for secret in [
                "t049-secret-sentinel",
                "bounded",
                &fixture.root.to_string_lossy(),
            ] {
                assert!(!rendered.contains(secret), "{point:?}: {rendered}");
            }
            let (status, retry) = post(&runtime, "t049-secret-sentinel", &request).await;
            assert_eq!(status, 200, "retry {point:?}: {retry}");
            assert_eq!(counts(&fixture.database).await, (1, 1, 1), "{point:?}");
            shutdown_after_admission(runtime).await;
        }
    }
}

#[tokio::test]
async fn malformed_and_target_hidden_fail_before_mutation() {
    let fixture = Fixture::new();
    let runtime = fixture.start(FaultPoint::BeforeSnapshot).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/a2a/worker/rpc", runtime.a2a_addr().unwrap());
    let response = client
        .post(&url)
        .bearer_auth("t049-secret-sentinel")
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 400);
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["error"]["code"], -32602);
    let hidden = client
        .post(url.replace("/worker/", "/hidden/"))
        .bearer_auth("t049-secret-sentinel")
        .json(&standard("hidden", "x"))
        .send()
        .await
        .unwrap();
    assert_eq!(hidden.status().as_u16(), 404);
    for (body, code) in [
        (
            json!({"jsonrpc":"2.0","id":"method","method":"unknown","params":{}}),
            -32601,
        ),
        (
            json!({"jsonrpc":"2.0","id":"version","method":"message/send","params":{"protocolVersion":"9.9.9"}}),
            -32601,
        ),
        (
            json!({"jsonrpc":"2.0","id":"uri","method":"message/send","params":{"message":{"messageId":"m","role":"user","parts":[{"kind":"file_uri","uri":"scheme:value"}]}}}),
            -32601,
        ),
    ] {
        let (status, value) = post(&runtime, "t049-secret-sentinel", &body).await;
        assert_eq!((status, value["error"]["code"].as_i64()), (400, Some(code)));
    }
    let oversized = "x".repeat(1_048_577);
    let response = client
        .post(format!(
            "http://{}/a2a/worker/rpc",
            runtime.a2a_addr().unwrap()
        ))
        .bearer_auth("wrong")
        .body(oversized)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 413);
    assert_eq!(counts(&fixture.database).await, (0, 0, 0));
    shutdown_without_admission(runtime).await;
}

#[tokio::test]
async fn concurrent_winners_and_restart_replay_are_deterministic() {
    let fixture = Fixture::new();
    let runtime = fixture.start_clean().await;
    let identical = standard("same", "same");
    let (left, right) = tokio::join!(
        post(&runtime, "t049-secret-sentinel", &identical),
        post(&runtime, "t049-secret-sentinel", &identical)
    );
    assert_eq!((left.0, right.0), (200, 200), "{left:?} {right:?}");
    assert_eq!(left.1["result"]["id"], right.1["result"]["id"]);
    let identical_id = left.1["result"]["id"].clone();

    let conflict = standard("same", "other");
    let (status, value) = post(&runtime, "t049-secret-sentinel", &conflict).await;
    assert_eq!(status, 409, "{value}");
    assert_eq!(value["error"]["message"], "idempotency conflict");
    assert_eq!(counts(&fixture.database).await, (1, 1, 1));
    wait_completed(&runtime, identical_id.as_str().unwrap()).await;
    shutdown_after_admission(runtime).await;

    let restarted = fixture.start_clean().await;
    let (status, replay) = post(&restarted, "t049-secret-sentinel", &identical).await;
    assert_eq!(status, 200);
    assert_eq!(replay["result"]["id"], identical_id);
    assert_eq!(counts(&fixture.database).await, (1, 1, 1));
    shutdown_after_admission(restarted).await;
}
