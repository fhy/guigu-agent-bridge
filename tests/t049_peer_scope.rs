#![cfg(feature = "test-support")]

use guigu_agent_bridge::{
    a2a::NoFault,
    app::AppRuntime,
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
        let root = std::env::temp_dir().join(format!("t049-peer-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let config_path = root.join("bridge.toml");
        let database = root.join("state.db");
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
[transports.a2a.peers.a]
url = "http://127.0.0.1:9/a2a/worker/rpc"
expected_peer_id = "peer-a"
token = "{{env:T049_A}}"
allowed_targets = ["worker"]
[transports.a2a.peers.b]
url = "http://127.0.0.1:9/a2a/worker/rpc"
expected_peer_id = "peer-b"
token = "{{env:T049_B}}"
allowed_targets = ["worker"]
[agents.worker]
transport = "acp"
command = "/bin/sh"
args = ["-c", "sleep 60"]
workspace = {:?}
enabled = true
"#,
            database,
            root.join("sessions"),
            root
        );
        let env = BTreeMap::from([
            ("HOME".into(), root.to_string_lossy().into_owned()),
            ("T049_A".into(), "peer-a-token".into()),
            ("T049_B".into(), "peer-b-token".into()),
        ]);
        // The actual startup guard authenticates the config file even when the
        // test constructor receives an already parsed Config.
        std::fs::write(&config_path, &source).unwrap();
        Self {
            root,
            config_path,
            database,
            config: load_from_str_with_env(&source, &env).unwrap(),
        }
    }
    async fn start(&self) -> AppRuntime {
        AppRuntime::start_with_config_and_a2a_faults(
            &self.config_path,
            self.config.clone(),
            Arc::new(NoFault),
        )
        .await
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn request(id: Value, message_id: &str, text: &str, legacy: bool) -> Value {
    if legacy {
        json!({"jsonrpc":"2.0","id":"rpc","method":"message/send","params":{"protocolVersion":"0.3.0","requestId":id,"contextId":format!("ctx-{message_id}"),"message":{"messageId":message_id,"role":"user","parts":[{"kind":"text","text":text}]}}})
    } else {
        json!({"jsonrpc":"2.0","id":id,"method":"message/send","params":{"message":{"messageId":message_id,"role":"user","parts":[{"kind":"text","text":text}]}}})
    }
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
    (response.status().as_u16(), response.json().await.unwrap())
}
async fn counts(path: &PathBuf) -> (i64, i64, i64) {
    let pool = connect(path).await.unwrap();
    let result = (
        sqlx::query_scalar("SELECT COUNT(*) FROM a2a_exchanges")
            .fetch_one(&pool)
            .await
            .unwrap(),
        sqlx::query_scalar("SELECT COUNT(*) FROM transport_receipts WHERE transport='a2a'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        sqlx::query_scalar("SELECT COUNT(*) FROM tasks")
            .fetch_one(&pool)
            .await
            .unwrap(),
    );
    pool.close().await;
    result
}

#[tokio::test]
async fn cross_peer_same_ids_run_concurrently_and_replay_independently() {
    for (index, body) in [
        request(json!("shared"), "m-string", "string", false),
        request(json!(7), "m-number", "number", false),
        request(json!("legacy"), "m-legacy", "legacy", true),
    ]
    .into_iter()
    .enumerate()
    {
        let fixture = Fixture::new();
        let runtime = fixture.start().await;
        let (a, b) = tokio::join!(
            post(&runtime, "peer-a-token", &body),
            post(&runtime, "peer-b-token", &body)
        );
        assert_eq!(a.0, 200, "case {index}: {a:?}");
        assert_eq!(b.0, 200, "case {index}: {b:?}");
        assert_ne!(a.1["result"]["id"], b.1["result"]["id"]);
        assert_eq!(counts(&fixture.database).await, (2, 2, 2));
        let (a_status, a_replay) = post(&runtime, "peer-a-token", &body).await;
        let (b_status, b_replay) = post(&runtime, "peer-b-token", &body).await;
        assert_eq!((a_status, b_status), (200, 200));
        assert_eq!(a_replay["result"]["id"], a.1["result"]["id"]);
        assert_eq!(b_replay["result"]["id"], b.1["result"]["id"]);
        let _ = runtime.shutdown().await;
    }
}
