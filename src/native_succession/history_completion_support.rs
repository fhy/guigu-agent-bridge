//! Helpers for C4: actual loopback AppRuntime/SDK/ACP completion and same-store restart.
//! Plaintext loopback transport is deliberate; this is not live encrypted acceptance.
use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tracing::instrument::WithSubscriber;

pub(super) fn event(id: &str, body: &str) -> serde_json::Value {
    json!({"type":"m.room.message","event_id":id,"sender":"@user:example.test",
        "origin_server_ts":42,"content":{"msgtype":"m.text","body":body}})
}

#[derive(Clone)]
pub(super) struct Stream(pub(super) Arc<Mutex<(String, Vec<serde_json::Value>)>>);
impl wiremock::Respond for Stream {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let state = self.0.lock().unwrap();
        let since = request
            .url
            .query_pairs()
            .find(|(key, _)| key == "since")
            .map(|(_, v)| v.into_owned());
        let events = if since.as_deref() == Some(state.0.as_str()) {
            vec![]
        } else {
            state.1.clone()
        };
        ResponseTemplate::new(200)
            .set_body_json(hold::response(&state.0, false, events))
            .set_delay(Duration::from_millis(200))
    }
}
#[derive(Clone)]
struct Replies {
    calls: Arc<AtomicUsize>,
}
impl wiremock::Respond for Replies {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let delay = if request.url.path().contains("/t051-") {
            0
        } else {
            3
        };
        ResponseTemplate::new(200)
            .set_body_json(json!({"event_id":format!("$sent-{n}")}))
            .set_delay(Duration::from_secs(delay))
    }
}
fn config(f: &Fixture, server: &MockServer) -> Config {
    let mut config = f.config(true, &server.uri());
    config.bridge.shutdown_timeout_seconds = 30;
    let worker = config.agents.get_mut("worker").unwrap();
    worker.enabled = true;
    worker.command = Some(
        std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/acp-mock-backend")
            .display()
            .to_string(),
    );
    worker.args = vec!["basic".into()];
    config.transports.matrix.monitor_room = "!room:example.test".into();
    config.transports.matrix.allowed_users = vec!["@user:example.test".into()];
    config.transports.matrix.admin_users = vec!["@user:example.test".into()];
    config.transports.matrix.admin_rooms = vec!["!room:example.test".into()];
    config
        .transports
        .matrix
        .routes
        .rooms
        .insert("!room:example.test".into(), "worker".into());
    config
}
pub(super) async fn start(f: &Fixture, server: &MockServer) -> AppRuntime {
    let capture = crate::test_diagnostics::Capture::default();
    let result = AppRuntime::start_with_config_and_a2a_faults(
        f.root.path().join("bridge.toml"),
        config(f, server),
        Arc::new(crate::a2a::NoFault),
    )
    .with_subscriber(capture.subscriber())
    .await;
    if result.is_err() {
        eprintln!("{}", capture.output());
    }
    let runtime = result.unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if runtime.health_state().snapshot().await.readiness == crate::runtime::Readiness::Ready
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("runtime ready");
    runtime
}
pub(super) async fn watermark(pool: &sqlx::SqlitePool, expected: &str) {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let actual: Option<String> =
                sqlx::query_scalar("SELECT watermark FROM matrix_recovery_control")
                    .fetch_one(pool)
                    .await
                    .unwrap();
            if actual.as_deref() == Some(expected) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("committed checkpoint");
}
pub(super) async fn completed(pool: &sqlx::SqlitePool, expected: i64) {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let n: i64 =
                sqlx::query_scalar("SELECT count(*) FROM task_events WHERE status='completed'")
                    .fetch_one(pool)
                    .await
                    .unwrap();
            if n == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("ACP successful terminal task");
}
pub(super) async fn replies(server: &MockServer) -> Vec<(String, serde_json::Value)> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("/send/m.room.message/"))
        .map(|r| {
            (
                r.url.path().to_owned(),
                serde_json::from_slice(&r.body).unwrap(),
            )
        })
        .collect()
}
pub(super) async fn successful_reply(server: &MockServer, id: &str) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let found = replies(server).await.into_iter().any(|(_, v)| {
                v["m.relates_to"]["m.in_reply_to"]["event_id"] == id
                    && v["body"]
                        .as_str()
                        .is_some_and(|b| b.contains("Hello, world"))
            });
            if found {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("independent loopback successful reply");
}

pub(super) async fn prepare() -> (Fixture, MockServer, Stream) {
    let f = Fixture::new();
    let server = MockServer::start().await;
    mount_server(&server).await;
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
    crate::storage::MatrixRecoveryStore::new_scoped(
        pool.clone(),
        Some(history_fixture::scope(&pool).await),
    )
    .activate_cutover(&operation, "B", &chrono::Utc::now().to_rfc3339())
    .await
    .unwrap();
    pool.close().await;
    seed_device(&f, &server).await;
    let stream = Stream(Arc::new(Mutex::new(("C".into(), vec![]))));
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(stream.clone())
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(
            "/_matrix/client/.*/rooms/.*/state/m.room.encryption",
        ))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(json!({"errcode":"M_NOT_FOUND","error":"unencrypted fixture"})),
        )
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path_regex(
            "/_matrix/client/.*/rooms/.*/send/m.room.message/.*",
        ))
        .respond_with(Replies {
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .mount(&server)
        .await;
    (f, server, stream)
}
