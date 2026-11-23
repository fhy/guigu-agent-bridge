use super::*;
use crate::storage::{
    CompatibilityGate, MatrixCompatibilityStore, ReliabilityStore, RuntimeOwnerEvidence,
    recovery_eligibility::{EffectDomain, LiveFence, RecoveryScope, bootstrap_nonadmin},
};
use matrix_sdk::{SessionMeta, SessionTokens, authentication::matrix::MatrixSession};
use sha2::{Digest, Sha256};

pub(super) struct Fixture {
    pub root: tempfile::TempDir,
    pub sync: MatrixSync,
    pub pool: SqlitePool,
    pub owner: ReliabilityStore,
}

pub(super) fn value(id: &str) -> serde_json::Value {
    json!({"type":"m.room.message","event_id":id,"sender":"@alice:example.test",
        "origin_server_ts":1,"content":{"msgtype":"m.notice","body":"private-fixture-body"}})
}

pub(super) async fn fixture(server: &MockServer) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap()
    else {
        panic!("native")
    };
    let (owner, store, epoch) = native_owner(&pool, &operation).await;
    seed_anchor(&store, &operation, epoch).await;
    let client = client(server).await;
    let sync = MatrixSync::new(client, Arc::new(SqliteSyncTokenStore::new(pool.clone())), 4)
        .unwrap()
        .with_recovery(store, "@bridge:example.test", operation, epoch)
        .with_component_owner("owner".into());
    Fixture {
        root,
        sync,
        pool,
        owner,
    }
}

pub(super) async fn page(
    server: &MockServer,
    index: usize,
    total: usize,
    delay: Duration,
    changed: bool,
) {
    let event = if index == total {
        "$anchor".to_owned()
    } else {
        format!("$page-{index}")
    };
    let mut raw = value(&event);
    if changed && index == total {
        raw["content"]["body"] = "changed-secret".into();
    }
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
        .and(query_param("from", format!("page-{index}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(delay)
                .set_body_json(json!({
            "start":format!("page-{index}"),"end":format!("page-{}",index+1),"chunk":[raw]})),
        )
        .mount(server)
        .await;
}

pub(super) async fn preserved(pool: &SqlitePool) -> (String, String, String) {
    sqlx::query_as("SELECT c.watermark,b.covered_events_json,r.payload_digest FROM matrix_recovery_control c JOIN matrix_recovery_batches b ON b.batch_id='prior' JOIN matrix_recovery_receipts r USING(batch_id)")
        .fetch_one(pool).await.unwrap()
}

pub(super) async fn joined_stop(f: &Fixture) {
    assert!(
        f.owner
            .stopping_runtime("owner", &timestamp())
            .await
            .unwrap()
    );
    let gate = crate::acp::AcpSpawnGate::for_generation("owner");
    gate.close_and_join().await;
    gate.reap_transports().await;
    assert!(
        f.owner
            .finish_controlled_stop(
                "owner",
                &timestamp(),
                &RuntimeOwnerEvidence::capture().unwrap(),
                &gate.joined_children().unwrap()
            )
            .await
            .unwrap()
    );
}

async fn native_owner(
    pool: &SqlitePool,
    operation: &str,
) -> (ReliabilityStore, MatrixRecoveryStore, i64) {
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(&pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let owner = ReliabilityStore::with_recovery(pool.clone(), scope.clone());
    assert!(
        owner
            .begin_runtime_with_evidence(
                "owner",
                &RuntimeOwnerEvidence::fingerprint().unwrap(),
                &timestamp(),
                &RuntimeOwnerEvidence::capture().unwrap()
            )
            .await
            .unwrap()
    );
    let store = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
    store
        .activate_cutover(&operation, "A", &timestamp())
        .await
        .unwrap();
    let epoch = store
        .acquire_component_lease(
            "MatrixSync",
            "owner",
            &timestamp(),
            &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap();
    scope
        .install_matrix_fence(LiveFence {
            component: "MatrixSync".into(),
            owner: "owner".into(),
            epoch,
        })
        .unwrap();
    let store = MatrixRecoveryStore::new_scoped(
        pool.clone(),
        Some(scope.for_component(LiveFence {
            component: "MatrixSync".into(),
            owner: "owner".into(),
            epoch,
        })),
    );
    (owner, store, epoch)
}

async fn seed_anchor(store: &MatrixRecoveryStore, operation: &str, epoch: i64) {
    let now = timestamp();
    store
        .begin_batch(
            "prior",
            Some("A"),
            "B",
            "digest",
            "A",
            "owner",
            epoch,
            &["$anchor".into()],
            &now,
        )
        .await
        .unwrap();
    let digest = format!(
        "{:x}",
        Sha256::digest(value("$anchor").to_string().as_bytes())
    );
    store
        .reserve_event(
            "@bridge:example.test",
            "!room:example.test",
            "$anchor",
            &operation,
            &digest,
            "prior",
            &now,
        )
        .await
        .unwrap();
    store
        .finish_event(
            "@bridge:example.test",
            "!room:example.test",
            "$anchor",
            &operation,
            "prior",
            "owner",
            epoch,
            &now,
        )
        .await
        .unwrap();
    store
        .complete_batch("prior", "B", "owner", epoch, &now)
        .await
        .unwrap();
}

async fn client(server: &MockServer) -> MatrixClient {
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"versions":["v1.1"]})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"next_batch":"C",
            "rooms":{"join":{"!room:example.test":{"timeline":{"limited":true,
                "prev_batch":"page-1","events":[value("$last")]}}}}})),
        )
        .mount(server)
        .await;
    let mut client = MatrixClient::for_test().await;
    client.inner = matrix_sdk::Client::builder()
        .homeserver_url(server.uri())
        .build()
        .await
        .unwrap();
    client
        .inner
        .restore_session(MatrixSession {
            meta: SessionMeta {
                user_id: "@bridge:example.test".parse().unwrap(),
                device_id: "FIXTURE".into(),
            },
            tokens: SessionTokens {
                access_token: "private-fixture-token".into(),
                refresh_token: None,
            },
        })
        .await
        .unwrap();
    client
}
