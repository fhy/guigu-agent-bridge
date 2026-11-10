use super::*;
use crate::storage::{
    CompatibilityGate, MatrixCompatibilityStore,
    recovery_eligibility::{EffectDomain, RecoveryScope, bootstrap_nonadmin},
};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex},
};

async fn transport() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/versions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"versions":["v1.11"],"unstable_features":{}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET")).and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(|request: &wiremock::Request| {
            if request.url.query_pairs().any(|(key,_)|key=="since") {
                ResponseTemplate::new(200).set_body_json(json!({"next_batch":"C","rooms":{"join":{}}}))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({"next_batch":"B","rooms":{"join":{"!room:example.test":{"timeline":{"limited":true,"prev_batch":"old","events":[{"type":"m.room.message","event_id":"$old","sender":"@alice:example.test","origin_server_ts":1,"content":{"msgtype":"m.text","body":"old command"}}]}}}}}))
            }
        }).mount(&server).await;
    server
}
async fn scope(pool: &SqlitePool) -> RecoveryScope {
    RecoveryScope::new(
        bootstrap_nonadmin(pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    )
}
fn config() -> Config {
    let mut config = crate::config::load_from_str("").unwrap();
    config.transports.matrix.enabled = true;
    config.transports.matrix.user_id = "@bridge:example.test".into();
    config
}
#[tokio::test]
async fn real_prepare_sync_publishes_once_and_reopens_without_rebaseline() {
    let server = transport().await;
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("state.db");
    let mut pool = crate::storage::connect(&database).await.unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    assert!(matches!(
        MatrixCompatibilityStore::new(pool.clone())
            .gate(true, None)
            .await
            .unwrap(),
        CompatibilityGate::Native(_)
    ));
    for _ in 0..2 {
        let scope = scope(&pool).await;
        let client = MatrixClient::for_test_session(&server.uri()).await;
        let (client, sync) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            prepare_sync(client, &config(), &pool, "owner", Some(scope.clone())),
        )
        .await
        .unwrap()
        .unwrap();
        let store = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope));
        assert_eq!(
            store.control().await.unwrap().unwrap().watermark.as_deref(),
            Some("B")
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        let epoch: i64 = sqlx::query_scalar(
            "SELECT epoch FROM matrix_component_leases WHERE component='MatrixSync'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        store
            .release_component_lease("MatrixSync", "owner", epoch, &Utc::now().to_rfc3339())
            .await
            .unwrap();
        drop(sync);
        drop(client);
        drop(store);
        pool.close().await;
        pool = crate::storage::connect(&database).await.unwrap();
    }
    let requests = server.received_requests().await.unwrap();
    let baseline = requests
        .iter()
        .filter(|r| {
            r.url.path().ends_with("/sync") && !r.url.query_pairs().any(|(k, _)| k == "since")
        })
        .count();
    assert_eq!(baseline, 1);
}
#[tokio::test]
async fn activation_commit_failure_preserves_baseline_then_exact_retry_publishes() {
    let server = transport().await;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.db");
    let pool = crate::storage::connect(&path).await.unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap();
    let before = MatrixRecoveryStore::new(pool.clone())
        .control()
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fail_activation BEFORE UPDATE ON offline_recovery_rows WHEN OLD.table_name='matrix_recovery_control' BEGIN SELECT RAISE(ABORT,'activation write fault'); END").execute(&pool).await.unwrap();
    let client = MatrixClient::for_test_session(&server.uri()).await;
    assert!(
        prepare_sync(client, &config(), &pool, "owner", Some(scope(&pool).await))
            .await
            .is_err()
    );
    assert_eq!(
        MatrixRecoveryStore::new(pool.clone())
            .control()
            .await
            .unwrap(),
        before
    );
    sqlx::query("DROP TRIGGER fail_activation")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let pool = crate::storage::connect(path).await.unwrap();
    let client = MatrixClient::for_test_session(&server.uri()).await;
    let (_client, _sync) =
        prepare_sync(client, &config(), &pool, "owner", Some(scope(&pool).await))
            .await
            .unwrap();
    assert_eq!(
        MatrixRecoveryStore::new(pool)
            .control()
            .await
            .unwrap()
            .unwrap()
            .watermark
            .as_deref(),
        Some("B")
    );
}
