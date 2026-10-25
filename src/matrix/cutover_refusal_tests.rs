use std::{sync::Arc, time::Duration};

use super::{fixture::Fixture, sync_owner};
use crate::matrix::{MatrixSync, MemorySyncTokenStore};
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path_regex},
};

#[tokio::test]
async fn pending_sdk_checkpoint_without_publication_cannot_enable_ingress() {
    let fixture = Fixture::new().await;
    sqlx::query("CREATE TRIGGER refuse_activation BEFORE UPDATE ON matrix_cutover BEGIN SELECT RAISE(ABORT,'fixture'); END")
        .execute(&fixture.pool).await.unwrap();
    {
        let _response = fixture.sync_mock("A", &["$historical"], false).await;
        assert!(
            fixture
                .anchor
                .prepare_cutover(&fixture.store)
                .await
                .is_err()
        );
        assert_eq!(
            fixture.anchor.stored_sync_token().await.unwrap().as_deref(),
            Some("A")
        );
        assert!(fixture.store.state().await.unwrap().baseline.is_none());
    }
    sqlx::query("DROP TRIGGER refuse_activation")
        .execute(&fixture.pool)
        .await
        .unwrap();
    {
        let _response = fixture.sync_mock("A", &[], false).await;
        assert!(
            fixture
                .anchor
                .prepare_cutover(&fixture.store)
                .await
                .is_err()
        );
        assert!(fixture.store.state().await.unwrap().baseline.is_none());
    }
    let _response = fixture
        .sync_mock("B", &["$quarantined-until-publication"], false)
        .await;
    let active = fixture
        .anchor
        .prepare_cutover(&fixture.store)
        .await
        .unwrap();
    assert_eq!(
        active.stored_sync_token().await.unwrap().as_deref(),
        Some("B")
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_cutover_dispositions")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn shutdown_during_ack_preserves_watermark_and_unknown_receipt_refuses_replay() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    let _response = fixture.sync_mock("B", &["$unknown"], false).await;
    let sync = sync_owner(active, fixture.store.clone()).await;
    let (handle, mut events, _dispositions) = sync.start_with_dispositions();
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.event_id, "$unknown");
    handle
        .shutdown_bounded(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        fixture.store.state().await.unwrap().watermark.as_deref(),
        Some("A")
    );
    let digest: String = sqlx::query_scalar(
        "SELECT content_digest FROM matrix_cutover_dispositions WHERE event_id='$unknown'",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert!(
        fixture
            .store
            .begin_event("!room:example.test", "$unknown", &digest)
            .await
            .is_err()
    );
    let state: String = sqlx::query_scalar(
        "SELECT state FROM matrix_cutover_dispositions WHERE event_id='$unknown'",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(state, "manual_required");
}

#[tokio::test]
async fn unknown_since_reset_response_and_no_disposition_entrypoint_do_not_advance_b() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    let sync = sync_owner(active, fixture.store.clone()).await;
    let (events, _receiver) = tokio::sync::mpsc::channel(8);
    assert!(sync.sync_once(&events).await.is_err());
    let _response = Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(400).set_body_json(
            serde_json::json!({"errcode":"M_UNKNOWN_POS","error":"synthetic reset"}),
        ))
        .mount_as_scoped(&fixture.server)
        .await;
    assert!(sync.request_response().await.is_err());
    assert_eq!(
        fixture.store.state().await.unwrap().watermark.as_deref(),
        Some("A")
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_cutover_dispositions")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn late_initial_response_cannot_bypass_cutover_or_start_without_ack() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    let sync = sync_owner(active.clone(), fixture.store.clone())
        .await
        .with_initial_response(matrix_sdk::sync::SyncResponse::default());
    let (handle, mut events, _dispositions) = sync.start_with_dispositions();
    assert!(handle.wait().await.is_err());
    assert!(events.recv().await.is_none());
    let sync = MatrixSync::new(active, Arc::new(MemorySyncTokenStore::default()), 8)
        .unwrap()
        .with_cutover(fixture.store.clone())
        .await
        .unwrap();
    let (handle, mut events) = sync.start();
    assert!(handle.wait().await.is_err());
    assert!(events.recv().await.is_none());
}
