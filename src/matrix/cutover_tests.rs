use std::{sync::Arc, time::Duration};

use super::ProcessResult;
use crate::{
    matrix::{MatrixError, MatrixSync, MemorySyncTokenStore},
    storage::{CutoverStore, connect, cutover_digest},
};
use tokio::sync::mpsc;

#[path = "cutover_fixture.rs"]
mod fixture;
use fixture::Fixture;

#[path = "cutover_refusal_tests.rs"]
mod refusal;

#[path = "cutover_repeat_tests.rs"]
mod repeated;

async fn sync_owner(client: crate::matrix::MatrixClient, store: CutoverStore) -> MatrixSync {
    MatrixSync::new(client, Arc::new(MemorySyncTokenStore::default()), 8)
        .unwrap()
        .with_cutover(store)
        .await
        .unwrap()
}

#[tokio::test]
async fn baseline_is_suppressed_and_outbound_uses_memory_room_state() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    assert_eq!(
        fixture.anchor.stored_sync_token().await.unwrap().as_deref(),
        Some("A")
    );
    assert_eq!(
        active.stored_sync_token().await.unwrap().as_deref(),
        Some("A")
    );
    assert!(
        active
            .inner
            .get_room(
                &"!room:example.test"
                    .parse::<matrix_sdk::ruma::OwnedRoomId>()
                    .unwrap()
            )
            .is_some()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_cutover_dispositions")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let _mock = fixture.sync_mock("B", &["$new"], false).await;
    let sync = sync_owner(active, fixture.store.clone()).await;
    let (handle, mut events, dispositions) = sync.start_with_dispositions();
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.event_id, "$new");
    dispositions.send(Err(MatrixError::Storage)).await.unwrap();
    assert!(handle.wait().await.is_err());
    assert_eq!(
        fixture.store.state().await.unwrap().watermark.as_deref(),
        Some("A")
    );
}

#[tokio::test]
async fn sdk_ahead_crash_reopen_replays_full_batch_from_a_not_b() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    fixture.sdk_ahead_without_disposition(&active).await;
    assert_eq!(
        active.stored_sync_token().await.unwrap().as_deref(),
        Some("B")
    );
    assert_eq!(
        fixture.anchor.stored_sync_token().await.unwrap().as_deref(),
        Some("A")
    );
    drop(active);
    let anchor = fixture::client(&fixture.server, &fixture.root.path().join("crypto")).await;
    let pool = connect(&fixture.root.path().join("state.db"))
        .await
        .unwrap();
    let store = CutoverStore::open(pool, &cutover_digest(b"fixture-binding"))
        .await
        .unwrap()
        .unwrap();
    let active = anchor
        .active_cutover_client(&store.state().await.unwrap())
        .await
        .unwrap();
    let _mock = fixture.sync_mock("B", &["$new"], false).await;
    let sync = sync_owner(active, store.clone()).await;
    let response = sync.request_response().await.unwrap();
    assert_eq!(
        response
            .rooms
            .joined
            .values()
            .next()
            .unwrap()
            .timeline
            .events
            .len(),
        1
    );
    assert_eq!(store.state().await.unwrap().watermark.as_deref(), Some("A"));
    let requests = fixture.server.received_requests().await.unwrap();
    let active: Vec<_> = requests
        .iter()
        .filter(|request| {
            request.url.path().ends_with("/sync")
                && request.url.query_pairs().any(|(key, _)| key == "since")
        })
        .collect();
    assert!(active.len() >= 2);
    assert!(active.iter().all(|request| {
        request
            .url
            .query_pairs()
            .any(|(key, value)| key == "since" && value == "A")
    }));
}

#[tokio::test]
async fn b_is_application_only_all_subsequent_requests_still_use_a() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    let sync = sync_owner(active, fixture.store.clone()).await;
    let (events, mut receiver) = mpsc::channel(8);
    let (ack, ack_receiver) = mpsc::channel(8);
    ack.send(Ok(())).await.unwrap();
    let mut dispositions = Some(ack_receiver);
    {
        let _mock = fixture.sync_mock("B", &["$first"], false).await;
        let response = sync.request_response().await.unwrap();
        assert!(matches!(
            sync.process_response(&events, &mut dispositions, None, response)
                .await
                .unwrap(),
            ProcessResult::Complete(1)
        ));
        assert_eq!(receiver.recv().await.unwrap().event_id, "$first");
    }
    {
        let _mock = fixture.sync_mock("C", &["$first", "$second"], false).await;
        ack.send(Ok(())).await.unwrap();
        let response = sync.request_response().await.unwrap();
        assert!(matches!(
            sync.process_response(&events, &mut dispositions, None, response)
                .await
                .unwrap(),
            ProcessResult::Complete(1)
        ));
        assert_eq!(receiver.recv().await.unwrap().event_id, "$second");
    }
    let state = fixture.store.state().await.unwrap();
    assert_eq!(state.baseline.as_deref(), Some("A"));
    assert_eq!(state.watermark.as_deref(), Some("C"));
    assert_eq!(
        fixture.anchor.stored_sync_token().await.unwrap().as_deref(),
        Some("A")
    );
    let requests = fixture.server.received_requests().await.unwrap();
    assert!(
        requests
            .iter()
            .filter(|request| request.url.path().ends_with("/sync"))
            .skip(1)
            .all(|request| request
                .url
                .query_pairs()
                .any(|(key, value)| key == "since" && value == "A"))
    );
}

#[tokio::test]
async fn sdk_default_and_limited_batches_refuse_without_advancing_b() {
    assert!(super::validate_response(&matrix_sdk::sync::SyncResponse::default(), None).is_err());
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    let sync = sync_owner(active, fixture.store.clone()).await;
    {
        let _mock = fixture.sync_mock("B", &["$new"], true).await;
        assert!(sync.request_response().await.is_err());
    }
    {
        let _mock = fixture.sync_mock("", &["$new"], false).await;
        assert!(sync.request_response().await.is_err());
    }
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
async fn anchor_drift_missing_token_and_initial_response_bypass_refuse() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    let state = fixture.store.state().await.unwrap();
    let _mock = fixture.sync_mock("drift", &[], false).await;
    fixture
        .anchor
        .inner
        .sync_once(matrix_sdk::config::SyncSettings::new())
        .await
        .unwrap();
    assert!(fixture.anchor.active_cutover_client(&state).await.is_err());
    let missing = crate::matrix::MatrixClient::for_test().await;
    assert!(missing.active_cutover_client(&state).await.is_err());
    let sync = MatrixSync::new(active, Arc::new(MemorySyncTokenStore::default()), 8)
        .unwrap()
        .with_initial_response(matrix_sdk::sync::SyncResponse::default());
    assert!(sync.with_cutover(fixture.store.clone()).await.is_err());
}

#[tokio::test]
async fn failed_checkpoint_keeps_receipts_for_reopen_dedup() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    let sync = sync_owner(active, fixture.store.clone()).await;
    let _mock = fixture.sync_mock("B", &["$new"], false).await;
    sqlx::query("CREATE TRIGGER refuse_watermark BEFORE UPDATE ON matrix_cutover BEGIN SELECT RAISE(ABORT,'fixture'); END")
        .execute(&fixture.pool).await.unwrap();
    let (events, mut receiver) = mpsc::channel(8);
    let (ack, ack_receiver) = mpsc::channel(8);
    ack.send(Ok(())).await.unwrap();
    let mut dispositions = Some(ack_receiver);
    let response = sync.request_response().await.unwrap();
    assert!(
        sync.process_response(&events, &mut dispositions, None, response)
            .await
            .is_err()
    );
    assert_eq!(receiver.recv().await.unwrap().event_id, "$new");
    drop(sync);
    sqlx::query("DROP TRIGGER refuse_watermark")
        .execute(&fixture.pool)
        .await
        .unwrap();
    let active = fixture
        .anchor
        .active_cutover_client(&fixture.store.state().await.unwrap())
        .await
        .unwrap();
    let sync = sync_owner(active, fixture.store.clone()).await;
    let response = sync.request_response().await.unwrap();
    assert!(matches!(
        sync.process_response(&events, &mut dispositions, None, response)
            .await
            .unwrap(),
        ProcessResult::Complete(0)
    ));
    assert!(receiver.try_recv().is_err());
    assert_eq!(
        fixture.store.state().await.unwrap().watermark.as_deref(),
        Some("B")
    );
}
