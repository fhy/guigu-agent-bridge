use std::{sync::atomic::Ordering, time::Duration};

use matrix_sdk::config::SyncSettings;

use super::{fixture::Fixture, sync_owner};
use crate::{matrix::MatrixSyncHandle, storage::CutoverStore};

async fn wait_watermark(store: &CutoverStore, expected: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if store.state().await.unwrap().watermark.as_deref() == Some(expected) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

async fn wait_requests(fixture: &Fixture, minimum: usize, handle: &MatrixSyncHandle) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(handle.alive().load(Ordering::Acquire));
            let requests = fixture.server.received_requests().await.unwrap();
            if requests
                .iter()
                .filter(|r| r.url.path().ends_with("/sync"))
                .count()
                >= minimum
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn real_sdk_owner_survives_a_b_b_c_and_reopen_deduplicates() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    let mock = fixture.sync_mock("B", &["$first"], false).await;
    let (handle, mut events, ack) = sync_owner(active, fixture.store.clone())
        .await
        .start_with_dispositions();
    assert_eq!(events.recv().await.unwrap().event_id, "$first");
    ack.send(Ok(())).await.unwrap();
    wait_watermark(&fixture.store, "B").await;
    wait_requests(&fixture, 4, &handle).await;
    assert!(events.try_recv().is_err());
    assert_eq!(
        fixture.store.state().await.unwrap().watermark.as_deref(),
        Some("B")
    );
    drop(mock);
    let mock = fixture.sync_mock("C", &["$first", "$second"], false).await;
    let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.event_id, "$second");
    ack.send(Ok(())).await.unwrap();
    wait_watermark(&fixture.store, "C").await;
    handle
        .shutdown_bounded(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(events.try_recv().is_err());
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM matrix_cutover_dispositions WHERE state='complete'",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    drop(mock);
    assert_reopen_no_effects(&fixture).await;
    assert_eq!(
        fixture.anchor.stored_sync_token().await.unwrap().as_deref(),
        Some("A")
    );
    assert_a_only(&fixture).await;
}

async fn assert_reopen_no_effects(fixture: &Fixture) {
    let anchor = super::fixture::client(&fixture.server, &fixture.root.path().join("crypto")).await;
    let pool = crate::storage::connect(&fixture.root.path().join("state.db"))
        .await
        .unwrap();
    let store = CutoverStore::open(pool, &crate::storage::cutover_digest(b"fixture-binding"))
        .await
        .unwrap()
        .unwrap();
    let active = anchor
        .active_cutover_client(&store.state().await.unwrap())
        .await
        .unwrap();
    let before = fixture
        .server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path().ends_with("/sync"))
        .count();
    let _mock = fixture.sync_mock("C", &["$first", "$second"], false).await;
    let (handle, mut events, _ack) = sync_owner(active, store.clone())
        .await
        .start_with_dispositions();
    wait_requests(fixture, before + 2, &handle).await;
    handle
        .shutdown_bounded(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(events.try_recv().is_err());
    assert_eq!(store.state().await.unwrap().watermark.as_deref(), Some("C"));
}

async fn assert_a_only(fixture: &Fixture) {
    let requests = fixture.server.received_requests().await.unwrap();
    assert!(
        requests
            .iter()
            .filter(|r| r.url.path().ends_with("/sync"))
            .skip(1)
            .all(|r| r
                .url
                .query_pairs()
                .any(|(key, value)| key == "since" && value == "A"))
    );
}

#[tokio::test]
async fn repeated_token_retains_real_payload_and_limited_repeat_still_refuses() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    let sync = sync_owner(active, fixture.store.clone()).await;
    {
        let _mock = fixture.sync_mock("B", &["$first"], false).await;
        for _ in 0..2 {
            let response = sync.request_response().await.unwrap();
            assert_eq!(response.next_batch, "B");
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
        }
    }
    let _mock = fixture.sync_mock("B", &["$first"], true).await;
    assert!(sync.request_response().await.is_err());
    assert_eq!(
        fixture.store.state().await.unwrap().watermark.as_deref(),
        Some("A")
    );
    assert_eq!(
        fixture.anchor.stored_sync_token().await.unwrap().as_deref(),
        Some("A")
    );
}

#[tokio::test]
async fn baseline_idle_is_paced_cancellable_and_ordinary_sdk_stays_default() {
    let fixture = Fixture::new().await;
    let active = fixture.baseline().await;
    assert!(fixture.anchor.inner.enable_isolated_sync_replay().is_err());
    let _mock = fixture.sync_mock("A", &[], false).await;
    let response = fixture
        .anchor
        .inner
        .sync_once(SyncSettings::new().token("A"))
        .await
        .unwrap();
    assert_eq!(response.next_batch, "A");
    assert!(response.rooms.joined.is_empty());
    let (handle, mut events, _ack) = sync_owner(active, fixture.store.clone())
        .await
        .start_with_dispositions();
    wait_requests(&fixture, 4, &handle).await;
    let before = fixture.server.received_requests().await.unwrap().len();
    tokio::time::sleep(Duration::from_millis(350)).await;
    let after = fixture.server.received_requests().await.unwrap().len();
    assert!(
        after - before <= 5,
        "unpaced immediate replies: {}",
        after - before
    );
    assert!(handle.alive().load(Ordering::Acquire));
    handle
        .shutdown_bounded(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(events.try_recv().is_err());
    assert_eq!(
        fixture.store.state().await.unwrap().watermark.as_deref(),
        Some("A")
    );
    assert_eq!(
        fixture.anchor.stored_sync_token().await.unwrap().as_deref(),
        Some("A")
    );
}
