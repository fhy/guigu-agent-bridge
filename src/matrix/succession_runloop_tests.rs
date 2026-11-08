use super::*;
use crate::native_succession::{process::Exclusion, state};
use crate::storage::recovery_eligibility::writer::succession::{
    confirmed, publish, tests::fixture,
};
use matrix_sdk::{deserialized_responses::TimelineEvent, sync::SyncResponse};

#[tokio::test]
async fn succession_recovers_actual_run_loop_cancelled_before_first_receipt() {
    let f = fixture(false).await;
    let mut response = SyncResponse {
        next_batch: "C".into(),
        ..Default::default()
    };
    let mut room = matrix_sdk::sync::JoinedRoomUpdate::default();
    room.timeline.events.push(TimelineEvent::from_plaintext(
        serde_json::from_value(f.payloads[0].1.clone()).unwrap(),
    ));
    response
        .rooms
        .joined
        .insert("!room:example.test".parse().unwrap(), room);
    let mut sync = MatrixSync::new(
        MatrixClient::for_test().await,
        Arc::new(SqliteSyncTokenStore::new(f.pool.clone())),
        2,
    )
    .unwrap()
    .with_initial_response(response);
    sync.recovery = Some(RecoveryBinding {
        owner_token: "owner".into(),
        store: f.store.clone(),
        account_id: "@bridge:example.test".into(),
        operation_id: f.operation.clone(),
        owner_epoch: f.epoch,
    });
    *sync.initial_token.lock().unwrap() = Some("B".into());
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    sync.before_receipt = Some((entered.clone(), resume));
    let (mut handle, mut events, _dispositions) = sync.start_with_dispositions();
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .unwrap();
    handle.join.abort();
    assert!((&mut handle.join).await.unwrap_err().is_cancelled());
    assert!(events.try_recv().is_err());
    let receipts: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_receipts")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(receipts, 0);
    assert!(f.store.reconcile_restart().await.is_err());
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let proof = Exclusion::modeled(state.owner.clone());
    publish(&f.pool, &state, &proof, &f.payloads, || Ok(()))
        .await
        .unwrap();
    assert!(
        confirmed(&f.root.path().join("state.db"), &state, &proof)
            .await
            .unwrap()
    );
    let tasks: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(tasks, 0);
    let receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM matrix_recovery_receipts WHERE state='dispositioned'",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(receipts, 1);
    f.pool.close().await;
}
