use super::*;
use crate::{
    matrix::sync::MatrixSyncHandle,
    storage::{
        CompatibilityGate, MatrixCompatibilityStore, MatrixRecoveryStore,
        recovery_eligibility::{EffectDomain, RecoveryScope, bootstrap_nonadmin},
    },
};
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

async fn binding() -> (tempfile::TempDir, sqlx::SqlitePool, RecoveryBinding) {
    let root = tempfile::Builder::new()
        .prefix("r092-sync-release-")
        .tempdir_in("/tmp")
        .unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap()
    else {
        panic!("native required")
    };
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(&pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let store = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope));
    store
        .activate_cutover(&operation, "B", &super::super::timestamp())
        .await
        .unwrap();
    let epoch = store
        .acquire_component_lease(
            "MatrixSync",
            "owner",
            &super::super::timestamp(),
            &(chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
        )
        .await
        .unwrap();
    (
        root,
        pool,
        RecoveryBinding {
            owner_token: "owner".into(),
            store,
            account_id: "synthetic".into(),
            operation_id: operation,
            owner_epoch: epoch,
        },
    )
}

async fn is_live(pool: &sqlx::SqlitePool) -> bool {
    sqlx::query_scalar("SELECT julianday(expires_at)>julianday('now') FROM matrix_component_leases WHERE component='MatrixSync'")
        .fetch_one(pool).await.unwrap()
}

fn handle(binding: RecoveryBinding, pending: bool) -> MatrixSyncHandle {
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    MatrixSyncHandle {
        shutdown_tx,
        join: tokio::spawn(async move {
            if pending {
                std::future::pending::<()>().await;
            }
            Ok(())
        }),
        alive: Arc::new(AtomicBool::new(true)),
        recovery_lease: Some(MatrixSyncLease::new(binding)),
    }
}

#[tokio::test]
async fn joined_sync_defers_shared_fence_until_dependents_finish() {
    let (_root, pool, binding) = binding().await;
    let lease = handle(binding, false)
        .shutdown_bounded_deferred(Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    assert!(is_live(&pool).await);
    lease.release().await.unwrap();
    assert!(!is_live(&pool).await);
    bootstrap_nonadmin(&pool, true).await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn joined_processing_failure_preserves_cause_and_exact_deferred_lease() {
    let (_root, pool, binding) = binding().await;
    let mut owner = handle(binding, false);
    owner.join.abort();
    let _ = (&mut owner.join).await;
    owner.join = tokio::spawn(async { Err(MatrixError::CryptoInitialization) });
    let (result, lease) = owner.shutdown_joined(Duration::from_secs(1)).await.unwrap();
    assert!(matches!(result, Err(MatrixError::CryptoInitialization)));
    assert!(is_live(&pool).await);
    lease.unwrap().release().await.unwrap();
    assert!(!is_live(&pool).await);
    pool.close().await;
}

#[tokio::test]
async fn panicked_sync_join_cannot_return_a_cleanup_lease() {
    let (_root, pool, binding) = binding().await;
    let mut owner = handle(binding, false);
    owner.join.abort();
    let _ = (&mut owner.join).await;
    owner.join = tokio::spawn(async { panic!("controlled join failure") });
    assert!(matches!(
        owner.shutdown_joined(Duration::from_secs(1)).await,
        Err(MatrixError::Join)
    ));
    assert!(is_live(&pool).await);
    pool.close().await;
}

#[tokio::test]
async fn timeout_retains_fence_and_standalone_shutdown_releases_after_join() {
    let (_root, pool, binding) = binding().await;
    assert!(
        handle(binding.clone(), true)
            .shutdown_bounded_deferred(Duration::from_millis(1))
            .await
            .is_err()
    );
    assert!(is_live(&pool).await);
    handle(binding, false).shutdown().await.unwrap();
    assert!(!is_live(&pool).await);
    pool.close().await;
}

async fn processing_owner(
    binding: RecoveryBinding,
    pool: &sqlx::SqlitePool,
) -> (
    MatrixSyncHandle,
    tokio::sync::mpsc::Sender<super::super::Disposition>,
) {
    use crate::matrix::{MatrixClient, MatrixSync, SqliteSyncTokenStore};
    use matrix_sdk::{deserialized_responses::TimelineEvent, sync::SyncResponse};
    let mut response = SyncResponse {
        next_batch: "C".into(),
        ..Default::default()
    };
    let mut room = matrix_sdk::sync::JoinedRoomUpdate::default();
    room.timeline.events.push(TimelineEvent::from_plaintext(
        serde_json::from_value(serde_json::json!({
            "type": "m.room.message", "event_id": "$drain:example.test",
            "sender": "@alice:example.test", "origin_server_ts": 1,
            "content": {"msgtype": "m.text", "body": "harmless fixture"}
        }))
        .unwrap(),
    ));
    response
        .rooms
        .joined
        .insert("!room:example.test".parse().unwrap(), room);
    let mut sync = MatrixSync::new(
        MatrixClient::for_test().await,
        Arc::new(SqliteSyncTokenStore::new(pool.clone())),
        2,
    )
    .unwrap()
    .with_initial_response(response);
    sync.recovery = Some(binding);
    *sync.initial_token.lock().unwrap() = Some("B".into());
    let (handle, mut events, dispositions) = sync.start_with_dispositions();
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.event_id, "$drain:example.test");
    (handle, dispositions)
}

async fn batch_state(pool: &sqlx::SqlitePool) -> (String, String, String) {
    sqlx::query_as("SELECT watermark,b.state,r.state FROM matrix_recovery_control CROSS JOIN matrix_recovery_batches b CROSS JOIN matrix_recovery_receipts r")
        .fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn durable_shutdown_drains_disposition_before_checkpoint_and_join() {
    let (_root, pool, binding) = binding().await;
    let store = binding.store.clone();
    let (mut owner, dispositions) = processing_owner(binding, &pool).await;
    owner.shutdown_tx.send(true).unwrap();
    let prematurely_joined = tokio::time::timeout(Duration::from_millis(30), &mut owner.join).await;
    assert!(
        prematurely_joined.is_err(),
        "durable batch was canceled at shutdown"
    );
    assert_eq!(
        batch_state(&pool).await,
        ("B".into(), "processing".into(), "processing".into())
    );
    dispositions.send(Ok(())).await.unwrap();
    let (result, lease) = owner.shutdown_joined(Duration::from_secs(5)).await.unwrap();
    result.unwrap();
    assert_eq!(
        batch_state(&pool).await,
        ("C".into(), "terminal".into(), "dispositioned".into())
    );
    assert!(is_live(&pool).await);
    lease.unwrap().release().await.unwrap();
    assert!(!is_live(&pool).await);
    store.reconcile_restart().await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn durable_shutdown_timeout_keeps_unfinished_batch_and_restart_hold() {
    let (_root, pool, binding) = binding().await;
    let store = binding.store.clone();
    let (owner, _dispositions) = processing_owner(binding, &pool).await;
    assert!(matches!(
        owner.shutdown_joined(Duration::from_millis(30)).await,
        Err(MatrixError::Join)
    ));
    assert!(is_live(&pool).await);
    assert_eq!(
        batch_state(&pool).await,
        ("B".into(), "processing".into(), "processing".into())
    );
    assert!(store.reconcile_restart().await.is_err());
    assert_eq!(
        store
            .control()
            .await
            .unwrap()
            .unwrap()
            .restart_hold_state
            .as_deref(),
        Some("processing")
    );
    pool.close().await;
}

#[tokio::test]
async fn durable_shutdown_preserves_failed_disposition_without_checkpoint() {
    let (_root, pool, binding) = binding().await;
    let store = binding.store.clone();
    let (mut owner, dispositions) = processing_owner(binding, &pool).await;
    owner.shutdown_tx.send(true).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut owner.join)
            .await
            .is_err()
    );
    dispositions.send(Err(MatrixError::Storage)).await.unwrap();
    let (result, _lease) = owner.shutdown_joined(Duration::from_secs(5)).await.unwrap();
    assert!(matches!(result, Err(MatrixError::Storage)));
    assert!(is_live(&pool).await);
    assert_eq!(
        batch_state(&pool).await,
        ("B".into(), "processing".into(), "processing".into())
    );
    assert!(store.reconcile_restart().await.is_err());
    pool.close().await;
}
