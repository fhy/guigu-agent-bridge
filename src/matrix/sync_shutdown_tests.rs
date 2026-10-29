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
