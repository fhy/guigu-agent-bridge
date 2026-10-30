use super::*;

async fn fixture(enabled: bool) -> (tempfile::TempDir, sqlx::SqlitePool, RecoveryScope, String) {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let crate::storage::CompatibilityGate::Native(operation) =
        crate::storage::MatrixCompatibilityStore::new(pool.clone())
            .gate(true, None)
            .await
            .unwrap()
    else {
        panic!("native baseline required");
    };
    let context = crate::storage::recovery_eligibility::bootstrap_nonadmin(&pool, enabled)
        .await
        .unwrap();
    (
        root,
        pool,
        RecoveryScope::new(context, super::super::EffectDomain::Local, None),
        operation,
    )
}

#[tokio::test]
async fn baseline_matrix_sync_lease_does_not_grant_business_authority_or_change_epoch() {
    let (_root, pool, scope, operation) = fixture(true).await;
    let store = crate::storage::MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
    let now = chrono::Utc::now();
    let expires = (now + chrono::Duration::seconds(60)).to_rfc3339();
    assert!(
        store
            .acquire_component_lease("OutboxDrain", "owner", &now.to_rfc3339(), &expires)
            .await
            .is_err()
    );
    let epoch = store
        .acquire_component_lease("MatrixSync", "owner", &now.to_rfc3339(), &expires)
        .await
        .unwrap();
    assert_eq!(epoch, 1);
    let matrix_scope = scope.for_component(crate::storage::recovery_eligibility::LiveFence {
        component: "MatrixSync".into(),
        owner: "owner".into(),
        epoch,
    });
    assert!(
        super::super::begin(&pool, Some(&matrix_scope))
            .await
            .is_err()
    );
    let classified: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_rows WHERE table_name='matrix_component_leases' AND disposition='eligible_new'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(classified, 1);
    store
        .activate_cutover(&operation, "real-response-B", &now.to_rfc3339())
        .await
        .unwrap();
    let stored: i64 = sqlx::query_scalar(
        "SELECT epoch FROM matrix_component_leases WHERE component='MatrixSync'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored, epoch);
    let transaction = super::super::begin(&pool, Some(&matrix_scope))
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn disabled_native_baseline_cannot_acquire_a_matrix_lease() {
    let (_root, pool, scope, _operation) = fixture(false).await;
    let store = crate::storage::MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope));
    let now = chrono::Utc::now();
    assert!(
        store
            .acquire_component_lease(
                "MatrixSync",
                "owner",
                &now.to_rfc3339(),
                &(now + chrono::Duration::seconds(60)).to_rfc3339()
            )
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_component_leases")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    pool.close().await;
}
