use super::*;
use crate::runtime::{Lease, ReleaseDisposition};

async fn claimed(seed: &Seed) -> (SqliteRuntimeStore, Lease, String) {
    let store = SqliteRuntimeStore::new_scoped(seed.pool.clone(), Some(seed.scope.clone()));
    let resource = ExecutionResourceKey::new(
        seed.endpoint,
        WorkspaceId::from_canonical_path(seed.root.path()).unwrap(),
    );
    let AcquireOutcome::Acquired(lease) = store
        .acquire(resource, seed.task, Utc::now(), Duration::from_secs(60))
        .await
        .unwrap()
    else {
        panic!("lease not acquired")
    };
    store
        .claim_workspaces(&lease, &[seed.root.path().to_path_buf()])
        .await
        .unwrap();
    let claim = sqlx::query_scalar("SELECT claim_id FROM workspace_claims")
        .fetch_one(&seed.pool)
        .await
        .unwrap();
    (store, lease, claim)
}

#[tokio::test]
async fn release_receipt_is_atomic_immutable_and_not_effect_authority() {
    let seed = Seed::new().await;
    let (store, lease, claim) = claimed(&seed).await;
    let identity = writer::text_identity("workspace_claims", &claim).unwrap();
    let mut tx = writer::begin(&seed.pool, Some(&seed.scope)).await.unwrap();
    assert!(
        writer::delete_workspace(
            &mut tx,
            Some(&seed.scope),
            &claim,
            "wrong-owner",
            &lease.task_id.to_string(),
            i64::try_from(lease.fence).unwrap()
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    let mut tx = writer::begin(&seed.pool, Some(&seed.scope)).await.unwrap();
    assert!(
        writer::delete_workspace(
            &mut tx,
            Some(&seed.scope),
            &claim,
            &lease.owner.to_string(),
            &lease.task_id.to_string(),
            i64::try_from(lease.fence).unwrap(),
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(counts(&seed).await, (1, 0));
    sqlx::query("CREATE TRIGGER fail_workspace_delete BEFORE DELETE ON workspace_claims BEGIN SELECT RAISE(ABORT,'injected-delete-failure'); END")
        .execute(&seed.pool).await.unwrap();
    assert!(
        store
            .release(&lease, ReleaseDisposition::Released, Utc::now())
            .await
            .is_err()
    );
    assert_eq!(counts(&seed).await, (1, 0));
    sqlx::query("DROP TRIGGER fail_workspace_delete")
        .execute(&seed.pool)
        .await
        .unwrap();
    store
        .release(&lease, ReleaseDisposition::Released, Utc::now())
        .await
        .unwrap();
    assert_eq!(counts(&seed).await, (0, 1));
    bootstrap_nonadmin(&seed.pool, false).await.unwrap();
    let mut tx = writer::begin(&seed.pool, Some(&seed.scope)).await.unwrap();
    assert!(
        writer::require(&mut tx, Some(&seed.scope), &identity)
            .await
            .is_err()
    );
    assert!(
        writer::delete_workspace(
            &mut tx,
            Some(&seed.scope),
            &claim,
            &lease.owner.to_string(),
            &lease.task_id.to_string(),
            i64::try_from(lease.fence).unwrap()
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    assert_immutable(&seed).await;
    seed.pool.close().await;
}

async fn assert_immutable(seed: &Seed) {
    for statement in [
        "UPDATE offline_recovery_workspace_deletions SET runtime_owner='forged'",
        "DELETE FROM offline_recovery_workspace_deletions",
        "INSERT OR REPLACE INTO offline_recovery_workspace_deletions SELECT * FROM offline_recovery_workspace_deletions",
        "UPDATE offline_recovery_rows SET original_digest='forged' WHERE table_name='workspace_claims'",
    ] {
        assert!(
            sqlx::query(statement).execute(&seed.pool).await.is_err(),
            "{statement}"
        );
    }
}

async fn counts(seed: &Seed) -> (i64, i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM workspace_claims),(SELECT count(*) FROM offline_recovery_workspace_deletions)")
        .fetch_one(&seed.pool).await.unwrap()
}

#[tokio::test]
async fn stale_context_refuses_release_and_historical_proof_survives_parent_advance() {
    let seed = Seed::new().await;
    let (store, lease, _) = claimed(&seed).await;
    sqlx::query("UPDATE runtime_eligibility_context SET revision=revision+1")
        .execute(&seed.pool)
        .await
        .unwrap();
    assert!(
        store
            .release(&lease, ReleaseDisposition::Released, Utc::now())
            .await
            .is_err()
    );
    assert_eq!(counts(&seed).await, (1, 0));
    let context = bootstrap_nonadmin(&seed.pool, false).await.unwrap();
    let scope = RecoveryScope::new(context, EffectDomain::Local, None);
    let store = SqliteRuntimeStore::new_scoped(seed.pool.clone(), Some(scope.clone()));
    store
        .release(&lease, ReleaseDisposition::Released, Utc::now())
        .await
        .unwrap();
    let before: String =
        sqlx::query_scalar("SELECT classification_json FROM offline_recovery_workspace_deletions")
            .fetch_one(&seed.pool)
            .await
            .unwrap();
    let lifecycle = SqliteTaskLifecycle::new_scoped(seed.pool.clone(), Some(scope));
    lifecycle
        .transition(&seed.dispatched(DeliveryId::generate()))
        .await
        .unwrap();
    sqlx::query("UPDATE runtime_eligibility_context SET revision=revision+1")
        .execute(&seed.pool)
        .await
        .unwrap();
    bootstrap_nonadmin(&seed.pool, false).await.unwrap();
    let after: String =
        sqlx::query_scalar("SELECT classification_json FROM offline_recovery_workspace_deletions")
            .fetch_one(&seed.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    seed.pool.close().await;
}

#[tokio::test]
async fn missing_tampered_and_resurrected_sources_refuse_restart() {
    let seed = Seed::new().await;
    let (store, lease, claim) = claimed(&seed).await;
    sqlx::query("CREATE TABLE saved_claim AS SELECT * FROM workspace_claims")
        .execute(&seed.pool)
        .await
        .unwrap();
    let mut tx = seed.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("DELETE FROM workspace_claims")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(bootstrap_nonadmin(&seed.pool, false).await.is_err());
    sqlx::query("INSERT INTO workspace_claims SELECT * FROM saved_claim")
        .execute(&seed.pool)
        .await
        .unwrap();
    store
        .release(&lease, ReleaseDisposition::Released, Utc::now())
        .await
        .unwrap();
    assert!(
        sqlx::query("INSERT INTO workspace_claims SELECT * FROM saved_claim")
            .execute(&seed.pool)
            .await
            .is_err()
    );
    sqlx::query("DROP TRIGGER offline_recovery_workspace_no_resurrection")
        .execute(&seed.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspace_claims SELECT * FROM saved_claim")
        .execute(&seed.pool)
        .await
        .unwrap();
    assert!(bootstrap_nonadmin(&seed.pool, false).await.is_err());
    let mut tx = writer::begin(&seed.pool, Some(&seed.scope)).await.unwrap();
    assert!(
        writer::require(
            &mut tx,
            Some(&seed.scope),
            &writer::text_identity("workspace_claims", &claim).unwrap()
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    sqlx::query("DELETE FROM workspace_claims")
        .execute(&seed.pool)
        .await
        .unwrap();
    bootstrap_nonadmin(&seed.pool, false).await.unwrap();
    sqlx::query("DROP TRIGGER offline_recovery_workspace_deletion_no_update")
        .execute(&seed.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE offline_recovery_workspace_deletions SET snapshot_json='[[],[]]'")
        .execute(&seed.pool)
        .await
        .unwrap();
    assert!(bootstrap_nonadmin(&seed.pool, false).await.is_err());
    seed.pool.close().await;
}
