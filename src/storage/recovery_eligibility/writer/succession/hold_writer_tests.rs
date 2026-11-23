use super::*;
use crate::storage::recovery_eligibility::inventory;

async fn control_digest(pool: &SqlitePool) -> (String, String) {
    let mut c = pool.acquire().await.unwrap();
    let actual = inventory::snapshot(&mut c, &lineage::control())
        .await
        .unwrap();
    let stored: String = sqlx::query_scalar("SELECT original_digest FROM offline_recovery_rows WHERE table_name='matrix_recovery_control'").fetch_one(&mut *c).await.unwrap();
    (actual.digest, stored)
}

#[tokio::test]
async fn restart_hold_writer_preserves_classification_and_authority_refusal() {
    let f = tests::fixture(true).await;
    assert!(f.store.reconcile_restart().await.is_err());
    let (actual, stored) = control_digest(&f.pool).await;
    assert_eq!(actual, stored);
    let control = f.store.control().await.unwrap().unwrap();
    assert_eq!(control.restart_hold_state.as_deref(), Some("processing"));
    assert_eq!(control.watermark.as_deref(), Some("B"));
    let reopened = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    assert!(
        crate::storage::recovery_eligibility::bootstrap_nonadmin(&reopened, true)
            .await
            .is_err()
    );
    assert!(f.store.reconcile_restart().await.is_err());
    assert_eq!(control_digest(&f.pool).await, (actual, stored));
    // Existing succession reconstructs only inside its atomic recovery transaction.
    let state = crate::native_succession::state::read(&mut *f.pool.acquire().await.unwrap())
        .await
        .unwrap()
        .unwrap();
    let mut tx = f.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    lineage::reconstruct_control(&mut tx, &state).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(f.store.control().await.unwrap().unwrap(), control);
}

#[tokio::test]
async fn restart_hold_classification_failure_rolls_back_and_retries() {
    let f = tests::fixture(true).await;
    let before = f.store.control().await.unwrap();
    sqlx::query("CREATE TRIGGER fail_hold BEFORE UPDATE ON offline_recovery_rows WHEN OLD.table_name='matrix_recovery_control' BEGIN SELECT RAISE(ABORT,'hold fault'); END").execute(&f.pool).await.unwrap();
    assert!(f.store.reconcile_restart().await.is_err());
    assert_eq!(f.store.control().await.unwrap(), before);
    sqlx::query("DROP TRIGGER fail_hold")
        .execute(&f.pool)
        .await
        .unwrap();
    assert!(f.store.reconcile_restart().await.is_err());
    let (actual, stored) = control_digest(&f.pool).await;
    assert_eq!(actual, stored);
    assert!(
        f.store
            .control()
            .await
            .unwrap()
            .unwrap()
            .restart_hold_state
            .is_some()
    );
}

#[tokio::test]
async fn response_refusal_is_classified_and_cannot_advance_checkpoint() {
    let f = tests::fixture(false).await;
    f.store
        .refuse_response(
            &f.operation,
            "owner",
            f.epoch,
            "rebuild-history-gap",
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
    let (actual, stored) = control_digest(&f.pool).await;
    assert_eq!(actual, stored);
    let c = f.store.control().await.unwrap().unwrap();
    assert_eq!(c.phase, "refused");
    assert_eq!(c.watermark.as_deref(), Some("B"));
    assert!(
        f.store
            .activate_cutover(&f.operation, "replacement", "now")
            .await
            .is_err()
    );
    assert_eq!(f.store.control().await.unwrap().unwrap(), c);
}
