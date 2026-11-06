use super::*;
use crate::storage::{ReliabilityStore, RuntimeOwnerEvidence};

#[tokio::test]
async fn stopped_runtime_with_pending_evidence_is_not_successful_recovery() {
    for state in ["live", "scan_pending", "unknown", "observing", "changed"] {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("state.db");
        let pool = crate::storage::connect(&database).await.unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        let store = ReliabilityStore::new(pool.clone());
        let now = Utc::now().to_rfc3339();
        let evidence = RuntimeOwnerEvidence::capture().unwrap();
        assert!(
            store
                .begin_runtime_with_evidence("owner", "fingerprint", &now, &evidence)
                .await
                .unwrap()
        );
        sqlx::query("UPDATE runtime_instances SET state='stopped' WHERE instance_token='owner'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE runtime_owner_evidence SET state=? WHERE instance_token='owner'")
            .bind(state)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            recover_stale_runtime(&database, "owner", "foreign", &now).await,
            Err(RecoveryError::OwnerIdentity)
        );
        assert_eq!(
            recover_stale_runtime(&database, "owner", "fingerprint", &now).await,
            Err(RecoveryError::OwnerEvidencePending)
        );
        let actual: (String, i64, String, String) = sqlx::query_as(
            "SELECT state,evidence_revision,child_scan_state,child_snapshot FROM runtime_owner_evidence WHERE instance_token='owner' AND is_current=1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            actual,
            (state.into(), 1, "inaccessible".into(), "{}".into())
        );
        pool.close().await;
    }
}

#[tokio::test]
async fn stopped_runtime_final_evidence_readback_is_read_only() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("state.db");
    let pool = crate::storage::connect(&database).await.unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let store = ReliabilityStore::new(pool.clone());
    let now = Utc::now().to_rfc3339();
    let evidence = RuntimeOwnerEvidence::capture().unwrap();
    assert!(
        store
            .begin_runtime_with_evidence("owner", "fingerprint", &now, &evidence)
            .await
            .unwrap()
    );
    assert!(store.stopping_runtime("owner", &now).await.unwrap());
    let observation = crate::storage::owner_observation::FinalObservation::modeled(
        crate::storage::owner_observation::ScanOutcome::Complete,
        1,
        "owner",
    );
    assert!(
        store
            .finish_runtime_stop("owner", &now, Some((&evidence, &observation)))
            .await
            .unwrap()
    );
    for _ in 0..2 {
        assert_eq!(
            recover_stale_runtime(&database, "owner", "fingerprint", &now).await,
            Ok(RecoveryOutcome::AlreadyStopped)
        );
    }
    let revisions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM runtime_owner_evidence WHERE instance_token='owner'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(revisions, 3);
    pool.close().await;
}
