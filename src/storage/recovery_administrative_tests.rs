use super::*;
use chrono::{Duration, Utc};

async fn fixture() -> (tempfile::TempDir, MatrixRecoveryStore) {
    let root = tempfile::Builder::new()
        .prefix("r092-activation-")
        .tempdir_in(std::env::var_os("HOME").unwrap())
        .unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest,schema_complete_digest,plan_digest,identity_digest,classification_digest,transition_intent_digest,transition_complete_digest,native_operation_id,phase,revision) VALUES(1,2,'generation','db','root','admin','schema','complete','plan','identity','class','intent','complete','native','baseline_ready',4)")
        .execute(&pool).await.unwrap();
    sqlx::query(
        "UPDATE runtime_eligibility_bootstrap SET state='admin_reserved' WHERE singleton=1",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO matrix_recovery_control(control_id,cutover_operation_id,phase,watermark,updated_at) VALUES(1,'native','baseline',NULL,'fixture')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO matrix_recovery_compatibility(singleton,decision,source_schema_version,source_state_digest,native_operation_id,reason_code,created_at) VALUES(1,'native',24,'fixture','native','administrative-transition','fixture')").execute(&pool).await.unwrap();
    (root, MatrixRecoveryStore::new(pool))
}

async fn begin(store: &MatrixRecoveryStore, owner: &str) -> i64 {
    let now = Utc::now();
    let epoch = store
        .acquire_administrative_lease(
            owner,
            &now.to_rfc3339(),
            &(now + Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap();
    store
        .begin_activation_intent(owner, epoch, "attempt", "request", &now.to_rfc3339())
        .await
        .unwrap();
    epoch
}

async fn state(store: &MatrixRecoveryStore) -> (String, String, String, Option<String>) {
    sqlx::query_as("SELECT b.phase,i.state,c.phase,c.watermark FROM offline_recovery_binding b JOIN matrix_activation_intent i ON i.native_operation_id=b.native_operation_id JOIN matrix_recovery_control c ON c.cutover_operation_id=i.native_operation_id")
        .fetch_one(&store.pool).await.unwrap()
}

#[tokio::test]
async fn unknown_response_holds_inflight_without_automatic_retry() {
    let (_root, store) = fixture().await;
    let epoch = begin(&store, "owner").await;
    assert_eq!(
        state(&store).await,
        (
            "baseline_inflight".into(),
            "inflight".into(),
            "baseline".into(),
            None
        )
    );
    assert!(
        store
            .begin_activation_intent(
                "owner",
                epoch,
                "attempt",
                "request",
                &Utc::now().to_rfc3339()
            )
            .await
            .is_err()
    );
    assert!(
        store
            .commit_activation_intent(
                "native",
                "owner",
                epoch,
                "invented",
                &Utc::now().to_rfc3339()
            )
            .await
            .is_err()
    );
    assert!(
        store
            .acquire_administrative_lease(
                "replacement",
                &Utc::now().to_rfc3339(),
                &(Utc::now() + Duration::minutes(1)).to_rfc3339()
            )
            .await
            .is_err()
    );
    assert_eq!(state(&store).await.0, "baseline_inflight");
}

#[tokio::test]
async fn persisted_prepared_recovers_exact_response_under_fresh_lease() {
    let (root, store) = fixture().await;
    let epoch = begin(&store, "old-owner").await;
    let response = crate::storage::cutover_digest(b"actual-B");
    store
        .prepare_activation_intent(
            "old-owner",
            epoch,
            "actual-B",
            &response,
            &Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
    assert_eq!(
        state(&store).await,
        (
            "activation_prepared".into(),
            "prepared".into(),
            "baseline".into(),
            None
        )
    );
    store.pool.close().await;
    let store = MatrixRecoveryStore::new(
        crate::storage::connect(root.path().join("state.db"))
            .await
            .unwrap(),
    );
    sqlx::query("UPDATE matrix_component_leases SET expires_at='2000-01-01T00:00:00Z'")
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(
        store
            .commit_activation_intent(
                "native",
                "old-owner",
                epoch,
                "actual-B",
                &Utc::now().to_rfc3339()
            )
            .await
            .is_err()
    );
    let now = Utc::now();
    let current = store
        .acquire_administrative_lease(
            "new-owner",
            &now.to_rfc3339(),
            &(now + Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap();
    assert!(current > epoch);
    assert_eq!(
        store
            .resume_prepared_activation("new-owner", current)
            .await
            .unwrap(),
        "actual-B"
    );
    assert!(
        store
            .commit_activation_intent("native", "old-owner", epoch, "actual-B", &now.to_rfc3339())
            .await
            .is_err()
    );
    assert!(
        store
            .commit_activation_intent(
                "native",
                "new-owner",
                current,
                "different-B",
                &now.to_rfc3339()
            )
            .await
            .is_err()
    );
    store
        .commit_activation_intent(
            "native",
            "new-owner",
            current,
            "actual-B",
            &now.to_rfc3339(),
        )
        .await
        .unwrap();
    assert_eq!(
        state(&store).await,
        (
            "active".into(),
            "committed".into(),
            "active".into(),
            Some("actual-B".into())
        )
    );
    crate::storage::recovery_eligibility::administrative::context(&store.pool, true)
        .await
        .unwrap();
    assert!(
        store
            .begin_activation_intent(
                "new-owner",
                current,
                "new-attempt",
                "request",
                &now.to_rfc3339()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn exact_binding_response_and_live_owner_are_required_at_prepare() {
    let (_root, store) = fixture().await;
    let epoch = begin(&store, "owner").await;
    let response = crate::storage::cutover_digest(b"actual-B");
    assert!(
        store
            .prepare_activation_intent(
                "stale",
                epoch,
                "actual-B",
                &response,
                &Utc::now().to_rfc3339()
            )
            .await
            .is_err()
    );
    assert!(
        store
            .prepare_activation_intent(
                "owner",
                epoch,
                "actual-B",
                "wrong-digest",
                &Utc::now().to_rfc3339()
            )
            .await
            .is_err()
    );
    sqlx::query("UPDATE matrix_activation_intent SET plan_digest='drift'")
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(
        store
            .prepare_activation_intent(
                "owner",
                epoch,
                "actual-B",
                &response,
                &Utc::now().to_rfc3339()
            )
            .await
            .is_err()
    );
    assert_eq!(state(&store).await.0, "baseline_inflight");
    sqlx::query("UPDATE matrix_activation_intent SET plan_digest='plan'")
        .execute(&store.pool)
        .await
        .unwrap();
    store
        .prepare_activation_intent(
            "owner",
            epoch,
            "actual-B",
            &response,
            &Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE matrix_activation_intent SET identity_digest='drift'")
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(
        store
            .commit_activation_intent(
                "native",
                "owner",
                epoch,
                "actual-B",
                &Utc::now().to_rfc3339()
            )
            .await
            .is_err()
    );
    assert_eq!(
        state(&store).await,
        (
            "activation_prepared".into(),
            "prepared".into(),
            "baseline".into(),
            None
        )
    );
}
