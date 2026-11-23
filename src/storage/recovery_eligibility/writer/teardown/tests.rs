use super::*;
use crate::storage::{
    CompatibilityGate, MatrixCompatibilityStore, MatrixRecoveryStore, ReliabilityStore,
    RuntimeOwnerEvidence,
    recovery_eligibility::{LiveFence, bootstrap_nonadmin},
};
use chrono::{Duration, Utc};

struct Fixture {
    _root: tempfile::TempDir,
    pool: SqlitePool,
    scope: RecoveryScope,
    sync: MatrixRecoveryStore,
    owner: ReliabilityStore,
    operation: String,
    epoch: i64,
    outbox: Option<(MatrixRecoveryStore, i64)>,
}

async fn fixture(with_outbox: bool) -> Fixture {
    let root = tempfile::tempdir().unwrap();
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
    let owner = ReliabilityStore::with_recovery(pool.clone(), scope.clone());
    let now = Utc::now().to_rfc3339();
    assert!(
        owner
            .begin_runtime_with_evidence(
                "owner",
                &RuntimeOwnerEvidence::fingerprint().unwrap(),
                &now,
                &RuntimeOwnerEvidence::capture().unwrap()
            )
            .await
            .unwrap()
    );
    let local = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
    let epoch = local
        .acquire_component_lease(
            "MatrixSync",
            "owner",
            &now,
            &(Utc::now() + Duration::minutes(5)).to_rfc3339(),
        )
        .await
        .unwrap();
    local
        .activate_cutover(&operation, "checkpoint-before", &now)
        .await
        .unwrap();
    let sync = MatrixRecoveryStore::new_scoped(
        pool.clone(),
        Some(scope.for_component(LiveFence {
            component: "MatrixSync".into(),
            owner: "owner".into(),
            epoch,
        })),
    );
    let mut fixture = Fixture {
        _root: root,
        pool,
        scope,
        sync,
        owner,
        operation,
        epoch,
        outbox: None,
    };
    if with_outbox {
        fixture.outbox = Some(outbox(&fixture).await);
    }
    refuse(&fixture).await;
    fixture
}

async fn outbox(fixture: &Fixture) -> (MatrixRecoveryStore, i64) {
    let store = MatrixRecoveryStore::new_scoped(fixture.pool.clone(), Some(fixture.scope.clone()));
    let epoch = store
        .acquire_component_lease(
            "OutboxDrain",
            "owner",
            &Utc::now().to_rfc3339(),
            &(Utc::now() + Duration::minutes(5)).to_rfc3339(),
        )
        .await
        .unwrap();
    let scope = fixture.scope.for_component(LiveFence {
        component: "OutboxDrain".into(),
        owner: "owner".into(),
        epoch,
    });
    (
        MatrixRecoveryStore::new_scoped(fixture.pool.clone(), Some(scope)),
        epoch,
    )
}

async fn refuse(fixture: &Fixture) {
    let sync = &fixture.sync;
    let operation = &fixture.operation;
    let epoch = fixture.epoch;
    let now = Utc::now().to_rfc3339();
    let expected = std::iter::once("$event".to_owned())
        .chain((1..9).map(|index| format!("$unprocessed-{index}")))
        .collect::<Vec<_>>();
    sync.begin_batch(
        "batch",
        Some("checkpoint-before"),
        "checkpoint-after",
        "response-digest",
        "checkpoint-before",
        "owner",
        epoch,
        &expected,
        &now,
    )
    .await
    .unwrap();
    sync.reserve_event(
        "account",
        "room",
        "$event",
        operation,
        "ciphertext-digest",
        "batch",
        &now,
    )
    .await
    .unwrap();
    sync.refuse_event(
        "account",
        "room",
        "$event",
        operation,
        "batch",
        "owner",
        epoch,
        "missing-room-key-unrecoverable",
        &now,
    )
    .await
    .unwrap();
    sync.refuse_response(
        operation,
        "owner",
        epoch,
        "invalid-or-unrecoverable-response",
        &now,
    )
    .await
    .unwrap();
}

async fn preserved(pool: &SqlitePool) -> (String, String, String, String, String) {
    sqlx::query_as("SELECT c.phase,c.restart_hold_state,c.watermark,r.state,b.covered_events_json FROM matrix_recovery_control c JOIN matrix_recovery_receipts r ON r.cutover_operation_id=c.cutover_operation_id JOIN matrix_recovery_batches b ON b.batch_id=r.batch_id")
        .fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn refused_matrix_teardown_preserves_hold_receipt_batch_and_checkpoint() {
    let fixture = fixture(false).await;
    let before = preserved(&fixture.pool).await;
    assert!(
        super::super::begin(&fixture.pool, Some(&fixture.scope))
            .await
            .is_err()
    );
    assert!(
        fixture
            .owner
            .stopping_runtime("owner", &Utc::now().to_rfc3339())
            .await
            .unwrap()
    );
    fixture
        .sync
        .release_component_lease(
            "MatrixSync",
            "owner",
            fixture.epoch,
            &Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
    let gate = crate::acp::AcpSpawnGate::for_generation("owner");
    gate.close_and_join().await;
    gate.reap_transports().await;
    assert!(
        fixture
            .owner
            .finish_controlled_stop(
                "owner",
                &Utc::now().to_rfc3339(),
                &RuntimeOwnerEvidence::capture().unwrap(),
                &gate.joined_children().unwrap()
            )
            .await
            .unwrap()
    );
    assert_eq!(preserved(&fixture.pool).await, before);
    let actual: (String, String, String) = sqlx::query_as("SELECT r.state,e.state,l.state FROM runtime_instances r JOIN runtime_owner_evidence e USING(instance_token) JOIN matrix_component_leases l ON l.owner_token=r.instance_token WHERE e.is_current=1")
        .fetch_one(&fixture.pool).await.unwrap();
    assert_eq!(
        actual,
        ("stopped".into(), "observing".into(), "expired".into())
    );
    assert!(
        super::super::begin(&fixture.pool, Some(&fixture.scope))
            .await
            .is_err()
    );
    assert!(bootstrap_nonadmin(&fixture.pool, true).await.is_err());
    fixture.pool.close().await;
}

#[tokio::test]
async fn refused_teardown_denies_foreign_epoch_owner_and_unclassified_revision() {
    for mutation in [
        "UPDATE matrix_component_leases SET epoch=epoch+1",
        "UPDATE matrix_component_leases SET owner_token='foreign'",
        "UPDATE runtime_owner_evidence SET process_fingerprint='foreign' WHERE is_current=1",
        "UPDATE runtime_owner_evidence SET state='unknown' WHERE is_current=1",
        "UPDATE runtime_eligibility_context SET revision=revision+1",
        "UPDATE matrix_recovery_control SET watermark='changed'",
        "UPDATE offline_recovery_rows SET original_digest='stale' WHERE table_name='matrix_component_leases'",
        "UPDATE matrix_recovery_receipts SET payload_digest='changed'",
        "UPDATE matrix_recovery_batches SET owner_epoch=owner_epoch+1",
        "UPDATE offline_recovery_rows SET original_digest='stale' WHERE table_name='matrix_recovery_receipts'",
        "UPDATE runtime_instances SET heartbeat_at='changed'",
        "UPDATE runtime_owner_evidence SET captured_at='2000-01-01T00:00:00Z' WHERE is_current=1",
    ] {
        let fixture = fixture(false).await;
        sqlx::query(sqlx::AssertSqlSafe(mutation.to_owned()))
            .execute(&fixture.pool)
            .await
            .unwrap();
        let before = preserved(&fixture.pool).await;
        assert!(
            fixture
                .sync
                .release_component_lease(
                    "MatrixSync",
                    "owner",
                    fixture.epoch,
                    &Utc::now().to_rfc3339()
                )
                .await
                .is_err(),
            "{mutation}"
        );
        assert_eq!(preserved(&fixture.pool).await, before);
        let active: i64 =
            sqlx::query_scalar("SELECT count(*) FROM matrix_component_leases WHERE state='active'")
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(active, 1);
        fixture.pool.close().await;
    }
}

#[tokio::test]
async fn refused_teardown_does_not_grant_ordinary_effect_authority() {
    let fixture = fixture(false).await;
    let now = Utc::now().to_rfc3339();
    let before = preserved(&fixture.pool).await;
    assert!(
        fixture
            .sync
            .acquire_component_lease(
                "MatrixSync",
                "owner",
                &now,
                &(Utc::now() + Duration::minutes(5)).to_rfc3339()
            )
            .await
            .is_err()
    );
    assert!(
        fixture
            .sync
            .renew_component_lease("MatrixSync", "owner", fixture.epoch, &now)
            .await
            .is_err()
    );
    assert!(
        fixture
            .sync
            .reserve_event(
                "account",
                "room",
                "$another",
                &fixture.operation,
                "digest",
                "batch",
                &now
            )
            .await
            .is_err()
    );
    assert!(
        fixture
            .sync
            .complete_batch("batch", "checkpoint-after", "owner", fixture.epoch, &now)
            .await
            .is_err()
    );
    assert!(
        fixture
            .owner
            .stopping_runtime("foreign", &now)
            .await
            .is_err()
    );
    assert_eq!(preserved(&fixture.pool).await, before);
    fixture.pool.close().await;
}

#[tokio::test]
async fn refused_stop_requires_typed_join_and_rejects_remaining_active_lease() {
    let fixture = fixture(false).await;
    fixture
        .owner
        .stopping_runtime("owner", &Utc::now().to_rfc3339())
        .await
        .unwrap();
    let gate = crate::acp::AcpSpawnGate::for_generation("owner");
    assert!(gate.joined_children().is_none());
    gate.close_and_join().await;
    gate.reap_transports().await;
    assert!(
        !fixture
            .owner
            .finish_controlled_stop(
                "owner",
                &Utc::now().to_rfc3339(),
                &RuntimeOwnerEvidence::capture().unwrap(),
                &gate.joined_children().unwrap()
            )
            .await
            .unwrap()
    );
    let state: String =
        sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token='owner'")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(state, "stopping");
    fixture.pool.close().await;
}

#[path = "outbox_tests.rs"]
mod outbox_tests;
