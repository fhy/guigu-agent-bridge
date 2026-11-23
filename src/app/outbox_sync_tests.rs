use super::*;
use crate::storage::{
    CompatibilityGate, MatrixCompatibilityStore, MatrixRecoveryStore,
    recovery_eligibility::{EffectDomain, LiveFence, RecoveryScope, bootstrap_nonadmin},
};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

struct NoSend;
impl MatrixOutboxSender for NoSend {
    fn send_stable<'a>(
        &'a self,
        _: &'a str,
        _: Option<&'a str>,
        _: Option<&'a str>,
        _: &'a str,
        _: &'a str,
    ) -> crate::matrix::ReplyFuture<'a> {
        Box::pin(async { panic!("busy/empty fixture cannot send") })
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    sync: MatrixRecoveryStore,
    drain: OutboxDrain,
    operation: String,
    epoch: i64,
}
async fn fixture() -> Fixture {
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
        panic!("native")
    };
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(&pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let local = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
    let now = chrono::Utc::now();
    local
        .activate_cutover(&operation, "B", &now.to_rfc3339())
        .await
        .unwrap();
    let epoch = local
        .acquire_component_lease(
            "MatrixSync",
            "sync",
            &now.to_rfc3339(),
            &(now + chrono::Duration::minutes(10)).to_rfc3339(),
        )
        .await
        .unwrap();
    let outbox = crate::app::matrix_recovery::outbox_store(&pool, "outbox", Some(scope.clone()))
        .await
        .unwrap();
    let sync = MatrixRecoveryStore::new_scoped(
        pool,
        Some(scope.for_component(LiveFence {
            component: "MatrixSync".into(),
            owner: "sync".into(),
            epoch,
        })),
    );
    sync.begin_batch(
        "batch",
        Some("B"),
        "C",
        "digest",
        "B",
        "sync",
        epoch,
        &[],
        &now.to_rfc3339(),
    )
    .await
    .unwrap();
    Fixture {
        _root: root,
        sync,
        drain: OutboxDrain::new(outbox, Arc::new(NoSend), 1),
        operation,
        epoch,
    }
}
async fn finish(f: &Fixture) {
    f.sync
        .complete_batch(
            "batch",
            "C",
            "sync",
            f.epoch,
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn ordinary_sync_busy_never_grants_send_and_owned_drain_resumes_after_commit() {
    let f = fixture().await;
    assert!(matches!(
        f.drain.drain_checked().await,
        Err(DrainFailure::SyncBusy)
    ));
    let alive = Arc::new(AtomicBool::new(true));
    let guard = crate::app::required_owner::RequiredOwner::observed(alive.clone(), |_| {});
    let handle = f.drain.clone().start_bounded(
        Duration::from_millis(5),
        Some(guard),
        Duration::from_secs(3),
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(alive.load(Ordering::Acquire));
    assert_eq!(
        f.sync
            .control()
            .await
            .unwrap()
            .unwrap()
            .watermark
            .as_deref(),
        Some("B")
    );
    finish(&f).await;
    assert_eq!(f.drain.drain_once().await.unwrap(), 0);
    assert!(handle.shutdown().await.unwrap().is_ok());
    assert!(!alive.load(Ordering::Acquire));
}

#[tokio::test]
async fn sync_busy_budget_exhaustion_is_visible_without_sending_or_checkpoint_advance() {
    let f = fixture().await;
    let alive = Arc::new(AtomicBool::new(true));
    let cause = Arc::new(std::sync::Mutex::new(None));
    let seen = cause.clone();
    let guard = crate::app::required_owner::RequiredOwner::observed(alive.clone(), move |class| {
        *seen.lock().unwrap() = Some(class);
    });
    let handle = f.drain.clone().start_bounded(
        Duration::from_millis(5),
        Some(guard),
        Duration::from_millis(30),
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while alive.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*cause.lock().unwrap(), Some("sync-busy-budget"));
    assert!(handle.shutdown().await.unwrap().is_err());
    assert_eq!(
        f.sync
            .control()
            .await
            .unwrap()
            .unwrap()
            .watermark
            .as_deref(),
        Some("B")
    );
    finish(&f).await;
    f.drain.store.release_outbox_component().await.unwrap();
}

#[tokio::test]
async fn unknown_hold_and_lost_fence_are_fatal_even_when_sync_is_busy() {
    let f = fixture().await;
    f.sync
        .refuse_response(
            &f.operation,
            "sync",
            f.epoch,
            "synthetic-refusal",
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
    assert!(matches!(
        f.drain.drain_checked().await,
        Err(DrainFailure::Storage)
    ));

    let f = fixture().await;
    // Deliberate negative fixture: expired authority must win over the busy category.
    sqlx::query("UPDATE matrix_component_leases SET expires_at='2000-01-01T00:00:00Z' WHERE component='OutboxDrain'")
        .execute(f.sync.pool()).await.unwrap();
    assert!(matches!(
        f.drain.drain_checked().await,
        Err(DrainFailure::Storage)
    ));
}
