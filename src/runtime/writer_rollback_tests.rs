use super::*;

#[tokio::test]
async fn checkpoint_advance_fences_detached_scope_but_shared_clone_remains_usable() {
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
        panic!("native");
    };
    let context = bootstrap_nonadmin(&pool, true).await.unwrap();
    let shared = RecoveryScope::new(context.clone(), EffectDomain::Local, None);
    let detached = RecoveryScope::new(context, EffectDomain::Local, None);
    let clone = shared.clone();
    let store = crate::storage::MatrixRecoveryStore::new_scoped(pool.clone(), Some(shared));
    store
        .activate_cutover(&operation, "B", "now")
        .await
        .unwrap();
    assert!(writer::begin(&pool, Some(&detached)).await.is_err());
    writer::begin(&pool, Some(&clone))
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
}

async fn attempt(
    store: &SqliteRuntimeStore,
    lease: &Lease,
    claimed: &Continuation,
    finish: bool,
) -> Result<(), RuntimeError> {
    if finish {
        store
            .finish_with_generation(
                lease,
                claimed.revision,
                &claimed.runtime_generation,
                ContinuationState::RecoveryNeeded,
                0,
                Utc::now(),
            )
            .await
    } else {
        store
            .record_continue_with_receipt(
                lease,
                claimed.revision,
                &claimed.runtime_generation,
                "next",
                1,
                "structured",
                "continue",
                Utc::now(),
            )
            .await
            .map(|_| ())
    }
}

#[tokio::test]
async fn late_classification_failure_rolls_back_continue_and_unknown_finish() {
    for finish in [false, true] {
        let (root, pool) = mechanism_diagnostic_fixture().await;
        let (store, _, lease, claimed) =
            prepare(&pool, root.path(), crate::models::EndpointId::generate()).await;
        let before = inventory_image(&pool).await;
        // The final continuation classification update fails after business writes.
        sqlx::query("CREATE TRIGGER fail_classification BEFORE UPDATE ON offline_recovery_rows WHEN OLD.table_name='task_continuations' BEGIN SELECT RAISE(ABORT,'injected late classification failure'); END")
            .execute(&pool).await.unwrap();
        assert!(attempt(&store, &lease, &claimed, finish).await.is_err());
        assert_eq!(inventory_image(&pool).await, before);
        sqlx::query("DROP TRIGGER fail_classification")
            .execute(&pool)
            .await
            .unwrap();
        attempt(&store, &lease, &claimed, finish).await.unwrap();
        assert!(attempt(&store, &lease, &claimed, finish).await.is_err());
        drop(store);
        pool.close().await;
        let reopened = crate::storage::connect(root.path().join("state.db"))
            .await
            .unwrap();
        assert_eq!(mechanism_diagnostic_drift(&reopened).await, (0, 0));
        bootstrap_nonadmin(&reopened, true).await.unwrap();
    }
}

async fn inventory_image(pool: &SqlitePool) -> Vec<(String, String, String)> {
    use crate::storage::recovery_eligibility::inventory;
    let mut c = pool.acquire().await.unwrap();
    let mut rows = Vec::new();
    for id in inventory::identities(&mut c).await.unwrap() {
        let row = inventory::snapshot(&mut c, &id).await.unwrap();
        rows.push((id.table, id.key, row.digest));
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn cancellation_before_commit_releases_scoped_writer_and_rolls_back() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let (store, _, lease, claimed) =
        prepare(&pool, root.path(), crate::models::EndpointId::generate()).await;
    let before = inventory_image(&pool).await;
    let (sent, ready) = tokio::sync::oneshot::channel();
    let writer = store.clone();
    let writer_lease = lease.clone();
    let task = tokio::spawn(async move {
        let mut tx = writer.transaction().await.unwrap();
        let mutation = writer
            .prepare(
                &mut tx,
                "task_continuations",
                &writer_lease.task_id.to_string(),
            )
            .await
            .unwrap();
        sqlx::query("UPDATE task_continuations SET observed_output_bytes=observed_output_bytes+1 WHERE task_id=?")
            .bind(writer_lease.task_id.to_string()).execute(&mut *tx).await.unwrap();
        writer.complete(&mut tx, mutation).await.unwrap();
        sent.send(()).unwrap();
        std::future::pending::<()>().await;
        tx.commit().await.unwrap();
    });
    ready.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(inventory_image(&pool).await, before);
    // The same scope/connection remains usable and publishes exactly once.
    attempt(&store, &lease, &claimed, false).await.unwrap();
    assert_eq!(mechanism_diagnostic_drift(&pool).await, (0, 0));
}

#[tokio::test]
async fn legacy_helpers_maintain_classification_and_stale_generation_refuses() {
    let (root, pool) = mechanism_diagnostic_fixture().await;
    let (store, _, lease, claimed) =
        prepare(&pool, root.path(), crate::models::EndpointId::generate()).await;
    let mut wrong = claimed.clone();
    wrong.runtime_generation = "stale".into();
    let before = inventory_image(&pool).await;
    assert!(attempt(&store, &lease, &wrong, false).await.is_err());
    assert!(attempt(&store, &lease, &wrong, true).await.is_err());
    assert_eq!(inventory_image(&pool).await, before);
    let ready = store
        .record_continue(&lease, claimed.revision, "next", 1, Utc::now())
        .await
        .unwrap();
    store
        .claim_turn(&lease, ready.revision, Utc::now())
        .await
        .unwrap();
    assert_eq!(mechanism_diagnostic_drift(&pool).await, (0, 0));
    bootstrap_nonadmin(&pool, true).await.unwrap();
}
