use super::*;
use crate::storage::MatrixRecoveryStore;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

struct BatchSender {
    sync: MatrixRecoveryStore,
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
}
impl crate::matrix::MatrixOutboxSender for BatchSender {
    fn send_stable<'a>(
        &'a self,
        _: &'a str,
        _: Option<&'a str>,
        _: Option<&'a str>,
        _: &'a str,
        _: &'a str,
    ) -> crate::matrix::ReplyFuture<'a> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.sync
                    .begin_batch(
                        "overlap",
                        Some("opaque-B"),
                        "C",
                        "digest",
                        "opaque-B",
                        "owner",
                        1,
                        &[],
                        &chrono::Utc::now().to_rfc3339(),
                    )
                    .await
                    .unwrap();
            }
            self.entered.notify_one();
            Ok(())
        })
    }
}

#[tokio::test]
async fn supervised_completion_survives_real_scoped_batch_without_resend() {
    let (fixture, store) = seeded().await;
    let scope = store.test_recovery_scope().unwrap();
    let sync = classified_sync(&fixture, &scope).await;
    let sender = Arc::new(BatchSender {
        sync,
        calls: AtomicUsize::new(0),
        entered: Default::default(),
    });
    let drain = crate::app::OutboxDrain::new(store.clone(), sender.clone(), 1);
    let alive = Arc::new(AtomicBool::new(true));
    let cause = Arc::new(std::sync::Mutex::new(None));
    let observed = cause.clone();
    let guard = crate::app::required_owner::RequiredOwner::observed(alive.clone(), move |c| {
        *observed.lock().unwrap() = Some(c)
    });
    let handle = drain.start_supervised(Duration::from_millis(10), Some(guard));
    sender.entered.notified().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let survived = alive.load(Ordering::Acquire);
    let guard_error = writer::begin(&fixture.pool, Some(&scope))
        .await
        .err()
        .unwrap();
    assert!(crate::storage::recovery_eligibility::matrix_effect_busy(
        &guard_error
    ));
    println!("controlled completion begin=offline-recovery-matrix-effect-busy");
    sender
        .sync
        .complete_batch("overlap", "C", "owner", 1, &chrono::Utc::now().to_rfc3339())
        .await
        .unwrap();
    if survived {
        tokio::time::timeout(Duration::from_secs(3), async {
            while state(&fixture).await.0 != "sent" {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    if survived {
        new_work(&fixture, &scope).await;
    }
    if survived {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let count: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM projection_outbox WHERE state='sent' AND attempt_count=1",
                )
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
                if count == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    let shutdown = handle.shutdown().await.unwrap();
    assert!(
        survived,
        "completion owner died: {:?}, state={:?}",
        *cause.lock().unwrap(),
        state(&fixture).await
    );
    assert!(shutdown.is_ok());
    assert_eq!(state(&fixture).await, ("sent".into(), 1, 1));
    assert_eq!(sender.calls.load(Ordering::SeqCst), 2);
    matching_classification(&fixture).await;
}

async fn busy_claim() -> (Fixture, ReliabilityStore, crate::storage::PendingProjection) {
    let (f, store) = seeded().await;
    let mut rows = store
        .claim_projections("claim", "now", "unused", 1)
        .await
        .unwrap();
    let sender = super::race_tests::OverlapSender {
        pool: f.pool.clone(),
        calls: Default::default(),
    };
    crate::matrix::MatrixOutboxSender::send_stable(&sender, "", None, None, "", "")
        .await
        .unwrap();
    (f, store, rows.remove(0))
}

#[tokio::test]
async fn completion_busy_budget_and_shutdown_preserve_sending_without_retrying_send() {
    for cancel in [false, true] {
        let (f, store, row) = busy_claim().await;
        let (stop, receiver) = tokio::sync::watch::channel(false);
        let timer = tokio::time::Instant::now();
        let future = crate::app::test_finish_projection(
            &store,
            &row,
            true,
            Some(receiver),
            Duration::from_millis(120),
        );
        let signal = async {
            if cancel {
                tokio::time::sleep(Duration::from_millis(20)).await;
                stop.send(true).unwrap();
            }
        };
        let (result, _) = tokio::join!(future, signal);
        assert!(
            matches!(result, Err(crate::storage::ReliabilityError::Sql(ref e)) if crate::storage::recovery_eligibility::matrix_effect_busy(e))
                || matches!(result, Err(crate::storage::ReliabilityError::Malformed))
        );
        assert!(timer.elapsed() < Duration::from_secs(3));
        assert_eq!(state(&f).await, ("sending".into(), 0, 1));
    }
}

#[tokio::test]
async fn completion_retry_rechecks_fence_hold_and_storage_failure() {
    for mutation in [
        "UPDATE matrix_component_leases SET owner_token='foreign' WHERE component='OutboxDrain'",
        "UPDATE matrix_component_leases SET expires_at='2000-01-01T00:00:00Z' WHERE component='OutboxDrain'",
        "UPDATE matrix_recovery_control SET restart_hold_state='processing'",
        "UPDATE matrix_recovery_batches SET state='unknown' WHERE batch_id='overlap'",
        "CREATE TRIGGER terminal_refused BEFORE UPDATE ON projection_outbox WHEN NEW.state='sent' BEGIN SELECT RAISE(ABORT,'secret-must-not-be-logged'); END",
    ] {
        let (f, store, row) = busy_claim().await;
        let future =
            crate::app::test_finish_projection(&store, &row, true, None, Duration::from_secs(2));
        let interfere = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            sqlx::query(sqlx::AssertSqlSafe(mutation))
                .execute(&f.pool)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE matrix_recovery_batches SET state='terminal' WHERE state='processing'",
            )
            .execute(&f.pool)
            .await
            .unwrap();
        };
        let (result, _) = tokio::join!(future, interfere);
        assert!(result.is_err());
        assert_eq!(state(&f).await, ("sending".into(), 0, 1));
    }
}

#[tokio::test]
async fn cancelled_completion_and_reopen_cannot_resend_successful_network_effect() {
    let (f, store) = seeded().await;
    let sender = Arc::new(super::race_tests::OverlapSender {
        pool: f.pool.clone(),
        calls: Default::default(),
    });
    let drain = crate::app::OutboxDrain::new(store.clone(), sender.clone(), 1);
    let running = tokio::spawn({
        let drain = drain.clone();
        async move { drain.drain_once().await }
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while sender.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    assert_eq!(state(&f).await, ("sending".into(), 0, 1));
    drop(drain);
    f.pool.close().await;
    let reopened = crate::storage::connect(f._directory.path().join("state.db"))
        .await
        .unwrap();
    let scoped =
        ReliabilityStore::with_recovery(reopened.clone(), store.test_recovery_scope().unwrap());
    let again = crate::app::OutboxDrain::new(scoped, sender.clone(), 1);
    assert!(again.drain_once().await.is_err());
    assert_eq!(sender.calls.load(Ordering::SeqCst), 1);
    reopened.close().await;
}

async fn new_work(f: &Fixture, scope: &RecoveryScope) {
    let mut tx = writer::begin(&f.pool, Some(scope)).await.unwrap();
    let source: String = sqlx::query_scalar("INSERT INTO projection_outbox(source_kind,source_id,projection,transport,stable_txn_id,room_id,render_version,body,body_hash,state,attempt_count,created_at,updated_at) SELECT source_kind,source_id,'observer',transport,'second-txn',room_id,render_version,body,body_hash,'pending',0,'now','now' FROM projection_outbox RETURNING source_id").fetch_one(&mut *tx).await.unwrap();
    let id =
        writer::composite_text_identity("projection_outbox", &["task_event", &source, "observer"])
            .unwrap();
    writer::inserted(&mut tx, Some(scope), &id).await.unwrap();
    tx.commit().await.unwrap();
}

async fn classified_sync(fixture: &Fixture, scope: &RecoveryScope) -> MatrixRecoveryStore {
    let sync_scope = scope.for_component(fixture.fence.clone());
    let mut tx = writer::begin(&fixture.pool, Some(&sync_scope))
        .await
        .unwrap();
    writer::inserted(
        &mut tx,
        Some(&sync_scope),
        &RowIdentity {
            table: "matrix_recovery_control".into(),
            key: "[\"integer:31\"]".into(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    MatrixRecoveryStore::new_scoped(fixture.pool.clone(), Some(sync_scope))
}

async fn matching_classification(f: &Fixture) {
    let rows: Vec<(String, String, String)> = sqlx::query_as("SELECT canonical_pk,original_revision,original_digest FROM offline_recovery_rows WHERE table_name='projection_outbox'").fetch_all(&f.pool).await.unwrap();
    for (key, revision, digest) in rows {
        let actual = crate::storage::recovery_eligibility::inventory::snapshot(
            &mut f.pool.acquire().await.unwrap(),
            &RowIdentity {
                table: "projection_outbox".into(),
                key,
            },
        )
        .await
        .unwrap();
        assert_eq!((actual.revision, actual.digest), (revision, digest));
    }
}

#[tokio::test]
async fn completion_deadline_bounds_scope_wait_without_claiming_sql_cancellation() {
    let (f, store) = seeded().await;
    let row = store
        .claim_projections("claim", "now", "unused", 1)
        .await
        .unwrap()
        .remove(0);
    let scope = store.test_recovery_scope().unwrap();
    let transaction = writer::begin(&f.pool, Some(&scope)).await.unwrap();
    let capture = crate::test_diagnostics::Capture::default();
    use tracing::instrument::WithSubscriber;
    let result =
        crate::app::test_finish_projection(&store, &row, true, None, Duration::from_millis(50))
            .with_subscriber(capture.subscriber())
            .await;
    assert!(matches!(
        result,
        Err(crate::storage::ReliabilityError::Malformed)
    ));
    assert!(capture.output().contains("completion-timeout"));
    transaction.rollback().await.unwrap();
    assert_eq!(state(&f).await, ("sending".into(), 0, 1));
    // This is a lock-wait timeout, not proof that an executing SQL statement stopped.
    assert!(store.mark_projection_sent(&row).await.unwrap());
    assert_eq!(state(&f).await, ("sent".into(), 1, 1));
}
