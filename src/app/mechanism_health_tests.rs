use super::assembly_tests::runtime_fixture;
use super::*;
use crate::storage::ReliabilityStore;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

async fn dead(alive: &AtomicBool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while alive.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn never_polled_required_worker_abort_is_visible_before_shutdown() {
    let (runtime, root) = runtime_fixture().await;
    let alive = Arc::new(AtomicBool::new(true));
    runtime.health_state.register_required(alive.clone());
    let task = spawn_task_worker(
        std::future::pending(),
        alive.clone(),
        runtime.first_failure.clone(),
        runtime.health_state.clone(),
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!alive.load(Ordering::Acquire));
    assert_ne!(
        runtime.health_state.snapshot().await.readiness,
        crate::runtime::Readiness::Ready
    );
    assert_eq!(
        runtime.first_failure.lock().unwrap().unwrap().class,
        "unexpected-exit"
    );
    assert!(runtime.shutdown().await.is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn required_broadcaster_abort_and_heartbeat_refusal_are_unhealthy() {
    let (mut runtime, root) = runtime_fixture().await;
    let broadcaster = runtime.broadcaster.take().unwrap();
    broadcaster.abort();
    assert!(broadcaster.await.unwrap_err().is_cancelled());
    assert_ne!(
        runtime.health_state.snapshot().await.readiness,
        crate::runtime::Readiness::Ready
    );
    assert_eq!(
        runtime.first_failure.lock().unwrap().unwrap().component,
        "event-broadcaster"
    );
    assert!(runtime.shutdown().await.is_err());
    std::fs::remove_dir_all(root).unwrap();

    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("config.toml");
    std::fs::write(&config,format!("[bridge]\ndatabase={:?}\nsession_root={:?}\nhealth_bind=\"127.0.0.1:0\"\nowner_evidence_max_age_seconds=3\n",root.path().join("state.db"),root.path().join("sessions"))).unwrap();
    let runtime = AppRuntime::start(config).await.unwrap();
    sqlx::query("CREATE TRIGGER heartbeat_refused BEFORE UPDATE ON runtime_instances BEGIN SELECT RAISE(ABORT,'heartbeat refusal'); END").execute(runtime.pool.as_ref().unwrap()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if runtime.first_failure.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        runtime.first_failure.lock().unwrap().unwrap().component,
        "owner-heartbeat"
    );
    assert_ne!(
        runtime.health_state.snapshot().await.readiness,
        crate::runtime::Readiness::Ready
    );
    sqlx::query("DROP TRIGGER heartbeat_refused")
        .execute(runtime.pool.as_ref().unwrap())
        .await
        .unwrap();
    assert!(runtime.shutdown().await.is_err());
}

struct CasSender {
    pool: SqlitePool,
    calls: AtomicUsize,
}

#[tokio::test]
async fn explicit_completion_failure_during_stopping_keeps_first_cause() {
    let (runtime, root) = runtime_fixture().await;
    let alive = Arc::new(AtomicBool::new(true));
    runtime.health_state.register_required(alive.clone());
    let mut guard = required_owner(
        runtime.health_state.clone(),
        runtime.first_failure.clone(),
        "outbox-drain",
        alive.clone(),
    );
    runtime.health_state.set_owner(OwnerState::Stopping);
    guard.fail("completion-blocked");
    assert!(!alive.load(Ordering::Acquire));
    assert!(matches!(
        runtime.shutdown().await,
        Err(AppError::RequiredOwner {
            component: "outbox-drain",
            class: "completion-blocked",
        })
    ));
    std::fs::remove_dir_all(root).unwrap();
}
impl crate::matrix::MatrixOutboxSender for CasSender {
    fn send_stable<'a>(
        &'a self,
        _: &'a str,
        _: Option<&'a str>,
        _: Option<&'a str>,
        _: &'a str,
        txn: &'a str,
    ) -> crate::matrix::ReplyFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            sqlx::query("UPDATE projection_outbox SET claim_revision=claim_revision+1 WHERE stable_txn_id=?").bind(txn).execute(&self.pool).await.unwrap();
            Ok(())
        })
    }
}

#[tokio::test]
async fn completion_cas_failure_stops_actual_drain_marks_health_and_never_resends() {
    let (runtime, root) = runtime_fixture().await;
    let private = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(private.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    sqlx::query("INSERT INTO projection_outbox(source_kind,source_id,projection,transport,stable_txn_id,room_id,render_version,body,body_hash,state,attempt_count,created_at,updated_at) VALUES('transport_receipt','source','command_reply','matrix','stable','room','v1','synthetic','hash','pending',0,'now','now')").execute(&pool).await.unwrap();
    let sender = Arc::new(CasSender {
        pool: pool.clone(),
        calls: AtomicUsize::new(0),
    });
    let drain =
        crate::app::OutboxDrain::new(ReliabilityStore::new(pool.clone()), sender.clone(), 1);
    let alive = Arc::new(AtomicBool::new(true));
    runtime.health_state.register_required(alive.clone());
    let guard = required_owner(
        runtime.health_state.clone(),
        runtime.first_failure.clone(),
        "outbox-drain",
        alive.clone(),
    );
    let handle = drain
        .clone()
        .start_supervised(Duration::from_millis(1), Some(guard));
    dead(&alive).await;
    assert_ne!(
        runtime.health_state.snapshot().await.readiness,
        crate::runtime::Readiness::Ready
    );
    assert_eq!(
        runtime.first_failure.lock().unwrap().unwrap().class,
        "completion-blocked"
    );
    assert!(handle.shutdown().await.unwrap().is_err());
    assert!(drain.clone().drain_once().await.is_err());
    let state: String = sqlx::query_scalar("SELECT state FROM projection_outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "sending");
    drop(drain);
    pool.close().await;
    let reopened = crate::storage::connect(private.path().join("state.db"))
        .await
        .unwrap();
    let again = crate::app::OutboxDrain::new(ReliabilityStore::new(reopened), sender.clone(), 1);
    assert!(again.drain_once().await.is_err());
    assert_eq!(sender.calls.load(Ordering::SeqCst), 1);
    assert!(runtime.shutdown().await.is_err());
    std::fs::remove_dir_all(root).unwrap();
}
