use std::{path::PathBuf, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use guigu_agent_bridge::{
    bus::derive_endpoint_id,
    models::{DeliveryId, TaskId},
    runtime::{
        AcquireOutcome, ContinuationPolicy, ContinuationState, Degradation, ExecutionResourceKey,
        Lease, Metric, PolicyLimit, Readiness, ReleaseDisposition, RuntimeError, RuntimeMetrics,
        SqliteRuntimeStore, WorkspaceId, health_snapshot,
    },
    storage::{connect, migrate},
};

fn at() -> DateTime<Utc> {
    "2026-09-17T12:00:00Z".parse().unwrap()
}

struct Db {
    pool: sqlx::SqlitePool,
    path: PathBuf,
    task: TaskId,
    delivery: DeliveryId,
}
impl Db {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!("runtime-{}.db", uuid::Uuid::now_v7()));
        let pool = connect(&path).await.unwrap();
        migrate(&pool).await.unwrap();
        let task = TaskId::generate();
        let delivery = DeliveryId::generate();
        let from = derive_endpoint_id("from");
        let to = derive_endpoint_id("to");
        for (endpoint, name) in [(from, "from"), (to, "to")] {
            sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES(?,?,'acp',1,'{\"acp\":{\"command\":\"mock\",\"args\":[]}}','[]')").bind(endpoint.to_string()).bind(name).execute(&pool).await.unwrap();
        }
        let conversation = uuid::Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
            .bind(&conversation)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'prompt',5,0,0,0)").bind(task.to_string()).bind(task.to_string()).bind(from.to_string()).bind(to.to_string()).bind(conversation).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at,acknowledged_at) VALUES(?,?,1,?,?,?)").bind(delivery.to_string()).bind(task.to_string()).bind(to.to_string()).bind(at().to_rfc3339()).bind(at().to_rfc3339()).execute(&pool).await.unwrap();
        Self {
            pool,
            path,
            task,
            delivery,
        }
    }
    async fn close(self) {
        self.pool.close().await;
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{}", self.path.display(), suffix));
        }
    }
}

fn key() -> ExecutionResourceKey {
    ExecutionResourceKey::new(
        derive_endpoint_id("to"),
        WorkspaceId::from_canonical_path("/tmp").unwrap(),
    )
}

#[derive(Clone, Copy)]
enum ContinuationWrite {
    Claim,
    Activity,
    Continue,
    Finish,
}

async fn prepared_write(
    write: ContinuationWrite,
) -> (
    Db,
    SqliteRuntimeStore,
    SqliteRuntimeStore,
    Lease,
    u64,
    String,
) {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let other_pool = connect(&db.path).await.unwrap();
    migrate(&other_pool).await.unwrap();
    let other = SqliteRuntimeStore::new(other_pool);
    let lease = match store
        .acquire(key(), db.task, at(), Duration::from_secs(1))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(value) => value,
        _ => panic!("acquire"),
    };
    store
        .begin_continuation(&lease, db.delivery, "first", at())
        .await
        .unwrap();
    let ready = store.continuation(db.task).await.unwrap().unwrap();
    let continuation = if matches!(write, ContinuationWrite::Claim) {
        ready
    } else {
        store
            .claim_turn(&lease, ready.revision, at())
            .await
            .unwrap()
    };
    let revision = continuation.revision;
    let generation = continuation.runtime_generation;
    (db, store, other, lease, revision, generation)
}

async fn attempt_write(
    store: &SqliteRuntimeStore,
    lease: &Lease,
    revision: u64,
    generation: &str,
    write: ContinuationWrite,
    now: DateTime<Utc>,
) -> Result<(), RuntimeError> {
    match write {
        ContinuationWrite::Claim => store.claim_turn(lease, revision, now).await.map(drop),
        ContinuationWrite::Activity => {
            store
                .record_activity_with_generation(lease, revision, generation, 1, now)
                .await
        }
        ContinuationWrite::Continue => store
            .record_continue(lease, revision, "next", 1, now)
            .await
            .map(drop),
        ContinuationWrite::Finish => {
            store
                .finish_with_generation(
                    lease,
                    revision,
                    generation,
                    ContinuationState::Terminal,
                    1,
                    now,
                )
                .await
        }
    }
}

#[tokio::test]
async fn every_continuation_write_is_fenced_after_cross_connection_recovery() {
    for write in [
        ContinuationWrite::Claim,
        ContinuationWrite::Activity,
        ContinuationWrite::Continue,
        ContinuationWrite::Finish,
    ] {
        let (db, store, other, lease, revision, generation) = prepared_write(write).await;
        other
            .release(&lease, ReleaseDisposition::RecoveryNeeded, at())
            .await
            .unwrap();
        assert!(matches!(
            attempt_write(&store, &lease, revision, &generation, write, at()).await,
            Err(RuntimeError::Fenced)
        ));
        drop(other);
        db.close().await;
    }
}

#[tokio::test]
async fn every_continuation_write_is_fenced_after_cross_connection_expiry() {
    let expired_at = at() + chrono::Duration::seconds(2);
    for write in [
        ContinuationWrite::Claim,
        ContinuationWrite::Activity,
        ContinuationWrite::Continue,
        ContinuationWrite::Finish,
    ] {
        let (db, store, other, lease, revision, generation) = prepared_write(write).await;
        assert_eq!(other.counts_at(expired_at).await.unwrap().expired_leases, 1);
        assert!(matches!(
            attempt_write(&store, &lease, revision, &generation, write, expired_at).await,
            Err(RuntimeError::Fenced)
        ));
        drop(other);
        db.close().await;
    }
}

#[tokio::test]
async fn continuation_response_receipts_are_idempotent_and_hash_bound() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let lease = match store
        .acquire(key(), db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(value) => value,
        _ => panic!("acquire"),
    };
    store
        .begin_continuation(&lease, db.delivery, "first", at())
        .await
        .unwrap();
    let continuation = store.continuation(db.task).await.unwrap().unwrap();
    store
        .record_response_receipt(&continuation, "structured", "receipt", at())
        .await
        .unwrap();
    store
        .record_response_receipt(&continuation, "structured", "receipt", at())
        .await
        .unwrap();
    assert!(matches!(
        store
            .record_response_receipt(&continuation, "structured", "altered", at())
            .await,
        Err(RuntimeError::Continuation)
    ));
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM continuation_response_receipts WHERE task_id=?")
            .bind(db.task.to_string())
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    db.close().await;
}

#[tokio::test]
async fn continuation_generation_mismatch_is_fenced_before_claim() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let lease = match store
        .acquire(key(), db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(value) => value,
        _ => panic!("acquire"),
    };
    store
        .begin_continuation(&lease, db.delivery, "first", at())
        .await
        .unwrap();
    let ready = store.continuation(db.task).await.unwrap().unwrap();
    sqlx::query(
        "UPDATE task_continuations SET runtime_generation='generation.v2:changed' WHERE task_id=?",
    )
    .bind(db.task.to_string())
    .execute(&db.pool)
    .await
    .unwrap();
    assert!(matches!(
        store.claim_turn(&lease, ready.revision, at()).await,
        Err(RuntimeError::Fenced)
    ));
    let state: String = sqlx::query_scalar("SELECT state FROM task_continuations WHERE task_id=?")
        .bind(db.task.to_string())
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(state, "ready");
    db.close().await;
}

#[tokio::test]
async fn captured_generation_is_used_by_atomic_continue_receipt() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let lease = match store
        .acquire(key(), db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(value) => value,
        _ => panic!("acquire"),
    };
    store
        .begin_continuation(&lease, db.delivery, "first", at())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_continuations SET runtime_generation='generation.v2:valid' WHERE task_id=?",
    )
    .bind(db.task.to_string())
    .execute(&db.pool)
    .await
    .unwrap();
    let ready = store.continuation(db.task).await.unwrap().unwrap();
    let running = store
        .claim_turn_with_generation(&lease, ready.revision, "generation.v2:valid", at())
        .await
        .unwrap();
    let next = store
        .record_continue_with_receipt(
            &lease,
            running.revision,
            "generation.v2:valid",
            "next",
            1,
            "structured",
            "response",
            at(),
        )
        .await
        .unwrap();
    assert_eq!(next.runtime_generation, "generation.v2:valid");
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM continuation_response_receipts WHERE task_id=?")
            .bind(db.task.to_string())
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(receipts, 1);
    db.close().await;
}

#[tokio::test]
async fn activity_requires_the_captured_generation() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let lease = match store
        .acquire(key(), db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(value) => value,
        _ => panic!("acquire"),
    };
    store
        .begin_continuation(&lease, db.delivery, "first", at())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_continuations SET runtime_generation='generation.v2:valid' WHERE task_id=?",
    )
    .bind(db.task.to_string())
    .execute(&db.pool)
    .await
    .unwrap();
    let ready = store.continuation(db.task).await.unwrap().unwrap();
    let running = store
        .claim_turn_with_generation(&lease, ready.revision, "generation.v2:valid", at())
        .await
        .unwrap();
    store
        .record_activity_with_generation(&lease, running.revision, "generation.v2:valid", 3, at())
        .await
        .unwrap();
    assert!(matches!(
        store
            .record_activity_with_generation(
                &lease,
                running.revision,
                "generation.v1:stale",
                1,
                at()
            )
            .await,
        Err(RuntimeError::Fenced)
    ));
    db.close().await;
}

#[tokio::test]
async fn concurrent_acquire_is_atomic_and_stale_owners_are_fenced() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let a = store.clone();
    let b = store.clone();
    let task = db.task;
    let (left, right) = tokio::join!(
        a.acquire(key(), task, at(), Duration::from_secs(30)),
        b.acquire(key(), task, at(), Duration::from_secs(30))
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|v| matches!(v, AcquireOutcome::Acquired(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|v| matches!(v, AcquireOutcome::Busy))
            .count(),
        1
    );
    let first = outcomes
        .into_iter()
        .find_map(|v| match v {
            AcquireOutcome::Acquired(v) => Some(v),
            _ => None,
        })
        .unwrap();
    store
        .release(&first, ReleaseDisposition::Released, at())
        .await
        .unwrap();
    let second = match store
        .acquire(key(), task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(v) => v,
        _ => panic!("reacquire"),
    };
    assert!(second.fence > first.fence);
    assert!(matches!(
        store
            .release(&first, ReleaseDisposition::Released, at())
            .await,
        Err(RuntimeError::Fenced)
    ));
    store
        .release(&second, ReleaseDisposition::RecoveryNeeded, at())
        .await
        .unwrap();
    assert_eq!(
        store
            .acquire(key(), task, at(), Duration::from_secs(30))
            .await
            .unwrap(),
        AcquireOutcome::RecoveryNeeded
    );
    db.close().await;
}

#[tokio::test]
async fn equal_and_nested_workspace_claims_conflict_until_atomic_release() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let root = std::env::temp_dir().join(format!("claims-{}", uuid::Uuid::now_v7()));
    let nested = root.join("nested");
    std::fs::create_dir_all(&nested).unwrap();
    let first_key = ExecutionResourceKey::new(
        derive_endpoint_id("to"),
        WorkspaceId::from_canonical_path(&root).unwrap(),
    );
    let second_key = ExecutionResourceKey::new(
        derive_endpoint_id("from"),
        WorkspaceId::from_canonical_path(&nested).unwrap(),
    );
    let first = match store
        .acquire(first_key, db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(value) => value,
        _ => panic!("first lease"),
    };
    let second = match store
        .acquire(second_key, db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(value) => value,
        _ => panic!("second lease"),
    };
    store
        .claim_workspaces(&first, std::slice::from_ref(&root))
        .await
        .unwrap();
    assert!(matches!(
        store
            .claim_workspaces(&second, std::slice::from_ref(&root))
            .await,
        Err(RuntimeError::Busy)
    ));
    assert!(matches!(
        store
            .claim_workspaces(&second, std::slice::from_ref(&nested))
            .await,
        Err(RuntimeError::Busy)
    ));
    store
        .release(&first, ReleaseDisposition::Released, at())
        .await
        .unwrap();
    store
        .claim_workspaces(&second, std::slice::from_ref(&nested))
        .await
        .unwrap();
    store
        .release(&second, ReleaseDisposition::Released, at())
        .await
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
    db.close().await;
}

#[tokio::test]
async fn workspace_claim_batch_failure_rolls_back_all_rows_and_reuses_connection() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let root = std::env::temp_dir().join(format!("claims-fault-{}", uuid::Uuid::now_v7()));
    let failed = root.join("failed");
    std::fs::create_dir_all(&failed).unwrap();
    sqlx::query("CREATE TRIGGER fail_workspace_claim AFTER INSERT ON workspace_claims WHEN NEW.canonical_path LIKE '%/failed' BEGIN SELECT RAISE(ABORT, 'injected claim failure'); END")
        .execute(&db.pool).await.unwrap();
    let lease = match store
        .acquire(key(), db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(value) => value,
        _ => panic!("lease"),
    };
    assert!(
        store
            .claim_workspaces(&lease, &[root.clone(), failed.clone()])
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workspace_claims WHERE task_id=?")
            .bind(db.task.to_string())
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_workspace_claim")
        .execute(&db.pool)
        .await
        .unwrap();
    store
        .claim_workspaces(&lease, std::slice::from_ref(&root))
        .await
        .unwrap();
    store
        .release(&lease, ReleaseDisposition::Released, at())
        .await
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
    db.close().await;
}

#[tokio::test]
async fn release_failure_rolls_back_lease_and_claims_for_connection_reuse() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let root = std::env::temp_dir().join(format!("claims-release-fault-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let lease = match store
        .acquire(key(), db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(value) => value,
        _ => panic!("lease"),
    };
    store
        .claim_workspaces(&lease, std::slice::from_ref(&root))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fail_claim_delete BEFORE DELETE ON workspace_claims BEGIN SELECT RAISE(ABORT, 'injected release failure'); END")
        .execute(&db.pool).await.unwrap();
    assert!(
        store
            .release(&lease, ReleaseDisposition::Released, at())
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM execution_leases WHERE resource_key=?")
            .bind(lease.resource.to_string())
            .fetch_one(&db.pool)
            .await
            .unwrap(),
        "active"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workspace_claims WHERE task_id=? AND state='active'"
        )
        .bind(db.task.to_string())
        .fetch_one(&db.pool)
        .await
        .unwrap(),
        1
    );
    sqlx::query("DROP TRIGGER fail_claim_delete")
        .execute(&db.pool)
        .await
        .unwrap();
    store
        .release(&lease, ReleaseDisposition::Released, at())
        .await
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
    db.close().await;
}

#[tokio::test]
async fn expired_active_lease_becomes_recovery_needed_without_takeover() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    assert!(matches!(
        store
            .acquire(key(), db.task, at(), Duration::from_secs(1))
            .await
            .unwrap(),
        AcquireOutcome::Acquired(_)
    ));
    let later = at() + chrono::Duration::seconds(2);
    assert_eq!(
        store
            .acquire(key(), db.task, later, Duration::from_secs(30))
            .await
            .unwrap(),
        AcquireOutcome::RecoveryNeeded
    );
    let counts = store.counts_at(later).await.unwrap();
    assert_eq!(
        (counts.active_leases, counts.recovery_needed_leases),
        (0, 1)
    );
    db.close().await;
}

#[tokio::test]
async fn continuation_transitions_are_revision_fenced_and_persist_bounds() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let lease = match store
        .acquire(key(), db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(v) => v,
        _ => panic!("acquire"),
    };
    store
        .begin_continuation(&lease, db.delivery, "first", at())
        .await
        .unwrap();
    let ready = store.continuation(db.task).await.unwrap().unwrap();
    let running = store
        .claim_turn(&lease, ready.revision, at())
        .await
        .unwrap();
    assert_eq!(running.state, ContinuationState::InFlight);
    assert!(matches!(
        store.claim_turn(&lease, ready.revision, at()).await,
        Err(RuntimeError::Fenced)
    ));
    let next = store
        .record_continue(&lease, running.revision, "second", 12, at())
        .await
        .unwrap();
    assert_eq!(
        (
            next.next_turn,
            next.completed_turns,
            next.consecutive_no_progress,
            next.observed_output_bytes
        ),
        (2, 1, 1, 12)
    );
    let running = store.claim_turn(&lease, next.revision, at()).await.unwrap();
    store
        .finish_with_generation(
            &lease,
            running.revision,
            &running.runtime_generation,
            ContinuationState::Terminal,
            4,
            at(),
        )
        .await
        .unwrap();
    let final_state = store.continuation(db.task).await.unwrap().unwrap();
    assert_eq!(
        (
            final_state.state,
            final_state.completed_turns,
            final_state.observed_output_bytes
        ),
        (ContinuationState::Terminal, 2, 16)
    );
    db.close().await;
}

#[tokio::test]
async fn restart_snapshot_separates_ready_from_ambiguous_without_replay() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let lease = match store
        .acquire(key(), db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(value) => value,
        _ => panic!("acquire"),
    };
    store
        .begin_continuation(&lease, db.delivery, "resume me", at())
        .await
        .unwrap();
    assert_eq!(
        store.recovery_snapshot(at(), 8).await.unwrap().ready,
        vec![db.task]
    );
    let ready = store.continuation(db.task).await.unwrap().unwrap();
    store
        .claim_turn(&lease, ready.revision, at())
        .await
        .unwrap();
    let snapshot = store.recovery_snapshot(at(), 8).await.unwrap();
    assert!(snapshot.ready.is_empty());
    assert_eq!(snapshot.ambiguous, vec![db.task]);
    db.close().await;
}

#[tokio::test]
async fn policy_health_and_metrics_use_only_bounded_dimensions() {
    let db = Db::new().await;
    let store = SqliteRuntimeStore::new(db.pool.clone());
    let lease = match store
        .acquire(key(), db.task, at(), Duration::from_secs(30))
        .await
        .unwrap()
    {
        AcquireOutcome::Acquired(v) => v,
        _ => panic!("acquire"),
    };
    store
        .begin_continuation(&lease, db.delivery, "first", at())
        .await
        .unwrap();
    let state = store.continuation(db.task).await.unwrap().unwrap();
    let policy = ContinuationPolicy {
        max_turns: 0,
        max_wall_time: Duration::from_secs(60),
        max_inactivity: Duration::from_secs(60),
        max_consecutive_no_progress: 3,
        max_observed_output_bytes: 100,
        lease_ttl: Duration::from_secs(30),
    };
    assert!(matches!(
        policy.check(&state, at(), None),
        Err(RuntimeError::Exhausted(PolicyLimit::Turns))
    ));
    assert_eq!(
        health_snapshot(&store, at(), true, true).await.readiness,
        Readiness::Ready
    );
    store
        .release(&lease, ReleaseDisposition::RecoveryNeeded, at())
        .await
        .unwrap();
    let degraded = health_snapshot(&store, at(), true, true).await;
    assert_eq!(degraded.readiness, Readiness::RecoveryBlocked);
    assert!(degraded.degraded.contains(&Degradation::RecoveryBacklog));
    let metrics = Arc::new(RuntimeMetrics::default());
    metrics.increment(Metric::LeaseBusy);
    metrics.increment(Metric::LeaseBusy);
    metrics.set_gauges(store.counts().await.unwrap());
    assert_eq!(metrics.snapshot().values.get(&Metric::LeaseBusy), Some(&2));
    db.close().await;
}
