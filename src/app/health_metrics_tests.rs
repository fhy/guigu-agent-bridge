use super::*;
use crate::storage::{connect, migrate};

async fn fixture() -> (super::HealthState, sqlx::SqlitePool, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("guigu-health-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let pool = connect(root.join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let state = super::HealthState::new(
        SqliteRuntimeStore::new(pool.clone()),
        Arc::new(RuntimeMetrics::default()),
        Arc::new(ProjectionMetrics::default()),
    );
    (state, pool, root)
}

async fn assert_ready_metrics(state: &super::HealthState) {
    assert_eq!(
        state.snapshot().await.readiness,
        crate::runtime::Readiness::Ready
    );
    let metrics = state.metrics_body().await;
    assert!(metrics.contains("guigu_readiness_evaluation{readiness=\"ready\"} 1"));
    assert!(metrics.contains("predicate=\"none\""));
}

#[tokio::test]
async fn metrics_matrix_matches_readiness_matrix() {
    let (state, _pool, root) = fixture().await;
    state.set_owner(OwnerState::Running);
    state.set_adapter_ready(true);
    assert_ready_metrics(&state).await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn metrics_matrix_failed_exposes_matrix_blocker() {
    let (state, _pool, root) = fixture().await;
    state.set_owner(OwnerState::Running);
    state.set_adapter_ready(true);
    let generation = state.matrix().begin();
    assert!(state.matrix().transition(
        generation,
        &[MatrixPhase::Preflight],
        MatrixPhase::Failed(MatrixFailure::Authentication)
    ));
    assert_eq!(
        state.snapshot().await.readiness,
        crate::runtime::Readiness::AdapterUnavailable
    );
    let metrics = state.metrics_body().await;
    assert!(metrics.contains("predicate=\"matrix-phase-not-ready\""));
    assert!(!metrics.contains("predicate=\"none\""));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn metrics_matrix_generic_child_dead_exposes_child_blocker() {
    let (state, _pool, root) = fixture().await;
    state.set_owner(OwnerState::Running);
    state.set_adapter_ready(true);
    state.register_required(Arc::new(std::sync::atomic::AtomicBool::new(false)));
    assert_eq!(
        state.snapshot().await.readiness,
        crate::runtime::Readiness::OwnerStopped
    );
    let metrics = state.metrics_body().await;
    assert!(metrics.contains("predicate=\"required-child-not-alive\""));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn metrics_first_matrix_child_dead_updates_phase_before_classifying() {
    let (state, _pool, root) = fixture().await;
    state.set_owner(OwnerState::Running);
    state.set_adapter_ready(true);
    let generation = state.matrix().begin();
    for (from, to) in [
        (MatrixPhase::Preflight, MatrixPhase::IdentityMatched),
        (MatrixPhase::IdentityMatched, MatrixPhase::StoreBound),
        (MatrixPhase::StoreBound, MatrixPhase::KeyProved),
        (MatrixPhase::KeyProved, MatrixPhase::Registering),
        (MatrixPhase::Registering, MatrixPhase::Ready),
    ] {
        assert!(state.matrix().transition(generation, &[from], to));
    }
    state.register_matrix_required(Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let metrics = state.metrics_body().await;
    assert!(metrics.contains("predicate=\"matrix-phase-not-ready\""));
    assert!(!metrics.contains("predicate=\"none\""));
    assert_eq!(
        state.snapshot().await.readiness,
        crate::runtime::Readiness::AdapterUnavailable
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn metrics_startup_recovery_block_matches_readiness() {
    let (state, _pool, root) = fixture().await;
    state.set_owner(OwnerState::Running);
    state.set_adapter_ready(true);
    state.set_recovery_blocked(true);
    assert_eq!(
        state.snapshot().await.readiness,
        crate::runtime::Readiness::RecoveryBlocked
    );
    let metrics = state.metrics_body().await;
    assert!(metrics.contains("predicate=\"recovery-blocked\""));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn metrics_current_recovery_lease_blocks_without_startup_flag() {
    let (state, pool, root) = fixture().await;
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys=OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES ('health-resource','health-task','health-owner',1,'recovery_needed','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')")
            .execute(&mut *connection)
            .await
            .unwrap();
    drop(connection);
    state.set_owner(OwnerState::Running);
    state.set_adapter_ready(true);
    assert_eq!(
        state.snapshot().await.readiness,
        crate::runtime::Readiness::RecoveryBlocked
    );
    let metrics = state.metrics_body().await;
    assert!(metrics.contains("current_recovery_leases 1"));
    assert!(metrics.contains("predicate=\"recovery-backlog\""));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn metrics_storage_failure_is_not_reported_as_none() {
    let (state, pool, root) = fixture().await;
    pool.close().await;
    let metrics = state.metrics_body().await;
    assert!(metrics.contains("readiness_storage_available 0"));
    assert!(metrics.contains("predicate=\"storage-unavailable\""));
    assert!(!metrics.contains("predicate=\"none\""));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn metrics_missing_room_key_counter_is_not_causal() {
    let (state, _pool, root) = fixture().await;
    state.set_owner(OwnerState::Running);
    state.set_adapter_ready(true);
    let generation = state.matrix().begin();
    for (from, to) in [
        (MatrixPhase::Preflight, MatrixPhase::IdentityMatched),
        (MatrixPhase::IdentityMatched, MatrixPhase::StoreBound),
        (MatrixPhase::StoreBound, MatrixPhase::KeyProved),
        (MatrixPhase::KeyProved, MatrixPhase::Registering),
        (MatrixPhase::Registering, MatrixPhase::Ready),
    ] {
        assert!(state.matrix().transition(generation, &[from], to));
    }
    state.matrix().record_missing_room_key();
    assert_ready_metrics(&state).await;
    let metrics = state.metrics_body().await;
    assert!(metrics.contains("matrix_missing_room_keys 1"));
    assert!(!metrics.contains("predicate=\"matrix-phase-not-ready\""));
    std::fs::remove_dir_all(root).unwrap();
}
