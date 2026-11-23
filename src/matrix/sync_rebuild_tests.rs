use super::*;
use serde_json::json;
use tracing::instrument::WithSubscriber;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex, query_param},
};
#[path = "sync_rebuild_tests/support.rs"]
mod support;
use support::*;

#[tokio::test]
async fn slow_pagination_outlives_original_lease_without_admitting_partial_history() {
    let server = MockServer::start().await;
    let f = fixture(&server).await;
    for i in 1..=13 {
        page(&server, i, 13, Duration::from_millis(5100), false).await;
    }
    let before = preserved(&f.pool).await;
    let start = std::time::Instant::now();
    let heartbeat = async {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            assert!(
                f.owner
                    .publish_owner_heartbeat(
                        "owner",
                        &crate::storage::RuntimeOwnerEvidence::fingerprint().unwrap(),
                        &timestamp(),
                        &crate::storage::RuntimeOwnerEvidence::capture().unwrap()
                    )
                    .await
                    .unwrap()
            );
        }
    };
    tokio::select! {
        result = f.sync.rebuild_state() => result.unwrap(),
        () = heartbeat => unreachable!(),
    }
    assert!(start.elapsed() > Duration::from_secs(60));
    let valid:bool=sqlx::query_scalar("SELECT julianday(expires_at)>julianday('now') FROM matrix_component_leases WHERE component='MatrixSync'").fetch_one(&f.pool).await.unwrap();
    assert!(valid);
    assert!(f.sync.initial_response.lock().unwrap().is_some());
    assert_eq!(preserved(&f.pool).await, before);
    // Drop/startup cancellation cannot leave a detached renewal task.
    let binding = f.sync.recovery.as_ref().unwrap();
    MatrixSyncLease::new(binding.clone())
        .release()
        .await
        .unwrap();
    joined_stop(&f).await;
    f.pool.close().await;
}

#[tokio::test]
async fn failed_fill_records_hold_releases_exact_lease_and_joins_owner_without_replay() {
    let server = MockServer::start().await;
    let f = fixture(&server).await;
    page(&server, 1, 2, Duration::ZERO, false).await;
    page(&server, 2, 2, Duration::ZERO, true).await;
    let before = preserved(&f.pool).await;
    let capture = crate::test_diagnostics::Capture::default();
    assert!(matches!(
        f.sync
            .rebuild_state()
            .with_subscriber(capture.subscriber())
            .await,
        Err(MatrixError::CutoverRefused)
    ));
    let output = capture.output();
    assert!(
        output.contains("anchor-digest-mismatch") && output.contains("committed=true"),
        "{output}"
    );
    for secret in [
        "$anchor",
        "page-1",
        "changed-secret",
        "private-fixture-token",
        "private-fixture-body",
    ] {
        assert!(!output.contains(secret), "{secret}");
    }
    let state:(String,String,String)=sqlx::query_as("SELECT c.phase,c.restart_hold_reason,l.state FROM matrix_recovery_control c JOIN matrix_component_leases l ON l.component='MatrixSync'").fetch_one(&f.pool).await.unwrap();
    assert_eq!(
        state,
        (
            "refused".into(),
            "rebuild-history-gap".into(),
            "expired".into()
        )
    );
    assert!(f.sync.initial_response.lock().unwrap().is_none());
    assert_eq!(preserved(&f.pool).await, before);
    joined_stop(&f).await;
    assert_eq!(preserved(&f.pool).await, before);
    // Exact retry after the committed hold is read-only and remains refused.
    let bytes = std::fs::read(f.root.path().join("state.db-wal")).unwrap_or_default();
    assert!(f.sync.rebuild_state().await.is_err());
    assert_eq!(
        std::fs::read(f.root.path().join("state.db-wal")).unwrap_or_default(),
        bytes
    );
    assert_eq!(preserved(&f.pool).await, before);
    f.pool.close().await;
}

#[tokio::test]
async fn page_timeout_preserves_first_cause_and_commits_refusal() {
    let server = MockServer::start().await;
    let f = fixture(&server).await;
    page(&server, 1, 1, Duration::from_secs(7), false).await;
    let capture = crate::test_diagnostics::Capture::default();
    assert!(matches!(
        f.sync
            .rebuild_state()
            .with_subscriber(capture.subscriber())
            .await,
        Err(MatrixError::Transport)
    ));
    assert!(capture.output().contains("page-timeout"));
    assert!(capture.output().contains("committed=true"));
    joined_stop(&f).await;
    f.pool.close().await;
}

#[tokio::test]
async fn lease_drift_cancels_slow_fill_and_cannot_record_or_admit_response() {
    let server = MockServer::start().await;
    let f = fixture(&server).await;
    for i in 1..=8 {
        page(&server, i, 8, Duration::from_secs(5), false).await;
    }
    let before = preserved(&f.pool).await;
    let capture = crate::test_diagnostics::Capture::default();
    let drift = async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        // Deliberate isolated corruption must refuse through lineage/fence checks.
        sqlx::query("UPDATE matrix_component_leases SET owner_token='competitor',epoch=epoch+1 WHERE component='MatrixSync'").execute(&f.pool).await.unwrap();
    };
    let (result, ()) = tokio::join!(
        f.sync.rebuild_state().with_subscriber(capture.subscriber()),
        drift
    );
    assert!(matches!(result, Err(MatrixError::Storage)));
    assert!(capture.output().contains("lease-renewal"));
    assert!(!capture.output().contains("committed=true"));
    assert!(f.sync.initial_response.lock().unwrap().is_none());
    assert_eq!(preserved(&f.pool).await, before);
    let phase: String = sqlx::query_scalar("SELECT phase FROM matrix_recovery_control")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(phase, "active");
    let requests = server.received_requests().await.unwrap().len();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.received_requests().await.unwrap().len(), requests);
    f.pool.close().await;
}

#[tokio::test]
async fn final_fence_rejects_completion_before_periodic_tick() {
    let server = MockServer::start().await;
    let f = fixture(&server).await;
    page(&server, 1, 1, Duration::from_millis(500), false).await;
    let drift = async {
        tokio::time::sleep(Duration::from_millis(150)).await;
        sqlx::query("UPDATE matrix_component_leases SET epoch=epoch+1")
            .execute(&f.pool)
            .await
            .unwrap();
    };
    let capture = crate::test_diagnostics::Capture::default();
    let (result, ()) = tokio::join!(
        f.sync.rebuild_state().with_subscriber(capture.subscriber()),
        drift
    );
    assert!(matches!(result, Err(MatrixError::Storage)));
    assert!(capture.output().contains("final-lease-renewal"));
    assert!(f.sync.initial_response.lock().unwrap().is_none());
    f.pool.close().await;
}

#[tokio::test]
async fn refusal_write_failure_rolls_back_and_preserves_original_predicate() {
    let server = MockServer::start().await;
    let f = fixture(&server).await;
    page(&server, 1, 1, Duration::ZERO, true).await;
    sqlx::query("CREATE TRIGGER fixture_refusal_failure BEFORE UPDATE OF phase ON matrix_recovery_control WHEN NEW.phase='refused' BEGIN SELECT RAISE(ABORT,'fixture failure'); END").execute(&f.pool).await.unwrap();
    let before = preserved(&f.pool).await;
    let capture = crate::test_diagnostics::Capture::default();
    assert!(matches!(
        f.sync
            .rebuild_state()
            .with_subscriber(capture.subscriber())
            .await,
        Err(MatrixError::CutoverRefused)
    ));
    let output = capture.output();
    assert!(output.contains("anchor-digest-mismatch"));
    assert!(!output.contains("committed=true"));
    let phase: String = sqlx::query_scalar("SELECT phase FROM matrix_recovery_control")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(phase, "active");
    assert_eq!(preserved(&f.pool).await, before);
    sqlx::query("DROP TRIGGER fixture_refusal_failure")
        .execute(&f.pool)
        .await
        .unwrap();
    let retry_capture = crate::test_diagnostics::Capture::default();
    assert!(
        f.sync
            .rebuild_state()
            .with_subscriber(retry_capture.subscriber())
            .await
            .is_err()
    );
    assert!(retry_capture.output().contains("rebuild-retry-refused"));
    assert!(f.sync.initial_response.lock().unwrap().is_none());
    assert_eq!(preserved(&f.pool).await, before);
    MatrixSyncLease::new(f.sync.recovery.as_ref().unwrap().clone())
        .release()
        .await
        .unwrap();
    joined_stop(&f).await;
    f.pool.close().await;
}

#[tokio::test]
async fn coverage_deadline_cancels_work_while_lease_remains_valid() {
    let server = MockServer::start().await;
    let f = fixture(&server).await;
    let binding = f.sync.recovery.as_ref().unwrap();
    let result = guarded(
        binding,
        coverage(std::future::pending::<Result<(), Failure>>()),
    )
    .await;
    let Guarded::Finished(Err(failure)) = result else {
        panic!("deadline expected")
    };
    assert_eq!(failure.reason, "coverage-deadline");
    assert!(renew(binding).await.is_ok());
    finish_failure(binding, &failure).await;
    joined_stop(&f).await;
    f.pool.close().await;
}

#[tokio::test]
async fn history_refusal_teardown_rejects_pending_work_and_foreign_fence() {
    for mode in ["batch", "receipt", "fence", "reason"] {
        let server = MockServer::start().await;
        let f = fixture(&server).await;
        let binding = f.sync.recovery.as_ref().unwrap();
        if mode == "batch" || mode == "receipt" {
            binding
                .store
                .begin_batch(
                    "pending",
                    Some("B"),
                    "C",
                    "pending-digest",
                    "B",
                    "owner",
                    binding.owner_epoch,
                    &["$pending".into()],
                    &timestamp(),
                )
                .await
                .unwrap();
            if mode == "receipt" {
                binding
                    .store
                    .reserve_event(
                        "@bridge:example.test",
                        "!room:example.test",
                        "$pending",
                        &binding.operation_id,
                        "pending-digest",
                        "pending",
                        &timestamp(),
                    )
                    .await
                    .unwrap();
            }
        }
        binding
            .store
            .refuse_response(
                &binding.operation_id,
                &binding.owner_token,
                binding.owner_epoch,
                if mode == "reason" {
                    "other-reason"
                } else {
                    "rebuild-history-gap"
                },
                &timestamp(),
            )
            .await
            .unwrap();
        let mut attempt = binding.clone();
        if mode == "fence" {
            attempt.owner_epoch += 1;
        }
        assert!(
            MatrixSyncLease::new(attempt).release().await.is_err(),
            "{mode}"
        );
        let state: String = sqlx::query_scalar(
            "SELECT state FROM matrix_component_leases WHERE component='MatrixSync'",
        )
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert_eq!(state, "active");
        f.pool.close().await;
    }
}
