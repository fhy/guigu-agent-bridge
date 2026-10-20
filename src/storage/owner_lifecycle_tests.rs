use super::*;
use crate::storage::owner_observation::ScanOutcome;

#[path = "owner_consumer_tests.rs"]
mod consumer;

#[path = "owner_clock_fixture_tests.rs"]
mod clock_fixture;

struct Fixture {
    _root: tempfile::TempDir,
    pool: sqlx::SqlitePool,
    store: ReliabilityStore,
    evidence: RuntimeOwnerEvidence,
    token: String,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let pool = crate::storage::connect(root.path().join("owner.db"))
            .await
            .unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        let store = ReliabilityStore::new(pool.clone());
        Self {
            _root: root,
            pool,
            store,
            evidence: RuntimeOwnerEvidence {
                pid: 42,
                proc_starttime: 100,
                boot_id: "modeled-boot".into(),
                boottime_ns: 10_000_000_000,
            },
            token: uuid::Uuid::now_v7().to_string(),
        }
    }

    async fn start(&self) {
        assert!(
            self.store
                .begin_runtime_with_evidence(
                    &self.token,
                    "fingerprint",
                    &Utc::now().to_rfc3339(),
                    &self.evidence
                )
                .await
                .unwrap()
        );
    }

    async fn stop(&self) {
        assert!(
            self.store
                .stopping_runtime(&self.token, &Utc::now().to_rfc3339())
                .await
                .unwrap()
        );
    }

    fn observation(&self, outcome: ScanOutcome) -> FinalObservation {
        FinalObservation::modeled(outcome, 1, &self.token)
    }

    async fn qualified_count(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence e JOIN runtime_instances r USING(instance_token) WHERE e.state='reaped' AND e.is_current=1 AND r.state='stopped'")
            .fetch_one(&self.pool).await.unwrap()
    }
}

#[tokio::test]
async fn owner_conditional_start_heartbeat_finalizer_and_consumer() {
    let fixture = Fixture::new().await;
    fixture.start().await;
    let mut next = fixture.evidence.clone();
    next.boottime_ns += 1_000_000;
    assert!(
        fixture
            .store
            .publish_owner_heartbeat(
                &fixture.token,
                "fingerprint",
                &Utc::now().to_rfc3339(),
                &next
            )
            .await
            .unwrap()
    );
    fixture.stop().await;
    let observation = fixture.observation(ScanOutcome::Complete);
    let now = Utc::now().to_rfc3339();
    assert!(
        fixture
            .store
            .finalize_owner(&fixture.token, &now, &next, &observation)
            .await
            .unwrap()
    );
    assert!(
        fixture
            .store
            .verify_final_owner(&fixture.token, &now, &next, &observation)
            .await
            .unwrap()
    );
    assert_eq!(fixture.qualified_count().await, 1);
    let revisions: Vec<i64> = sqlx::query_scalar(
        "SELECT evidence_revision FROM runtime_owner_evidence ORDER BY evidence_revision",
    )
    .fetch_all(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(revisions, [1, 2, 3, 4]);
    assert!(
        !fixture
            .store
            .finalize_owner(&fixture.token, &now, &next, &observation)
            .await
            .unwrap()
    );
    fixture.pool.close().await;
}

#[tokio::test]
async fn owner_publication_failures_roll_back_startup_and_final_pair() {
    let fixture = Fixture::new().await;
    sqlx::query("CREATE TRIGGER fail_start BEFORE INSERT ON runtime_owner_evidence BEGIN SELECT RAISE(ABORT,'injected'); END").execute(&fixture.pool).await.unwrap();
    assert!(
        fixture
            .store
            .begin_runtime_with_evidence(
                &fixture.token,
                "fingerprint",
                &Utc::now().to_rfc3339(),
                &fixture.evidence
            )
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_instances")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query("DROP TRIGGER fail_start")
        .execute(&fixture.pool)
        .await
        .unwrap();
    fixture.start().await;
    fixture.stop().await;
    sqlx::query("CREATE TRIGGER fail_final BEFORE UPDATE ON runtime_instances WHEN NEW.state='stopped' BEGIN SELECT RAISE(ABORT,'injected'); END").execute(&fixture.pool).await.unwrap();
    assert!(
        fixture
            .store
            .finalize_owner(
                &fixture.token,
                &Utc::now().to_rfc3339(),
                &fixture.evidence,
                &fixture.observation(ScanOutcome::Complete)
            )
            .await
            .is_err()
    );
    let current: (i64, String) = sqlx::query_as(
        "SELECT evidence_revision,state FROM runtime_owner_evidence WHERE is_current=1",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(current, (2, "scan_pending".into()));
    assert_eq!(fixture.qualified_count().await, 0);
    fixture.pool.close().await;
}

#[tokio::test]
async fn owner_scan_outcomes_and_legacy_string_never_fabricate_reaped() {
    let fixture = Fixture::new().await;
    fixture.start().await;
    fixture.stop().await;
    for outcome in [
        ScanOutcome::Unavailable,
        ScanOutcome::Inaccessible,
        ScanOutcome::Overflow,
        ScanOutcome::Raced,
        ScanOutcome::ListedMembers,
    ] {
        assert!(
            !fixture
                .store
                .finalize_owner(
                    &fixture.token,
                    &Utc::now().to_rfc3339(),
                    &fixture.evidence,
                    &fixture.observation(outcome)
                )
                .await
                .unwrap()
        );
    }
    assert!(
        !fixture
            .store
            .publish_runtime_reaped(
                &fixture.token,
                &Utc::now().to_rfc3339(),
                1,
                "{\"outcome\":\"complete\",\"children\":[]}"
            )
            .await
            .unwrap()
    );
    let wrong_generation = FinalObservation::modeled(ScanOutcome::Complete, 1, "other-generation");
    assert!(
        !fixture
            .store
            .finalize_owner(
                &fixture.token,
                &Utc::now().to_rfc3339(),
                &fixture.evidence,
                &wrong_generation
            )
            .await
            .unwrap()
    );
    assert_eq!(fixture.qualified_count().await, 0);
    fixture.pool.close().await;
}

#[tokio::test]
async fn owner_clock_identity_failures_are_unknown_and_never_qualify() {
    for field in ["boot", "monotonic", "pid", "starttime", "missing"] {
        let fixture = Fixture::new().await;
        fixture.start().await;
        let mut changed = fixture.evidence.clone();
        match field {
            "boot" => changed.boot_id = "another-boot".into(),
            "monotonic" => changed.boottime_ns -= 1,
            "pid" => changed.pid += 1,
            "starttime" => changed.proc_starttime += 1,
            _ => changed.boot_id.clear(),
        }
        assert!(
            !fixture
                .store
                .publish_owner_heartbeat(
                    &fixture.token,
                    "fingerprint",
                    &Utc::now().to_rfc3339(),
                    &changed
                )
                .await
                .unwrap()
        );
        let state: String =
            sqlx::query_scalar("SELECT state FROM runtime_owner_evidence WHERE is_current=1")
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(state, "unknown");
        fixture.stop().await;
        assert!(
            !fixture
                .store
                .finalize_owner(
                    &fixture.token,
                    &Utc::now().to_rfc3339(),
                    &fixture.evidence,
                    &fixture.observation(ScanOutcome::Complete)
                )
                .await
                .unwrap()
        );
        fixture.pool.close().await;
    }
}

#[tokio::test]
async fn owner_time_and_revision_gap_cases_fail_closed() {
    clock_fixture::controlled_time_and_revision_cases().await;
}

#[tokio::test]
async fn owner_old_binary_missing_evidence_and_missing_schema_refuse() {
    let fixture = Fixture::new().await;
    assert!(
        fixture
            .store
            .begin_runtime(&fixture.token, "legacy", &Utc::now().to_rfc3339())
            .await
            .unwrap()
    );
    fixture.stop().await;
    assert!(
        !fixture
            .store
            .finalize_owner(
                &fixture.token,
                &Utc::now().to_rfc3339(),
                &fixture.evidence,
                &fixture.observation(ScanOutcome::Complete)
            )
            .await
            .unwrap()
    );
    sqlx::query("DROP TABLE runtime_owner_evidence")
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert!(
        fixture
            .store
            .finalize_owner(
                &fixture.token,
                &Utc::now().to_rfc3339(),
                &fixture.evidence,
                &fixture.observation(ScanOutcome::Complete)
            )
            .await
            .is_err()
    );
    fixture.pool.close().await;
}

#[test]
fn owner_window_and_clock_policy() {
    for window in [1, 30, 300] {
        assert!(fresh_timestamp(
            "2026-10-20T00:00:00Z",
            "2026-10-20T00:00:00Z",
            window
        ));
    }
    for window in [0, 301] {
        assert!(!fresh_timestamp(
            "2026-10-20T00:00:00Z",
            "2026-10-20T00:00:00Z",
            window
        ));
    }
    let config =
        crate::config::load_from_str("[bridge]\nowner_evidence_max_age_seconds=1").unwrap();
    assert_eq!(config.bridge.owner_evidence_max_age_seconds, 1);
    assert!(crate::config::load_from_str("[bridge]\nowner_evidence_max_age_seconds=0").is_err());
}
