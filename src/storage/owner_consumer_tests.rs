use super::*;

#[tokio::test]
async fn owner_consumer_rechecks_clock_identity_epoch_and_revision_chain() {
    let fixture = Fixture::new().await;
    fixture.start().await;
    fixture.stop().await;
    let observation = fixture.observation(ScanOutcome::Complete);
    let now = Utc::now().to_rfc3339();
    assert!(
        fixture
            .store
            .finalize_owner(&fixture.token, &now, &fixture.evidence, &observation)
            .await
            .unwrap()
    );
    for seconds in [-40, 40] {
        let observed = (Utc::now() + chrono::Duration::seconds(seconds)).to_rfc3339();
        assert!(
            !fixture
                .store
                .verify_final_owner(&fixture.token, &observed, &fixture.evidence, &observation)
                .await
                .unwrap()
        );
    }
    let mut changed = fixture.evidence.clone();
    changed.boot_id = "changed".into();
    assert!(
        !fixture
            .store
            .verify_final_owner(&fixture.token, &now, &changed, &observation)
            .await
            .unwrap()
    );
    let wrong_epoch = FinalObservation::modeled(ScanOutcome::Complete, 2, &fixture.token);
    assert!(
        !fixture
            .store
            .verify_final_owner(&fixture.token, &now, &fixture.evidence, &wrong_epoch)
            .await
            .unwrap()
    );
    for query in [
        "SELECT count(*) FROM historical_unknown_abandonments",
        "SELECT count(*) FROM transport_receipts",
        "SELECT count(*) FROM projection_outbox",
        "SELECT count(*) FROM task_events",
        "SELECT count(*) FROM agent_work_queue",
    ] {
        let count: i64 = sqlx::query_scalar(query)
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    sqlx::query("DELETE FROM runtime_owner_evidence WHERE evidence_revision=1")
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert!(
        !fixture
            .store
            .verify_final_owner(&fixture.token, &now, &fixture.evidence, &observation)
            .await
            .unwrap()
    );
    fixture.pool.close().await;
}

#[tokio::test]
async fn owner_duplicate_capture_and_changed_fingerprint_are_unknown() {
    for changed_fingerprint in [false, true] {
        let fixture = Fixture::new().await;
        fixture.start().await;
        let captured: String =
            sqlx::query_scalar("SELECT captured_at FROM runtime_owner_evidence WHERE is_current=1")
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        let now = if changed_fingerprint {
            Utc::now().to_rfc3339()
        } else {
            captured
        };
        let fingerprint = if changed_fingerprint {
            "changed"
        } else {
            "fingerprint"
        };
        assert!(
            !fixture
                .store
                .publish_owner_heartbeat(&fixture.token, fingerprint, &now, &fixture.evidence)
                .await
                .unwrap()
        );
        let state: String =
            sqlx::query_scalar("SELECT state FROM runtime_owner_evidence WHERE is_current=1")
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(state, "unknown");
        fixture.pool.close().await;
    }
}
