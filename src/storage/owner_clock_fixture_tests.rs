use super::*;

async fn set_capture(fixture: &Fixture, captured: &str) {
    sqlx::query(
        "UPDATE runtime_owner_evidence SET captured_at=? WHERE instance_token=? AND is_current=1",
    )
    .bind(captured)
    .bind(&fixture.token)
    .execute(&fixture.pool)
    .await
    .unwrap();
}

async fn assert_valid_clock(fixture: &Fixture, reference: &str) {
    let row =
        sqlx::query("SELECT * FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1")
            .bind(&fixture.token)
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert!(valid_owner_clock(&fixture.evidence, reference));
    assert!(
        fixture
            .store
            .matches_clock(&row, "fingerprint", reference, &fixture.evidence)
    );
}

async fn finalize(fixture: &Fixture, reference: &str) -> bool {
    fixture
        .store
        .finalize_owner(
            &fixture.token,
            reference,
            &fixture.evidence,
            &fixture.observation(ScanOutcome::Complete),
        )
        .await
        .unwrap()
}

pub(super) async fn controlled_time_and_revision_cases() {
    for age in [-40, -6, 40] {
        let fixture = Fixture::new().await;
        fixture.start().await;
        fixture.stop().await;
        let reference = Utc::now();
        let now = reference.to_rfc3339();
        set_capture(&fixture, &now).await;
        assert_valid_clock(&fixture, &now).await;
        let captured = (reference - chrono::Duration::seconds(age)).to_rfc3339();
        set_capture(&fixture, &captured).await;
        assert!(valid_owner_clock(&fixture.evidence, &now));
        assert!(!fresh_timestamp(
            &captured,
            &now,
            fixture.store.owner_evidence_max_age
        ));
        assert!(!finalize(&fixture, &now).await, "age={age}");
        assert_eq!(fixture.qualified_count().await, 0);
        set_capture(&fixture, &now).await;
        assert_valid_clock(&fixture, &now).await;
        assert!(finalize(&fixture, &now).await, "positive control age={age}");
        assert_eq!(fixture.qualified_count().await, 1);
        fixture.pool.close().await;
    }
    malformed_and_revision_gap().await;
}

async fn malformed_and_revision_gap() {
    let fixture = Fixture::new().await;
    fixture.start().await;
    fixture.stop().await;
    let now = Utc::now().to_rfc3339();
    set_capture(&fixture, &now).await;
    assert_valid_clock(&fixture, &now).await;
    assert!(!valid_owner_clock(&fixture.evidence, "malformed"));
    assert!(!finalize(&fixture, "malformed").await);
    assert_eq!(fixture.qualified_count().await, 0);
    sqlx::query("CREATE TABLE saved_history AS SELECT * FROM runtime_owner_evidence WHERE evidence_revision=1")
        .execute(&fixture.pool).await.unwrap();
    sqlx::query("DELETE FROM runtime_owner_evidence WHERE evidence_revision=1")
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert_valid_clock(&fixture, &now).await;
    assert!(!finalize(&fixture, &now).await);
    assert_eq!(fixture.qualified_count().await, 0);
    sqlx::query("INSERT INTO runtime_owner_evidence SELECT * FROM saved_history")
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert_valid_clock(&fixture, &now).await;
    assert!(finalize(&fixture, &now).await);
    assert_eq!(fixture.qualified_count().await, 1);
    fixture.pool.close().await;
}
