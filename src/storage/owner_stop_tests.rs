use super::*;

async fn assert_unqualified_stop(fixture: &Fixture) {
    let state: String =
        sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token=?")
            .bind(&fixture.token)
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(state, "stopped");
    let evidence: String = sqlx::query_scalar(
        "SELECT state FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1",
    )
    .bind(&fixture.token)
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert!(matches!(evidence.as_str(), "scan_pending" | "unknown"));
    assert_eq!(fixture.qualified_count().await, 0);
    let observation = fixture.observation(ScanOutcome::Complete);
    assert!(
        !fixture
            .store
            .verify_final_owner(
                &fixture.token,
                &Utc::now().to_rfc3339(),
                &fixture.evidence,
                &observation,
            )
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM historical_unknown_abandonments")
            .fetch_one(&fixture.pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn ordinary_stop_without_qualification_never_authorizes_disposition() {
    for case in 0..4 {
        let fixture = Fixture::new().await;
        fixture.start().await;
        fixture.stop().await;
        let observation = match case {
            0 => crate::storage::owner_observation::DomainBinding::for_generation(&fixture.token)
                .scan(1),
            1 => fixture.observation(ScanOutcome::Unavailable),
            _ => FinalObservation::modeled(ScanOutcome::Complete, 1, "wrong-generation"),
        };
        let final_owner = (case != 3).then_some((&fixture.evidence, &observation));
        assert!(
            fixture
                .store
                .finish_runtime_stop(&fixture.token, &Utc::now().to_rfc3339(), final_owner,)
                .await
                .unwrap()
        );
        assert_unqualified_stop(&fixture).await;
        fixture.pool.close().await;
    }
}

#[tokio::test]
async fn ordinary_stop_keeps_conditional_qualified_publication_and_consumer() {
    let fixture = Fixture::new().await;
    fixture.start().await;
    fixture.stop().await;
    let observation = fixture.observation(ScanOutcome::Complete);
    let now = Utc::now().to_rfc3339();
    assert!(
        fixture
            .store
            .finish_runtime_stop(
                &fixture.token,
                &now,
                Some((&fixture.evidence, &observation)),
            )
            .await
            .unwrap()
    );
    assert_eq!(fixture.qualified_count().await, 1);
    assert!(
        fixture
            .store
            .verify_final_owner(&fixture.token, &now, &fixture.evidence, &observation,)
            .await
            .unwrap()
    );
    fixture.pool.close().await;
}

#[tokio::test]
async fn ordinary_stop_does_not_fall_back_on_database_failure() {
    for qualified in [false, true] {
        let fixture = Fixture::new().await;
        fixture.start().await;
        fixture.stop().await;
        let trigger = if qualified {
            "CREATE TRIGGER fail_stop BEFORE INSERT ON runtime_owner_evidence WHEN NEW.state='reaped' BEGIN SELECT RAISE(ABORT,'injected publication failure'); END"
        } else {
            "CREATE TRIGGER fail_stop BEFORE UPDATE ON runtime_instances WHEN NEW.state='stopped' BEGIN SELECT RAISE(ABORT,'injected stop failure'); END"
        };
        sqlx::query(trigger).execute(&fixture.pool).await.unwrap();
        let observation = fixture.observation(if qualified {
            ScanOutcome::Complete
        } else {
            ScanOutcome::Unavailable
        });
        assert!(
            fixture
                .store
                .finish_runtime_stop(
                    &fixture.token,
                    &Utc::now().to_rfc3339(),
                    Some((&fixture.evidence, &observation)),
                )
                .await
                .is_err()
        );
        let state: String =
            sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token=?")
                .bind(&fixture.token)
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(state, "stopping");
        assert_eq!(fixture.qualified_count().await, 0);
        fixture.pool.close().await;
    }
}
