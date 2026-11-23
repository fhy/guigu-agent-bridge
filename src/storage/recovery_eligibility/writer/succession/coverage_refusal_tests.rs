//! Failed initial target, persisted refusal, and reopened real recovery entry.
use super::*;
use crate::matrix::coverage::{Clock, Coverage, Room, Target, persist_failure, recover};
use tracing::instrument::WithSubscriber;

async fn refused_target() -> (tests::Fixture, State, crate::config::Config, Coverage) {
    let (f, state, config, row) = coverage_tests::setup().await;
    coverage_tests::save(&f, &state, &config, None, &row).await;
    let mut charged = row.clone();
    charged.charge_sync(Clock::read().unwrap()).unwrap();
    charged.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &charged).await;
    let mut next = charged.clone();
    let value = serde_json::json!({"event_id":"$SECRET","sender":"@SECRET:example.test",
        "type":"m.room.message","content":{"msgtype":"m.text","body":"/status SECRET"}});
    next.freeze(Target {
        next: "C".into(),
        rooms: vec![Room {
            id: "!SECRET:example.test".into(),
            initial: vec![value],
            anchor: None,
            origin: None,
            cursor: None,
            complete: true,
        }],
    })
    .unwrap();
    next.revision += 1;
    let capture = crate::test_diagnostics::Capture::default();
    let error = coverage::save(&f.pool, &state, Some(&charged), &next, &config, || Ok(()))
        .with_subscriber(capture.subscriber())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("native-succession/admin-intent"));
    assert!(!capture.output().contains("SECRET"));
    assert!(capture.output().contains("target"));
    let mut c = f.pool.acquire().await.unwrap();
    assert_eq!(coverage::read(&mut c).await.unwrap(), Some(charged.clone()));
    assert_eq!(state::read(&mut c).await.unwrap(), Some(state.clone()));
    drop(c);
    let proof = Exclusion::modeled(state.owner.clone());
    persist_failure(&f.pool, &config, &state, &proof, &|| Ok(()), &error)
        .await
        .unwrap();
    let mut c = f.pool.acquire().await.unwrap();
    let refused = coverage::read(&mut c).await.unwrap().unwrap();
    assert_eq!(refused.status, "refused");
    assert_eq!(refused.refusal.as_deref(), Some("coverage-validation"));
    assert!(refused.target.is_none());
    assert_eq!(
        (
            refused.segments,
            refused.sync_attempts,
            refused.pages.len(),
            refused.publications
        ),
        (1, 1, 0, 0)
    );
    drop(c);
    (f, state, config, refused)
}

async fn reopen(f: &mut tests::Fixture) {
    f.pool.close().await;
    f.pool = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
}

#[tokio::test]
async fn refused_target_rollback_and_reopened_resume_never_refund_or_resync() {
    let (mut f, state, config, old) = refused_target().await;
    reopen(&mut f).await;
    let proof = Exclusion::modeled(state.owner.clone());
    for _ in 0..2 {
        let error = recover(&f.pool, &config, &state, &proof, &|| Ok(()))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("coverage-segment-budget"));
        let mut c = f.pool.acquire().await.unwrap();
        let mut row = coverage::read(&mut c).await.unwrap().unwrap();
        assert!(row.revision > old.revision);
        row.revision = old.revision;
        assert_eq!(row, old);
        assert_eq!(state::read(&mut c).await.unwrap().as_ref(), Some(&state));
        assert!(
            coverage::legacy_allowed(&mut c)
                .await
                .unwrap_err()
                .to_string()
                .contains("coverage-active-archive-conflict")
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM matrix_recovery_receipts")
                .fetch_one(&mut *c)
                .await
                .unwrap(),
            0
        );
        drop(c);
        reopen(&mut f).await;
    }
    let mut c = f.pool.acquire().await.unwrap();
    let actual = coverage::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let mut reset = actual.clone();
    reset.status = "initializing".into();
    reset.revision += 1;
    assert!(
        coverage::save(&f.pool, &state, Some(&actual), &reset, &config, || Ok(()))
            .await
            .unwrap_err()
            .to_string()
            .contains("coverage-immutable-drift")
    );
    f.pool.close().await;
}

#[tokio::test]
async fn expired_refused_fixture_stops_before_sdk_and_preserves_original_cause() {
    let (mut f, state, config, mut row) = refused_target().await;
    // Fixture-only passage of time: seed the same terminal row in the past.
    // Never used on an observed pair or as a proposed recovery operation.
    let age = 1_800_001;
    row.created_ms -= age;
    row.deadline_ms -= age;
    row.clock.utc_ms -= age;
    row.clock.boot_ms -= age;
    row.segment_start.utc_ms -= age;
    row.segment_start.boot_ms -= age;
    sqlx::query("UPDATE matrix_recovery_coverage SET document=? WHERE coverage_id=1")
        .bind(row.encoded().unwrap())
        .execute(&f.pool)
        .await
        .unwrap();
    reopen(&mut f).await;
    let proof = Exclusion::modeled(state.owner.clone());
    let error = recover(&f.pool, &config, &state, &proof, &|| Ok(()))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("coverage-lifetime-or-clock"));
    let mut c = f.pool.acquire().await.unwrap();
    // Synthetic aging deliberately invalidates the old classification: diagnostic
    // save also fails closed. Original refusal/counters/target are still untouched.
    assert_eq!(coverage::read(&mut c).await.unwrap().as_ref(), Some(&row));
    assert_eq!(state::read(&mut c).await.unwrap().as_ref(), Some(&state));
    drop(c);
    f.pool.close().await;
}

#[tokio::test]
#[ignore = "explicit privately saved NEW target; fresh fixture only, no network"]
async fn saved_new_target_scoped_freeze_rolls_back_and_preserves_refusal() {
    let root = std::path::PathBuf::from(
        std::env::var_os("R112_OBSERVATION_ROOT").expect("private target"),
    );
    let target: Target =
        serde_json::from_slice(&std::fs::read(root.join("NEW-target.json")).unwrap()).unwrap();
    let (f, state, _, _) = coverage_tests::setup().await;
    let mut config = crate::config::load(root.join("config.toml")).unwrap();
    config.bridge.database = f.root.path().join("state.db");
    let proof = Exclusion::modeled(state.owner.clone());
    let row = Coverage::new(
        &state,
        proof.pairing(&config).unwrap(),
        Clock::read().unwrap(),
    )
    .unwrap();
    coverage_tests::save(&f, &state, &config, None, &row).await;
    let mut charged = row.clone();
    charged.charge_sync(Clock::read().unwrap()).unwrap();
    charged.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &charged).await;
    let mut next = charged.clone();
    next.freeze(target).unwrap();
    next.revision += 1;
    let capture = crate::test_diagnostics::Capture::default();
    let error = coverage::save(&f.pool, &state, Some(&charged), &next, &config, || Ok(()))
        .with_subscriber(capture.subscriber())
        .await
        .unwrap_err();
    assert_eq!(
        crate::matrix::intent_diagnostic::predicate(&error),
        "route-admission-intent"
    );
    std::fs::write(root.join("new-target-freeze.log"), capture.output()).unwrap();
    let mut c = f.pool.acquire().await.unwrap();
    assert_eq!(coverage::read(&mut c).await.unwrap(), Some(charged));
    drop(c);
    persist_failure(&f.pool, &config, &state, &proof, &|| Ok(()), &error)
        .await
        .unwrap();
    let mut c = f.pool.acquire().await.unwrap();
    let row = coverage::read(&mut c).await.unwrap().unwrap();
    assert_eq!(row.status, "refused");
    assert!(row.target.is_none());
    assert_eq!(
        (
            row.segments,
            row.sync_attempts,
            row.pages.len(),
            row.publications
        ),
        (1, 1, 0, 0)
    );
    assert_eq!(state::read(&mut c).await.unwrap().as_ref(), Some(&state));
    println!(
        "NEW-target fixture: route-admission-intent rollback=true refusal-persisted=true target-absent=true segments=1 sync=1 sends=0"
    );
    drop(c);
    f.pool.close().await;
}
