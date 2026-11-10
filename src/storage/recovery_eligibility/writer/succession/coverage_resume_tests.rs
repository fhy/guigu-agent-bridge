//! R112 F1: reopened production recovery, not a direct budget-helper retry.
use super::*;
use crate::{
    config::Config,
    matrix::coverage::{Clock, Coverage, Room, Target, recover},
};
use std::sync::atomic::{AtomicUsize, Ordering};

async fn seed(
    segments: usize,
    complete: bool,
    aged: bool,
) -> (tests::Fixture, State, Config, Coverage) {
    let (f, state, mut config, _) = coverage_tests::setup().await;
    // An unfinished row fails SDK identity parsing BEFORE any network or store access.
    config.transports.matrix.user_id = "invalid-fixture-user".into();
    let mut clock = Clock::read().unwrap();
    if aged {
        clock.utc_ms -= 80_000;
        clock.boot_ms -= 80_000;
    }
    let mut row = Coverage::new(
        &state,
        Exclusion::modeled(state.owner.clone())
            .pairing(&config)
            .unwrap(),
        clock.clone(),
    )
    .unwrap();
    coverage_tests::save(&f, &state, &config, None, &row).await;
    for _ in 1..segments {
        let mut next = row.clone();
        next.segment(clock.clone()).unwrap();
        next.revision += 1;
        coverage_tests::save(&f, &state, &config, Some(&row), &next).await;
        row = next;
    }
    let mut next = row.clone();
    next.charge_sync(clock).unwrap();
    next.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &next).await;
    row = next;
    let mut next = row.clone();
    next.freeze(Target {
        next: "C".into(),
        rooms: vec![Room {
            id: "!room:example.test".into(),
            initial: f.payloads.iter().map(|p| p.1.clone()).collect(),
            anchor: (!complete).then(|| crate::matrix::rebuild_gap::Anchor {
                room: "!room:example.test".into(),
                event: "$anchor".into(),
                digest: "fixture-anchor".into(),
                batch: "prior".into(),
            }),
            origin: (!complete).then(|| "cursor".into()),
            cursor: (!complete).then(|| "cursor".into()),
            complete,
        }],
    })
    .unwrap();
    next.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &next).await;
    (f, state, config, next)
}

async fn reopen(f: &mut tests::Fixture, expected: Option<&State>) -> Coverage {
    f.pool.close().await;
    f.pool = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    let mut c = f.pool.acquire().await.unwrap();
    if let Some(state) = expected {
        assert_eq!(state::read(&mut c).await.unwrap().as_ref(), Some(state));
        let receipts: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_receipts")
            .fetch_one(&mut *c)
            .await
            .unwrap();
        assert_eq!(receipts, 0);
    }
    coverage::read(&mut c).await.unwrap().unwrap()
}

async fn resume_ready(
    f: &tests::Fixture,
    state: &State,
    config: &Config,
    old: &Coverage,
) -> Exclusion {
    let proof = Exclusion::modeled(state.owner.clone());
    let (rebuild, payloads, row) = recover(&f.pool, config, state, &proof, &|| Ok(()))
        .await
        .unwrap();
    assert_eq!(payloads, old.payloads().unwrap());
    assert_eq!(row.publications, old.publications + 1);
    assert_eq!(row.revision, old.revision + 1);
    assert!(row.elapsed_ms >= old.elapsed_ms);
    let mut unchanged = row.clone();
    unchanged.publications = old.publications;
    unchanged.revision = old.revision;
    unchanged.elapsed_ms = old.elapsed_ms;
    unchanged.clock = old.clock.clone();
    assert_eq!(
        &unchanged, old,
        "collection and binding must remain byte-equivalent"
    );
    let mut proof = coverage_cost_tests::proof(state, config, row, &payloads);
    proof.rebuild = Some(rebuild);
    proof
}

async fn fail_publication(f: &tests::Fixture, state: &State, proof: &Exclusion) {
    sqlx::query("CREATE TRIGGER fail_completion BEFORE UPDATE ON matrix_recovery_coverage WHEN json_extract(NEW.document,'$.status')='published' BEGIN SELECT RAISE(ABORT,'fixture-publication-rollback'); END")
        .execute(&f.pool).await.unwrap();
    let payloads = proof.coverage.as_ref().unwrap().payloads().unwrap();
    let error = publish(&f.pool, state, proof, &payloads, || Ok(()))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("fixture-publication-rollback"),
        "{error}"
    );
    sqlx::query("DROP TRIGGER fail_completion")
        .execute(&f.pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn ready_segment16_reopens_after_rollback_without_spending_collection() {
    let (mut f, state, config, row) = seed(16, true, true).await;
    // Seed the already-spent first attempt, as in the reported failing restart.
    let mut first = row.clone();
    first.charge_publish(Clock::read().unwrap()).unwrap();
    first.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &first).await;
    let proof =
        coverage_cost_tests::proof(&state, &config, first.clone(), &first.payloads().unwrap());
    fail_publication(&f, &state, &proof).await;
    let old = reopen(&mut f, Some(&state)).await;
    assert_eq!(old, first);
    let now = Clock::read().unwrap();
    let remaining = (old.segment_start.boot_ms + 90_010 - now.boot_ms).max(0);
    tokio::time::sleep(std::time::Duration::from_millis(remaining as u64)).await;
    assert!(old.segment_remaining(&Clock::read().unwrap()).is_err());
    let proof = resume_ready(&f, &state, &config, &old).await;
    assert_eq!(proof.coverage.as_ref().unwrap().publications, 2);
    let next = reopen(&mut f, Some(&state)).await;
    assert_eq!(Some(&next), proof.coverage.as_ref());
    publish(
        &f.pool,
        &state,
        &proof,
        &next.payloads().unwrap(),
        || Ok(()),
    )
    .await
    .unwrap();
    assert!(
        confirmed(&config.bridge.database, &state, &proof)
            .await
            .unwrap()
    );
    f.pool.close().await;
    println!(
        "F1 case1: segment16 unchanged; expired collection; publication2 committed; reopened rollback exact"
    );
}

#[tokio::test]
async fn ready_segment15_allows_three_publications_then_refuses_fourth() {
    let (mut f, state, config, mut row) = seed(15, true, false).await;
    for attempt in 1..=3 {
        let proof = resume_ready(&f, &state, &config, &row).await;
        assert_eq!(proof.coverage.as_ref().unwrap().publications, attempt);
        fail_publication(&f, &state, &proof).await;
        row = reopen(&mut f, Some(&state)).await;
        assert_eq!(Some(&row), proof.coverage.as_ref());
        assert_eq!(row.segments, 15);
    }
    let error = recover(
        &f.pool,
        &config,
        &state,
        &Exclusion::modeled(state.owner.clone()),
        &|| Ok(()),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("coverage-publication-budget"),
        "{error}"
    );
    let mut after = reopen(&mut f, Some(&state)).await;
    assert_eq!(after.status, "refused");
    assert_eq!(after.refusal.as_deref(), Some("finite-budget"));
    assert_eq!(after.revision, row.revision + 1);
    after.status = row.status.clone();
    after.refusal = row.refusal.clone();
    after.revision = row.revision;
    assert_eq!(after, row);
    f.pool.close().await;
    println!("F1 case2: segments15; three spent publications; fourth refuses; no refunds");
}

#[tokio::test]
async fn collecting_segment15_reserves16_and_next_resume_refuses() {
    let (mut f, state, config, row) = seed(15, false, false).await;
    let calls = AtomicUsize::new(0);
    let error = recover(
        &f.pool,
        &config,
        &state,
        &Exclusion::modeled(state.owner.clone()),
        &|| {
            if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                Ok(())
            } else {
                Err(refused())
            }
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("coverage-restore"), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let spent = reopen(&mut f, Some(&state)).await;
    assert_eq!(spent.segments, 16);
    assert_eq!(spent.status, "collecting");
    assert_eq!(spent.target, row.target);
    assert_eq!(spent.pages, row.pages);
    assert_eq!(spent.attempts, row.attempts);
    assert_eq!(spent.sync_attempts, row.sync_attempts);
    assert_eq!(spent.publications, 0);
    let error = recover(
        &f.pool,
        &config,
        &state,
        &Exclusion::modeled(state.owner.clone()),
        &|| Ok(()),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("coverage-segment-budget"),
        "{error}"
    );
    let mut after = reopen(&mut f, Some(&state)).await;
    assert_eq!(after.status, "refused");
    assert_eq!(after.refusal.as_deref(), Some("finite-budget"));
    assert_eq!(after.revision, spent.revision + 1);
    after.status = spent.status.clone();
    after.refusal = spent.refusal.clone();
    after.revision = spent.revision;
    assert_eq!(after, spent);
    f.pool.close().await;
    println!(
        "F1 case3: collecting15 reserves16 durably; next refuses; no network; exclusion loss preserves progress"
    );
}

#[tokio::test]
async fn confirmed_publication_retry_is_read_only_after_reopen() {
    let (mut f, state, config, row) = seed(1, true, false).await;
    let proof = resume_ready(&f, &state, &config, &row).await;
    publish(&f.pool, &state, &proof, &row.payloads().unwrap(), || Ok(()))
        .await
        .unwrap();
    let published = reopen(&mut f, None).await;
    assert_eq!(published.status, "published");
    let before: (i64, String, String) = sqlx::query_as(
        "SELECT revision,owner_token,document FROM matrix_recovery_coverage WHERE coverage_id=1",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    for _ in 0..2 {
        assert!(
            confirmed(&config.bridge.database, &state, &proof)
                .await
                .unwrap()
        );
        assert_eq!(reopen(&mut f, None).await, published);
    }
    let after: (i64, String, String) = sqlx::query_as(
        "SELECT revision,owner_token,document FROM matrix_recovery_coverage WHERE coverage_id=1",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(after, before);
    assert_eq!((published.segments, published.publications), (1, 1));
    f.pool.close().await;
    println!("F1 case4: native confirmed retries read-only; exact row bytes; no budget spent");
}

#[tokio::test]
async fn ready_without_publication_resumes_after_collection_deadline() {
    let (mut f, state, config, row) = seed(16, true, true).await;
    let now = Clock::read().unwrap();
    let remaining = (row.segment_start.boot_ms + 90_010 - now.boot_ms).max(0);
    tokio::time::sleep(std::time::Duration::from_millis(remaining as u64)).await;
    assert!(row.segment_remaining(&Clock::read().unwrap()).is_err());
    assert_eq!(reopen(&mut f, Some(&state)).await, row);
    let proof = resume_ready(&f, &state, &config, &row).await;
    assert_eq!(proof.coverage.as_ref().unwrap().publications, 1);
    assert_eq!(reopen(&mut f, Some(&state)).await, proof.coverage.unwrap());
    f.pool.close().await;
    println!("F1 stale collection: publication0->1 persisted without uncharged ready save");
}

#[tokio::test]
async fn ready_reservation_exclusion_loss_rolls_back_and_binding_drift_refuses() {
    let (mut f, state, config, row) = seed(16, true, false).await;
    let calls = AtomicUsize::new(0);
    let error = recover(
        &f.pool,
        &config,
        &state,
        &Exclusion::modeled(state.owner.clone()),
        &|| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(refused())
            }
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), refused().to_string());
    assert!(calls.load(Ordering::SeqCst) >= 3);
    assert_eq!(reopen(&mut f, Some(&state)).await, row);
    let mut drift = config.clone();
    drift.transports.matrix.device_id.push_str("-drift");
    let error = recover(
        &f.pool,
        &drift,
        &state,
        &Exclusion::modeled(state.owner.clone()),
        &|| Ok(()),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("coverage-binding-drift"),
        "{error}"
    );
    assert_eq!(reopen(&mut f, Some(&state)).await, row);
    let proof = resume_ready(&f, &state, &config, &row).await;
    assert_eq!(reopen(&mut f, Some(&state)).await, proof.coverage.unwrap());
    f.pool.close().await;
    println!(
        "F1 fresh exclusion postwrite rollback + binding refusal exact; valid retry reserves once"
    );
}
