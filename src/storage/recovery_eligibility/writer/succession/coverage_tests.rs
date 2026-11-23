use super::*;
use crate::{
    config::Config,
    matrix::coverage::{Clock, Coverage, Room, Target},
};
pub(super) async fn setup() -> (tests::Fixture, State, Config, Coverage) {
    let f = tests::fixture(false).await;
    f.store
        .refuse_response(
            &f.operation,
            "owner",
            f.epoch,
            "rebuild-history-gap",
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let mut config = crate::config::load_from_str("").unwrap();
    config.bridge.database = f.root.path().join("state.db");
    config.transports.matrix.user_id = "@bridge:example.test".into();
    let row = Coverage::new(
        &state,
        Exclusion::modeled(state.owner.clone())
            .pairing(&config)
            .unwrap(),
        Clock::read().unwrap(),
    )
    .unwrap();
    (f, state, config, row)
}
pub(super) async fn save(
    f: &tests::Fixture,
    state: &State,
    config: &Config,
    old: Option<&Coverage>,
    next: &Coverage,
) {
    coverage::save(&f.pool, state, old, next, config, || Ok(()))
        .await
        .unwrap();
    let mut c = f.pool.acquire().await.unwrap();
    assert_eq!(state::read(&mut c).await.unwrap().as_ref(), Some(state));
    assert_eq!(coverage::read(&mut c).await.unwrap().as_ref(), Some(next));
}
#[tokio::test]
async fn coverage_scoped_progress_preserves_hold_and_publishes_atomically() {
    let (f, state, config, mut row) = setup().await;
    save(&f, &state, &config, None, &row).await;
    let mut next = row.clone();
    next.charge_sync(Clock::read().unwrap()).unwrap();
    next.revision += 1;
    save(&f, &state, &config, Some(&row), &next).await;
    row = next;
    let mut next = row.clone();
    next.freeze(Target {
        next: "C".into(),
        rooms: vec![Room {
            id: "!room:example.test".into(),
            initial: vec![f.payloads[0].1.clone()],
            anchor: None,
            origin: None,
            cursor: None,
            complete: true,
        }],
    })
    .unwrap();
    next.revision += 1;
    save(&f, &state, &config, Some(&row), &next).await;
    row = next;
    let mut next = row.clone();
    next.charge_publish(Clock::read().unwrap()).unwrap();
    next.revision += 1;
    save(&f, &state, &config, Some(&row), &next).await;
    row = next;
    let mut proof = Exclusion::modeled(state.owner.clone());
    proof.decision_config = Some(config.clone());
    proof.coverage = Some(row.clone());
    proof.rebuild = Some(crate::matrix::rebuild_gap::Rebuild {
        batch: crate::native_succession::state::Batch {
            id: row.batch_id.clone(),
            request: Some("B".into()),
            previous: "B".into(),
            response: "C".into(),
            digest: crate::matrix::response_identity::digest(Some("B"), "C", f.payloads.clone())
                .unwrap(),
            owner: "owner".into(),
            epoch: f.epoch,
            expected: vec![f.payloads[0].1["event_id"].as_str().unwrap().into()],
            covered: vec![],
        },
        anchors: vec![],
        unrouted: vec![],
    });
    publish(&f.pool, &state, &proof, &f.payloads, || Ok(()))
        .await
        .unwrap();
    assert!(
        confirmed(&config.bridge.database, &state, &proof)
            .await
            .unwrap()
    );
    let mut c = f.pool.acquire().await.unwrap();
    let result = coverage::read(&mut c).await.unwrap().unwrap();
    assert_eq!(result.status, "published");
    assert!(result.pages.is_empty());
    assert!(result.target.is_none());
    drop(c);
    f.pool.close().await;
}
#[tokio::test]
async fn coverage_progress_cas_and_exclusion_refusal_roll_back() {
    let (f, state, config, row) = setup().await;
    let mut forged = row.clone();
    forged.charge_sync(Clock::read().unwrap()).unwrap();
    assert!(
        coverage::save(&f.pool, &state, None, &forged, &config, || Ok(()))
            .await
            .is_err()
    );
    save(&f, &state, &config, None, &row).await;
    let mut next = row.clone();
    next.revision += 1;
    next.charge_sync(Clock::read().unwrap()).unwrap();
    assert!(
        coverage::save(&f.pool, &state, Some(&row), &next, &config, || Err(
            refused()
        ))
        .await
        .is_err()
    );
    save(&f, &state, &config, Some(&row), &next).await;
    assert!(
        coverage::save(&f.pool, &state, Some(&row), &next, &config, || Ok(()))
            .await
            .is_err()
    );
    f.pool.close().await;
}

#[tokio::test]
async fn coverage_archive_route_cannot_bypass_an_active_budget() {
    let (f, state, config, row) = setup().await;
    let mut c = f.pool.acquire().await.unwrap();
    coverage::legacy_allowed(&mut c).await.unwrap();
    drop(c);
    save(&f, &state, &config, None, &row).await;
    let mut c = f.pool.acquire().await.unwrap();
    assert!(coverage::legacy_allowed(&mut c).await.is_err());
    drop(c);
    f.pool.close().await;
}

#[tokio::test]
async fn coverage_progress_deadline_after_write_rolls_back_exactly() {
    let (f, state, config, mut row) = setup().await;
    // Enter with a valid segment, then cross its deadline at the post-write
    // exclusion recheck. No DB clock mutation or cancellable-SQL assumption.
    row.clock.utc_ms -= 85_000;
    row.clock.boot_ms -= 85_000;
    row.created_ms = row.clock.utc_ms;
    row.deadline_ms = row.created_ms + 1_800_000;
    row.segment_start = row.clock.clone();
    let calls = std::cell::Cell::new(0);
    let error = coverage::save(&f.pool, &state, None, &row, &config, || {
        calls.set(calls.get() + 1);
        if calls.get() == 2 {
            let now = Clock::read()?;
            let remaining = (row.segment_start.boot_ms + 90_100 - now.boot_ms).max(0);
            std::thread::sleep(std::time::Duration::from_millis(remaining as u64));
        }
        Ok(())
    })
    .await
    .unwrap_err();
    assert_eq!(calls.get(), 2);
    assert!(
        error.to_string().contains("coverage-segment-time"),
        "{error}"
    );
    let mut c = f.pool.acquire().await.unwrap();
    assert!(coverage::read(&mut c).await.unwrap().is_none());
    assert_eq!(state::read(&mut c).await.unwrap().as_ref(), Some(&state));
    drop(c);
    // A new initial row is legal because the rejected transaction committed none.
    let fresh = Coverage::new(&state, row.pairing, Clock::read().unwrap()).unwrap();
    save(&f, &state, &config, None, &fresh).await;
    f.pool.close().await;
}
