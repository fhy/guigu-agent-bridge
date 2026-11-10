use super::*;
use crate::matrix::coverage::{Clock, Coverage, Room, Target};

#[tokio::test]
async fn coverage_final_failure_rolls_back_hold_receipts_and_completion_then_retries() {
    let (f, state, config, row) = coverage_tests::setup().await;
    let (row, payloads) = coverage_cost_tests::ready(&f, &state, &config, row, 2).await;
    let mut next = row.clone();
    next.charge_publish(Clock::read().unwrap()).unwrap();
    next.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &next).await;
    let proof = coverage_cost_tests::proof(&state, &config, next.clone(), &payloads);
    sqlx::query("CREATE TRIGGER fail_completion BEFORE UPDATE ON matrix_recovery_coverage WHEN json_extract(NEW.document,'$.status')='published' BEGIN SELECT RAISE(ABORT,'fixture'); END").execute(&f.pool).await.unwrap();
    assert!(
        publish(&f.pool, &state, &proof, &payloads, || Ok(()))
            .await
            .is_err()
    );
    let mut c = f.pool.acquire().await.unwrap();
    assert_eq!(state::read(&mut c).await.unwrap().as_ref(), Some(&state));
    assert_eq!(coverage::read(&mut c).await.unwrap().as_ref(), Some(&next));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM matrix_recovery_receipts")
            .fetch_one(&mut *c)
            .await
            .unwrap(),
        0
    );
    drop(c);
    sqlx::query("DROP TRIGGER fail_completion")
        .execute(&f.pool)
        .await
        .unwrap();
    let mut retry = next.clone();
    retry.charge_publish(Clock::read().unwrap()).unwrap();
    retry.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&next), &retry).await;
    let proof = coverage_cost_tests::proof(&state, &config, retry, &payloads);
    publish(&f.pool, &state, &proof, &payloads, || Ok(()))
        .await
        .unwrap();
    let before: String =
        sqlx::query_scalar("SELECT group_concat(document) FROM matrix_recovery_coverage")
            .fetch_one(&f.pool)
            .await
            .unwrap();
    for _ in 0..2 {
        assert!(
            confirmed(&config.bridge.database, &state, &proof)
                .await
                .unwrap()
        );
    }
    assert_eq!(
        before,
        sqlx::query_scalar::<_, String>(
            "SELECT group_concat(document) FROM matrix_recovery_coverage"
        )
        .fetch_one(&f.pool)
        .await
        .unwrap()
    );
    f.pool.close().await;
}
#[tokio::test]
async fn coverage_progress_rejects_refunds_target_drift_and_postwrite_exclusion_loss() {
    let (f, state, config, row) = coverage_tests::setup().await;
    coverage_tests::save(&f, &state, &config, None, &row).await;
    let mut charged = row.clone();
    charged.charge_sync(Clock::read().unwrap()).unwrap();
    charged.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &charged).await;
    let mut next = charged.clone();
    next.revision += 1;
    next.sync_attempts = 0;
    assert!(
        coverage::save(&f.pool, &state, Some(&charged), &next, &config, || Ok(()))
            .await
            .is_err()
    );
    next = charged.clone();
    next.revision += 1;
    next.charge_sync(Clock::read().unwrap()).unwrap();
    let calls = std::cell::Cell::new(0);
    assert!(
        coverage::save(&f.pool, &state, Some(&charged), &next, &config, || {
            calls.set(calls.get() + 1);
            if calls.get() == 2 {
                Err(refused())
            } else {
                Ok(())
            }
        })
        .await
        .is_err()
    );
    let mut c = f.pool.acquire().await.unwrap();
    assert_eq!(
        coverage::read(&mut c).await.unwrap().as_ref(),
        Some(&charged)
    );
    assert_eq!(state::read(&mut c).await.unwrap().as_ref(), Some(&state));
    drop(c);
    let mut ready = charged.clone();
    ready
        .freeze(Target {
            next: "C".into(),
            rooms: vec![],
        })
        .unwrap();
    ready.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&charged), &ready).await;
    let mut bad = ready.clone();
    bad.revision += 1;
    bad.target.as_mut().unwrap().next = "replacement".into();
    assert!(
        coverage::save(&f.pool, &state, Some(&ready), &bad, &config, || Ok(()))
            .await
            .is_err()
    );
    let mut charged = ready.clone();
    charged.charge_publish(Clock::read().unwrap()).unwrap();
    charged.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&ready), &charged).await;
    let mut proof = coverage_cost_tests::proof(&state, &config, charged, &[]);
    proof
        .decision_config
        .as_mut()
        .unwrap()
        .transports
        .matrix
        .monitor_room = "!drift:example.test".into();
    assert!(
        publish(&f.pool, &state, &proof, &[], || Ok(()))
            .await
            .is_err()
    );
    f.pool.close().await;
}

fn saved_inputs() -> (crate::config::Config, String, Vec<Value>) {
    let root = std::path::PathBuf::from(
        std::env::var_os("R112_COVERAGE_INPUTS").expect("saved evidence root"),
    );
    let config_path = std::env::var_os("R112_COVERAGE_CONFIG").expect("frozen offline config");
    let config =
        crate::config::load_from_str(&std::fs::read_to_string(config_path).unwrap()).unwrap();
    let sync: Value =
        serde_json::from_slice(&std::fs::read(root.join("traversal/room-1-sync.json")).unwrap())
            .unwrap();
    let room = sync["room"].as_str().unwrap().to_owned();
    let mut values = vec![];
    for (p, i) in [
        (6, 27),
        (10, 50),
        (13, 56),
        (20, 43),
        (23, 43),
        (24, 12),
        (25, 4),
        (28, 25),
        (29, 8),
        (29, 80),
        (30, 99),
    ] {
        let page: Value = serde_json::from_slice(
            &std::fs::read(root.join(format!("traversal/room-1-page-{p}.json"))).unwrap(),
        )
        .unwrap();
        values.push(page["events"][i].clone());
    }
    (config, room, values)
}

#[tokio::test]
#[ignore = "explicit saved eleven inputs; offline disposable production writer"]
async fn coverage_eleven_saved_inputs_native_writer_and_final_recheck() {
    let (mut config, room, values) = saved_inputs();
    let (f, state, _, _) = coverage_tests::setup().await;
    config.bridge.database = f.root.path().join("state.db");
    let mut row = Coverage::new(
        &state,
        Exclusion::modeled(state.owner.clone())
            .pairing(&config)
            .unwrap(),
        Clock::read().unwrap(),
    )
    .unwrap();
    coverage_tests::save(&f, &state, &config, None, &row).await;
    let mut next = row.clone();
    next.charge_sync(Clock::read().unwrap()).unwrap();
    next.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &next).await;
    row = next;
    let mut next = row.clone();
    next.freeze(Target {
        next: "C".into(),
        rooms: vec![Room {
            id: room.into(),
            initial: values,
            anchor: None,
            origin: None,
            cursor: None,
            complete: true,
        }],
    })
    .unwrap();
    next.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &next).await;
    row = next;
    let payloads = row.payloads().unwrap();
    let mut next = row.clone();
    next.charge_publish(Clock::read().unwrap()).unwrap();
    next.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &next).await;
    let proof = coverage_cost_tests::proof(&state, &config, next, &payloads);
    publish(&f.pool, &state, &proof, &payloads, || Ok(()))
        .await
        .unwrap();
    assert!(
        confirmed(&config.bridge.database, &state, &proof)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM matrix_recovery_receipts WHERE state='dispositioned'"
        )
        .fetch_one(&f.pool)
        .await
        .unwrap(),
        11
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM projection_outbox")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        0
    );
    f.pool.close().await;
    println!(
        "actual_inputs=11 production_scoped_writer=true final_decision_recheck=true sends=0 tasks=0 replies=0 modeled_process=true"
    );
}

#[tokio::test]
async fn coverage_completed_slot_rebinds_only_after_native_successor_and_terminal_checkpoint() {
    let (f, state, config, row) = coverage_tests::setup().await;
    let (row, payloads) = coverage_cost_tests::ready(&f, &state, &config, row, 1).await;
    let mut next = row.clone();
    next.charge_publish(Clock::read().unwrap()).unwrap();
    next.revision += 1;
    coverage_tests::save(&f, &state, &config, Some(&row), &next).await;
    let proof = coverage_cost_tests::proof(&state, &config, next, &payloads);
    publish(&f.pool, &state, &proof, &payloads, || Ok(()))
        .await
        .unwrap();
    next_owner_hold(&f).await;
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    let old = coverage::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let mut next = Coverage::new(
        &state,
        Exclusion::modeled(state.owner.clone())
            .pairing(&config)
            .unwrap(),
        Clock::read().unwrap(),
    )
    .unwrap();
    next.revision = old.revision + 1;
    coverage_tests::save(&f, &state, &config, Some(&old), &next).await;
    assert_ne!(next.batch_id, old.batch_id);
    assert_eq!(next.state.watermark, "D");
    assert_eq!(next.attempts, 0);
    f.pool.close().await;
}

async fn next_owner_hold(f: &tests::Fixture) {
    let context = crate::storage::recovery_eligibility::bootstrap_nonadmin(&f.pool, true)
        .await
        .unwrap();
    let scope = RecoveryScope::new(context, EffectDomain::Local, None);
    let reliability =
        crate::storage::ReliabilityStore::with_recovery(f.pool.clone(), scope.clone());
    let now = chrono::Utc::now();
    let at = now.to_rfc3339();
    let expires = (now + chrono::Duration::minutes(5)).to_rfc3339();
    assert!(
        reliability
            .begin_runtime_with_evidence(
                "successor",
                &crate::storage::RuntimeOwnerEvidence::fingerprint().unwrap(),
                &at,
                &crate::storage::RuntimeOwnerEvidence::capture().unwrap()
            )
            .await
            .unwrap()
    );
    let store = crate::storage::MatrixRecoveryStore::new_scoped(f.pool.clone(), Some(scope));
    let epoch = store
        .acquire_component_lease("MatrixSync", "successor", &at, &expires)
        .await
        .unwrap();
    store
        .acquire_component_lease("OutboxDrain", "successor", &at, &expires)
        .await
        .unwrap();
    // A later genuine terminal response may advance beyond the prior tombstone.
    store
        .begin_batch(
            "later",
            Some("C"),
            "D",
            "fixture-empty",
            "C",
            "successor",
            epoch,
            &[],
            &at,
        )
        .await
        .unwrap();
    store
        .complete_batch("later", "D", "successor", epoch, &at)
        .await
        .unwrap();
    assert!(
        reliability
            .stopping_runtime("successor", &at)
            .await
            .unwrap()
    );
    store
        .refuse_response(&f.operation, "successor", epoch, "rebuild-history-gap", &at)
        .await
        .unwrap();
}
