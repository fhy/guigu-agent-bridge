use super::*;
use crate::{
    matrix::rebuild_gap::Rebuild, native_succession::state::Batch,
    storage::recovery_eligibility::LiveFence,
};

async fn stopped_hold(predecessor: bool) -> tests::Fixture {
    let mut f = super::tests::fixture_with_options(false, false).await;
    let token = if predecessor {
        advance_idle(&mut f).await;
        "next"
    } else {
        "owner"
    };
    let scope = crate::storage::recovery_eligibility::RecoveryScope::new(
        crate::storage::recovery_eligibility::bootstrap_nonadmin(&f.pool, true)
            .await
            .unwrap(),
        crate::storage::recovery_eligibility::EffectDomain::Local,
        None,
    );
    scope
        .install_matrix_fence(LiveFence {
            component: "MatrixSync".into(),
            owner: token.into(),
            epoch: f.epoch,
        })
        .unwrap();
    let owner_scope = scope.clone().for_component(LiveFence {
        component: "MatrixSync".into(),
        owner: token.into(),
        epoch: f.epoch,
    });
    let owner = crate::storage::ReliabilityStore::with_recovery(f.pool.clone(), owner_scope);
    let now = chrono::Utc::now().to_rfc3339();
    f.store
        .refuse_response(&f.operation, token, f.epoch, "rebuild-history-gap", &now)
        .await
        .unwrap();
    for component in ["MatrixSync", "OutboxDrain"] {
        let epoch =
            sqlx::query_scalar("SELECT epoch FROM matrix_component_leases WHERE component=?")
                .bind(component)
                .fetch_one(&f.pool)
                .await
                .unwrap();
        let store = crate::storage::MatrixRecoveryStore::new_scoped(
            f.pool.clone(),
            Some(scope.for_component(LiveFence {
                component: component.into(),
                owner: token.into(),
                epoch,
            })),
        );
        store
            .release_component_lease(component, token, epoch, &chrono::Utc::now().to_rfc3339())
            .await
            .unwrap();
    }
    assert!(
        owner
            .stopping_runtime(token, &chrono::Utc::now().to_rfc3339())
            .await
            .unwrap()
    );
    let gate = crate::acp::AcpSpawnGate::for_generation(token);
    gate.close_and_join().await;
    gate.reap_transports().await;
    assert!(
        owner
            .finish_controlled_stop(
                token,
                &chrono::Utc::now().to_rfc3339(),
                &crate::storage::RuntimeOwnerEvidence::capture().unwrap(),
                &gate.joined_children().unwrap()
            )
            .await
            .unwrap()
    );
    f
}

fn coverage(state: &State, epoch: i64) -> Exclusion {
    let mut proof = Exclusion::modeled(state.owner.clone());
    proof.rebuild = Some(Rebuild {
        batch: Batch {
            id: "filled".into(),
            request: Some(state.watermark.clone()),
            previous: state.watermark.clone(),
            response: "C".into(),
            digest: crate::matrix::response_identity::digest(
                Some(&state.watermark),
                "C",
                Vec::new(),
            )
            .unwrap(),
            owner: state.owner.token.clone(),
            epoch,
            expected: Vec::new(),
            covered: Vec::new(),
        },
        anchors: Vec::new(),
        unrouted: Vec::new(),
    });
    proof
}

#[tokio::test]
async fn controlled_stop_owner_remains_candidate_for_exact_rebuild_hold() {
    let f = stopped_hold(true).await;
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    assert!(state.history_hold_owner);
    assert_eq!(state.owner.token, "next");
    let proof = coverage(&state, f.epoch);
    // All failures retain the exact owner, hold, leases, receipts and classifications.
    let before = snapshot(&f.pool).await;
    let mut missing = proof.clone();
    missing.rebuild = None;
    assert!(
        publish(&f.pool, &state, &missing, &[], || Ok(()))
            .await
            .is_err()
    );
    assert_eq!(snapshot(&f.pool).await, before);
    let mut stale = state.clone();
    stale.history_hold_owner = false;
    assert!(
        publish(&f.pool, &stale, &proof, &[], || Ok(()))
            .await
            .is_err()
    );
    assert_eq!(snapshot(&f.pool).await, before);
    let calls = std::cell::Cell::new(0);
    assert!(
        publish(&f.pool, &state, &proof, &[], || {
            calls.set(calls.get() + 1);
            if calls.get() == 2 {
                Err(state::refuse("injected-precommit"))
            } else {
                Ok(())
            }
        })
        .await
        .is_err()
    );
    assert_eq!(snapshot(&f.pool).await, before);
    publish(&f.pool, &state, &proof, &[], || Ok(()))
        .await
        .unwrap();
    assert!(
        confirmed(&f.root.path().join("state.db"), &state, &proof)
            .await
            .unwrap()
    );
    assert!(
        publish(&f.pool, &state, &proof, &[], || Ok(()))
            .await
            .is_err()
    );
    assert!(
        confirmed(&f.root.path().join("state.db"), &state, &proof)
            .await
            .unwrap()
    );
    let original: (String, String) = sqlx::query_as("SELECT barrier_state,child_snapshot FROM runtime_owner_evidence WHERE instance_token='next' AND evidence_revision=?")
        .bind(state.owner.revision).fetch_one(&f.pool).await.unwrap();
    assert_eq!(original.0, "closed_joined");
    assert!(original.1.contains("controlled-stop-v1"));
    let mut c = f.pool.acquire().await.unwrap();
    assert!(state::read(&mut c).await.unwrap().is_none());
}

async fn snapshot(pool: &SqlitePool) -> Vec<String> {
    let mut c = pool.acquire().await.unwrap();
    let mut result = Vec::new();
    for id in crate::storage::recovery_eligibility::inventory::identities(&mut c)
        .await
        .unwrap()
    {
        result.push(
            crate::storage::recovery_eligibility::inventory::snapshot(&mut c, &id)
                .await
                .unwrap()
                .digest,
        );
    }
    let classifications: Vec<String> = sqlx::query_scalar("SELECT json_array(table_name,canonical_pk,original_revision,original_digest,parent_relations,disposition,evidence_digest) FROM offline_recovery_rows ORDER BY table_name,canonical_pk")
        .fetch_all(&mut *c).await.unwrap();
    result.extend(classifications);
    result
}

#[tokio::test]
async fn controlled_hold_rejects_wrong_control_missing_owner_and_unresolved_batch() {
    let f = stopped_hold(false).await;
    let before = snapshot(&f.pool).await;
    for (query, predicate) in [
        (
            "UPDATE matrix_recovery_control SET restart_hold_reason='other'",
            "not-native",
        ),
        (
            "UPDATE matrix_recovery_control SET cutover_operation_id='foreign'",
            "not-native",
        ),
        (
            "UPDATE matrix_component_leases SET owner_token='missing' WHERE component='MatrixSync'",
            "rebuild-owner-absent",
        ),
        (
            "UPDATE matrix_recovery_batches SET state='unknown' WHERE batch_id='prior'",
            "history-unresolved-effect",
        ),
    ] {
        let mut tx = f.pool.begin().await.unwrap();
        sqlx::query(query).execute(&mut *tx).await.unwrap();
        let error = state::read(&mut tx).await.unwrap_err();
        assert!(error.to_string().contains(predicate), "{error}");
        tx.rollback().await.unwrap();
        assert_eq!(snapshot(&f.pool).await, before);
    }
}

async fn advance_idle(f: &mut tests::Fixture) {
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let proof = Exclusion::modeled(state.owner.clone());
    publish(&f.pool, &state, &proof, &[], || Ok(()))
        .await
        .unwrap();
    let scope = RecoveryScope::new(
        crate::storage::recovery_eligibility::bootstrap_nonadmin(&f.pool, true)
            .await
            .unwrap(),
        EffectDomain::Local,
        None,
    );
    let reliability =
        crate::storage::ReliabilityStore::with_recovery(f.pool.clone(), scope.clone());
    let at = chrono::Utc::now().to_rfc3339();
    assert!(
        reliability
            .begin_runtime_with_evidence(
                "next",
                &crate::storage::RuntimeOwnerEvidence::fingerprint().unwrap(),
                &at,
                &crate::storage::RuntimeOwnerEvidence::capture().unwrap()
            )
            .await
            .unwrap()
    );
    f.store = crate::storage::MatrixRecoveryStore::new_scoped(f.pool.clone(), Some(scope));
    let expires = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    f.epoch = f
        .store
        .acquire_component_lease("MatrixSync", "next", &at, &expires)
        .await
        .unwrap();
    f.store
        .acquire_component_lease("OutboxDrain", "next", &at, &expires)
        .await
        .unwrap();
}
