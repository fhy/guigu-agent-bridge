use super::tests::fixture;
use super::*;
use crate::{matrix::rebuild_gap::Rebuild, native_succession::state::Batch};

async fn input(f: &tests::Fixture) -> (State, Exclusion) {
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
    let mut proof = Exclusion::modeled(state.owner.clone());
    proof.rebuild = Some(Rebuild {
        batch: Batch {
            id: "filled".into(),
            request: Some("B".into()),
            previous: "B".into(),
            response: "C".into(),
            digest: crate::matrix::response_identity::digest(Some("B"), "C", f.payloads.clone())
                .unwrap(),
            owner: "owner".into(),
            epoch: f.epoch,
            expected: vec!["$notice:example.test".into()],
            covered: vec![],
        },
        anchors: vec![],
        unrouted: vec![],
    });
    (state, proof)
}

#[tokio::test]
async fn succession_rebuild_hold_requires_complete_atomic_disposition_and_retains_first_hold_on_error()
 {
    for mode in 0..4 {
        let f = fixture(false).await;
        let (state, mut proof) = input(&f).await;
        match mode {
            1 => proof.rebuild = None,
            2 => proof.rebuild.as_mut().unwrap().batch.digest = "changed".into(),
            3 => {
                sqlx::query("CREATE TRIGGER interrupted BEFORE UPDATE ON matrix_recovery_receipts WHEN NEW.state='dispositioned' BEGIN SELECT RAISE(ABORT,'injected'); END").execute(&f.pool).await.unwrap();
            }
            _ => (),
        }
        let result = publish(&f.pool, &state, &proof, &f.payloads, || Ok(())).await;
        assert_eq!(result.is_ok(), mode == 0, "mode {mode}: {result:?}");
        if mode == 0 {
            assert!(
                confirmed(&f.root.path().join("state.db"), &state, &proof)
                    .await
                    .unwrap()
            );
            let terminal:i64=sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_batches WHERE batch_id='filled' AND state='terminal'").fetch_one(&f.pool).await.unwrap();
            assert_eq!(terminal, 1);
        } else {
            let mut c = f.pool.acquire().await.unwrap();
            assert_eq!(state::read(&mut c).await.unwrap(), Some(state));
            let absent: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM matrix_recovery_batches WHERE batch_id='filled'",
            )
            .fetch_one(&mut *c)
            .await
            .unwrap();
            assert_eq!(absent, 0);
        }
        f.pool.close().await;
    }
}

#[tokio::test]
async fn succession_mixed_epoch_restart_keeps_retired_owner() {
    let f = fixture(false).await;
    let mut c = f.pool.acquire().await.unwrap();
    let first = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    publish(
        &f.pool,
        &first,
        &Exclusion::modeled(first.owner.clone()),
        &[],
        || Ok(()),
    )
    .await
    .unwrap();
    let context = crate::storage::recovery_eligibility::bootstrap_nonadmin(&f.pool, true)
        .await
        .unwrap();
    let scope = RecoveryScope::new(context, EffectDomain::Local, None);
    let reliability =
        crate::storage::ReliabilityStore::with_recovery(f.pool.clone(), scope.clone());
    let now = chrono::Utc::now();
    let at = now.to_rfc3339();
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
    let store = crate::storage::MatrixRecoveryStore::new_scoped(f.pool.clone(), Some(scope));
    store
        .acquire_component_lease(
            "MatrixSync",
            "next",
            &at,
            &(now + chrono::Duration::minutes(1)).to_rfc3339(),
        )
        .await
        .unwrap();
    let mut c = f.pool.acquire().await.unwrap();
    let next = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    assert!(
        next.leases
            .iter()
            .any(|(c, o, _, s, _)| c == "OutboxDrain" && o == "owner" && s == "expired")
    );
    publish(
        &f.pool,
        &next,
        &Exclusion::modeled(next.owner.clone()),
        &[],
        || Ok(()),
    )
    .await
    .unwrap();
    f.pool.close().await;
}
