use super::*;
use crate::storage::{
    CompatibilityGate, MatrixCompatibilityStore, MatrixRecoveryStore, ReliabilityStore,
    RuntimeOwnerEvidence, recovery_eligibility::bootstrap_nonadmin,
};
use sqlx::Row;

pub(crate) struct Fixture {
    pub root: tempfile::TempDir,
    pub pool: SqlitePool,
    pub store: MatrixRecoveryStore,
    pub operation: String,
    pub epoch: i64,
    pub payloads: Vec<(String, Value)>,
}
pub(crate) async fn fixture(batch: bool) -> Fixture {
    fixture_with_options(batch, true).await
}

pub(super) async fn fixture_with_options(batch: bool, stop: bool) -> Fixture {
    let root = tempfile::Builder::new()
        .prefix("r105-succession-")
        .tempdir_in("/tmp")
        .unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap()
    else {
        panic!("native")
    };
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(&pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let store = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
    let now = chrono::Utc::now();
    let at = now.to_rfc3339();
    let expires = (now + chrono::Duration::minutes(5)).to_rfc3339();
    store.activate_cutover(&operation, "A", &at).await.unwrap();
    let reliability = ReliabilityStore::with_recovery(pool.clone(), scope);
    assert!(
        reliability
            .begin_runtime_with_evidence(
                "owner",
                &RuntimeOwnerEvidence::fingerprint().unwrap(),
                &at,
                &RuntimeOwnerEvidence::capture().unwrap()
            )
            .await
            .unwrap()
    );
    let epoch = store
        .acquire_component_lease("MatrixSync", "owner", &at, &expires)
        .await
        .unwrap();
    store
        .acquire_component_lease("OutboxDrain", "owner", &at, &expires)
        .await
        .unwrap();
    prior_batch(&store, epoch, &at).await;
    if stop {
        assert!(reliability.stopping_runtime("owner", &at).await.unwrap());
    }
    let payloads = vec![(
        "!room:example.test".into(),
        json!({"type":"m.room.message","event_id":"$notice:example.test","sender":"@alice:example.test","origin_server_ts":1,"content":{"msgtype":"m.notice","body":"fixture notice"}}),
    )];
    if batch {
        interrupted(&store, &payloads, epoch, &at).await;
    }
    Fixture {
        root,
        pool,
        store,
        operation,
        epoch,
        payloads,
    }
}
async fn prior_batch(store: &MatrixRecoveryStore, epoch: i64, at: &str) {
    store
        .begin_batch(
            "prior",
            Some("A"),
            "B",
            "prior-digest",
            "A",
            "owner",
            epoch,
            &[],
            at,
        )
        .await
        .unwrap();
    store
        .complete_batch("prior", "B", "owner", epoch, at)
        .await
        .unwrap();
}
async fn interrupted(
    store: &MatrixRecoveryStore,
    payloads: &[(String, Value)],
    epoch: i64,
    at: &str,
) {
    let digest =
        crate::matrix::response_identity::digest(Some("B"), "C", payloads.to_vec()).unwrap();
    store
        .begin_batch(
            "interrupted",
            Some("B"),
            "C",
            &digest,
            "B",
            "owner",
            epoch,
            &["$notice:example.test".into()],
            &at,
        )
        .await
        .unwrap();
    assert!(store.reconcile_restart().await.is_err());
}
async fn input(f: &Fixture) -> (State, Exclusion) {
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    let proof = Exclusion::modeled(state.owner.clone());
    (state, proof)
}
async fn unchanged(f: &Fixture, state: &State) {
    let mut c = f.pool.acquire().await.unwrap();
    assert_eq!(state::read(&mut c).await.unwrap().as_ref(), Some(state));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_receipts")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

async fn unknown_owner_fixture(stopped: bool) -> Fixture {
    let f = fixture_with_options(false, false).await;
    let context = bootstrap_nonadmin(&f.pool, true).await.unwrap();
    let scope = RecoveryScope::new(context, EffectDomain::Local, None);
    let reliability = ReliabilityStore::with_recovery(f.pool.clone(), scope);
    let mut invalid = RuntimeOwnerEvidence::capture().unwrap();
    invalid.boot_id.push_str("-unknown-fixture");
    assert!(
        !reliability
            .publish_owner_heartbeat(
                "owner",
                &RuntimeOwnerEvidence::fingerprint().unwrap(),
                &chrono::Utc::now().to_rfc3339(),
                &invalid,
            )
            .await
            .unwrap()
    );
    if stopped {
        assert!(
            reliability
                .stop_runtime("owner", &chrono::Utc::now().to_rfc3339())
                .await
                .unwrap()
        );
    }
    f
}

#[tokio::test]
async fn safe_succession_keeps_historical_joins_and_fences_old_writers() {
    let f = fixture(true).await;
    let (state, proof) = input(&f).await;
    publish(&f.pool, &state, &proof, &f.payloads, || Ok(()))
        .await
        .unwrap();
    assert!(
        confirmed(&f.root.path().join("state.db"), &state, &proof)
            .await
            .unwrap()
    );
    let rows:Vec<(String,String,String,i64)>=sqlx::query_as("SELECT state,barrier_state,child_scan_state,is_current FROM runtime_owner_evidence ORDER BY evidence_revision").fetch_all(&f.pool).await.unwrap();
    assert_eq!(
        rows.last().unwrap(),
        &("observing".into(), "open".into(), "inaccessible".into(), 1)
    );
    assert!(
        rows.iter()
            .all(|(state, barrier, _, _)| state != "reaped" && barrier != "closed_joined")
    );
    let mut c = f.pool.acquire().await.unwrap();
    assert!(
        !crate::storage::owner_restart::restart_eligible(&mut c, "owner")
            .await
            .unwrap()
    );
    drop(c);
    assert!(
        f.store
            .renew_component_lease(
                "MatrixSync",
                "owner",
                f.epoch,
                &chrono::Utc::now().to_rfc3339()
            )
            .await
            .is_err()
    );
    let context = bootstrap_nonadmin(&f.pool, true).await.unwrap();
    let scope = RecoveryScope::new(context, EffectDomain::Local, None);
    let reliability = ReliabilityStore::with_recovery(f.pool.clone(), scope);
    assert!(
        reliability
            .begin_runtime_with_evidence(
                "successor",
                &RuntimeOwnerEvidence::fingerprint().unwrap(),
                &chrono::Utc::now().to_rfc3339(),
                &RuntimeOwnerEvidence::capture().unwrap()
            )
            .await
            .unwrap()
    );
    f.pool.close().await;
}

#[tokio::test]
async fn refusal_and_mid_transaction_errors_leave_exact_hold_and_leases() {
    for query in [
        "CREATE TRIGGER interrupted BEFORE INSERT ON matrix_recovery_receipts BEGIN SELECT RAISE(ABORT,'reserve'); END",
        "CREATE TRIGGER interrupted BEFORE UPDATE ON matrix_recovery_receipts WHEN NEW.state='processing' BEGIN SELECT RAISE(ABORT,'start'); END",
        "CREATE TRIGGER interrupted BEFORE UPDATE ON matrix_recovery_receipts WHEN NEW.state='dispositioned' BEGIN SELECT RAISE(ABORT,'disposition'); END",
        "CREATE TRIGGER interrupted BEFORE INSERT ON runtime_owner_evidence WHEN NEW.state='observing' BEGIN SELECT RAISE(ABORT,'publish'); END",
        "CREATE TRIGGER interrupted BEFORE UPDATE ON matrix_recovery_control WHEN NEW.watermark='C' BEGIN SELECT RAISE(ABORT,'checkpoint'); END",
    ] {
        let f = fixture(true).await;
        let (state, proof) = input(&f).await;
        sqlx::query(query).execute(&f.pool).await.unwrap();
        assert!(
            publish(&f.pool, &state, &proof, &f.payloads, || Ok(()))
                .await
                .is_err(),
            "{query}"
        );
        unchanged(&f, &state).await;
        assert!(
            !confirmed(&f.root.path().join("state.db"), &state, &proof)
                .await
                .unwrap()
        );
        f.pool.close().await;
    }
}

#[tokio::test]
async fn payload_coverage_receipt_and_control_drift_refuse() {
    for kind in 0..7 {
        let f = fixture(true).await;
        let (state, proof) = input(&f).await;
        let mut payloads = f.payloads.clone();
        match kind {
            0 => payloads.clear(),
            1 => payloads[0].1["content"]["body"] = "changed".into(),
            2 => payloads[0].1["type"] = "m.room.encrypted".into(),
            3 => payloads[0].0 = "!other:example.test".into(),
            4 => payloads[0].1["content"]["msgtype"] = "m.text".into(),
            5 => {
                sqlx::query("UPDATE offline_recovery_rows SET original_digest='changed' WHERE table_name='matrix_recovery_control'").execute(&f.pool).await.unwrap();
            }
            _ => {
                sqlx::query("INSERT INTO matrix_recovery_receipts(account_id,room_id,event_id,cutover_operation_id,payload_digest,state,batch_id,created_at,updated_at) VALUES('a','r','e','o','d','processing','interrupted','t','t')").execute(&f.pool).await.unwrap();
            }
        }
        assert!(
            publish(&f.pool, &state, &proof, &payloads, || Ok(()))
                .await
                .is_err(),
            "{kind}"
        );
        if kind != 6 {
            unchanged(&f, &state).await;
        }
        f.pool.close().await;
    }
}

#[tokio::test]
async fn publication_reread_and_retry_are_idempotent() {
    let f = fixture(true).await;
    let (state, proof) = input(&f).await;
    let calls = std::cell::Cell::new(0);
    assert!(
        publish(&f.pool, &state, &proof, &f.payloads, || {
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
    unchanged(&f, &state).await;
    publish(&f.pool, &state, &proof, &f.payloads, || Ok(()))
        .await
        .unwrap();
    assert!(
        confirmed(&f.root.path().join("state.db"), &state, &proof)
            .await
            .unwrap()
    );
    assert!(
        publish(&f.pool, &state, &proof, &f.payloads, || Ok(()))
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM matrix_recovery_receipts WHERE state='dispositioned'",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
    f.pool.close().await;
}

#[tokio::test]
async fn source_identity_revision_and_epoch_drift_are_not_rebased() {
    for query in [
        "UPDATE runtime_owner_evidence SET proc_starttime=proc_starttime+1 WHERE is_current=1",
        "UPDATE runtime_owner_evidence SET evidence_revision=evidence_revision+2 WHERE is_current=1",
        "UPDATE matrix_component_leases SET epoch=epoch+1 WHERE component='MatrixSync'",
        "UPDATE runtime_eligibility_context SET revision=revision+1",
        "INSERT INTO runtime_instances VALUES('competitor','t','t','active','other')",
    ] {
        let f = fixture(true).await;
        let (state, proof) = input(&f).await;
        sqlx::query(query).execute(&f.pool).await.unwrap();
        assert!(
            publish(&f.pool, &state, &proof, &f.payloads, || Ok(()))
                .await
                .is_err(),
            "{query}"
        );
        assert_eq!(
            sqlx::query("SELECT state FROM runtime_instances WHERE instance_token='owner'")
                .fetch_one(&f.pool)
                .await
                .unwrap()
                .get::<String, _>(0),
            "stopping"
        );
        f.pool.close().await;
    }
}

#[tokio::test]
async fn idle_native_owner_can_succeed_without_changing_checkpoint() {
    let f = fixture(false).await;
    let (state, proof) = input(&f).await;
    publish(&f.pool, &state, &proof, &[], || Ok(()))
        .await
        .unwrap();
    assert!(
        confirmed(&f.root.path().join("state.db"), &state, &proof)
            .await
            .unwrap()
    );
    f.pool.close().await;
}

#[tokio::test]
async fn unknown_owner_evidence_uses_native_exclusion_for_active_and_recovered_rows() {
    for stopped in [false, true] {
        let f = unknown_owner_fixture(stopped).await;
        let (state, proof) = input(&f).await;
        assert_eq!(state.owner.revision, 2);
        publish(&f.pool, &state, &proof, &[], || Ok(()))
            .await
            .unwrap();
        assert!(
            confirmed(&f.root.path().join("state.db"), &state, &proof)
                .await
                .unwrap(),
            "stopped={stopped}"
        );
        let evidence: (String, String, i64) = sqlx::query_as(
            "SELECT state,barrier_state,evidence_revision FROM runtime_owner_evidence WHERE instance_token='owner' AND is_current=1",
        )
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert_eq!(evidence, ("observing".into(), "open".into(), 3));
        let history: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runtime_owner_evidence WHERE instance_token='owner' AND state IN ('joined','reaped')",
        )
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert_eq!(history, 0);
        f.pool.close().await;
    }
}

#[tokio::test]
async fn unknown_owner_with_closed_join_barrier_is_refused_before_publication() {
    let f = unknown_owner_fixture(true).await;
    sqlx::query(
        "UPDATE runtime_owner_evidence SET barrier_state='closed_joined' WHERE instance_token='owner' AND is_current=1",
    )
    .execute(&f.pool)
    .await
    .unwrap();
    let mut c = f.pool.acquire().await.unwrap();
    let error = state::read(&mut c).await.unwrap_err().to_string();
    assert!(error.contains("native-succession/owner-identity"));
    drop(c);
    f.pool.close().await;
}
