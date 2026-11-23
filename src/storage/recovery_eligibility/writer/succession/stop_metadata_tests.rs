use super::*;
use crate::{
    native_succession::{process::Exclusion, state, stop_recovery},
    storage::recovery_eligibility::writer::succession::{
        self,
        tests::{Fixture, fixture},
    },
};
use std::path::PathBuf;

async fn archive(f: &Fixture) -> PathBuf {
    let path = f.root.path().join("before.db");
    sqlx::query("VACUUM INTO ?")
        .bind(path.to_str().unwrap())
        .execute(&f.pool)
        .await
        .unwrap();
    path
}
async fn stopped(f: &Fixture) {
    sqlx::query(
        "UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token='owner'",
    )
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&f.pool)
    .await
    .unwrap();
}
async fn input(f: &Fixture, archive: &std::path::Path) -> (State, Exclusion) {
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    let mut proof = Exclusion::modeled(state.owner.clone());
    proof.stop_metadata = Some(
        stop_recovery::witness(&mut c, archive, &state)
            .await
            .unwrap(),
    );
    (state, proof)
}
async fn snapshot(f: &Fixture) -> Vec<(String, String, String)> {
    let mut c = f.pool.acquire().await.unwrap();
    let mut rows: Vec<(String, String, String)> = sqlx::query_as("SELECT 'class:'||table_name,canonical_pk,json_array(original_revision,original_digest,parent_relations) FROM offline_recovery_rows ORDER BY table_name,canonical_pk")
        .fetch_all(&mut *c).await.unwrap();
    for id in inventory::identities(&mut c).await.unwrap() {
        let value = inventory::snapshot(&mut c, &id).await.unwrap();
        rows.push((format!("row:{}", id.table), id.key, value.digest));
    }
    let context: String = sqlx::query_scalar("SELECT json_array(generation,revision,compatibility_operation_id,expected_b_digest) FROM runtime_eligibility_context")
        .fetch_one(&mut *c).await.unwrap();
    rows.push(("context".into(), "1".into(), context));
    rows
}

#[tokio::test]
async fn classified_direct_producers_refuse_without_writes_and_native_remains_usable() {
    let f = fixture(false).await;
    let before = snapshot(&f).await;
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let database = f.root.path().join("state.db");
    let now = chrono::Utc::now().to_rfc3339();
    assert_eq!(
        crate::offline_recovery::recover_stale_runtime(
            &database,
            "owner",
            &state.owner.fingerprint,
            &now
        )
        .await,
        Err(crate::offline_recovery::RecoveryError::NativeScopeRequired)
    );
    assert_eq!(
        crate::offline_recovery::recover_proven_stale_runtime(
            &database,
            "owner",
            &state.owner.fingerprint,
            &now
        )
        .await,
        Err(crate::offline_recovery::RecoveryError::NativeScopeRequired)
    );
    assert_eq!(snapshot(&f).await, before);
    let current: String =
        sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token='owner'")
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(current, "stopping");
    let proof = Exclusion::modeled(state.owner.clone());
    succession::publish(&f.pool, &state, &proof, &[], || Ok(()))
        .await
        .unwrap();
    assert!(
        succession::confirmed(&database, &state, &proof)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn archived_stop_delta_repairs_without_restore_and_retry_is_read_only() {
    let f = fixture(false).await;
    let archive = archive(&f).await;
    stopped(&f).await;
    let (state, proof) = input(&f, &archive).await;
    // Causal reproduction: the pre-fix direct stop invalidates ordinary publication.
    let mut unqualified = proof.clone();
    unqualified.stop_metadata = None;
    let before = snapshot(&f).await;
    assert!(
        succession::publish(&f.pool, &state, &unqualified, &[], || Ok(()))
            .await
            .is_err()
    );
    assert_eq!(snapshot(&f).await, before);
    succession::publish(&f.pool, &state, &proof, &[], || Ok(()))
        .await
        .unwrap();
    let database = f.root.path().join("state.db");
    assert!(
        succession::confirmed(&database, &state, &proof)
            .await
            .unwrap()
    );
    let after = snapshot(&f).await;
    assert!(
        stop_recovery::retry(&f.pool, &database, &archive)
            .await
            .unwrap()
    );
    assert_eq!(snapshot(&f).await, after);
    let row: (String,String) = sqlx::query_as("SELECT r.state,e.child_snapshot FROM runtime_instances r JOIN runtime_owner_evidence e USING(instance_token) WHERE e.is_current=1").fetch_one(&f.pool).await.unwrap();
    assert_eq!(row.0, "stopped");
    assert!(row.1.contains("stop_metadata"));
    assert!(row.1.contains("\"historical_joined\":false"));
}

#[tokio::test]
async fn witness_missing_wrong_or_extra_column_drift_refuses() {
    for kind in 0..6 {
        let f = fixture(false).await;
        let archive = archive(&f).await;
        stopped(&f).await;
        if kind == 0 {
            std::fs::remove_file(&archive).unwrap();
        }
        if kind == 1 {
            let p = crate::storage::connect(&archive).await.unwrap();
            sqlx::query("UPDATE runtime_instances SET started_at='2020-01-01T00:00:00Z'")
                .execute(&p)
                .await
                .unwrap();
            p.close().await;
        }
        if kind == 2 {
            sqlx::query("UPDATE runtime_instances SET started_at='2020-01-01T00:00:00Z'")
                .execute(&f.pool)
                .await
                .unwrap();
        }
        if kind == 3 {
            sqlx::query("UPDATE runtime_instances SET heartbeat_at='not-a-time'")
                .execute(&f.pool)
                .await
                .unwrap();
        }
        if kind == 4 {
            sqlx::query("UPDATE runtime_instances SET heartbeat_at='2100-01-01T00:00:00Z'")
                .execute(&f.pool)
                .await
                .unwrap();
        }
        if kind == 5 {
            let p = crate::storage::connect(&archive).await.unwrap();
            sqlx::query("UPDATE runtime_instances SET state='stopped'")
                .execute(&p)
                .await
                .unwrap();
            p.close().await;
        }
        let before = snapshot(&f).await;
        let mut c = f.pool.acquire().await.unwrap();
        let state = state::read(&mut c).await.unwrap().unwrap();
        match stop_recovery::witness(&mut c, &archive, &state).await {
            Err(_) => (),
            Ok(w) => {
                drop(c);
                let mut proof = Exclusion::modeled(state.owner.clone());
                proof.stop_metadata = Some(w);
                assert!(
                    succession::publish(&f.pool, &state, &proof, &[], || Ok(()))
                        .await
                        .is_err(),
                    "kind={kind}"
                );
            }
        }
        assert_eq!(snapshot(&f).await, before, "kind={kind}");
    }
}

#[tokio::test]
async fn stale_context_evidence_current_image_and_other_rows_refuse_atomically() {
    for query in [
        "UPDATE runtime_eligibility_context SET revision=revision+1",
        "UPDATE runtime_owner_evidence SET child_snapshot='changed' WHERE is_current=1",
        "UPDATE runtime_instances SET heartbeat_at='2026-01-01T00:00:00Z'",
        "UPDATE runtime_instances SET started_at='2020-01-01T00:00:00Z'",
        "UPDATE matrix_recovery_batches SET response_digest='changed'",
        "INSERT INTO runtime_instances VALUES('competitor','t','t','active','other')",
    ] {
        let f = fixture(false).await;
        let archive = archive(&f).await;
        stopped(&f).await;
        let (state, proof) = input(&f, &archive).await;
        sqlx::query(query).execute(&f.pool).await.unwrap();
        let before = snapshot(&f).await;
        assert!(
            succession::publish(&f.pool, &state, &proof, &[], || Ok(()))
                .await
                .is_err(),
            "{query}"
        );
        assert_eq!(snapshot(&f).await, before, "{query}");
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runtime_owner_evidence WHERE state='observing'",
        )
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert_eq!(n, 0);
    }
}

#[tokio::test]
async fn interrupted_qualification_rolls_back_then_exact_retry_commits_once() {
    for last_check in [false, true] {
        let f = fixture(false).await;
        let archive = archive(&f).await;
        stopped(&f).await;
        let (state, proof) = input(&f, &archive).await;
        if !last_check {
            sqlx::query("CREATE TRIGGER interrupt BEFORE INSERT ON runtime_owner_evidence WHEN NEW.state='observing' BEGIN SELECT RAISE(ABORT,'interrupted'); END").execute(&f.pool).await.unwrap();
        }
        let before = snapshot(&f).await;
        let checks = std::cell::Cell::new(0);
        assert!(
            succession::publish(&f.pool, &state, &proof, &[], || {
                checks.set(checks.get() + 1);
                if last_check && checks.get() == 2 {
                    Err(refused())
                } else {
                    Ok(())
                }
            })
            .await
            .is_err()
        );
        assert_eq!(snapshot(&f).await, before);
        sqlx::query("DROP TRIGGER IF EXISTS interrupt")
            .execute(&f.pool)
            .await
            .unwrap();
        succession::publish(&f.pool, &state, &proof, &[], || Ok(()))
            .await
            .unwrap();
        assert!(
            stop_recovery::retry(&f.pool, &f.root.path().join("state.db"), &archive)
                .await
                .unwrap()
        );
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runtime_owner_evidence WHERE state='observing'",
        )
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert_eq!(n, 1);
    }
}

#[tokio::test]
async fn explicit_recovery_refuses_current_live_owner_before_metadata_writes() {
    let f = fixture(false).await;
    let archive = archive(&f).await;
    stopped(&f).await;
    let root = f.root.path();
    for sub in ["sessions", "crypto"] {
        std::fs::create_dir(root.join(sub)).unwrap();
    }
    let path = root.join("bridge.toml");
    let text = format!(
        r#"
[bridge]
database = {:?}
session_root = {:?}
[agents.worker]
transport = "acp"
command = "must-not-start"
workspace = {:?}
enabled = false
[transports.matrix]
enabled = true
homeserver = "http://127.0.0.1:1"
user_id = "@bridge:example.test"
device_id = "BRIDGE"
device_trusted = true
access_token = "{{env:FIXTURE_TOKEN}}"
crypto_store_path = {:?}
"#,
        root.join("state.db"),
        root.join("sessions"),
        root,
        root.join("crypto")
    );
    std::fs::write(&path, &text).unwrap();
    let config = crate::config::load_from_str_with_env(
        &text,
        &std::collections::BTreeMap::from([
            ("FIXTURE_TOKEN".into(), "synthetic-no-network-token".into()),
            ("HOME".into(), root.to_string_lossy().into_owned()),
        ]),
    )
    .unwrap();
    let before = snapshot(&f).await;
    let error = stop_recovery::recover(&config, &path, &archive)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("live-or-reused-owner-pid"),
        "{error}"
    );
    assert_eq!(snapshot(&f).await, before);
}
