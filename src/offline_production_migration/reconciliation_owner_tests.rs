use super::*;
use crate::offline_production_migration::reconciliation_records::{OwnerProof, Parent};
use crate::offline_production_migration::{Failure, reconciliation_graph, reconciliation_owner};
use crate::storage::recovery_eligibility::{ParentIdentity, ParentRole, inventory};
use sqlx::SqliteConnection;

pub(super) async fn seed(connection: &mut SqliteConnection) {
    for index in 0..126 {
        let token = format!("owner-{index:03}");
        let fingerprint = format!("frozen-fingerprint-{index:03}");
        sqlx::query("INSERT INTO runtime_instances VALUES(?,'t','t','stopped',?)")
            .bind(&token)
            .bind(&fingerprint)
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) VALUES(?,?,?,'live',123,456,?,'historical-boot',789,'historical-time',0,'open',0,'inaccessible','{}',1,30)")
            .bind(&token).bind(index + 1).bind(index + 2).bind(&fingerprint)
            .execute(&mut *connection).await.unwrap();
    }
}

pub(super) async fn action_parents(
    connection: &mut SqliteConnection,
    snapshot: &crate::storage::recovery_eligibility::inventory::Snapshot,
) -> Vec<Parent> {
    let mut parents = Vec::new();
    for identity in &snapshot.parents {
        let image = inventory::snapshot(connection, identity).await.unwrap();
        parents.push(Parent {
            identity: identity.clone(),
            revision: image.revision,
            digest: image.digest,
            role: "effectful".into(),
        });
    }
    parents
}

pub(super) async fn parent_relations(
    connection: &mut SqliteConnection,
    snapshot: &crate::storage::recovery_eligibility::inventory::Snapshot,
) -> serde_json::Value {
    let parents = action_parents(connection, snapshot)
        .await
        .into_iter()
        .map(|parent| ParentIdentity {
            identity: parent.identity,
            revision: parent.revision,
            digest: parent.digest,
            role: ParentRole::Effectful,
        })
        .collect::<Vec<_>>();
    serde_json::to_value(parents).unwrap()
}

pub(super) async fn proofs(connection: &mut SqliteConnection) -> Vec<OwnerProof> {
    let tuples: Vec<(String, i64, i64)> = sqlx::query_as("SELECT instance_token,runtime_generation,evidence_revision FROM runtime_owner_evidence ORDER BY instance_token").fetch_all(&mut *connection).await.unwrap();
    let mut proofs = Vec::new();
    for (instance_token, runtime_generation, evidence_revision) in tuples {
        let key: String = sqlx::query_scalar("SELECT json_array(typeof(instance_token)||':'||hex(CAST(instance_token AS BLOB)),typeof(runtime_generation)||':'||hex(CAST(runtime_generation AS BLOB)),typeof(evidence_revision)||':'||hex(CAST(evidence_revision AS BLOB))) FROM runtime_owner_evidence WHERE instance_token=?").bind(&instance_token).fetch_one(&mut *connection).await.unwrap();
        let owner = inventory::RowIdentity {
            table: "runtime_owner_evidence".into(),
            key,
        };
        let snapshot = inventory::snapshot(connection, &owner).await.unwrap();
        let parent = inventory::snapshot(connection, &snapshot.parents[0])
            .await
            .unwrap();
        proofs.push(OwnerProof {
            instance_token,
            runtime_generation,
            evidence_revision,
            evidence_digest: snapshot.digest,
            parent_digest: parent.digest,
        });
    }
    proofs
}

async fn owner_bytes(
    request: &TransitionRequest,
) -> Vec<(String, String, Vec<String>, Vec<String>)> {
    let mut connection = verification::open(&request.database, true).await.unwrap();
    let mut rows = Vec::new();
    for identity in inventory::identities(&mut connection).await.unwrap() {
        if matches!(
            identity.table.as_str(),
            "runtime_owner_evidence" | "runtime_instances"
        ) {
            let (names, values) = inventory::values(&mut connection, &identity).await.unwrap();
            rows.push((identity.table, identity.key, names, values));
        }
    }
    connection.close().await.unwrap();
    rows
}

#[tokio::test]
async fn historical_live_owners_reproduce_reaped_mismatch_and_preserve_all_rows() {
    let fixture = Protocol2Fixture::with_owners(true).await;
    let plan: Plan = rec::load(&fixture.request.transition_plan).unwrap();
    let before = owner_bytes(&fixture.request).await;
    assert_eq!(before.len(), 252);
    let mut connection = verification::open(&fixture.request.database, true)
        .await
        .unwrap();
    let old_matches: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence evidence JOIN runtime_instances instance USING(instance_token) WHERE evidence.state='reaped' AND instance.state='stopped'").fetch_one(&mut connection).await.unwrap();
    assert_eq!(old_matches, 0, "R094 first failing D2 reaped predicate");
    assert_eq!(plan.d2_owner.len(), 126);
    reconciliation_owner::validate(&mut connection, &plan)
        .await
        .expect("exact owner tuple, parent, fingerprint and byte proofs");
    reconciliation_graph::validate_plan(&mut connection, &plan)
        .await
        .unwrap();
    connection.close().await.unwrap();
    let output = fixture.child("no-crash");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(owner_bytes(&fixture.request).await, before);
    assert_eq!(state(&fixture.request).await.0, "baseline_ready");
    final_graph_state(&fixture.request).await;
}

#[tokio::test]
async fn owner_proofs_refuse_missing_duplicate_stale_or_mismatched_evidence() {
    let fixture = Protocol2Fixture::with_owners(true).await;
    let plan: Plan = rec::load(&fixture.request.transition_plan).unwrap();
    let before = owner_bytes(&fixture.request).await;
    let mut connection = verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    for variant in 0..9 {
        let mut wrong = plan.clone();
        match variant {
            0 => {
                wrong.d2_owner.pop();
            }
            1 => wrong.d2_owner.push(wrong.d2_owner[0].clone()),
            2 => wrong.d2_owner[0].runtime_generation += 1,
            3 => wrong.d2_owner[0].evidence_revision += 1,
            4 => wrong.d2_owner[0].evidence_digest = "0".repeat(64),
            5 => wrong.d2_owner[0].parent_digest = "0".repeat(64),
            6 => wrong.d2_owner[0].instance_token = "wrong-parent".into(),
            7 => {
                let action = wrong
                    .actions
                    .iter_mut()
                    .find(|item| item.table_name == "runtime_owner_evidence")
                    .unwrap();
                action.action_type = "update".into();
            }
            _ => {
                let row = wrong
                    .classifications
                    .iter_mut()
                    .find(|item| item.table_name == "runtime_owner_evidence")
                    .unwrap();
                row.disposition = "quarantined_no_effect".into();
            }
        }
        assert!(
            matches!(
                reconciliation_owner::validate(&mut connection, &wrong).await,
                Err(Failure::ManualRequired)
            ),
            "variant {variant}"
        );
        if variant < 7 {
            refuse_plan(&fixture, &mut wrong).await;
            assert_eq!(owner_bytes(&fixture.request).await, before);
        }
    }
    connection.close().await.unwrap();
    assert_eq!(owner_bytes(&fixture.request).await, before);
    assert_eq!(
        state(&fixture.request).await,
        ("identity_ready".into(), "".into(), false, false)
    );
}

async fn refuse_plan(fixture: &Protocol2Fixture, plan: &mut Plan) {
    plan.plan_digest = rec::plan_digest(plan).unwrap();
    for classification in plan
        .classifications
        .iter_mut()
        .chain(&mut plan.inserted_classifications)
    {
        classification.plan_digest = Some(plan.plan_digest.clone());
    }
    fs::write(
        &fixture.request.transition_plan,
        serde_json::to_vec(plan).unwrap(),
    )
    .unwrap();
    let output = fixture.child("no-crash");
    assert!(!output.status.success(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("ManualRequired"),
        "{output:?}"
    );
    assert_eq!(
        state(&fixture.request).await,
        ("identity_ready".into(), "".into(), false, false)
    );
}

#[tokio::test]
async fn owner_parent_and_fingerprint_drift_refuses_without_partial_writes() {
    let fixture = Protocol2Fixture::with_owners(true).await;
    let plan: Plan = rec::load(&fixture.request.transition_plan).unwrap();
    let before = owner_bytes(&fixture.request).await;
    let mut connection = verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    for mutation in [
        "UPDATE runtime_instances SET state='active' WHERE instance_token='owner-000'",
        "UPDATE runtime_instances SET state='stopping' WHERE instance_token='owner-000'",
        "UPDATE runtime_instances SET process_fingerprint='changed' WHERE instance_token='owner-000'",
        "UPDATE runtime_owner_evidence SET process_fingerprint='changed' WHERE instance_token='owner-000'",
        "UPDATE runtime_owner_evidence SET state='changed' WHERE instance_token='owner-000'",
        "UPDATE runtime_owner_evidence SET state='unknown' WHERE instance_token='owner-000'",
        "UPDATE runtime_owner_evidence SET runtime_generation=999 WHERE instance_token='owner-000'",
        "UPDATE runtime_owner_evidence SET evidence_revision=999 WHERE instance_token='owner-000'",
        "DELETE FROM runtime_owner_evidence WHERE instance_token='owner-000'",
        "INSERT INTO runtime_instances VALUES('extra-parent','t','t','stopped','extra')",
        "INSERT INTO runtime_owner_evidence SELECT instance_token,999,999,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,0,max_age_seconds FROM runtime_owner_evidence WHERE instance_token='owner-000'",
    ] {
        let mut tx = connection.begin().await.unwrap();
        sqlx::query(mutation).execute(&mut *tx).await.unwrap();
        assert!(
            matches!(
                reconciliation_graph::validate_plan(&mut tx, &plan).await,
                Err(Failure::ManualRequired)
            ),
            "{mutation}"
        );
        tx.rollback().await.unwrap();
    }
    sqlx::query("PRAGMA foreign_keys=OFF")
        .execute(&mut connection)
        .await
        .unwrap();
    let mut tx = connection.begin().await.unwrap();
    sqlx::query("DELETE FROM runtime_instances WHERE instance_token='owner-000'")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        reconciliation_owner::validate(&mut tx, &plan).await,
        Err(Failure::ManualRequired)
    ));
    tx.rollback().await.unwrap();
    connection.close().await.unwrap();
    assert_eq!(owner_bytes(&fixture.request).await, before);
}

#[tokio::test]
async fn owner_crash_resume_preserves_bytes_and_expected_graph_parity() {
    for phase in [
        "reconciliation-after-intent",
        "reconciliation-after-first-commit",
        "reconciliation-after-completion",
        "reconciliation-after-final-commit",
    ] {
        let fixture = Protocol2Fixture::with_owners(true).await;
        let before = owner_bytes(&fixture.request).await;
        let output = fixture.child(phase);
        assert_eq!(output.status.code(), Some(86), "{phase}: {output:?}");
        assert_eq!(owner_bytes(&fixture.request).await, before);
        let output = fixture.child("owner-resume");
        assert!(output.status.success(), "{phase}: {output:?}");
        let final_graph = final_graph_state(&fixture.request).await;
        let output = fixture.child("owner-resume");
        assert!(output.status.success(), "{output:?}");
        assert_eq!(final_graph_state(&fixture.request).await, final_graph);
        assert_eq!(owner_bytes(&fixture.request).await, before);
    }
}

#[tokio::test]
async fn owner_exclusion_race_rolls_back_first_writer_and_refuses_finalization() {
    for stage in [
        "transition-v2-first-commit",
        "transition-v2-readback",
        "transition-v2-final-commit",
    ] {
        let fixture = Protocol2Fixture::with_owners(true).await;
        let before = owner_bytes(&fixture.request).await;
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "offline_production_migration::reconciliation_e2e_tests::child_entry",
                "--nocapture",
            ])
            .env(CHILD_ROOT, fixture.fixture.root.path())
            .env(CHILD_MANIFEST, &fixture.request.manifest_dir)
            .env(CHILD_GRANT, &fixture.grant_path)
            .env(CHILD_PHASE, "no-crash")
            .env("R071_TEST_LAUNCHER_FAIL", stage)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{stage}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("Busy"),
            "{output:?}"
        );
        assert_eq!(owner_bytes(&fixture.request).await, before);
        let observed = state(&fixture.request).await;
        assert!(
            fixture
                .request
                .manifest_dir
                .join("reconciliation-intent-v2.json")
                .exists()
        );
        if stage == "transition-v2-first-commit" {
            assert_eq!(observed, ("identity_ready".into(), "".into(), false, false));
        } else {
            assert_eq!(observed.0, "transition_pending");
            assert_eq!(observed.1, "committed");
        }
        assert!(fixture.child("owner-resume").status.success());
        assert_eq!(owner_bytes(&fixture.request).await, before);
    }
}
