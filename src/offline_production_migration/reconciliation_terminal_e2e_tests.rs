use super::super::reconciliation_d1_fixture as chain;
use super::*;

async fn assert_images(fixture: &Protocol2Fixture) {
    let plan: Plan = rec::load(&fixture.request.transition_plan).unwrap();
    let mut connection = verification::open(&fixture.request.database, true)
        .await
        .unwrap();
    let actual = chain::snapshot(&mut connection).await;
    assert_eq!(actual.len(), plan.actions.len());
    for action in &plan.actions {
        let row = actual
            .iter()
            .find(|row| row.0 == action.table_name && row.1 == action.canonical_pk)
            .unwrap();
        assert_eq!(Some(&row.2), action.after.as_ref(), "{}:{}", row.0, row.1);
    }
    let audits: Vec<(String, String)> = sqlx::query_as(
        "SELECT task_id,decision_id FROM historical_unknown_abandonments ORDER BY task_id",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    assert_eq!(
        audits,
        [("task-a".into(), plan.native_operation_id.clone())]
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&mut connection)
            .await
            .unwrap()
            .is_empty()
    );
    connection.close().await.unwrap();
}

#[tokio::test]
async fn terminal_protocol2_four_crash_boundaries_restart_graph_and_no_replay() {
    for phase in [
        "reconciliation-after-intent",
        "reconciliation-after-first-commit",
        "reconciliation-after-completion",
        "reconciliation-after-final-commit",
    ] {
        let fixture = Protocol2Fixture::with_chain().await;
        let output = fixture.child(phase);
        assert_eq!(output.status.code(), Some(86), "{phase}: {output:?}");
        let request = TransitionRequest {
            mode: Mode::Resume,
            ..fixture.request.clone()
        };
        let output = fixture.child("owner-resume");
        assert!(output.status.success(), "{phase} resume: {output:?}");
        assert_images(&fixture).await;
        let before = final_graph_state(&request).await;
        let output = fixture.child("owner-resume");
        assert!(output.status.success(), "{phase} replay: {output:?}");
        assert_eq!(
            before,
            final_graph_state(&request).await,
            "{phase} graph/digest parity"
        );
        assert_images(&fixture).await;
    }
}

fn publish(fixture: &Protocol2Fixture, plan: &mut Plan) {
    plan.plan_digest = rec::plan_digest(plan).unwrap();
    for item in plan
        .classifications
        .iter_mut()
        .chain(&mut plan.inserted_classifications)
    {
        item.plan_digest = Some(plan.plan_digest.clone());
    }
    fs::write(
        &fixture.request.transition_plan,
        serde_json::to_vec(plan).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn terminal_expected_graph_refuses_fk_admission_and_audit_drift() {
    use super::super::reconciliation_graph::ExpectedPostGraph;

    let fixture = Protocol2Fixture::with_chain().await;
    let output = fixture.child("reconciliation-after-first-commit");
    assert_eq!(output.status.code(), Some(86), "{output:?}");
    let plan: Plan = rec::load(&fixture.request.transition_plan).unwrap();
    let intent: rec::Intent = rec::load(
        &fixture
            .request
            .manifest_dir
            .join("reconciliation-intent-v2.json"),
    )
    .unwrap();
    let expected = ExpectedPostGraph::from_pre_state(
        intent.pre_transition.as_ref().unwrap(),
        &plan,
        &records::encoded_digest(&intent).unwrap(),
    )
    .unwrap();
    let mut connection = verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    for (label, mutation) in [
        (
            "fk",
            "UPDATE task_admissions SET runtime_instance='missing' WHERE task_id='task-a'",
        ),
        (
            "admission-cell",
            "UPDATE task_admissions SET reply_room='same-revision-drift' WHERE task_id='task-a'",
        ),
        (
            "missing-audit",
            "DELETE FROM historical_unknown_abandonments WHERE decision_id='native-operation'",
        ),
        (
            "audit-cell",
            "UPDATE historical_unknown_abandonments SET post_state_digest='wrong' WHERE decision_id='native-operation'",
        ),
        (
            "extra-audit",
            "INSERT INTO historical_unknown_abandonments(decision_id,task_id,delivery_id,resource_key,owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,disposition,reason,policy_ref,authorization_ref,not_replayed,created_at,session_id_digest,before_state_digest,post_state_digest,abandonment_kind,termination_proven) SELECT 'extra',task_id,delivery_id,resource_key||'-extra',owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,disposition,reason,policy_ref,authorization_ref,not_replayed,created_at,session_id_digest,before_state_digest,post_state_digest,abandonment_kind,termination_proven FROM historical_unknown_abandonments WHERE decision_id='native-operation'",
        ),
        (
            "extra-inventory",
            "INSERT INTO agents VALUES('unrelated','unrelated','acp',1,NULL,'[]')",
        ),
    ] {
        reject_graph_mutation(&mut connection, &plan, &expected, label, mutation).await;
    }
    connection.close().await.unwrap();
}

async fn reject_graph_mutation(
    connection: &mut sqlx::SqliteConnection,
    plan: &Plan,
    expected: &super::super::reconciliation_graph::ExpectedPostGraph,
    label: &str,
    mutation: &'static str,
) {
    let mut transaction = connection.begin().await.unwrap();
    sqlx::query("PRAGMA defer_foreign_keys=ON")
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::raw_sql(mutation)
        .execute(&mut *transaction)
        .await
        .unwrap();
    if label == "fk" {
        assert!(
            !sqlx::query("PRAGMA foreign_key_check")
                .fetch_all(&mut *transaction)
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert!(
        matches!(
            super::super::reconciliation_graph::verify_expected_graph(
                &mut transaction,
                plan,
                Some(expected)
            )
            .await,
            Err(super::super::Failure::ManualRequired)
        ),
        "{label}"
    );
    transaction.rollback().await.unwrap();
    super::super::reconciliation_graph::verify_expected_graph(connection, plan, Some(expected))
        .await
        .unwrap();
}

#[tokio::test]
async fn terminal_protocol2_entry_refuses_authorization_action_and_digest_mismatch() {
    let fixture = Protocol2Fixture::with_chain().await;
    let original: Plan = rec::load(&fixture.request.transition_plan).unwrap();
    let mut connection = verification::open(&fixture.request.database, true)
        .await
        .unwrap();
    let before = chain::snapshot(&mut connection).await;
    connection.close().await.unwrap();
    for case in [
        "decision",
        "evidence",
        "action",
        "action_digest",
        "grant",
        "tuple",
    ] {
        let mut plan = original.clone();
        match case {
            "decision" => {
                plan.d1_unknown_chain.as_mut().unwrap().decision_id = "unauthorized".into()
            }
            "evidence" => plan
                .d1_unknown_chain
                .as_mut()
                .unwrap()
                .evidence_digest
                .clear(),
            "action" => {
                let action = plan
                    .actions
                    .iter_mut()
                    .find(|action| {
                        action.table_name == "task_continuations" && action.action_type == "update"
                    })
                    .unwrap();
                let after = action.after.as_mut().unwrap();
                let index = after
                    .names
                    .iter()
                    .position(|name| name == "revision")
                    .unwrap();
                after.values[index] = "integer:393939".into();
                plan.action_digest = records::encoded_digest(&plan.actions).unwrap();
            }
            "action_digest" => plan.action_digest = "0".repeat(64),
            "grant" => plan.grant_digest = "0".repeat(64),
            "tuple" => plan.d1_unknown_chain.as_mut().unwrap().references[10] = "999".into(),
            _ => unreachable!(),
        }
        publish(&fixture, &mut plan);
        let output = fixture.child("no-crash");
        assert!(!output.status.success(), "{case}: {output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("ManualRequired"),
            "{case}: {output:?}"
        );
        let mut connection = verification::open(&fixture.request.database, true)
            .await
            .unwrap();
        assert_eq!(before, chain::snapshot(&mut connection).await, "{case}");
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(count, 0, "{case}");
        connection.close().await.unwrap();
        let intent = fixture
            .request
            .manifest_dir
            .join("reconciliation-intent-v2.json");
        if intent.exists() {
            fs::remove_file(intent).unwrap();
        }
    }
}
