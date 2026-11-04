use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

use super::{
    Mode, TransitionRequest, evidence, flow, reconciliation,
    reconciliation_records::{
        self as rec, Action, Anchor, Binding, Classification, Grant, Plan, RowImage,
    },
    records,
    test_fixture::Fixture,
    verification, version2_records,
};
use sqlx::{Connection, Row};

const CHILD_ROOT: &str = "R094_RECON_ROOT";
const CHILD_MANIFEST: &str = "R094_RECON_MANIFEST";
const CHILD_GRANT: &str = "R094_RECON_GRANT";
const CHILD_PHASE: &str = "R094_RECON_PHASE";

#[path = "reconciliation_owner_tests.rs"]
mod owner_tests;

struct Protocol2Fixture {
    fixture: Fixture,
    request: TransitionRequest,
    grant_path: PathBuf,
}

impl Protocol2Fixture {
    async fn new() -> Self {
        Self::with_owners(false).await
    }

    async fn with_owners(owners: bool) -> Self {
        let mut fixture = Fixture::new().await;
        fixture.request.target_schema = 25;
        let mut provenance: serde_json::Value =
            serde_json::from_slice(&fs::read(&fixture.request.release_provenance).unwrap())
                .unwrap();
        provenance["migration_set_sha256"] = evidence::migration_digest_through(25).into();
        fs::write(
            &fixture.request.release_provenance,
            serde_json::to_vec(&provenance).unwrap(),
        )
        .unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "offline_production_migration::reconciliation_e2e_tests::child_entry",
                "--nocapture",
            ])
            .env(CHILD_ROOT, fixture.root.path())
            .env(CHILD_PHASE, "bootstrap")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(86), "bootstrap: {output:?}");

        let original_executable = fixture.root.path().join("original-executable");
        fs::copy(&fixture.request.executable, &original_executable).unwrap();
        let original_executable_digest = evidence::hash_file(&original_executable).unwrap();
        provenance["executable"] = original_executable.to_string_lossy().into_owned().into();
        provenance["executable_sha256"] = original_executable_digest.clone().into();
        provenance["launcher_hold"]["executable"] =
            original_executable.to_string_lossy().into_owned().into();
        fs::write(
            &fixture.request.release_provenance,
            serde_json::to_vec(&provenance).unwrap(),
        )
        .unwrap();
        let provenance_digest = evidence::hash_file(&fixture.request.release_provenance).unwrap();
        let root_path = records::anchor(&fixture.request.database);
        let mut root: version2_records::Root =
            serde_json::from_slice(&fs::read(&root_path).unwrap()).unwrap();
        root.operation.provenance_digest = provenance_digest.clone();
        fs::write(&root_path, serde_json::to_vec(&root).unwrap()).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o600)).unwrap();
        let archive_path = fixture.request.manifest_dir.join("archive-v2.json");
        assert_eq!(root.operation.provenance_digest, provenance_digest);
        assert_eq!(
            evidence::hash_file(&original_executable).unwrap(),
            original_executable_digest
        );
        let mut resume_request = fixture.request.clone();
        resume_request.mode = Mode::Resume;
        flow::execute(resume_request, false)
            .await
            .unwrap_or_else(|error| panic!("resume bootstrap: {error:?}"));
        let identity_digest = "1".repeat(64);
        let mut connection = verification::open(&fixture.request.database, false)
            .await
            .unwrap();
        if owners {
            owner_tests::seed(&mut connection).await;
        }
        sqlx::query(
            "INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json)
             VALUES('fixture-agent','fixture-agent','acp',1,NULL,'[]')",
        )
        .execute(&mut connection)
        .await
        .unwrap();
        for conversation in ["fixture-conversation-a", "fixture-conversation-b"] {
            sqlx::query(
                "INSERT INTO conversations(conversation_id,transport,external_id,thread_ref,participants_json)
                 VALUES(?,NULL,NULL,NULL,'[]')",
            )
            .bind(conversation)
            .execute(&mut connection)
            .await
            .unwrap();
        }
        sqlx::query(
            "UPDATE offline_recovery_binding
             SET identity_digest=?,phase='identity_ready',revision=revision+1
             WHERE singleton=1 AND phase='schema_ready'",
        )
        .bind(&identity_digest)
        .execute(&mut connection)
        .await
        .unwrap();
        let binding = binding(&mut connection).await;
        let root: version2_records::Root = rec::load(&root_path).unwrap();
        let root_digest = records::encoded_digest(&root).unwrap();
        let archive: version2_records::Archive = rec::load(&archive_path).unwrap();
        let archive_digest = records::encoded_digest(&archive).unwrap();
        let source_state_digest =
            crate::storage::matrix_recovery_snapshot::snapshot(&mut connection)
                .await
                .unwrap()
                .digest;
        let identities =
            crate::storage::recovery_eligibility::inventory::identities(&mut connection)
                .await
                .unwrap();
        assert_eq!(identities.len(), if owners { 255 } else { 3 });
        let mut classifications = Vec::new();
        let mut anchors = Vec::new();
        let mut actions = Vec::new();
        for identity in identities {
            let snapshot = crate::storage::recovery_eligibility::inventory::snapshot(
                &mut connection,
                &identity,
            )
            .await
            .unwrap();
            let (names, values) =
                crate::storage::recovery_eligibility::inventory::values(&mut connection, &identity)
                    .await
                    .unwrap();
            let image = RowImage { names, values };
            actions.push(Action {
                action_id: format!("preserve:{}:{}", identity.table, identity.key),
                action_type: "preserve".into(),
                table_name: identity.table.clone(),
                canonical_pk: identity.key.clone(),
                original_revision: snapshot.revision.clone(),
                original_digest: snapshot.digest.clone(),
                before: Some(image.clone()),
                after: Some(image.clone()),
                set_columns: Vec::new(),
                unchanged_columns: image.names.clone(),
                parents: owner_tests::action_parents(&mut connection, &snapshot).await,
                evidence_digest: "2".repeat(64),
            });
            classifications.push(Classification {
                table_name: identity.table.clone(),
                canonical_pk: identity.key.clone(),
                origin_generation: "native-operation".into(),
                original_revision: snapshot.revision.clone(),
                original_digest: snapshot.digest.clone(),
                parent_relations: owner_tests::parent_relations(&mut connection, &snapshot).await,
                disposition: "terminal_evidenced".into(),
                evidence_digest: "2".repeat(64),
                context_kind: "admin".into(),
                context_generation: None,
                admin_operation_id: Some(fixture.request.operation_id.clone()),
                plan_digest: None,
                matrix_operation_id: Some("native-operation".into()),
            });
            if matches!(identity.table.as_str(), "agents" | "conversations") {
                anchors.push(Anchor {
                    table_name: identity.table,
                    canonical_pk: identity.key,
                    original_revision: snapshot.revision,
                    original_digest: snapshot.digest,
                });
            }
        }
        let encoded_text = |value: &str| {
            format!(
                "text:{}",
                value
                    .as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02X}"))
                    .collect::<String>()
            )
        };
        let native_image = RowImage {
            names: [
                "control_id",
                "cutover_operation_id",
                "phase",
                "watermark",
                "updated_at",
                "restart_hold_state",
                "restart_hold_event_id",
                "restart_hold_reason",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
            values: vec![
                "integer:31".into(),
                encoded_text("native-operation"),
                encoded_text("baseline"),
                "null:".into(),
                encoded_text("2026-11-03T00:00:00Z"),
                "null:".into(),
                "null:".into(),
                "null:".into(),
            ],
        };
        let native = Classification {
            table_name: "matrix_recovery_control".into(),
            canonical_pk: "[\"integer:31\"]".into(),
            origin_generation: "native-operation".into(),
            original_revision: records::encoded_digest(&(
                &native_image.names,
                &native_image.values,
            ))
            .unwrap(),
            original_digest: records::encoded_digest(&(&native_image.names, &native_image.values))
                .unwrap(),
            parent_relations: serde_json::json!([]),
            disposition: "eligible_new".into(),
            evidence_digest: "2".repeat(64),
            context_kind: "admin".into(),
            context_generation: None,
            admin_operation_id: Some(fixture.request.operation_id.clone()),
            plan_digest: None,
            matrix_operation_id: Some("native-operation".into()),
        };
        actions.push(Action {
            action_id: "insert-native-control".into(),
            action_type: "insert".into(),
            table_name: native.table_name.clone(),
            canonical_pk: native.canonical_pk.clone(),
            original_revision: "absent".into(),
            original_digest: records::encoded_digest(&Option::<RowImage>::None).unwrap(),
            before: None,
            after: Some(native_image.clone()),
            set_columns: native_image.names,
            unchanged_columns: Vec::new(),
            parents: Vec::new(),
            evidence_digest: native.evidence_digest.clone(),
        });
        let inserted_classifications = vec![native];
        let d2_owner = owner_tests::proofs(&mut connection).await;
        connection.close().await.unwrap();

        let successor_executable_digest = evidence::hash_file(&fixture.request.executable).unwrap();
        let grant = Grant {
            version: 1,
            operation_id: fixture.request.operation_id.clone(),
            root_digest: root_digest.clone(),
            original_provenance_digest: root.operation.provenance_digest.clone(),
            original_executable_digest: original_executable_digest.clone(),
            archive_digest: archive_digest.clone(),
            schema_complete_digest: binding.schema_complete_digest.clone().unwrap(),
            identity_digest: identity_digest.clone(),
            successor_executable_digest: successor_executable_digest.clone(),
            successor_package_digest: "3".repeat(64),
            successor_source: "fixture-source".into(),
            successor_review: "fixture-review".into(),
            protocol: 2,
            entry: "offline-transition-matrix-compatibility".into(),
            allowed_modes: vec!["apply".into(), "resume".into(), "verify".into()],
        };
        let grant_digest = records::encoded_digest(&grant).unwrap();
        let mut ordered_classifications = classifications
            .iter()
            .chain(&inserted_classifications)
            .collect::<Vec<_>>();
        ordered_classifications.sort_by_key(|item| (&item.table_name, &item.canonical_pk));
        let classification_digest = records::encoded_digest(
            &ordered_classifications
                .iter()
                .map(|classification| {
                    (
                        &classification.table_name,
                        &classification.canonical_pk,
                        &classification.origin_generation,
                        &classification.original_revision,
                        &classification.original_digest,
                        &classification.parent_relations,
                        &classification.disposition,
                        &classification.evidence_digest,
                        &classification.matrix_operation_id,
                    )
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut plan = Plan {
            protocol: 2,
            plan_digest: String::new(),
            operation_id: fixture.request.operation_id.clone(),
            root_digest,
            original_provenance_digest: root.operation.provenance_digest,
            original_executable_digest: original_executable_digest.clone(),
            archive_digest,
            schema_complete_digest: binding.schema_complete_digest.clone().unwrap(),
            identity_digest,
            grant_digest,
            successor_executable_digest,
            successor_package_digest: grant.successor_package_digest.clone(),
            successor_source: grant.successor_source.clone(),
            successor_review: grant.successor_review.clone(),
            native_operation_id: "native-operation".into(),
            source_state_digest,
            original_binding: Binding {
                plan_digest: None,
                identity_digest: binding.identity_digest.clone(),
                classification_digest: None,
                transition_intent_digest: None,
                transition_complete_digest: None,
                native_operation_id: None,
                phase: "identity_ready".into(),
                ..binding
            },
            classifications,
            inserted_classifications,
            action_digest: records::encoded_digest(&actions).unwrap(),
            actions,
            d1_unknown_chain: None,
            d1_quarantined: None,
            d2_owner,
            anchors,
            workspaces: Vec::new(),
            native_at: "2026-11-03T00:00:00Z".into(),
            abandonment_at: "2026-11-03 00:00:00".into(),
            classification_digest,
        };
        plan.plan_digest = rec::plan_digest(&plan).unwrap();
        for classification in plan
            .classifications
            .iter_mut()
            .chain(&mut plan.inserted_classifications)
        {
            classification.plan_digest = Some(plan.plan_digest.clone());
        }
        assert_eq!(rec::plan_digest(&plan).unwrap(), plan.plan_digest);

        let plan_path = fixture
            .request
            .manifest_dir
            .join("reconciliation-plan-v2.json");
        rec::publish(&plan_path, &plan).unwrap();
        let grant_dir = fixture.root.path().join("protected-grant");
        fs::create_dir(&grant_dir).unwrap();
        fs::set_permissions(&grant_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let grant_path = grant_dir.join("fixture-operation.json");
        rec::publish(&grant_path, &grant).unwrap();
        let request = TransitionRequest {
            mode: Mode::Apply,
            config: fixture.request.config.clone(),
            database: fixture.request.database.clone(),
            operation_id: fixture.request.operation_id.clone(),
            manifest_dir: fixture.request.manifest_dir.clone(),
            transition_plan: plan_path,
            release_provenance: fixture.request.release_provenance.clone(),
            executable: fixture.request.executable.clone(),
        };
        Self {
            fixture,
            request,
            grant_path,
        }
    }

    fn child(&self, phase: &str) -> std::process::Output {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "offline_production_migration::reconciliation_e2e_tests::child_entry",
                "--nocapture",
            ])
            .env(CHILD_ROOT, self.fixture.root.path())
            .env(CHILD_MANIFEST, &self.request.manifest_dir)
            .env(CHILD_GRANT, &self.grant_path)
            .env(CHILD_PHASE, phase)
            .output()
            .unwrap()
    }
}

async fn binding(connection: &mut sqlx::SqliteConnection) -> Binding {
    let row = sqlx::query(
        "SELECT db_generation,canonical_db_digest,root_digest,admin_operation_id,
                schema_intent_digest,schema_complete_digest,plan_digest,identity_digest,
                classification_digest,transition_intent_digest,transition_complete_digest,
                native_operation_id,phase,revision
         FROM offline_recovery_binding WHERE singleton=1",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    Binding {
        db_generation: row.get("db_generation"),
        canonical_db_digest: row.get("canonical_db_digest"),
        root_digest: row.get("root_digest"),
        admin_operation_id: row.get("admin_operation_id"),
        schema_intent_digest: row.get("schema_intent_digest"),
        schema_complete_digest: row.get("schema_complete_digest"),
        plan_digest: row.get("plan_digest"),
        identity_digest: row.get("identity_digest"),
        classification_digest: row.get("classification_digest"),
        transition_intent_digest: row.get("transition_intent_digest"),
        transition_complete_digest: row.get("transition_complete_digest"),
        native_operation_id: row.get("native_operation_id"),
        phase: row.get("phase"),
        revision: row.get("revision"),
    }
}

async fn state(request: &TransitionRequest) -> (String, String, bool, bool) {
    let mut connection = verification::open(&request.database, true).await.unwrap();
    let binding_phase: String =
        sqlx::query_scalar("SELECT phase FROM offline_recovery_binding WHERE singleton=1")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    let schema: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN ('r093_reconciliation_marker','r093_reconciliation_audit')",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    let marker_state = if schema == 2 {
        sqlx::query_scalar("SELECT state FROM r093_reconciliation_marker WHERE singleton=1")
            .fetch_optional(&mut connection)
            .await
            .unwrap()
    } else {
        None
    };
    connection.close().await.unwrap();
    (
        binding_phase,
        marker_state.unwrap_or_default(),
        schema == 2,
        request
            .manifest_dir
            .join("reconciliation-complete-v2.json")
            .exists(),
    )
}

#[tokio::test]
async fn protocol2_disposable_fixture_covers_resume_states_and_final_cas() {
    for (phase, expected) in [
        (
            "reconciliation-after-intent",
            ("identity_ready", "", false, false),
        ),
        (
            "reconciliation-after-first-commit",
            ("transition_pending", "committed", true, false),
        ),
        (
            "reconciliation-after-completion",
            ("transition_pending", "committed", true, true),
        ),
        (
            "reconciliation-after-final-commit",
            ("baseline_ready", "finalized", true, true),
        ),
    ] {
        let fixture = Protocol2Fixture::new().await;
        let lock = super::RuntimeLock::acquire(&fixture.request.database).unwrap();
        drop(lock);
        unsafe {
            std::env::set_var("R094_GRANT_PATH", &fixture.grant_path);
        }
        let output = fixture.child(phase);
        eprintln!(
            "{phase}: child status={:?} stdout={} stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.status.code(), Some(86), "{phase}: {output:?}");
        let (expected_phase, expected_marker, expected_intent, expected_complete) = expected;
        assert_eq!(
            state(&fixture.request).await,
            (
                expected_phase.to_string(),
                expected_marker.to_string(),
                expected_intent,
                expected_complete
            ),
            "{phase}"
        );
        unsafe {
            std::env::remove_var(CHILD_PHASE);
        }
        let request = TransitionRequest {
            mode: Mode::Resume,
            ..fixture.request.clone()
        };
        let result = reconciliation::execute_fixture(request.clone()).await;
        assert!(result.is_ok(), "{phase}: {result:?}");
        let first_final = state(&request).await;
        assert_eq!(
            first_final,
            ("baseline_ready".into(), "finalized".into(), true, true),
            "{phase} final"
        );
        let final_graph = final_graph_state(&request).await;
        let result = reconciliation::execute_fixture(request.clone()).await;
        assert!(result.is_ok(), "{phase} repeated resume: {result:?}");
        assert_eq!(state(&request).await, first_final, "{phase} idempotence");
        assert_eq!(
            final_graph_state(&request).await,
            final_graph,
            "{phase} graph/digest parity"
        );
        unsafe {
            std::env::remove_var("R094_GRANT_PATH");
        }
    }
}

async fn final_graph_state(request: &TransitionRequest) -> (String, String) {
    use super::reconciliation_graph::{ExpectedPostGraph, PreTransitionGraph};
    let plan: Plan = rec::load(&request.transition_plan).unwrap();
    let intent: rec::Intent =
        rec::load(&request.manifest_dir.join("reconciliation-intent-v2.json")).unwrap();
    let expected = ExpectedPostGraph::from_pre_state(
        intent.pre_transition.as_ref().unwrap(),
        &plan,
        &records::encoded_digest(&intent).unwrap(),
    )
    .unwrap();
    let mut connection = verification::open(&request.database, true).await.unwrap();
    let digests: (String, String, String) = sqlx::query_as("SELECT committed_graph_digest,final_graph_digest,final_cas_digest FROM r093_reconciliation_marker").fetch_one(&mut connection).await.unwrap();
    assert_eq!(
        digests,
        (
            expected.digest().unwrap(),
            expected.finalized(&plan).unwrap().digest().unwrap(),
            expected.final_cas_digest(&plan).unwrap()
        )
    );
    let graph = PreTransitionGraph::capture(&mut connection).await.unwrap();
    let marker: String = sqlx::query_scalar("SELECT json_array(singleton,protocol,operation_id,root_digest,state,intent_digest,grant_digest,plan_digest,action_digest,archive_digest,schema_digest,identity_digest,committed_graph_digest,committed_binding_revision,commit_digest,completion_digest,final_graph_digest,final_cas_digest) FROM r093_reconciliation_marker").fetch_one(&mut connection).await.unwrap();
    connection.close().await.unwrap();
    (records::encoded_digest(&graph).unwrap(), marker)
}

#[tokio::test]
async fn child_entry() {
    let Some(root) = std::env::var_os(CHILD_ROOT) else {
        return;
    };
    let phase = std::env::var_os(CHILD_PHASE).unwrap();
    if phase == "bootstrap" {
        let mut request = Fixture::request(Path::new(&root), Mode::Apply);
        request.target_schema = 25;
        unsafe {
            std::env::set_var("R071_TEST_CRASH", "v2-after-root");
        }
        flow::execute(request, false).await.unwrap();
        return;
    }
    let manifest = std::env::var_os(CHILD_MANIFEST).unwrap();
    let grant = std::env::var_os(CHILD_GRANT).unwrap();
    unsafe {
        std::env::set_var("R094_GRANT_PATH", grant);
        std::env::set_var("R071_TEST_CRASH", &phase);
    }
    let mut request = Fixture::request(Path::new(&root), Mode::Apply);
    request.target_schema = 25;
    request.manifest_dir = manifest.into();
    let request = TransitionRequest {
        mode: if phase == "owner-resume" {
            Mode::Resume
        } else {
            Mode::Apply
        },
        config: request.config,
        database: request.database,
        operation_id: request.operation_id,
        manifest_dir: request.manifest_dir.clone(),
        transition_plan: request.manifest_dir.join("reconciliation-plan-v2.json"),
        release_provenance: request.release_provenance,
        executable: request.executable,
    };
    reconciliation::execute_fixture(request).await.unwrap();
}

#[tokio::test]
async fn expected_graph_rejects_disposable_sql_corruption() {
    use super::reconciliation_graph::ExpectedPostGraph;
    use super::reconciliation_records::Intent;

    let fixture = Protocol2Fixture::new().await;
    let output = fixture.child("reconciliation-after-first-commit");
    assert_eq!(output.status.code(), Some(86), "{output:?}");
    let plan: Plan = rec::load(&fixture.request.transition_plan).unwrap();
    let intent: Intent = rec::load(
        &fixture
            .request
            .manifest_dir
            .join("reconciliation-intent-v2.json"),
    )
    .unwrap();
    let before = intent.pre_transition.as_ref().unwrap();
    let expected = ExpectedPostGraph::from_pre_state(
        before,
        &plan,
        &records::encoded_digest(&intent).unwrap(),
    )
    .unwrap();
    reject_missing_actions(before, &plan, &intent);
    let mut connection = verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    super::reconciliation_graph::verify_expected_graph(&mut connection, &plan, Some(&expected))
        .await
        .unwrap();
    super::reconciliation_graph::verify_committed_marker(
        &mut connection,
        &plan,
        &intent,
        &expected,
    )
    .await
    .unwrap();
    verify_corruptions(&mut connection, &plan, &intent, &expected).await;
    reject_stale_snapshot(before, &plan, &intent);
    connection.close().await.unwrap();
}

const GRAPH_CORRUPTIONS: &[(&str, &str, bool)] = &[
    (
        "binding",
        "UPDATE offline_recovery_binding SET canonical_db_digest='wrong'",
        false,
    ),
    (
        "marker",
        "UPDATE r093_reconciliation_marker SET root_digest='wrong'",
        true,
    ),
    (
        "marker-cas",
        "UPDATE r093_reconciliation_marker SET commit_digest='wrong'",
        true,
    ),
    (
        "native",
        "UPDATE matrix_recovery_compatibility SET reason_code='wrong'",
        false,
    ),
    (
        "control",
        "UPDATE matrix_recovery_control SET restart_hold_state='wrong'",
        false,
    ),
    (
        "extra-row",
        "INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES('unrelated','unrelated','acp',1,NULL,'[]')",
        false,
    ),
    (
        "missing-action-cell",
        "UPDATE agents SET enabled=0 WHERE endpoint_id='fixture-agent'",
        false,
    ),
    (
        "missing-classification",
        "DELETE FROM offline_recovery_rows WHERE table_name='agents'",
        false,
    ),
    (
        "parent-cell",
        "UPDATE offline_recovery_rows SET parent_relations='[{}]' WHERE table_name='agents'",
        false,
    ),
    (
        "anchor",
        "UPDATE offline_recovery_metadata_anchors SET original_digest='wrong' WHERE table_name='agents'",
        false,
    ),
    (
        "receipt-deletion",
        "INSERT INTO offline_recovery_workspace_deletions(table_name,canonical_pk,classification_json,snapshot_json,context_json,authority_fence_json,receipt_digest,runtime_owner,task_id,owner_fence) VALUES('workspace_claims','[\"missing\"]','{}','{}','{}','{}','0000000000000000000000000000000000000000000000000000000000000000','owner','task',1)",
        false,
    ),
    (
        "extra-audit",
        "INSERT INTO r093_reconciliation_audit(operation_id,action_id,action_type,table_name,canonical_pk,original_revision,original_digest,pre_digest,post_digest,evidence_digest) SELECT operation_id,'extra','preserve','agents','[]','1','wrong','wrong','wrong','wrong' FROM r093_reconciliation_marker",
        false,
    ),
];

async fn verify_corruptions(
    connection: &mut sqlx::SqliteConnection,
    plan: &Plan,
    intent: &rec::Intent,
    expected: &super::reconciliation_graph::ExpectedPostGraph,
) {
    for (label, mutation, marker) in GRAPH_CORRUPTIONS.iter().copied() {
        let mut transaction = connection.begin().await.unwrap();
        sqlx::query("PRAGMA defer_foreign_keys=ON")
            .execute(&mut *transaction)
            .await
            .unwrap();
        let triggers: Vec<String> =
            sqlx::query_scalar("SELECT name FROM sqlite_schema WHERE type='trigger'")
                .fetch_all(&mut *transaction)
                .await
                .unwrap();
        for trigger in triggers {
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "DROP TRIGGER \"{}\"",
                trigger.replace('"', "\"\"")
            )))
            .execute(&mut *transaction)
            .await
            .unwrap();
        }
        sqlx::raw_sql(mutation)
            .execute(&mut *transaction)
            .await
            .unwrap_or_else(|failure| panic!("{label}: mutation failed: {failure}"));
        let result = if marker {
            super::reconciliation_graph::verify_committed_marker(
                &mut transaction,
                plan,
                intent,
                expected,
            )
            .await
        } else {
            super::reconciliation_graph::verify_expected_graph(
                &mut transaction,
                plan,
                Some(expected),
            )
            .await
        };
        assert!(
            matches!(result, Err(super::Failure::ManualRequired)),
            "{label}: {result:?}"
        );
        transaction.rollback().await.unwrap();
    }
}

fn reject_stale_snapshot(
    before: &super::reconciliation_graph::PreTransitionGraph,
    plan: &Plan,
    intent: &rec::Intent,
) {
    use super::reconciliation_graph::{ExpectedPostGraph, PreTransitionGraph};
    let mut stale = serde_json::to_value(before).unwrap();
    let rows = stale["rows"].as_array_mut().unwrap();
    let image = rows.iter_mut().find(|row| row[0] == "image").unwrap();
    let mut cells: rec::RowImage = serde_json::from_str(image[2].as_str().unwrap()).unwrap();
    cells.values[0].push_str("00");
    image[2] = serde_json::json!(serde_json::to_string(&cells).unwrap());
    let stale: PreTransitionGraph = serde_json::from_value(stale).unwrap();
    assert!(matches!(
        ExpectedPostGraph::from_pre_state(&stale, plan, &records::encoded_digest(intent).unwrap()),
        Err(super::Failure::ManualRequired)
    ));
}

fn reject_missing_actions(
    before: &super::reconciliation_graph::PreTransitionGraph,
    plan: &Plan,
    intent: &rec::Intent,
) {
    use super::reconciliation_graph::ExpectedPostGraph;
    for index in [0, plan.actions.len() - 1] {
        let mut missing_action = plan.clone();
        missing_action.actions.remove(index);
        missing_action.action_digest = records::encoded_digest(&missing_action.actions).unwrap();
        assert!(matches!(
            ExpectedPostGraph::from_pre_state(
                before,
                &missing_action,
                &records::encoded_digest(intent).unwrap()
            ),
            Err(super::Failure::ManualRequired)
        ));
    }
    let mut missing_native = plan.clone();
    missing_native.inserted_classifications.clear();
    assert!(matches!(
        ExpectedPostGraph::from_pre_state(
            before,
            &missing_native,
            &records::encoded_digest(intent).unwrap()
        ),
        Err(super::Failure::ManualRequired)
    ));
    let mut wrong_parent = plan.clone();
    wrong_parent.actions[0].parents.push(rec::Parent {
        identity: crate::storage::recovery_eligibility::RowIdentity {
            table: "agents".into(),
            key: "[\"text:666978747572652D6167656E74\"]".into(),
        },
        revision: "wrong".into(),
        digest: "0".repeat(64),
        role: "effectful".into(),
    });
    wrong_parent.action_digest = records::encoded_digest(&wrong_parent.actions).unwrap();
    assert!(matches!(
        ExpectedPostGraph::from_pre_state(
            before,
            &wrong_parent,
            &records::encoded_digest(intent).unwrap()
        ),
        Err(super::Failure::ManualRequired)
    ));
    let mut wrong_classification = plan.clone();
    wrong_classification.classification_digest = "0".repeat(64);
    assert!(matches!(
        ExpectedPostGraph::from_pre_state(
            before,
            &wrong_classification,
            &records::encoded_digest(intent).unwrap()
        ),
        Err(super::Failure::ManualRequired)
    ));
}
