use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

use super::{
    Mode, TransitionRequest, evidence, flow, reconciliation,
    reconciliation_records::{self as rec, Anchor, Binding, Classification, Grant, Plan},
    records,
    test_fixture::Fixture,
    verification, version2_records,
};
use sqlx::{Connection, Row};

const CHILD_ROOT: &str = "R094_RECON_ROOT";
const CHILD_MANIFEST: &str = "R094_RECON_MANIFEST";
const CHILD_GRANT: &str = "R094_RECON_GRANT";
const CHILD_PHASE: &str = "R094_RECON_PHASE";

struct Protocol2Fixture {
    fixture: Fixture,
    request: TransitionRequest,
    grant_path: PathBuf,
}

impl Protocol2Fixture {
    async fn new() -> Self {
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
        flow::execute(fixture.request.clone(), false).await.unwrap();

        let identity_digest = "1".repeat(64);
        let mut connection = verification::open(&fixture.request.database, false)
            .await
            .unwrap();
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
        let root: version2_records::Root =
            rec::load(&records::anchor(&fixture.request.database)).unwrap();
        let root_digest = records::encoded_digest(&root).unwrap();
        let archive: version2_records::Archive =
            rec::load(&fixture.request.manifest_dir.join("archive-v2.json")).unwrap();
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
        assert_eq!(identities.len(), 3);
        let mut classifications = Vec::new();
        let mut anchors = Vec::new();
        for identity in identities {
            let snapshot = crate::storage::recovery_eligibility::inventory::snapshot(
                &mut connection,
                &identity,
            )
            .await
            .unwrap();
            classifications.push(Classification {
                table_name: identity.table.clone(),
                canonical_pk: identity.key.clone(),
                origin_generation: "native-operation".into(),
                original_revision: snapshot.revision.clone(),
                original_digest: snapshot.digest.clone(),
                parent_relations: serde_json::to_value(snapshot.parents).unwrap(),
                disposition: "terminal_evidenced".into(),
                evidence_digest: "2".repeat(64),
                context_kind: "admin".into(),
                context_generation: None,
                admin_operation_id: Some(fixture.request.operation_id.clone()),
                plan_digest: None,
                matrix_operation_id: Some("native-operation".into()),
            });
            anchors.push(Anchor {
                table_name: identity.table,
                canonical_pk: identity.key,
                original_revision: snapshot.revision,
                original_digest: snapshot.digest,
            });
        }
        connection.close().await.unwrap();

        let executable_digest = evidence::hash_file(&fixture.request.executable).unwrap();
        let grant = Grant {
            version: 1,
            operation_id: fixture.request.operation_id.clone(),
            root_digest: root_digest.clone(),
            original_provenance_digest: root.operation.provenance_digest.clone(),
            original_executable_digest: executable_digest.clone(),
            archive_digest: archive_digest.clone(),
            schema_complete_digest: binding.schema_complete_digest.clone().unwrap(),
            identity_digest: identity_digest.clone(),
            successor_executable_digest: executable_digest.clone(),
            successor_package_digest: "3".repeat(64),
            successor_source: "fixture-source".into(),
            successor_review: "fixture-review".into(),
            protocol: 2,
            entry: "offline-transition-matrix-compatibility".into(),
            allowed_modes: vec!["apply".into(), "resume".into(), "verify".into()],
        };
        let grant_digest = records::encoded_digest(&grant).unwrap();
        let classification_digest = records::encoded_digest(
            &classifications
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
            original_executable_digest: executable_digest.clone(),
            archive_digest,
            schema_complete_digest: binding.schema_complete_digest.clone().unwrap(),
            identity_digest,
            grant_digest,
            successor_executable_digest: executable_digest,
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
            inserted_classifications: Vec::new(),
            actions: Vec::new(),
            d1_unknown_chain: None,
            d1_quarantined: None,
            d2_owner: Vec::new(),
            anchors,
            workspaces: Vec::new(),
            native_at: "2026-11-03T00:00:00Z".into(),
            abandonment_at: "2026-11-03 00:00:00".into(),
            action_digest: "4".repeat(64),
            classification_digest,
        };
        plan.plan_digest = rec::plan_digest(&plan).unwrap();
        for classification in &mut plan.classifications {
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
        let result = reconciliation::execute_fixture(request.clone()).await;
        assert!(result.is_ok(), "{phase} repeated resume: {result:?}");
        assert_eq!(state(&request).await, first_final, "{phase} idempotence");
        unsafe {
            std::env::remove_var("R094_GRANT_PATH");
        }
    }
}

#[tokio::test]
async fn child_entry() {
    let Some(root) = std::env::var_os(CHILD_ROOT) else {
        return;
    };
    let manifest = std::env::var_os(CHILD_MANIFEST).unwrap();
    let grant = std::env::var_os(CHILD_GRANT).unwrap();
    let phase = std::env::var_os(CHILD_PHASE).unwrap();
    unsafe {
        std::env::set_var("R094_GRANT_PATH", grant);
        std::env::set_var("R071_TEST_CRASH", phase);
    }
    let mut request = Fixture::request(Path::new(&root), Mode::Apply);
    request.target_schema = 25;
    request.manifest_dir = manifest.into();
    let request = TransitionRequest {
        mode: Mode::Apply,
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
