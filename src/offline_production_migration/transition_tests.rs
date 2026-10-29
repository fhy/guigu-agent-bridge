use std::fs;

use super::{Mode, TransitionRequest, evidence, flow, records, test_fixture::Fixture, transition};
use sqlx::{Connection, Row};
use std::process::Command;

async fn prepared_transition() -> (Fixture, TransitionRequest) {
    let mut fixture = Fixture::new().await;
    fixture.request.target_schema = 25;
    let mut provenance: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.request.release_provenance).unwrap()).unwrap();
    provenance["migration_set_sha256"] = evidence::migration_digest_through(25).into();
    fs::write(
        &fixture.request.release_provenance,
        serde_json::to_vec(&provenance).unwrap(),
    )
    .unwrap();
    flow::execute(fixture.request.clone(), false).await.unwrap();
    let mut connection = super::verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    let row =
        sqlx::query("SELECT root_digest,schema_complete_digest FROM offline_recovery_binding")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    let root_digest: String = row.get(0);
    let schema_digest: String = row.get(1);
    sqlx::query("UPDATE offline_recovery_binding SET identity_digest='identity',phase='identity_ready',revision=revision+1")
        .execute(&mut connection).await.unwrap();
    connection.close().await.unwrap();
    let classification_digest =
        records::encoded_digest(&Vec::<transition::Classification>::new()).unwrap();
    let mut plan = transition::Plan {
        protocol: 1,
        plan_digest: String::new(),
        root_digest,
        schema_complete_digest: schema_digest,
        identity_digest: "identity".into(),
        classification_digest,
        native_operation_id: "native-op".into(),
        classifications: Vec::new(),
    };
    plan.plan_digest = transition::plan_digest(&plan).unwrap();
    let plan_path = fixture.request.manifest_dir.join("plan.json");
    records::publish(&plan_path, &plan).unwrap();
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
    (fixture, request)
}

fn crash_transition(fixture: &Fixture) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "offline_production_migration::transition_tests::transition_child_entry",
            "--nocapture",
        ])
        .env("R092_TRANSITION_ROOT", fixture.root.path())
        .env("R092_TRANSITION_MANIFEST", &fixture.request.manifest_dir)
        .env(
            "R071_TEST_CRASH",
            std::env::var("R092_TRANSITION_PHASE").unwrap(),
        )
        .output()
        .unwrap()
}

#[tokio::test]
async fn transition_crash_points_resume_same_operation() {
    for phase in [
        "transition-after-intent",
        "transition-after-sql",
        "transition-after-complete",
        "transition-after-cas",
    ] {
        let (fixture, request) = prepared_transition().await;
        unsafe { std::env::set_var("R092_TRANSITION_PHASE", phase) };
        let output = crash_transition(&fixture);
        unsafe { std::env::remove_var("R092_TRANSITION_PHASE") };
        assert_eq!(
            output.status.code(),
            Some(86),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        transition::run(request.clone()).await.unwrap();
        let mut connection = super::verification::open(&fixture.request.database, true)
            .await
            .unwrap();
        let phase_after: String = sqlx::query_scalar("SELECT phase FROM offline_recovery_binding")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(phase_after, "baseline_ready", "{phase}");
    }
}

#[tokio::test]
async fn transition_child_entry() {
    let Some(root) = std::env::var_os("R092_TRANSITION_ROOT") else {
        return;
    };
    let manifest = std::env::var_os("R092_TRANSITION_MANIFEST").unwrap();
    let root = std::path::PathBuf::from(root);
    let mut request = Fixture::request(&root, Mode::Apply);
    request.manifest_dir = manifest.into();
    request.target_schema = 25;
    let transition = TransitionRequest {
        mode: Mode::Apply,
        config: request.config,
        database: request.database,
        operation_id: request.operation_id,
        manifest_dir: request.manifest_dir.clone(),
        transition_plan: request.manifest_dir.join("plan.json"),
        release_provenance: request.release_provenance,
        executable: request.executable,
    };
    transition::run(transition).await.unwrap();
}

#[tokio::test]
async fn transition_publishes_intent_then_complete_and_is_idempotent() {
    let mut fixture = Fixture::new().await;
    fixture.request.target_schema = 25;
    let mut provenance: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.request.release_provenance).unwrap()).unwrap();
    provenance["migration_set_sha256"] = evidence::migration_digest_through(25).into();
    fs::write(
        &fixture.request.release_provenance,
        serde_json::to_vec(&provenance).unwrap(),
    )
    .unwrap();
    flow::execute(fixture.request.clone(), false).await.unwrap();
    let mut connection = super::verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    let row =
        sqlx::query("SELECT root_digest,schema_complete_digest FROM offline_recovery_binding")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    let root_digest: String = row.get(0);
    let schema_digest: String = row.get(1);
    sqlx::query("UPDATE offline_recovery_binding SET identity_digest='identity',phase='identity_ready',revision=revision+1")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();

    let classification_digest =
        records::encoded_digest(&Vec::<transition::Classification>::new()).unwrap();
    let mut plan = transition::Plan {
        protocol: 1,
        plan_digest: String::new(),
        root_digest,
        schema_complete_digest: schema_digest,
        identity_digest: "identity".into(),
        classification_digest,
        native_operation_id: "native-op".into(),
        classifications: Vec::new(),
    };
    plan.plan_digest = transition::plan_digest(&plan).unwrap();
    records::publish(&fixture.request.manifest_dir.join("plan.json"), &plan).unwrap();
    let request = TransitionRequest {
        mode: Mode::Apply,
        config: fixture.request.config.clone(),
        database: fixture.request.database.clone(),
        operation_id: fixture.request.operation_id.clone(),
        manifest_dir: fixture.request.manifest_dir.clone(),
        transition_plan: fixture.request.manifest_dir.join("plan.json"),
        release_provenance: fixture.request.release_provenance.clone(),
        executable: fixture.request.executable.clone(),
    };
    transition::run(request.clone()).await.unwrap();
    transition::run(TransitionRequest {
        mode: Mode::Resume,
        ..request.clone()
    })
    .await
    .unwrap_or_else(|error| panic!("resume failed: {error:?}"));
    let mut connection = super::verification::open(&fixture.request.database, true)
        .await
        .unwrap();
    let phase: String = sqlx::query_scalar("SELECT phase FROM offline_recovery_binding")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(phase, "baseline_ready");
    assert!(
        fixture
            .request
            .manifest_dir
            .join("transition-intent-v1.json")
            .exists()
    );
    assert!(
        fixture
            .request
            .manifest_dir
            .join("transition-complete-v1.json")
            .exists()
    );
    drop(connection);
    let plan_path = fixture.request.manifest_dir.join("plan.json");
    let mut drifted: transition::Plan =
        serde_json::from_slice(&fs::read(&plan_path).unwrap()).unwrap();
    drifted.native_operation_id = "different-native-op".into();
    drifted.plan_digest = transition::plan_digest(&drifted).unwrap();
    fs::write(&plan_path, serde_json::to_vec(&drifted).unwrap()).unwrap();
    let drift_error = transition::run(TransitionRequest {
        mode: Mode::Resume,
        transition_plan: plan_path,
        ..request
    })
    .await
    .unwrap_err();
    assert!(matches!(drift_error, super::Failure::ManualRequired));
}
