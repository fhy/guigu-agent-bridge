use super::*;

pub(super) async fn transition(
    fixture: &Fixture,
    executable: &std::path::Path,
    manager: &PrivateManager,
) {
    let mut connection = verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    let binding: (String, String, String) = sqlx::query_as(
        "SELECT root_digest,schema_complete_digest,identity_digest FROM offline_recovery_binding",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    let classifications = Vec::new();
    let mut plan = crate::offline_production_migration::transition::Plan {
        protocol: 1,
        plan_digest: String::new(),
        root_digest: binding.0,
        schema_complete_digest: binding.1,
        identity_digest: binding.2,
        classification_digest: records::encoded_digest(&classifications).unwrap(),
        native_operation_id: "r092-rooted-native".into(),
        classifications,
    };
    plan.plan_digest = crate::offline_production_migration::transition::plan_digest(&plan).unwrap();
    let plan_path = fixture.request.manifest_dir.join("plan.json");
    records::publish(&plan_path, &plan).unwrap();
    connection.close().await.unwrap();
    let transition = super::child(
        fixture,
        executable,
        manager,
        "offline-transition-matrix-compatibility",
        "apply",
        &["--transition-plan", plan_path.to_str().unwrap()],
    )
    .await;
    assert!(
        transition.status.success(),
        "transition {:?}: {} {}",
        transition.status.code(),
        String::from_utf8_lossy(&transition.stdout),
        String::from_utf8_lossy(&transition.stderr)
    );
    let mut connection = verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    let native: (String, String, Option<String>, String) = sqlx::query_as("SELECT binding.phase,control.phase,control.watermark,compatibility.decision FROM offline_recovery_binding binding JOIN matrix_recovery_control control ON control.cutover_operation_id=binding.native_operation_id JOIN matrix_recovery_compatibility compatibility ON compatibility.native_operation_id=control.cutover_operation_id")
        .fetch_one(&mut connection).await.unwrap();
    assert_eq!(
        native,
        (
            "baseline_ready".into(),
            "baseline".into(),
            None,
            "native".into()
        )
    );
    connection.close().await.unwrap();
}

pub(super) async fn resume(
    fixture: &Fixture,
    executable: &std::path::Path,
    manager: &PrivateManager,
) {
    for mode in ["resume", "verify"] {
        let plan = fixture.request.manifest_dir.join("plan.json");
        let output = super::child(
            fixture,
            executable,
            manager,
            "offline-transition-matrix-compatibility",
            mode,
            &["--transition-plan", plan.to_str().unwrap()],
        )
        .await;
        assert!(
            output.status.success(),
            "active same-OP {mode} {:?}: {} {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
