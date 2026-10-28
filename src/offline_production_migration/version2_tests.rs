use std::{fs, process::Command};

use super::{
    Failure, Mode, Request, evidence, flow, records, test_fixture::Fixture,
    version2_archive as archive, version2_records as graph,
};

async fn v2_fixture() -> Fixture {
    let mut fixture = Fixture::new().await;
    fixture.request.target_schema = 25;
    provenance(&fixture.request);
    fixture
}

fn provenance(req: &Request) {
    let mut record: serde_json::Value =
        serde_json::from_slice(&fs::read(&req.release_provenance).unwrap()).unwrap();
    record["migration_set_sha256"] = evidence::migration_digest_through(25).into();
    fs::write(
        &req.release_provenance,
        serde_json::to_vec(&record).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn target25_apply_verify_and_resume_bind_exact_graph_without_activation() {
    let fixture = v2_fixture().await;
    let req = &fixture.request;
    flow::execute(fixture.mode(Mode::Preflight), false)
        .await
        .unwrap();
    assert!(!records::anchor(&req.database).exists());
    flow::execute(req.clone(), false).await.unwrap();
    let root = fs::read(records::anchor(&req.database)).unwrap();
    let bytes = fs::read(req.manifest_dir.join("archive-v2.db")).unwrap();
    for mode in [Mode::Verify, Mode::Resume, Mode::Preflight] {
        flow::execute(fixture.mode(mode), false).await.unwrap();
    }
    assert_eq!(root, fs::read(records::anchor(&req.database)).unwrap());
    assert_eq!(
        bytes,
        fs::read(req.manifest_dir.join("archive-v2.db")).unwrap()
    );
    let mut connection = super::verification::open(&req.database, true)
        .await
        .unwrap();
    let phase: String = sqlx::query_scalar("SELECT phase FROM offline_recovery_binding")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(phase, "schema_ready");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_control")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(super::acquire_runtime_lock(&req.database).is_err());
}

#[tokio::test]
async fn target25_resumes_every_committed_prefix_and_publication_gap() {
    for phase in [
        "v2-after-root",
        "v2-after-intent",
        "v2-after-19",
        "v2-after-20",
        "v2-after-21",
        "v2-after-22",
        "v2-after-23",
        "v2-after-24",
        "v2-after-25",
        "v2-after-binding",
        "v2-after-complete",
    ] {
        let fixture = v2_fixture().await;
        let output = crash(&fixture, phase);
        assert_eq!(
            output.status.code(),
            Some(86),
            "{phase}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(super::acquire_runtime_lock(&fixture.request.database).is_err());
        let root = fs::read(records::anchor(&fixture.request.database)).unwrap();
        flow::execute(fixture.mode(Mode::Resume), false)
            .await
            .unwrap();
        flow::execute(fixture.mode(Mode::Verify), false)
            .await
            .unwrap();
        assert_eq!(
            root,
            fs::read(records::anchor(&fixture.request.database)).unwrap()
        );
        assert_eq!(
            archive::inspect(&fixture.request.database)
                .await
                .unwrap()
                .version,
            25
        );
    }
}

#[tokio::test]
async fn orphan_archive_bytes_and_wrong_operation_refuse_without_adoption() {
    let fixture = v2_fixture().await;
    assert_eq!(
        crash(&fixture, "v2-after-archive-bytes").status.code(),
        Some(86)
    );
    assert!(
        flow::execute(fixture.mode(Mode::Resume), false)
            .await
            .is_err()
    );
    assert_eq!(
        archive::inspect(&fixture.request.database)
            .await
            .unwrap()
            .version,
        18
    );
    let mut other = fixture.mode(Mode::Resume);
    other.operation_id = "other".into();
    assert!(flow::execute(other, false).await.is_err());
}

#[tokio::test]
async fn complete_v1_successor_preserves_exact_predecessor_and_backup() {
    let fixture = Fixture::new().await;
    flow::execute(fixture.request.clone(), false).await.unwrap();
    let old_root = fs::read(records::anchor(&fixture.request.database)).unwrap();
    let old_complete = fs::read(fixture.request.manifest_dir.join("complete.json")).unwrap();
    let mut next = fixture.mode(Mode::Apply);
    next.target_schema = 25;
    next.manifest_dir = fixture.root.path().join("successor");
    fs::create_dir(&next.manifest_dir).unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&next.manifest_dir, fs::Permissions::from_mode(0o700)).unwrap();
    provenance(&next);
    flow::execute(next.clone(), false).await.unwrap();
    assert_eq!(
        old_root,
        fs::read(next.manifest_dir.join("predecessor-root-v1.json")).unwrap()
    );
    assert_eq!(
        old_complete,
        fs::read(fixture.request.manifest_dir.join("complete.json")).unwrap()
    );
    let root: graph::Root = graph::load(&records::anchor(&next.database)).unwrap();
    assert_eq!(root.initial_ledger, 20);
    next.mode = Mode::Verify;
    flow::execute(next, false).await.unwrap();
}

#[tokio::test]
async fn unexplained_schema20_and_changed_business_rows_refuse() {
    let fixture = v2_fixture().await;
    let mut connection = super::verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    super::verification::embedded_through(20)
        .run(&mut connection)
        .await
        .unwrap();
    assert!(matches!(
        flow::execute(fixture.request.clone(), false).await,
        Err(Failure::ManualRequired)
    ));
    let fixture = v2_fixture().await;
    assert_eq!(crash(&fixture, "v2-after-intent").status.code(), Some(86));
    let mut connection = super::verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES('forged','forged','a2a',1,NULL,'[]')")
        .execute(&mut connection).await.unwrap();
    assert!(
        flow::execute(fixture.mode(Mode::Resume), false)
            .await
            .is_err()
    );
}

fn crash(fixture: &Fixture, phase: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "offline_production_migration::version2_tests::child_entry",
            "--nocapture",
        ])
        .env("R092_V2_ROOT", fixture.root.path())
        .env("R092_V2_MANIFEST", &fixture.request.manifest_dir)
        .env("R071_TEST_CRASH", phase)
        .output()
        .unwrap()
}

#[tokio::test]
async fn child_entry() {
    let Some(root) = std::env::var_os("R092_V2_ROOT") else {
        return;
    };
    let mut req = Fixture::request(std::path::Path::new(&root), Mode::Apply);
    if let Some(directory) = std::env::var_os("R092_V2_MANIFEST") {
        req.manifest_dir = directory.into();
    }
    req.target_schema = 25;
    flow::execute(req, false).await.unwrap();
}

#[tokio::test]
async fn successor_root_replacement_crash_resumes_exact_same_successor() {
    let mut fixture = Fixture::new().await;
    flow::execute(fixture.request.clone(), false).await.unwrap();
    let before = fs::read(records::anchor(&fixture.request.database)).unwrap();
    fixture.request.target_schema = 25;
    fixture.request.manifest_dir = fixture.root.path().join("successor");
    use std::os::unix::fs::PermissionsExt;
    fs::create_dir(&fixture.request.manifest_dir).unwrap();
    fs::set_permissions(
        &fixture.request.manifest_dir,
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    provenance(&fixture.request);
    assert_eq!(
        crash(&fixture, "v2-before-root-replacement").status.code(),
        Some(86)
    );
    assert_eq!(
        before,
        fs::read(records::anchor(&fixture.request.database)).unwrap()
    );
    let candidate = fs::read(fixture.request.manifest_dir.join("root-v2.json")).unwrap();
    flow::execute(fixture.mode(Mode::Resume), false)
        .await
        .unwrap();
    assert_eq!(
        candidate,
        fs::read(records::anchor(&fixture.request.database)).unwrap()
    );
    flow::execute(fixture.mode(Mode::Verify), false)
        .await
        .unwrap();
}

#[test]
fn omitted_target_stays_v1_and_only_explicit25_selects_v2() {
    let arguments = [
        "offline-migrate-production",
        "--mode",
        "preflight",
        "--config",
        "/tmp/config",
        "--database",
        "/tmp/state.db",
        "--operation-id",
        "fixture",
        "--manifest-dir",
        "/tmp/audit",
        "--release-provenance",
        "/tmp/release",
        "--executable",
        "/tmp/main",
    ];
    let mut args = arguments.map(std::ffi::OsString::from).to_vec();
    assert_eq!(super::parse_args(&args).unwrap().target_schema, 20);
    args.extend(["--target-schema".into(), "25".into()]);
    assert_eq!(super::parse_args(&args).unwrap().target_schema, 25);
    for invalid in ["20", "24", "26", "025"] {
        *args.last_mut().unwrap() = invalid.into();
        assert!(super::parse_args(&args).is_err());
    }
}

#[tokio::test]
async fn v2_archive_digest_detects_embedded_nul_tail_without_changing_v1_encoding() {
    let fixture = v2_fixture().await;
    let mut connection = super::verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES('endpoint','agent','a2a',1,NULL,'[]')")
        .execute(&mut connection).await.unwrap();
    let before = archive::inspect(&fixture.request.database).await.unwrap();
    let legacy_before = super::snapshot::inspect(&fixture.request.database)
        .await
        .unwrap();
    sqlx::query("UPDATE agents SET agent_id=?")
        .bind("agent\0hidden")
        .execute(&mut connection)
        .await
        .unwrap();
    let after = archive::inspect(&fixture.request.database).await.unwrap();
    let legacy_after = super::snapshot::inspect(&fixture.request.database)
        .await
        .unwrap();
    assert_ne!(before.tables["agents"], after.tables["agents"]);
    assert_eq!(
        legacy_before.tables["agents"],
        legacy_after.tables["agents"]
    );
}

#[test]
fn discovery_of_a_missing_pristine_path_is_noncreating() {
    let root = tempfile::Builder::new()
        .prefix("r092-pristine-discovery-")
        .tempdir_in("/tmp")
        .unwrap();
    let database = root.path().join("missing").join("state.db");
    super::check_runtime_discovery(&database).unwrap();
    assert!(!database.parent().unwrap().exists());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
