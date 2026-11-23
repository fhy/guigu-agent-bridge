use super::{
    Failure, Mode,
    evidence::Evidence,
    flow,
    records::{self, Backup},
    snapshot,
    test_fixture::Fixture,
    verification,
};
use sqlx::Connection;
use std::{fs, process::Command};

#[tokio::test]
async fn fresh_apply_verify_and_repeat_preserve_original_backup() {
    let fixture = Fixture::new().await;
    flow::execute(fixture.request.clone(), false).await.unwrap();
    let backup = fs::read(fixture.request.manifest_dir.join("snapshot.db")).unwrap();
    flow::execute(fixture.mode(Mode::Verify), false)
        .await
        .unwrap();
    flow::execute(fixture.mode(Mode::Resume), false)
        .await
        .unwrap();
    assert_eq!(
        backup,
        fs::read(fixture.request.manifest_dir.join("snapshot.db")).unwrap()
    );
    assert!(matches!(
        flow::execute(fixture.request.clone(), false).await,
        Err(Failure::ManualRequired)
    ));
    assert!(super::acquire_runtime_lock(&fixture.request.database).is_ok());
}

#[tokio::test]
async fn readonly_preflight_leaves_database_and_operation_untouched() {
    let fixture = Fixture::new().await;
    let before = snapshot::inspect(&fixture.request.database).await.unwrap();
    flow::execute(fixture.mode(Mode::Preflight), false)
        .await
        .unwrap();
    assert!(!records::anchor(&fixture.request.database).exists());
    assert_eq!(
        fs::read_dir(&fixture.request.manifest_dir).unwrap().count(),
        0
    );
    assert_eq!(
        before,
        snapshot::inspect(&fixture.request.database).await.unwrap()
    );
}

#[tokio::test]
async fn backup_ready_without_intent_resumes_same_operation_only() {
    let fixture = Fixture::new().await;
    let evidence = Evidence::read(&fixture.request).unwrap();
    let operation = records::operation(&fixture.request, &evidence);
    records::publish(&records::anchor(&fixture.request.database), &operation).unwrap();
    snapshot::create(&fixture.request, operation).await.unwrap();
    assert!(super::acquire_runtime_lock(&fixture.request.database).is_err());
    let alias = fixture.root.path().join("alias.db");
    std::os::unix::fs::symlink(&fixture.request.database, &alias).unwrap();
    assert!(super::acquire_runtime_lock(&alias).is_err());
    let mut other = fixture.mode(Mode::Resume);
    other.operation_id = "different".into();
    assert!(matches!(
        flow::execute(other, false).await,
        Err(Failure::ManualRequired)
    ));
    flow::execute(fixture.mode(Mode::Resume), false)
        .await
        .unwrap();
}

#[tokio::test]
async fn incomplete_operation_never_adopts_or_overwrites_orphan_backup() {
    let fixture = Fixture::new().await;
    let evidence = Evidence::read(&fixture.request).unwrap();
    records::publish(
        &records::anchor(&fixture.request.database),
        &records::operation(&fixture.request, &evidence),
    )
    .unwrap();
    fs::write(fixture.request.manifest_dir.join("snapshot.db"), b"orphan").unwrap();
    assert!(matches!(
        flow::execute(fixture.mode(Mode::Resume), false).await,
        Err(Failure::ManualRequired)
    ));
    assert_eq!(
        fs::read(fixture.request.manifest_dir.join("snapshot.db")).unwrap(),
        b"orphan"
    );
}

#[tokio::test]
async fn unproven_intermediate_and_complete_ledgers_are_refused() {
    for version in [19, 20] {
        let fixture = Fixture::new().await;
        let mut connection = verification::open(&fixture.request.database, false)
            .await
            .unwrap();
        verification::embedded_through(version)
            .run(&mut connection)
            .await
            .unwrap();
        connection.close().await.unwrap();
        assert!(matches!(
            flow::execute(fixture.mode(Mode::Resume), false).await,
            Err(Failure::ManualRequired)
        ));
        assert!(matches!(
            flow::execute(fixture.mode(Mode::Preflight), false).await,
            Err(Failure::ManualRequired)
        ));
    }
}

#[tokio::test]
async fn corrupt_backup_and_identity_drift_are_durable_refusals() {
    let fixture = Fixture::new().await;
    flow::execute(fixture.request.clone(), false).await.unwrap();
    let path = fixture.request.manifest_dir.join("backup_published.json");
    let mut backup: Backup = records::load(&path).unwrap();
    backup.sha256 = "0".repeat(64);
    fs::write(&path, serde_json::to_vec(&backup).unwrap()).unwrap();
    assert!(matches!(
        flow::execute(fixture.mode(Mode::Verify), false).await,
        Err(Failure::ManualRequired)
    ));
}

#[tokio::test]
async fn child_entry() {
    let Ok(root) = std::env::var("R071_TEST_ROOT") else {
        return;
    };
    let request = Fixture::request(std::path::Path::new(&root), Mode::Apply);
    match flow::execute(request, false).await {
        Err(Failure::Busy) => std::process::exit(4),
        result => {
            result.unwrap();
        }
    }
}

#[tokio::test]
async fn committed_19_and_20_resume_without_postcommit_record_or_second_backup() {
    for phase in ["after-19", "after-20"] {
        let fixture = Fixture::new().await;
        let result = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "offline_production_migration::tests::child_entry",
                "--nocapture",
            ])
            .env("R071_TEST_ROOT", fixture.root.path())
            .env("R071_TEST_CRASH", phase)
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(!fixture.request.manifest_dir.join("complete.json").exists());
        let backup = fs::read(fixture.request.manifest_dir.join("snapshot.db")).unwrap();
        assert_eq!(
            snapshot::inspect(&fixture.request.database)
                .await
                .unwrap()
                .version,
            if phase == "after-19" { 19 } else { 20 }
        );
        assert!(super::acquire_runtime_lock(&fixture.request.database).is_err());
        flow::execute(fixture.mode(Mode::Resume), false)
            .await
            .unwrap();
        assert_eq!(
            backup,
            fs::read(fixture.request.manifest_dir.join("snapshot.db")).unwrap()
        );
    }
}

#[test]
fn launcher_requires_effective_runtime_mask_and_absence_of_active_sources() {
    let valid = "LoadState=masked\nUnitFileState=masked-runtime\nActiveState=inactive\nSubState=dead\nMainPID=0\nControlPID=0\nFragmentPath=/dev/null\nJob=\nTriggeredBy=\n";
    super::launcher::validate_show(valid).unwrap();
    for invalid in [
        valid.replace("masked-runtime", "enabled"),
        valid.replace("MainPID=0", "MainPID=1"),
        valid.replace("TriggeredBy=", "TriggeredBy=timer"),
        valid.replace("FragmentPath=/dev/null", "FragmentPath=/tmp/unit"),
    ] {
        assert!(matches!(
            super::launcher::validate_show(&invalid),
            Err(Failure::Busy)
        ));
    }
}
