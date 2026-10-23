use super::{
    Failure, Mode, evidence::Evidence, flow, records, snapshot, test_fixture::Fixture, verification,
};
use sqlx::Connection;
use std::fs;

#[tokio::test]
async fn checksum_and_schema_drift_leave_no_operation_or_backup() {
    for sql in [
        "UPDATE _sqlx_migrations SET checksum=x'00' WHERE version=18",
        "CREATE TABLE unexpected(value TEXT)",
    ] {
        let fixture = Fixture::new().await;
        let mut connection = verification::open(&fixture.request.database, false)
            .await
            .unwrap();
        sqlx::query(sql).execute(&mut connection).await.unwrap();
        connection.close().await.unwrap();
        assert!(matches!(
            flow::execute(fixture.request.clone(), false).await,
            Err(Failure::ManualRequired)
        ));
        assert!(!records::anchor(&fixture.request.database).exists());
        assert_eq!(
            fs::read_dir(&fixture.request.manifest_dir).unwrap().count(),
            0
        );
    }
}

#[tokio::test]
async fn canonical_alias_and_untrusted_executable_are_refused_without_mutation() {
    let fixture = Fixture::new().await;
    let alias = fixture.root.path().join("alias.db");
    std::os::unix::fs::symlink(&fixture.request.database, &alias).unwrap();
    let mut request = fixture.request.clone();
    request.database = alias;
    assert!(matches!(
        flow::execute(request, false).await,
        Err(Failure::Preflight)
    ));
    let mut request = fixture.request.clone();
    request.executable = fixture.request.config.clone();
    assert!(matches!(
        flow::execute(request, false).await,
        Err(Failure::ManualRequired)
    ));
    assert_eq!(
        snapshot::inspect(&fixture.request.database)
            .await
            .unwrap()
            .version,
        18
    );
}

#[tokio::test]
async fn registry_provenance_rejects_zero_source_and_drift_without_git_fallback() {
    for field in [
        "source_commit",
        "package_sha256",
        "migration_set_sha256",
        "executable_sha256",
    ] {
        let fixture = Fixture::new().await;
        let path = &fixture.request.release_provenance;
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        value[field] =
            serde_json::Value::String("0".repeat(if field == "source_commit" { 40 } else { 64 }));
        fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(matches!(
            Evidence::read(&fixture.request),
            Err(Failure::ManualRequired)
        ));
        assert!(!records::anchor(&fixture.request.database).exists());
    }
}

#[tokio::test]
async fn immutable_publication_refuses_overwrite_and_retains_pending_crash_residue() {
    let fixture = Fixture::new().await;
    let path = records::anchor(&fixture.request.database);
    fs::write(path.with_extension("pending"), b"interrupted").unwrap();
    assert!(matches!(
        flow::execute(fixture.request.clone(), false).await,
        Err(Failure::ManualRequired)
    ));
    assert!(!path.exists());
    assert!(super::acquire_runtime_lock(&fixture.request.database).is_err());
    let record = fixture.request.manifest_dir.join("immutable.json");
    records::publish(&record, &"original").unwrap();
    assert!(records::publish(&record, &"replacement").is_err());
    assert_eq!(records::load::<String>(&record).unwrap(), "original");
}

#[tokio::test]
async fn empty_and_absent_wal_are_valid_backup_inputs() {
    for keep_open in [false, true] {
        let fixture = Fixture::new().await;
        let mut connection = verification::open(&fixture.request.database, false)
            .await
            .unwrap();
        if !keep_open {
            connection.close().await.unwrap();
            assert!(!fixture.root.path().join("state.db-wal").exists());
            flow::execute(fixture.request.clone(), false).await.unwrap();
        } else {
            sqlx::query("SELECT count(*) FROM runtime_instances")
                .fetch_one(&mut connection)
                .await
                .unwrap();
            let wal = fixture.root.path().join("state.db-wal");
            assert_eq!(fs::metadata(wal).unwrap().len(), 0);
            flow::execute(fixture.request.clone(), false).await.unwrap();
            connection.close().await.unwrap();
        }
        flow::execute(fixture.mode(Mode::Verify), false)
            .await
            .unwrap();
    }
}

#[test]
fn strict_arguments_refuse_duplicates_unknown_flags_and_unsafe_operation_names() {
    let valid = [
        "offline-migrate-production",
        "--mode",
        "apply",
        "--config",
        "/config",
        "--database",
        "/db",
        "--operation-id",
        "operation",
        "--manifest-dir",
        "/manifest",
        "--release-provenance",
        "/release",
        "--executable",
        "/exe",
    ];
    let mut arguments = valid.map(std::ffi::OsString::from);
    super::parse_args(&arguments).unwrap();
    for replacement in ["--config", "--unknown"] {
        arguments[5] = replacement.into();
        assert!(matches!(
            super::parse_args(&arguments),
            Err(Failure::InvalidArguments)
        ));
    }
    arguments = valid.map(std::ffi::OsString::from);
    arguments[8] = "../bad".into();
    assert!(matches!(
        super::parse_args(&arguments),
        Err(Failure::InvalidArguments)
    ));
}
