use super::{Failure, Mode, RuntimeLock, flow, snapshot, test_fixture::Fixture, verification};
use sqlx::Connection;
use std::{
    fs,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

#[tokio::test]
async fn startup_refuses_maintenance_before_migration_and_open() {
    let fixture = Fixture::new().await;
    let _guard = RuntimeLock::acquire(&fixture.request.database).unwrap();
    assert!(
        crate::app::AppRuntime::start(&fixture.request.config)
            .await
            .is_err()
    );
    assert_eq!(
        snapshot::inspect(&fixture.request.database)
            .await
            .unwrap()
            .version,
        18
    );
    assert!(matches!(
        flow::execute(fixture.mode(Mode::Preflight), false).await,
        Err(Failure::Busy)
    ));
}

#[tokio::test]
async fn startup_holds_maintenance_until_all_runtime_connections_close() {
    let fixture = Fixture::new().await;
    let runtime = crate::app::AppRuntime::start(&fixture.request.config)
        .await
        .unwrap();
    assert!(RuntimeLock::acquire(&fixture.request.database).is_err());
    runtime.shutdown().await.unwrap();
    assert!(RuntimeLock::acquire(&fixture.request.database).is_ok());
}

#[tokio::test]
async fn surviving_runtime_pool_clones_retain_exclusion() {
    let fixture = Fixture::new().await;
    let guard =
        std::sync::Arc::new(super::acquire_runtime_lock(&fixture.request.database).unwrap());
    let pool = crate::storage::connect_for_runtime(&fixture.request.database, guard.clone())
        .await
        .unwrap();
    drop(guard);
    let survivor = pool.clone();
    drop(pool);
    assert!(RuntimeLock::acquire(&fixture.request.database).is_err());
    survivor.close().await;
    drop(survivor);
    assert!(RuntimeLock::acquire(&fixture.request.database).is_ok());
}

#[tokio::test]
async fn lock_child_entry() {
    let Ok(root) = std::env::var("R071_LOCK_ROOT") else {
        return;
    };
    let root = std::path::Path::new(&root);
    let _guard = super::acquire_runtime_lock(&root.join("state.db")).unwrap();
    fs::write(root.join("locked"), b"held").unwrap();
    std::thread::sleep(Duration::from_secs(30));
}

#[tokio::test]
async fn two_process_exclusion_and_crash_release_do_not_unlink_inode() {
    use std::os::unix::fs::MetadataExt;
    let fixture = Fixture::new().await;
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "offline_production_migration::exclusion_tests::lock_child_entry",
        ])
        .env("R071_LOCK_ROOT", fixture.root.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !fixture.root.path().join("locked").exists() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    let path = fixture.root.path().join(".state.db.offline-migration.lock");
    let inode = fs::metadata(&path).unwrap().ino();
    assert!(RuntimeLock::acquire(&fixture.request.database).is_err());
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert!(RuntimeLock::acquire(&fixture.request.database).is_ok());
    assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
}

#[tokio::test]
async fn lock_aliases_and_cli_mutators_share_the_same_guard() {
    let fixture = Fixture::new().await;
    let alias = fixture.root.path().join("alias.db");
    std::os::unix::fs::symlink(&fixture.request.database, &alias).unwrap();
    let _guard = RuntimeLock::acquire(&fixture.request.database).unwrap();
    assert!(RuntimeLock::acquire(&alias).is_err());
    for command in [
        "historical-terminal-cleanup",
        "historical-unknown-abandon",
        "recover-runtime",
        "reconcile-delivery",
        "recover-preacceptance",
        "reconcile-orphaned-preacceptance",
        "reconcile-lease-owner",
    ] {
        let args = [
            "fixture".into(),
            command.into(),
            "--database".into(),
            alias.as_os_str().to_owned(),
        ];
        assert!(super::cli_exclusion::acquire(&args).is_err());
    }
}

#[tokio::test]
async fn nonempty_wal_snapshot_preserves_rows_and_does_not_checkpoint_source() {
    let fixture = Fixture::new().await;
    let mut connection = verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    sqlx::query("PRAGMA wal_autocheckpoint=0")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO runtime_instances VALUES ('retained','before','before','stopped','synthetic')",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    let wal = fixture.root.path().join("state.db-wal");
    assert!(fs::metadata(&wal).unwrap().len() > 0);
    flow::execute(fixture.request.clone(), false).await.unwrap();
    let retained: String = sqlx::query_scalar(
        "SELECT process_fingerprint FROM runtime_instances WHERE instance_token='retained'",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(retained, "synthetic");
    connection.close().await.unwrap();
}

#[tokio::test]
async fn read_only_missing_source_does_not_create_database_or_operation() {
    let fixture = Fixture::new().await;
    let mut request = fixture.mode(Mode::Preflight);
    request.database = fixture.root.path().join("missing.db");
    assert!(matches!(
        flow::execute(request.clone(), false).await,
        Err(Failure::Preflight)
    ));
    assert!(!request.database.exists());
    assert_eq!(fs::read_dir(&request.manifest_dir).unwrap().count(), 0);
}
