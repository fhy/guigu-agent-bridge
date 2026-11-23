use super::environment::{PrivateManager, isolated_rooted_test};
use super::*;
#[path = "identity_cli_binding.rs"]
mod binding;
#[path = "identity_cli_faults.rs"]
mod faults;
#[path = "identity_cli_runtime.rs"]
mod online;
#[path = "identity_cli_sdk.rs"]
mod sdk;

pub(super) async fn execute() {
    execute_case("normal").await;
}

pub(super) async fn execute_case(scenario: &str) {
    if isolated_rooted_test(scenario).await {
        return;
    }
    let manager = PrivateManager::start().await;
    let (fixture, executable) = fixture().await;
    let (_store, server) = sdk::prepare(&fixture).await;
    let show = tokio::process::Command::new("/usr/bin/systemctl")
        .args([
            "--user",
            "show",
            "guigu-agent-bridge-observer.service",
            "--all",
            "--no-pager",
        ])
        .env("XDG_RUNTIME_DIR", &manager.runtime)
        .env("DBUS_SESSION_BUS_ADDRESS", &manager.bus)
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        String::from_utf8(show.stdout)
            .unwrap()
            .contains("LoadState=masked\n")
    );
    let migration = child(
        &fixture,
        &executable,
        &manager,
        "offline-migrate-production",
        "apply",
        &["--target-schema", "25"],
    )
    .await;
    assert!(
        migration.status.success(),
        "migration {:?}: {} {}",
        migration.status.code(),
        String::from_utf8_lossy(&migration.stdout),
        String::from_utf8_lossy(&migration.stderr)
    );
    capture(&fixture, &executable, &manager).await;
    binding::transition(&fixture, &executable, &manager).await;
    let scenario = std::env::var("R092_ROOTED_SCENARIO").unwrap_or_else(|_| scenario.into());
    if scenario == "normal" {
        online::activate_and_restart(&fixture, &executable, &manager, &server).await;
    } else {
        faults::execute(&scenario, &fixture, &executable, &manager, &server).await;
    }
    let manager_pid = manager.process.id();
    let runtime_path = manager.runtime.clone();
    drop(manager);
    assert!(!runtime_path.exists());
    assert_eq!(unsafe { libc::kill(manager_pid as i32, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    drop(server);
}

async fn fixture() -> (Fixture, std::path::PathBuf) {
    let mut fixture = Fixture::new().await;
    fixture.request.target_schema = 25;
    let mut release: crate::offline_production_migration::evidence::Provenance =
        records::load(&fixture.request.release_provenance).unwrap();
    release.migration_set_sha256 =
        crate::offline_production_migration::evidence::migration_digest_through(25);
    std::fs::write(
        &fixture.request.release_provenance,
        serde_json::to_vec(&release).unwrap(),
    )
    .unwrap();
    let executable = fixture.root.path().join("guigu-agent-bridge");
    std::fs::copy(crate::test_binary_path("guigu-agent-bridge"), &executable).unwrap();
    assert!(
        std::process::Command::new("/usr/bin/strip")
            .arg("--strip-debug")
            .arg(&executable)
            .status()
            .unwrap()
            .success()
    );
    release = records::load(&fixture.request.release_provenance).unwrap();
    release.executable = executable.clone();
    release.launcher_hold.executable = executable.clone();
    release.executable_sha256 =
        crate::offline_production_migration::evidence::hash_file(&executable).unwrap();
    let elf = std::fs::read(&executable).unwrap();
    release.elf_sha256 =
        crate::offline_production_migration::evidence::digest(&elf[..64.min(elf.len())]);
    std::fs::write(
        &fixture.request.release_provenance,
        serde_json::to_vec(&release).unwrap(),
    )
    .unwrap();
    (fixture, executable)
}

async fn child(
    fixture: &Fixture,
    executable: &std::path::Path,
    manager: &PrivateManager,
    command: &str,
    mode: &str,
    extra: &[&str],
) -> std::process::Output {
    tokio::time::timeout(
        std::time::Duration::from_secs(180),
        tokio::process::Command::new(executable)
            .args([command, "--mode", mode, "--config"])
            .arg(&fixture.request.config)
            .arg("--database")
            .arg(&fixture.request.database)
            .arg("--operation-id")
            .arg(&fixture.request.operation_id)
            .arg("--manifest-dir")
            .arg(&fixture.request.manifest_dir)
            .arg("--release-provenance")
            .arg(&fixture.request.release_provenance)
            .arg("--executable")
            .arg(executable)
            .args(extra)
            .env("R092_IDENTITY_TOKEN", "synthetic-secret")
            .env("XDG_RUNTIME_DIR", &manager.runtime)
            .env("DBUS_SESSION_BUS_ADDRESS", &manager.bus)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap()
}

async fn capture(fixture: &Fixture, executable: &std::path::Path, manager: &PrivateManager) {
    let output = child(
        fixture,
        executable,
        manager,
        "offline-capture-matrix-identity",
        "capture",
        &[],
    )
    .await;
    let audit_stage = std::fs::read(
        fixture
            .request
            .manifest_dir
            .join("identity-failure-v1.json"),
    )
    .ok()
    .and_then(|bytes| serde_json::from_slice::<IdentityFailure>(&bytes).ok())
    .map(|audit| (audit.stage, audit.category));
    assert_eq!(
        output.status.code(),
        Some(0),
        "child stdout: {}\nchild stderr: {}\nfailure stage: {audit_stage:?}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        fixture
            .request
            .manifest_dir
            .join("identity-evidence-v1.json")
            .is_file()
    );
    let mut connection = verification::open(&fixture.request.database, false)
        .await
        .unwrap();
    let phase: String = sqlx::query_scalar("SELECT phase FROM offline_recovery_binding")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(phase, "identity_ready");
    connection.close().await.unwrap();
}
