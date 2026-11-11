//! Operate only on a copied test executable; never chmod the shared build artifact.
use super::*;
use std::os::unix::fs::{MetadataExt, PermissionsExt};

pub(super) async fn check_if_private(runtime: &AppRuntime) {
    let Ok(path) = std::env::var("R112_PRIVATE_EXECUTABLE") else {
        return;
    };
    assert_eq!(
        std::env::current_exe().unwrap(),
        std::path::Path::new(&path)
    );
    let pool = runtime.test_storage_pool();
    let initial: (String, String) = sqlx::query_as(
        "SELECT instance_token,process_fingerprint FROM runtime_instances WHERE state='active'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    for _ in 0..3 {
        let before: String =
            sqlx::query_scalar("SELECT heartbeat_at FROM runtime_instances WHERE instance_token=?")
                .bind(&initial.0)
                .fetch_one(&pool)
                .await
                .unwrap();
        let m = std::fs::metadata(&path).unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(m.mode())).unwrap();
        let after = std::fs::metadata(&path).unwrap();
        assert_ne!(
            (m.ctime(), m.ctime_nsec()),
            (after.ctime(), after.ctime_nsec())
        );
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                assert_eq!(runtime.health_state().snapshot().await.readiness, crate::runtime::Readiness::Ready);
                let next: (String, String) = sqlx::query_as("SELECT heartbeat_at,process_fingerprint FROM runtime_instances WHERE instance_token=?")
                    .bind(&initial.0).fetch_one(&pool).await.unwrap();
                assert_eq!(next.1, initial.1);
                if next.0 != before { break; }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.unwrap();
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM runtime_owner_evidence WHERE instance_token=? AND state='unknown'")
            .bind(&initial.0).fetch_one(&pool).await.unwrap(), 0);
    }
    println!(
        "ctime_revalidations=3 heartbeat_advanced=true ready=true unknown=0 original_fingerprint=true"
    );
}

#[tokio::test]
#[ignore = "private copied executable, real AppRuntime and systemd child"]
async fn private_executable_ctime_and_same_store_lifecycle() {
    let root = tempfile::Builder::new()
        .prefix("r112-executable-")
        .tempdir_in(std::env::var_os("HOME").unwrap())
        .unwrap();
    let executable = root.path().join("private-test");
    std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = tokio::time::timeout(Duration::from_secs(240),
        tokio::process::Command::new(&executable)
            .env("R112_PRIVATE_EXECUTABLE", &executable)
            .args(["--exact", "native_succession::history_runtime::lifecycle::scoped_failure_new_work_abrupt_exit_and_same_store_restart", "--ignored", "--nocapture"])
            .kill_on_drop(true).output()).await.unwrap().unwrap();
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("ctime_revalidations=3"));
}
