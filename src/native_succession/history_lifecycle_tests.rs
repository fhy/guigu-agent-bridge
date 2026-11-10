//! One matched synthetic store through failure, success, abrupt exit and restart.
use super::*;
use crate::runtime::lifecycle_tests::task_cycle;
use tracing::instrument::WithSubscriber;

pub(super) async fn start(root: &std::path::Path, server: &str) -> AppRuntime {
    let captured = crate::test_diagnostics::Capture::default();
    let runtime = AppRuntime::start_with_config_and_a2a_faults(
        root.join("bridge.toml"),
        Fixture::config_at(root, true, server),
        Arc::new(crate::a2a::NoFault),
    )
    .with_subscriber(captured.subscriber())
    .await;
    for line in captured.output().lines().filter(|line| {
        line.contains("native-succession")
            || line.contains("bootstrap_nonadmin")
            || line.contains("rebuild-")
    }) {
        println!("{line}");
    }
    let runtime = runtime.unwrap();
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let state = runtime.health_state().snapshot().await.readiness;
            if state == crate::runtime::Readiness::Ready {
                break;
            }
            assert_ne!(
                state,
                crate::runtime::Readiness::RecoveryBlocked,
                "{:?}",
                runtime.health_state().recovery_diagnostic_line()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    runtime
}

#[tokio::test]
async fn idle_abrupt_child() {
    let Ok(root) = std::env::var("R115_IDLE_ROOT") else {
        return;
    };
    let server = std::env::var("R115_IDLE_SERVER").unwrap();
    let _runtime = start(std::path::Path::new(&root), &server).await;
    // Exit without shutdown or Drop: no controlled-stop/join evidence is written.
    println!("idle-child-ready abrupt-exit=86");
    std::process::exit(86);
}

async fn abrupt(root: &std::path::Path, server: &str) {
    let unit = format!("r115-idle-{}", uuid::Uuid::now_v7());
    let output = tokio::time::timeout(
        Duration::from_secs(75),
        tokio::process::Command::new("systemd-run")
            .args(["--user", "--wait", "--pipe", "--collect", "--unit", &unit])
            .arg("--property=RuntimeMaxSec=65")
            .arg(format!("--setenv=R115_IDLE_ROOT={}", root.display()))
            .arg(format!("--setenv=R115_IDLE_SERVER={server}"))
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_succession::history_runtime::lifecycle::idle_abrupt_child",
                "--nocapture",
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        output.status.code(),
        Some(86),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("idle-child-ready"));
}

#[tokio::test]
#[ignore = "explicit same-store AppRuntime lifecycle, private systemd child required"]
async fn scoped_failure_new_work_abrupt_exit_and_same_store_restart() {
    let f = Fixture::new();
    let server = MockServer::start().await;
    mount_server(&server).await;
    let pool = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let crate::storage::CompatibilityGate::Native(operation) =
        crate::storage::MatrixCompatibilityStore::new(pool.clone())
            .gate(true, None)
            .await
            .unwrap()
    else {
        panic!("native")
    };
    let scope = history_fixture::scope(&pool).await;
    crate::storage::MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope))
        .activate_cutover(&operation, "B", &chrono::Utc::now().to_rfc3339())
        .await
        .unwrap();
    pool.close().await;
    seed_device(&f, &server).await;
    let endpoint = crate::models::EndpointId::generate();
    let workspace = f.root.path().join("workspace");
    let runtime = start(f.root.path(), &server.uri()).await;
    let pool = runtime.test_storage_pool();
    let failed = task_cycle(&pool, &workspace, endpoint, true).await;
    let success = task_cycle(&pool, &workspace, endpoint, false).await;
    assert_ne!(failed, success);
    drop(pool);
    runtime.shutdown().await.unwrap();
    abrupt(f.root.path(), &server.uri()).await;
    let mut ids = vec![failed, success];
    for _ in 0..2 {
        let runtime = start(f.root.path(), &server.uri()).await;
        let pool = runtime.test_storage_pool();
        ids.push(task_cycle(&pool, &workspace, endpoint, false).await);
        for task in &ids {
            assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_events WHERE task_id=? AND status IN ('failed','completed')")
                .bind(task.to_string()).fetch_one(&pool).await.unwrap(), 1);
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
                .fetch_one(&pool)
                .await
                .unwrap(),
            ids.len() as i64
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workspace_claims")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        let recovered: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence WHERE is_current=1 AND barrier_state='open' AND json_extract(child_snapshot,'$.kind')='safe-succession-v1' AND json_extract(child_snapshot,'$.historical_joined')=0")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(recovered, 1);
        drop(pool);
        runtime.shutdown().await.unwrap();
    }
    println!(
        "same_pair=true failed=1 completed=3 terminal_events=4 abrupt_exits=1 normal_shutdowns=3 replay=0"
    );
}
