use guigu_agent_bridge::{app::AppRuntime, storage::connect};

fn config(root: &std::path::Path) -> std::path::PathBuf {
    let path = root.join("bridge.toml");
    std::fs::write(
        &path,
        format!(
            "[bridge]\ndatabase={:?}\nsession_root={:?}\nhealth_bind=\"127.0.0.1:0\"\n",
            root.join("state.db"),
            root.join("sessions"),
        ),
    )
    .unwrap();
    path
}

#[tokio::test]
async fn clean_shutdown_and_restart_stop_runtime_without_reaped_evidence() {
    let root = tempfile::tempdir().unwrap();
    let path = config(root.path());
    for generation in 1..=2 {
        let runtime = AppRuntime::start(&path).await.unwrap();
        let address = runtime.health_addr().unwrap();
        runtime.shutdown().await.unwrap();
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(listener);
        let pool = connect(root.path().join("state.db")).await.unwrap();
        let states: Vec<String> = sqlx::query_scalar("SELECT state FROM runtime_instances")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(states, vec!["stopped"; generation]);
        let evidence: Vec<String> =
            sqlx::query_scalar("SELECT state FROM runtime_owner_evidence WHERE is_current=1")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(evidence.len(), generation);
        assert!(
            evidence
                .iter()
                .all(|state| matches!(state.as_str(), "scan_pending" | "unknown"))
        );
        for query in [
            "SELECT count(*) FROM runtime_owner_evidence WHERE state='reaped'",
            "SELECT count(*) FROM historical_unknown_abandonments",
        ] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(query)
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                0
            );
        }
        pool.close().await;
    }
}

#[tokio::test]
async fn clean_resource_drain_does_not_hide_runtime_stop_database_failure() {
    let root = tempfile::tempdir().unwrap();
    let runtime = AppRuntime::start(config(root.path())).await.unwrap();
    let address = runtime.health_addr().unwrap();
    let pool = connect(root.path().join("state.db")).await.unwrap();
    sqlx::query("CREATE TRIGGER fail_stop BEFORE UPDATE ON runtime_instances WHEN NEW.state='stopped' BEGIN SELECT RAISE(ABORT,'injected stop failure'); END")
        .execute(&pool).await.unwrap();
    assert!(runtime.shutdown().await.is_err());
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    drop(listener);
    let state: String = sqlx::query_scalar("SELECT state FROM runtime_instances")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "stopping");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM runtime_owner_evidence WHERE state='reaped'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    pool.close().await;
}
