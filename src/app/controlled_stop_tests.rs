use super::*;

async fn configuration() -> (tempfile::TempDir, PathBuf, std::net::SocketAddr) {
    let root = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let path = root.path().join("config.toml");
    std::fs::write(&path,format!("[bridge]\ndatabase={:?}\nsession_root={:?}\nhealth_bind=\"{}\"\n[transports.matrix]\nenabled=false\n[agents.worker]\ntransport=\"acp\"\ncommand=\"must-not-start\"\nworkspace={:?}\nenabled=true\n",root.path().join("state.db"),root.path().join("sessions"),address,root.path())).unwrap();
    (root, path, address)
}

#[tokio::test]
async fn controlled_runtime_stop_then_restart_on_same_database() {
    let (_root, path, _address) = configuration().await;
    for _ in 0..2 {
        let runtime = AppRuntime::start(&path).await.unwrap();
        assert!(runtime.health_addr().is_some());
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn controlled_runtime_startup_abort_keeps_first_error_then_can_restart() {
    let (_root, path, address) = configuration().await;
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let error = match AppRuntime::start(&path).await {
        Ok(runtime) => {
            let _ = runtime.shutdown().await;
            panic!("occupied listener must refuse")
        }
        Err(error) => error,
    };
    assert!(matches!(error, AppError::Health(_)), "{error}");
    drop(listener);
    let runtime = AppRuntime::start(&path).await.unwrap();
    runtime.shutdown().await.unwrap();
}
