use std::future;
use std::io;
#[cfg(unix)]
use std::net::TcpListener;
#[cfg(unix)]
use std::process::{Command, Stdio};
#[cfg(unix)]
use std::time::Duration;

use guigu_agent_bridge::Error;

struct ConfigFile(std::path::PathBuf);

impl ConfigFile {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("guigu-shutdown-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("bridge.toml");
        std::fs::write(
            &path,
            format!(
                "[bridge]\ndatabase = {:?}\nsession_root = {:?}\n",
                root.join("state.db").to_string_lossy(),
                root.join("sessions").to_string_lossy()
            ),
        )
        .unwrap();
        Self(path)
    }
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_gracefully_stops_the_owner_and_allows_the_next_generation() {
    let root = std::env::temp_dir().join(format!("guigu-sigterm-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&root).unwrap();
    let database = root.join("state.db");
    let sessions = root.join("sessions");
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let config = root.join("bridge.toml");
    std::fs::write(
        &config,
        format!(
            "[bridge]\ndatabase = {:?}\nsession_root = {:?}\nhealth_bind = \"127.0.0.1:{port}\"\n",
            database.to_string_lossy(),
            sessions.to_string_lossy()
        ),
    )
    .unwrap();

    for generation in 0..2 {
        let mut child = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let ready = format!("http://127.0.0.1:{port}/ready");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if reqwest::get(&ready)
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    break;
                }
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "bridge exited before readiness"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("bridge becomes ready");

        assert!(
            Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = tokio::task::spawn_blocking(move || child.wait().unwrap())
            .await
            .unwrap();
        assert!(
            status.success(),
            "generation {generation} exited with {status}"
        );
        assert!(
            TcpListener::bind(("127.0.0.1", port)).is_ok(),
            "health port remains bound"
        );
    }

    let pool = guigu_agent_bridge::storage::connect(&database)
        .await
        .unwrap();
    let states: Vec<String> =
        sqlx::query_scalar("SELECT state FROM runtime_instances ORDER BY started_at")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(states, vec!["stopped", "stopped"]);
    pool.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

impl Drop for ConfigFile {
    fn drop(&mut self) {
        if let Some(root) = self.0.parent() {
            let _ = std::fs::remove_dir_all(root);
        }
    }
}

#[tokio::test]
async fn entry_returns_ok_on_immediate_shutdown() {
    let config = ConfigFile::new();
    let result = guigu_agent_bridge::run_with_shutdown(&config.0, future::ready(Ok(()))).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn entry_propagates_shutdown_error() {
    let config = ConfigFile::new();
    let inner = io::Error::other("signal failure");
    let result = guigu_agent_bridge::run_with_shutdown(
        &config.0,
        future::ready(Err::<(), io::Error>(inner)),
    )
    .await;

    match result {
        Err(Error::Application(guigu_agent_bridge::app::AppError::Shutdown(source))) => {
            assert_eq!(source.to_string(), "signal failure")
        }
        other => panic!("expected shutdown application error, got {other:?}"),
    }
}
