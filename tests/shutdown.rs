use std::future;
use std::io;
#[cfg(unix)]
use std::net::TcpListener;
#[cfg(unix)]
use std::process::{Child, Command};
#[cfg(unix)]
use std::time::Duration;

use guigu_agent_bridge::Error;

#[cfg(unix)]
struct FixtureChild(Child);

#[cfg(unix)]
impl std::ops::Deref for FixtureChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

#[cfg(unix)]
impl std::ops::DerefMut for FixtureChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

#[cfg(unix)]
impl Drop for FixtureChild {
    fn drop(&mut self) {
        // Readiness/assertion failures must not leave a runtime behind. A Child
        // remembers a reaped exit, so normal SIGTERM/wait keeps its assertions.
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

#[cfg(unix)]
#[test]
fn fixture_panic_reaps_the_child() {
    let mut pid = 0;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let child = Command::new("sleep").arg("30").spawn().unwrap();
        pid = child.id();
        let _owned = FixtureChild(child);
        panic!("fixture readiness failure");
    }));
    assert!(result.is_err());
    let mut status = 0;
    assert_eq!(
        unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD)
    );
}

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

    // Schema installation belongs to fixture preparation. The timed scenario
    // below exercises two real owner generations and SIGTERM shutdown.
    let pool = guigu_agent_bridge::storage::connect(&database)
        .await
        .unwrap();
    guigu_agent_bridge::storage::migrate(&pool).await.unwrap();
    pool.close().await;

    for generation in 0..2 {
        let output = tempfile::NamedTempFile::new().unwrap();
        let mut child = FixtureChild(
            Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
                .arg(&config)
                .stdout(output.as_file().try_clone().unwrap())
                .stderr(output.as_file().try_clone().unwrap())
                .spawn()
                .unwrap(),
        );
        let ready = format!("http://127.0.0.1:{port}/ready");
        let readiness = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if reqwest::get(&ready)
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    break;
                }
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "bridge exited before readiness: {}",
                    std::fs::read_to_string(output.path()).unwrap()
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            readiness.is_ok(),
            "generation {generation} readiness expired: {}",
            std::fs::read_to_string(output.path()).unwrap()
        );

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
