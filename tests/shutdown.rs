use std::future;
use std::io;

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
