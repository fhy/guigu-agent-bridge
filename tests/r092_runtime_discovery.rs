use guigu_agent_bridge::app::AppRuntime;

#[tokio::test]
async fn real_disabled_runtime_refuses_unresolved_root_before_directory_or_lock_creation() {
    for pending in [false, true] {
        let root = tempfile::Builder::new()
            .prefix("r092-discovery-")
            .tempdir_in("/tmp")
            .unwrap();
        let database = root.path().join("state.db");
        let sessions = root.path().join("must-not-create");
        let config = root.path().join("config.toml");
        std::fs::write(&config, format!("[bridge]\ndatabase={database:?}\nsession_root={sessions:?}\n[transports.matrix]\nenabled=false\n[agents.worker]\ntransport='acp'\ncommand='must-not-spawn'\nworkspace='/tmp'\nenabled=true\n")).unwrap();
        let anchor = root.path().join(if pending {
            ".state.db.offline-operation.pending"
        } else {
            ".state.db.offline-operation.json"
        });
        std::fs::write(&anchor, br#"{"protocol":2,"unresolved":true}"#).unwrap();
        let parsed = guigu_agent_bridge::config::load(&config).unwrap();
        assert!(!parsed.transports.matrix.enabled);
        let mut before = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        before.sort();
        assert!(matches!(
            AppRuntime::start(&config).await,
            Err(guigu_agent_bridge::app::AppError::Runtime)
        ));
        assert!(!sessions.exists());
        assert!(!database.exists());
        let mut after = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        after.sort();
        assert_eq!(before, after);
        assert_eq!(
            std::fs::read(&anchor).unwrap(),
            br#"{"protocol":2,"unresolved":true}"#
        );
    }
}
