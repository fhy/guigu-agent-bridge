use guigu_agent_bridge::app::AppRuntime;

fn configuration(root: &std::path::Path) -> std::path::PathBuf {
    let database = root.join("state.db");
    let sessions = root.join("must-not-create");
    let config = root.join("config.toml");
    std::fs::write(&config, format!("[bridge]\ndatabase={database:?}\nsession_root={sessions:?}\n[transports.matrix]\nenabled=false\n[agents.worker]\ntransport='acp'\ncommand='must-not-spawn'\nworkspace='/tmp'\nenabled=true\n")).unwrap();
    guigu_agent_bridge::config::load(&config).unwrap();
    config
}

fn snapshot(root: &std::path::Path) -> std::collections::BTreeMap<std::ffi::OsString, Vec<u8>> {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), std::fs::read(entry.path()).unwrap())
        })
        .collect()
}

#[tokio::test]
async fn real_disabled_startup_discovers_admin_binding_in_wal_without_root_or_disk_writes() {
    let root = tempfile::Builder::new()
        .prefix("r092-wal-discovery-")
        .tempdir_in("/tmp")
        .unwrap();
    let database = root.path().join("state.db");
    let pool = guigu_agent_bridge::storage::connect(&database)
        .await
        .unwrap();
    guigu_agent_bridge::storage::migrate(&pool).await.unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest,phase,revision) VALUES(1,2,'generation','database','root','operation','intent','schema_pending',0)")
        .execute(&pool).await.unwrap();
    let config = configuration(root.path());
    assert!(root.path().join("state.db-wal").metadata().unwrap().len() > 0);
    let before = snapshot(root.path());
    assert!(matches!(
        AppRuntime::start(&config).await,
        Err(guigu_agent_bridge::app::AppError::Runtime)
    ));
    assert_eq!(before, snapshot(root.path()));
    assert!(!root.path().join("must-not-create").exists());
    let phase: String = sqlx::query_scalar("SELECT phase FROM offline_recovery_binding")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(phase, "schema_pending");
    pool.close().await;
}

#[tokio::test]
async fn rootless_reserved_or_incomplete_context_refuses_before_lock_and_session_creation() {
    for state in ["admin_reserved", "corrupt_complete"] {
        let root = tempfile::Builder::new()
            .prefix("r092-context-discovery-")
            .tempdir_in("/tmp")
            .unwrap();
        let database = root.path().join("state.db");
        let pool = guigu_agent_bridge::storage::connect(&database)
            .await
            .unwrap();
        guigu_agent_bridge::storage::migrate(&pool).await.unwrap();
        if state == "admin_reserved" {
            sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest,phase,revision) VALUES(1,2,'generation','database','root','operation','intent','schema_pending',0)")
                .execute(&pool).await.unwrap();
            sqlx::query("UPDATE runtime_eligibility_bootstrap SET state='admin_reserved'")
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("DELETE FROM offline_recovery_binding")
                .execute(&pool)
                .await
                .unwrap();
        } else {
            sqlx::query("INSERT INTO runtime_eligibility_context(singleton,generation,bootstrap_nonce,provenance_kind,matrix_phase,state,revision,created_at,bootstrap_inventory_digest) SELECT 1,'generation',bootstrap_nonce,'pristine_local','absent','ready',0,'2026-10-28T00:00:00Z','inventory' FROM runtime_eligibility_bootstrap WHERE state='pending'")
                .execute(&pool).await.unwrap();
        }
        pool.close().await;
        let config = configuration(root.path());
        let before = snapshot(root.path());
        assert!(
            matches!(
                AppRuntime::start(&config).await,
                Err(guigu_agent_bridge::app::AppError::Runtime)
            ),
            "{state}"
        );
        assert_eq!(before, snapshot(root.path()), "{state}");
    }
}

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
