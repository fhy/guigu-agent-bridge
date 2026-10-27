use guigu_agent_bridge::{
    app::AppRuntime,
    config::{Config, load_from_str_with_env},
    storage::{connect, migrate},
};
use sqlx::SqlitePool;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct Fixture {
    pub root: tempfile::TempDir,
    pub pool: SqlitePool,
}

impl Fixture {
    pub async fn new() -> Self {
        let root = tempfile::tempdir_in(std::env::var_os("HOME").expect("HOME")).unwrap();
        std::fs::set_permissions(
            root.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        std::fs::write(
            root.path().join("bridge.toml"),
            b"synthetic R084 startup identity",
        )
        .unwrap();
        let pool = connect(root.path().join("state.db")).await.unwrap();
        migrate(&pool).await.unwrap();
        Self { root, pool }
    }

    pub fn store(&self) -> PathBuf {
        self.root.path().join("crypto")
    }

    pub fn config(&self, enabled: bool, server: &str) -> Config {
        let mut environment = BTreeMap::new();
        environment.insert(
            "HOME".to_owned(),
            self.root.path().to_str().unwrap().to_owned(),
        );
        environment.insert(
            "R084_TOKEN".to_owned(),
            "synthetic-no-network-token".to_owned(),
        );
        load_from_str_with_env(
            &format!(
                r#"
[bridge]
database = {:?}
session_root = {:?}
health_bind = "127.0.0.1:0"
shutdown_timeout_seconds = 5
[agents.worker]
transport = "acp"
command = "must-not-be-started-r084"
workspace = {:?}
enabled = true
[transports.matrix]
enabled = {enabled}
homeserver = {server:?}
user_id = "@bridge:example.test"
device_id = "R084"
device_trusted = true
access_token = "{{env:R084_TOKEN}}"
crypto_store_path = {:?}
"#,
                self.root.path().join("state.db").to_str().unwrap(),
                self.root.path().join("sessions").to_str().unwrap(),
                self.root.path().to_str().unwrap(),
                self.store().to_str().unwrap()
            ),
            &environment,
        )
        .unwrap()
    }

    pub async fn start(&self, config: Config) -> AppRuntime {
        AppRuntime::start_with_config_and_a2a_faults(
            self.root.path().join("bridge.toml"),
            config,
            Arc::new(guigu_agent_bridge::a2a::NoFault),
        )
        .await
        .unwrap()
    }

    pub async fn count(&self, table: &str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    pub async fn assert_no_owners(&self) {
        for table in [
            "runtime_instances",
            "runtime_owner_evidence",
            "agents",
            "agent_work_queue",
            "projection_outbox",
            "acp_output_sets",
            "matrix_component_leases",
        ] {
            assert_eq!(
                self.count(table).await,
                0,
                "unexpected owner/effect: {table}"
            );
        }
    }
}

pub fn seed_store(root: &Path) {
    std::fs::create_dir(root).unwrap();
    std::fs::set_permissions(root, std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
    std::fs::write(root.join("identity-only"), b"synthetic-not-a-token").unwrap();
}
