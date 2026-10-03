use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use guigu_agent_bridge::storage::{connect, migrate};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

fn digest(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for part in parts {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part);
    }
    format!("{:x}", h.finalize())
}

fn path_identity(path: &Path, include_content: bool) -> String {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let canonical = path.canonicalize().unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0o400000)
        .open(path)
        .unwrap();
    let meta = file.metadata().unwrap();
    let content = if include_content {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        format!("{:x}", Sha256::digest(bytes))
    } else {
        "identity-only".to_owned()
    };
    digest(&[
        &canonical.to_string_lossy(),
        &meta.dev().to_string(),
        &meta.ino().to_string(),
        &meta.mode().to_string(),
        &content,
    ])
}

fn tree(root: &Path) -> Vec<(String, u32, Vec<u8>)> {
    use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};
    fn walk(root: &std::path::Path, path: &std::path::Path, out: &mut Vec<(String, u32, Vec<u8>)>) {
        let mut entries = std::fs::read_dir(path)
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            let bytes = if meta.file_type().is_symlink() {
                std::fs::read_link(&path)
                    .unwrap()
                    .as_os_str()
                    .as_bytes()
                    .to_vec()
            } else if meta.is_file() {
                std::fs::read(&path).unwrap()
            } else {
                Vec::new()
            };
            out.push((rel, meta.mode(), bytes));
            if meta.is_dir() {
                walk(root, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out
}

fn proc_view(pid: Option<u32>, real_network: bool) -> PathBuf {
    let root = std::env::temp_dir().join(format!("t050-proc-view-{}", uuid::Uuid::now_v7()));
    let net = root.join("net");
    std::fs::create_dir_all(&net).unwrap();
    for table in ["tcp", "tcp6"] {
        let destination = net.join(table);
        if real_network {
            std::os::unix::fs::symlink(format!("/proc/net/{table}"), destination).unwrap();
        } else {
            std::fs::write(
                destination,
                b"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n",
            )
            .unwrap();
        }
    }
    if let Some(pid) = pid {
        std::os::unix::fs::symlink(format!("/proc/{pid}"), root.join(pid.to_string())).unwrap();
    }
    root
}

async fn logical_digest(c: &mut SqliteConnection) -> String {
    const TABLES: &[(&str, &str, &[&str])] = &[
        (
            "runtime_instances",
            "instance_token",
            &[
                "instance_token",
                "started_at",
                "heartbeat_at",
                "state",
                "process_fingerprint",
            ],
        ),
        (
            "task_admissions",
            "task_id",
            &[
                "task_id",
                "state",
                "revision",
                "runtime_instance",
                "updated_at",
            ],
        ),
        (
            "agent_work_queue",
            "queue_id",
            &[
                "queue_id",
                "state",
                "revision",
                "runtime_owner",
                "owner_fence",
                "send_started",
                "updated_at",
            ],
        ),
        (
            "execution_leases",
            "resource_key",
            &[
                "resource_key",
                "task_id",
                "owner_token",
                "fence",
                "state",
                "heartbeat_at",
                "expires_at",
            ],
        ),
        (
            "task_continuations",
            "task_id",
            &[
                "task_id",
                "resource_key",
                "delivery_id",
                "lease_fence",
                "revision",
                "state",
                "heartbeat_at",
            ],
        ),
        (
            "delivery_dispositions",
            "delivery_id,task_id,attempt",
            &["delivery_id", "task_id", "attempt", "state", "reason_code"],
        ),
    ];
    let mut h = Sha256::new();
    let schema: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    let user: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    for value in [schema.to_string(), user.to_string()] {
        h.update((value.len() as u64).to_be_bytes());
        h.update(value);
    }
    for (table, order, columns) in TABLES {
        let query = format!(
            "SELECT {} FROM {table} ORDER BY {order}",
            columns
                .iter()
                .map(|v| format!("quote({v})"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(query))
            .fetch_all(&mut *c)
            .await
            .unwrap();
        h.update((table.len() as u64).to_be_bytes());
        h.update(table);
        for row in rows {
            for (index, column) in columns.iter().enumerate() {
                h.update((column.len() as u64).to_be_bytes());
                h.update(column.as_bytes());
                let value: String = row.try_get(index).unwrap();
                h.update((value.len() as u64).to_be_bytes());
                h.update(value);
            }
        }
    }
    format!("{:x}", h.finalize())
}

struct Fixture {
    root: PathBuf,
    db: PathBuf,
    config: PathBuf,
    manifest: PathBuf,
    evidence: PathBuf,
    systemctl: PathBuf,
    config_bytes: Vec<u8>,
    proc_root: PathBuf,
}

impl Fixture {
    async fn new(extra_stopping: bool, active_lease: bool) -> Self {
        let root =
            std::env::temp_dir().join(format!("t050-reconcile-process-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let db = root.join("state.db");
        let pool = connect(&db).await.unwrap();
        migrate(&pool).await.unwrap();
        sqlx::query("INSERT INTO runtime_instances VALUES('legacy','start','heart','active','legacy-owner')").execute(&pool).await.unwrap();
        if extra_stopping {
            sqlx::query("INSERT INTO runtime_instances VALUES('other','start','heart','stopping','other-legacy')").execute(&pool).await.unwrap();
        }
        pool.close().await;
        let config = root.join("config.toml");
        let runtime_root =
            std::env::temp_dir().join(format!("t050-mapped-runtime-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&runtime_root).unwrap();
        let config_bytes = format!(
            "[bridge]\ndatabase = {:?}\nsession_root = {:?}\nhealth_bind = \"127.0.0.1:0\"\n",
            runtime_root.join("state.db").to_string_lossy(),
            runtime_root.join("sessions").to_string_lossy()
        )
        .into_bytes();
        std::fs::write(&config, &config_bytes).unwrap();
        let evidence = root.join("evidence.txt");
        let manifest = root.join("operation.json");
        let marker = root.join(".t050-isolated-marker");
        std::fs::write(
            &marker,
            b"t050-isolated-root-v1\noperation_id=operation\nunit=fixture.service\n",
        )
        .unwrap();
        std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o600)).unwrap();
        let systemctl = root.join("systemctl-fixture");
        std::fs::write(&systemctl, b"#!/bin/sh\nprintf 'inactive\\ndead\\n0\\n'\n").unwrap();
        std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut c = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
        if active_lease {
            sqlx::query("INSERT INTO conversations VALUES('conversation',NULL,NULL,NULL,'[]')")
                .execute(&mut c)
                .await
                .unwrap();
            sqlx::query("INSERT INTO tasks VALUES('task','task',NULL,'from','to','conversation',NULL,'text',5,0,0,NULL,0)")
                .execute(&mut c).await.unwrap();
            sqlx::query("INSERT INTO execution_leases VALUES('resource','task','owner',1,'active','start','heart','expires')")
                .execute(&mut c).await.unwrap();
        }
        let pre = logical_digest(&mut c).await;
        sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at='commit' WHERE instance_token='legacy'").execute(&mut c).await.unwrap();
        let post = logical_digest(&mut c).await;
        sqlx::query("UPDATE runtime_instances SET state='active',heartbeat_at='heart' WHERE instance_token='legacy'").execute(&mut c).await.unwrap();
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&mut c)
            .await
            .unwrap();
        c.close().await.unwrap();
        for suffix in ["-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", db.to_string_lossy()));
        }
        let binary = PathBuf::from(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
            .canonicalize()
            .unwrap();
        let config_s = config
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let db_s = db.canonicalize().unwrap().to_string_lossy().into_owned();
        let binary_s = binary.to_string_lossy().into_owned();
        let evidence_s = evidence.to_string_lossy().into_owned();
        let config_identity = path_identity(&config, true);
        let database_identity = path_identity(&db, false);
        let binary_identity = path_identity(&binary, true);
        let root_s = root.to_string_lossy().into_owned();
        let marker_s = marker.to_string_lossy().into_owned();
        let md = digest(&[
            "operation",
            &root_s,
            &marker_s,
            &config_s,
            &config_identity,
            &db_s,
            &database_identity,
            &binary_s,
            &binary_identity,
            "fixture.service",
            &evidence_s,
            &pre,
            &post,
            "commit",
        ]);
        let rd = digest(&["operation", &md, &pre, &post, "commit", "recovered"]);
        std::fs::write(&manifest,serde_json::to_vec(&json!({"operation_id":"operation","isolated_root":root_s,"marker":marker_s,"config":config_s,"config_identity":config_identity,"database":db_s,"database_identity":database_identity,"binary":binary_s,"binary_identity":binary_identity,"unit":"fixture.service","evidence":evidence_s,"manifest_digest":md,"pre_owner_state_digest":pre,"post_owner_state_digest":post,"receipt_digest":rd,"receipt_sibling_digest":digest(&[]),"commit_heartbeat":"commit"})).unwrap()).unwrap();
        std::fs::set_permissions(&manifest, std::fs::Permissions::from_mode(0o600)).unwrap();
        Self {
            root,
            db,
            config,
            manifest,
            evidence,
            systemctl,
            config_bytes,
            proc_root: proc_view(None, false),
        }
    }
    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"));
        command.env("T050_RECONCILE_TEST_SYSTEMCTL", &self.systemctl);
        command.env("T050_RECONCILE_TEST_PROC_ROOT", &self.proc_root);
        command
    }
    fn run(&self) -> std::process::Output {
        self.command()
            .args(["reconcile-isolated-legacy-owner", "--manifest"])
            .arg(&self.manifest)
            .output()
            .unwrap()
    }
    async fn rows(&self) -> Vec<(String, String, String)> {
        let mut connection = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&self.db)
                .create_if_missing(false),
        )
        .await
        .unwrap();
        let rows=sqlx::query_as("SELECT instance_token,state,heartbeat_at FROM runtime_instances ORDER BY instance_token").fetch_all(&mut connection).await.unwrap();
        connection.close().await.unwrap();
        rows
    }
    fn mapped_process(&self) -> Child {
        let mut child = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
            .arg(&self.config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "mapped fixture process exited"
            );
            let cmdline = std::fs::read(format!("/proc/{}/cmdline", child.id())).unwrap();
            if cmdline
                .split(|byte| *byte == 0)
                .any(|argument| argument == self.config.as_os_str().as_encoded_bytes())
            {
                return child;
            }
            assert!(
                Instant::now() < deadline,
                "mapped process was not observable"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn listener_process(&self, port: u16) -> Child {
        let runtime_root =
            std::env::temp_dir().join(format!("t050-listener-runtime-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&runtime_root).unwrap();
        let config = runtime_root.join("config.toml");
        std::fs::write(
            &config,
            format!(
                "[bridge]\ndatabase = {:?}\nsession_root = {:?}\nhealth_bind = \"127.0.0.1:{port}\"\n",
                runtime_root.join("state.db").to_string_lossy(),
                runtime_root.join("sessions").to_string_lossy()
            ),
        )
        .unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
            .arg(config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "listener fixture process exited"
            );
            if std::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).is_ok() {
                return child;
            }
            assert!(
                Instant::now() < deadline,
                "listener fixture was not observable"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[tokio::test]
async fn eligible_commit_repeat_and_unknown_evidence_adoption_use_real_cli() {
    let f = Fixture::new(false, false).await;
    let first = f.run();
    assert_eq!(
        first.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&first.stdout)
    );
    assert!(String::from_utf8_lossy(&first.stdout).contains("result=recovered status=0"));
    let committed = f.rows().await;
    std::fs::remove_file(&f.evidence).unwrap();
    let repeat = f.run();
    assert_eq!(repeat.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&repeat.stdout).contains("result=already-stopped status=0"));
    assert_eq!(f.rows().await, committed);
    assert!(f.evidence.exists());
    let pool = connect(&f.db).await.unwrap();
    sqlx::query("INSERT INTO legacy_owner_reconciliation_receipts VALUES('sibling','sibling-manifest','pre','post','sibling-receipt','siblings','commit','recovered')").execute(&pool).await.unwrap();
    pool.close().await;
    let sibling_drift = f.run();
    assert_eq!(sibling_drift.status.code(), Some(3));
    assert_eq!(f.rows().await, committed);
}

#[tokio::test]
async fn planner_and_cas_refusals_preserve_durable_rows() {
    let f = Fixture::new(true, false).await;
    let before = f.rows().await;
    let rejected = f.run();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(f.rows().await, before);
    assert!(!f.evidence.exists());
    let f = Fixture::new(false, false).await;
    sqlx::query("UPDATE runtime_instances SET heartbeat_at='drift'")
        .execute(&connect(&f.db).await.unwrap())
        .await
        .unwrap();
    let before = f.rows().await;
    let rejected = f.run();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(f.rows().await, before);
    assert!(!f.evidence.exists());
    let f = Fixture::new(false, true).await;
    let before = f.rows().await;
    let rejected = f.run();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(f.rows().await, before);
}

#[tokio::test]
async fn live_interlock_and_hostile_sidecar_refuse_before_sqlite_mutation() {
    let f = Fixture::new(false, false).await;
    let before = f.rows().await;
    let guard =
        guigu_agent_bridge::runtime_admission::Guard::acquire(&f.db, &f.config, "startup").unwrap();
    let rejected = f.run();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(f.rows().await, before);
    drop(guard);
    std::fs::write(&f.config, b"fixture=drift\n").unwrap();
    let before_tree = tree(&f.root);
    let rejected = f.run();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(tree(&f.root), before_tree);
    std::fs::write(&f.config, &f.config_bytes).unwrap();
    use std::os::unix::fs::PermissionsExt;
    let marker = f.root.join(".t050-isolated-marker");
    std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o644)).unwrap();
    let before_tree = tree(&f.root);
    let rejected = f.run();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(tree(&f.root), before_tree);
    std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o600)).unwrap();
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", f.db.to_string_lossy()));
    }
    let outside = f.root.join("outside");
    std::fs::write(&outside, b"unchanged").unwrap();
    let database_before = std::fs::read(&f.db).unwrap();
    std::os::unix::fs::symlink(&outside, format!("{}-wal", f.db.to_string_lossy())).unwrap();
    let rejected = f.run();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(std::fs::read(&outside).unwrap(), b"unchanged");
    assert_eq!(std::fs::read(&f.db).unwrap(), database_before);
}

#[tokio::test]
async fn process_listener_and_unknown_proc_facts_fail_closed_before_and_after_open() {
    let f = Fixture::new(false, false).await;
    let mut mapped = f.mapped_process();
    let mapped_proc = proc_view(Some(mapped.id()), false);
    let before = tree(&f.root);
    let rejected = f
        .command()
        .env("T050_RECONCILE_TEST_PROC_ROOT", &mapped_proc)
        .args(["reconcile-isolated-legacy-owner", "--manifest"])
        .arg(&f.manifest)
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(tree(&f.root), before);

    let missing_proc = proc_view(None, false);
    std::fs::remove_file(missing_proc.join("net/tcp6")).unwrap();
    let rejected = f
        .command()
        .env("T050_RECONCILE_TEST_PROC_ROOT", &missing_proc)
        .args(["reconcile-isolated-legacy-owner", "--manifest"])
        .arg(&f.manifest)
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(tree(&f.root), before);

    let permission_proc = proc_view(None, false);
    let candidate = permission_proc.join("424242");
    std::fs::create_dir(&candidate).unwrap();
    std::os::unix::fs::symlink(
        env!("CARGO_BIN_EXE_guigu-agent-bridge"),
        candidate.join("exe"),
    )
    .unwrap();
    std::fs::write(candidate.join("cmdline"), b"candidate\0").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        candidate.join("cmdline"),
        std::fs::Permissions::from_mode(0o000),
    )
    .unwrap();
    let rejected = f
        .command()
        .env("T050_RECONCILE_TEST_PROC_ROOT", &permission_proc)
        .args(["reconcile-isolated-legacy-owner", "--manifest"])
        .arg(&f.manifest)
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(tree(&f.root), before);
    mapped.kill().unwrap();
    assert!(!mapped.wait().unwrap().success());

    let mut listener = f.listener_process(9292);
    let listener_proc = proc_view(Some(listener.id()), true);
    let before = tree(&f.root);
    let rejected = f
        .command()
        .env("T050_RECONCILE_TEST_PROC_ROOT", &listener_proc)
        .args(["reconcile-isolated-legacy-owner", "--manifest"])
        .arg(&f.manifest)
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(tree(&f.root), before);
    listener.kill().unwrap();
    assert!(!listener.wait().unwrap().success());

    let fake_proc = f.root.join("fake-proc");
    std::fs::create_dir_all(fake_proc.join("net")).unwrap();
    std::fs::write(fake_proc.join("net/tcp"), b"malformed\n").unwrap();
    std::fs::write(fake_proc.join("net/tcp6"), b"malformed\n").unwrap();
    let before = tree(&f.root);
    let rejected = f
        .command()
        .env("T050_RECONCILE_TEST_PROC_ROOT", &fake_proc)
        .args(["reconcile-isolated-legacy-owner", "--manifest"])
        .arg(&f.manifest)
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(tree(&f.root), before);

    let f = Fixture::new(false, false).await;
    let before_rows = f.rows().await;
    let pause = f.root.join("precommit-runtime.ready");
    let precommit_proc = proc_view(None, false);
    let mut reconcile = f
        .command()
        .args(["reconcile-isolated-legacy-owner", "--manifest"])
        .arg(&f.manifest)
        .env("T050_RECONCILE_TEST_PRECOMMIT", &pause)
        .env("T050_RECONCILE_TEST_PROC_ROOT", &precommit_proc)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !pause.exists() {
        assert!(reconcile.try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline, "precommit pause was not reached");
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut mapped = f.mapped_process();
    std::os::unix::fs::symlink(
        format!("/proc/{}", mapped.id()),
        precommit_proc.join(mapped.id().to_string()),
    )
    .unwrap();
    std::fs::write(pause.with_extension("continue"), b"continue\n").unwrap();
    assert_eq!(reconcile.wait().unwrap().code(), Some(3));
    mapped.kill().unwrap();
    assert!(!mapped.wait().unwrap().success());
    assert_eq!(f.rows().await, before_rows);
    let pool = connect(&f.db).await.unwrap();
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM legacy_owner_reconciliation_receipts")
            .fetch_one(&pool)
            .await
            .unwrap();
    pool.close().await;
    assert_eq!(receipts, 0);
    assert!(!f.evidence.exists());

    let f = Fixture::new(false, false).await;
    let before_rows = f.rows().await;
    let pause = f.root.join("precommit-listener.ready");
    let precommit_proc = proc_view(None, true);
    let mut reconcile = f
        .command()
        .args(["reconcile-isolated-legacy-owner", "--manifest"])
        .arg(&f.manifest)
        .env("T050_RECONCILE_TEST_PRECOMMIT", &pause)
        .env("T050_RECONCILE_TEST_PROC_ROOT", &precommit_proc)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !pause.exists() {
        assert!(reconcile.try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline, "precommit pause was not reached");
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut listener = f.listener_process(9292);
    std::os::unix::fs::symlink(
        format!("/proc/{}", listener.id()),
        precommit_proc.join(listener.id().to_string()),
    )
    .unwrap();
    std::fs::write(pause.with_extension("continue"), b"continue\n").unwrap();
    assert_eq!(reconcile.wait().unwrap().code(), Some(3));
    listener.kill().unwrap();
    assert!(!listener.wait().unwrap().success());
    assert_eq!(f.rows().await, before_rows);
    let pool = connect(&f.db).await.unwrap();
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM legacy_owner_reconciliation_receipts")
            .fetch_one(&pool)
            .await
            .unwrap();
    pool.close().await;
    assert_eq!(receipts, 0);
    assert!(!f.evidence.exists());
}

#[tokio::test]
async fn crash_before_commit_rolls_back_and_restart_reclaims_stale_interlock() {
    let f = Fixture::new(false, false).await;
    let before = f.rows().await;
    let marker = f.root.join("precommit.ready");
    let mut child = f
        .command()
        .args(["reconcile-isolated-legacy-owner", "--manifest"])
        .arg(&f.manifest)
        .env("T050_RECONCILE_TEST_PRECOMMIT", &marker)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !marker.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "reconciliation exited before precommit failpoint"
        );
        assert!(
            Instant::now() < deadline,
            "precommit failpoint was not reached"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert_eq!(f.rows().await, before);
    let pool = connect(&f.db).await.unwrap();
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM legacy_owner_reconciliation_receipts")
            .fetch_one(&pool)
            .await
            .unwrap();
    pool.close().await;
    assert_eq!(receipts, 0);
    let retry = f.run();
    assert_eq!(retry.status.code(), Some(0));
}

#[tokio::test]
async fn committed_receipt_adopts_unknown_evidence_without_second_cas() {
    let f = Fixture::new(false, false).await;
    let lost = f
        .command()
        .args(["reconcile-isolated-legacy-owner", "--manifest"])
        .arg(&f.manifest)
        .env("T050_RECONCILE_TEST_LOSE_EVIDENCE", "1")
        .output()
        .unwrap();
    assert_eq!(lost.status.code(), Some(3));
    assert!(!f.evidence.exists());
    let committed = f.rows().await;
    assert_eq!(committed[0].1, "stopped");
    let repeat = f.run();
    assert_eq!(repeat.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&repeat.stdout).contains("already-stopped"));
    assert_eq!(f.rows().await, committed);
    assert!(f.evidence.exists());
}
