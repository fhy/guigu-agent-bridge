use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use guigu_agent_bridge::storage::{connect, migrate};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};

type DispositionRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[derive(Debug, PartialEq, Eq)]
struct DurableRows {
    owners: Vec<(String, String, String)>,
    continuation: (String, i64, String),
    receipts: i64,
    sessions: Vec<(String, String, String, String)>,
    dispositions: Vec<DispositionRow>,
    witnesses: Vec<String>,
}

fn proc_view() -> PathBuf {
    let root = std::env::temp_dir().join(format!("t050-migration-proc-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(root.join("net")).unwrap();
    for table in ["tcp", "tcp6"] {
        std::fs::write(root.join("net").join(table), b"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n").unwrap();
    }
    root
}

async fn durable_rows(path: &Path) -> DurableRows {
    let mut c = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false),
    )
    .await
    .unwrap();
    let owners = sqlx::query_as(
        "SELECT instance_token,state,heartbeat_at FROM runtime_instances ORDER BY instance_token",
    )
    .fetch_all(&mut c)
    .await
    .unwrap();
    let continuation = sqlx::query_as("SELECT state,revision,heartbeat_at FROM task_continuations")
        .fetch_one(&mut c)
        .await
        .unwrap();
    let receipts = sqlx::query_scalar("SELECT count(*) FROM isolated_fixture_migration_receipts")
        .fetch_one(&mut c)
        .await
        .unwrap();
    let sessions = sqlx::query_as(
        "SELECT session_id,state,updated_at,compatibility_hash FROM sessions ORDER BY session_id,endpoint_id",
    )
    .fetch_all(&mut c)
    .await
    .unwrap();
    let dispositions = sqlx::query_as("SELECT delivery_id,state,session_id,child_fingerprint,reap_status,reap_at FROM delivery_dispositions ORDER BY delivery_id")
        .fetch_all(&mut c)
        .await
        .unwrap();
    let witnesses = sqlx::query_scalar(
        "SELECT value FROM (
           SELECT 'task:'||quote(task_id)||':'||quote(version) AS value FROM tasks
           UNION ALL SELECT 'event:'||quote(event_id)||':'||quote(seq)||':'||quote(status)||':'||quote(payload) FROM task_events
           UNION ALL SELECT 'lease:'||quote(resource_key)||':'||quote(task_id)||':'||quote(owner_token)||':'||quote(fence)||':'||quote(state) FROM execution_leases
           UNION ALL SELECT 'admission:'||quote(task_id)||':'||quote(state)||':'||quote(revision)||':'||quote(runtime_instance) FROM task_admissions
           UNION ALL SELECT 'delivery:'||quote(delivery_id)||':'||quote(task_id)||':'||quote(attempt)||':'||quote(acknowledged_at) FROM deliveries
           UNION ALL SELECT 'response:'||quote(idempotency_key)||':'||quote(response_hash)||':'||quote(outcome)||':'||quote(output_bytes) FROM continuation_response_receipts
           UNION ALL SELECT 'queue:'||quote(queue_id)||':'||quote(state)||':'||quote(revision) FROM agent_work_queue
           UNION ALL SELECT 'claim:'||quote(claim_id)||':'||quote(state)||':'||quote(revision) FROM workspace_claims
           UNION ALL SELECT 'projection:'||quote(source_kind)||':'||quote(source_id)||':'||quote(projection)||':'||quote(state) FROM projection_outbox
         ) ORDER BY value",
    )
    .fetch_all(&mut c)
    .await
    .unwrap();
    c.close().await.unwrap();
    DurableRows {
        owners,
        continuation,
        receipts,
        sessions,
        dispositions,
        witnesses,
    }
}

fn fixture_tree(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut entries = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("state.db") {
                return None;
            }
            let metadata = std::fs::symlink_metadata(entry.path()).unwrap();
            let bytes = if metadata.is_file() {
                std::fs::read(entry.path()).unwrap()
            } else if metadata.file_type().is_symlink() {
                std::fs::read_link(entry.path())
                    .unwrap()
                    .as_os_str()
                    .as_encoded_bytes()
                    .to_vec()
            } else {
                Vec::new()
            };
            Some((name, bytes))
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    entries
}

struct Fixture {
    root: PathBuf,
    db: PathBuf,
    config: PathBuf,
    manifest: PathBuf,
    evidence: PathBuf,
    systemctl: PathBuf,
    proc_root: PathBuf,
}

impl Fixture {
    async fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root =
            std::env::temp_dir().join(format!("t050-fixture-migration-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let db = root.join("state.db");
        let pool = connect(&db).await.unwrap();
        migrate(&pool).await.unwrap();
        sqlx::query("INSERT INTO agents VALUES('agent-a','a','acp',1,NULL,'[]'),('agent-b','b','acp',1,NULL,'[]')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO conversations VALUES('conversation',NULL,NULL,NULL,'[]')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tasks VALUES('task','task',NULL,'agent-a','agent-b','conversation',NULL,'closed fixture',5,0,0,NULL,1)").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_events VALUES('event-1','task',1,'running','2026-10-01T00:00:00.000000000Z','{}'),('event-2','task',2,'failed','2026-10-01T00:00:03.000000000Z','{}')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO deliveries VALUES('delivery','task',1,'agent-b','2026-10-01T00:00:01.000000000Z','2026-10-01T00:00:03.000000000Z')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO runtime_instances VALUES('owner-s','2026-09-30T23:00:00.000000000Z','2026-09-30T23:30:00.000000000Z','stopping','legacy-stopping'),('owner-a','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','active','legacy-active')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES('task','terminal',4,'owner-a','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES('delivery','task',1,'terminal')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO sessions(session_id,endpoint_id,conversation_id,cwd,backend_id,protocol_version,state,created_at,updated_at) VALUES('closed-session','agent-b','conversation','/tmp','agent-b@1',1,'closed','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_leases VALUES('resource','task','owner-a',7,'released','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','2026-10-01T00:00:03.000000000Z')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes,runtime_generation,runtime_generation_version) VALUES('task','resource','delivery',7,2,'blocked',1,1,0,'','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','2026-10-01T00:00:03.000000000Z',12,'generation.v1:strict',1)").execute(&pool).await.unwrap();
        let response_hash = uuid::Uuid::new_v5(
            &uuid::Uuid::from_u128(0x7297ec04_1338_57b4_9bc4_f0ff9f84fa5a),
            b"{}",
        )
        .to_string();
        sqlx::query("INSERT INTO continuation_response_receipts VALUES('receipt','task','delivery','resource',7,1,?,'structured',2,'generation.v1:strict','2026-10-01T00:00:03.000000000Z')").bind(response_hash).execute(&pool).await.unwrap();
        pool.close().await;
        let config = root.join("config.toml");
        std::fs::write(&config, b"fixture=true\n").unwrap();
        let systemctl = root.join("systemctl");
        std::fs::write(
            &systemctl,
            b"#!/bin/sh\nprintf 'MainPID=0\nActiveState=inactive\nSubState=dead\n'\n",
        )
        .unwrap();
        std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o700)).unwrap();
        let fixture = Self {
            manifest: root.join("manifest.json"),
            evidence: root.join("evidence.txt"),
            proc_root: proc_view(),
            root,
            db,
            config,
            systemctl,
        };
        let output = fixture.prepare().output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(0),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        fixture
    }
    fn base(&self) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"));
        c.env("T050_RECONCILE_TEST_SYSTEMCTL", &self.systemctl)
            .env("T050_RECONCILE_TEST_PROC_ROOT", &self.proc_root);
        c
    }
    fn prepare(&self) -> Command {
        let mut c = self.base();
        c.args([
            "prepare-isolated-legacy-fixture-migration-manifest",
            "--root",
        ])
        .arg(&self.root)
        .arg("--config")
        .arg(&self.config)
        .arg("--database")
        .arg(&self.db)
        .arg("--unit")
        .arg("guigu-codex-test.service")
        .arg("--manifest")
        .arg(&self.manifest)
        .arg("--evidence")
        .arg(&self.evidence)
        .arg("--operation-id")
        .arg("00000000-0000-7000-8000-000000000150")
        .arg("--commit-timestamp")
        .arg("2026-10-04T00:00:00.000000000Z");
        c
    }
    fn run(&self) -> Command {
        let mut c = self.base();
        c.args(["migrate-isolated-legacy-fixture", "--manifest"])
            .arg(&self.manifest);
        c
    }
}

#[tokio::test]
async fn exact_closed_fixture_migrates_repeats_and_adopts_unknown_commit() {
    let f = Fixture::new().await;
    let first = f.run().output().unwrap();
    assert_eq!(
        first.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&first.stdout)
    );
    let committed = durable_rows(&f.db).await;
    assert_eq!(
        committed
            .owners
            .iter()
            .filter(|row| row.1 == "stopped")
            .count(),
        2
    );
    assert_eq!(committed.continuation.0, "terminal");
    assert_eq!(committed.continuation.1, 3);
    assert_eq!(committed.receipts, 1);
    std::fs::remove_file(&f.evidence).unwrap();
    let repeat = f.run().output().unwrap();
    assert_eq!(repeat.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&repeat.stdout).contains("already-migrated"));
    assert_eq!(durable_rows(&f.db).await, committed);
    let f = Fixture::new().await;
    let lost = f
        .run()
        .env("T050_FIXTURE_MIGRATION_TEST_LOSE_EVIDENCE", "1")
        .output()
        .unwrap();
    assert_eq!(lost.status.code(), Some(3));
    assert!(!f.evidence.exists());
    let committed = durable_rows(&f.db).await;
    assert_eq!(committed.receipts, 1);
    assert_eq!(f.run().output().unwrap().status.code(), Some(0));
    assert_eq!(durable_rows(&f.db).await, committed);
}

#[tokio::test]
async fn partial_cas_failpoints_roll_back_all_rows_and_receipt() {
    for stage in ["active", "stopping", "continuation"] {
        let f = Fixture::new().await;
        let before = durable_rows(&f.db).await;
        let rejected = f
            .run()
            .env("T050_FIXTURE_MIGRATION_TEST_FAIL_AFTER", stage)
            .output()
            .unwrap();
        assert_eq!(rejected.status.code(), Some(3));
        assert_eq!(durable_rows(&f.db).await, before);
        assert!(!f.evidence.exists());
    }
}

#[tokio::test]
async fn missing_extra_and_drifted_facts_are_rejected_without_mutation() {
    for sql in [
        "DELETE FROM continuation_response_receipts",
        "DELETE FROM task_admissions",
        "DELETE FROM delivery_dispositions",
        "UPDATE deliveries SET acknowledged_at=NULL",
        "UPDATE task_events SET status='running' WHERE seq=2",
        "UPDATE task_admissions SET runtime_instance='owner-s'",
        "UPDATE task_continuations SET runtime_generation='drift'",
        "UPDATE delivery_dispositions SET state='acknowledged'",
        "INSERT INTO runtime_instances VALUES('extra','2026-09-01T00:00:00Z','2026-09-01T00:00:00Z','stopping','legacy-extra')",
        "INSERT INTO workspace_claims VALUES('claim','owner-s','task','/tmp','[]',7,1,'active','2026-10-01T00:00:00Z','2026-10-01T00:00:00Z')",
    ] {
        let f = Fixture::new().await;
        std::fs::remove_file(&f.manifest).unwrap();
        let pool = connect(&f.db).await.unwrap();
        sqlx::query(sql).execute(&pool).await.unwrap();
        pool.close().await;
        let before = durable_rows(&f.db).await;
        assert_eq!(
            f.prepare().output().unwrap().status.code(),
            Some(3),
            "{sql}"
        );
        assert_eq!(durable_rows(&f.db).await, before);
        assert!(!f.manifest.exists());
    }
    let f = Fixture::new().await;
    let pool = connect(&f.db).await.unwrap();
    sqlx::query("UPDATE execution_leases SET fence=8")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let before = durable_rows(&f.db).await;
    assert_eq!(f.run().output().unwrap().status.code(), Some(3));
    assert_eq!(durable_rows(&f.db).await, before);
}

async fn assert_prepare_refusal(name: &str, sql: &'static str) {
    let f = Fixture::new().await;
    std::fs::remove_file(&f.manifest).unwrap();
    let pool = connect(&f.db).await.unwrap();
    sqlx::query(sql).execute(&pool).await.unwrap();
    pool.close().await;
    let before = durable_rows(&f.db).await;
    let tree = fixture_tree(&f.root);
    let rejected = f.prepare().output().unwrap();
    assert_eq!(rejected.status.code(), Some(3), "{name}");
    assert_eq!(durable_rows(&f.db).await, before, "{name}");
    assert_eq!(fixture_tree(&f.root), tree, "{name}");
    assert!(!f.manifest.exists(), "{name}");
    assert!(!f.evidence.exists(), "{name}");
}

#[tokio::test]
async fn open_session_refuses_before_manifest_publication() {
    assert_prepare_refusal(
        "open-session",
        "UPDATE sessions SET state='prompting' WHERE session_id='closed-session'",
    )
    .await;
}

#[tokio::test]
async fn eligibility_diagnostic_is_fixed_and_redacted() {
    let f = Fixture::new().await;
    std::fs::remove_file(&f.manifest).unwrap();
    let pool = connect(&f.db).await.unwrap();
    sqlx::query("UPDATE sessions SET state='prompting' WHERE session_id='closed-session'")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let output = f
        .prepare()
        .env("T050_FIXTURE_MIGRATION_DIAGNOSTIC", "1")
        .env("T050_FIXTURE_MIGRATION_PREFLIGHT_DIAGNOSTIC", "1")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("stage=eligibility cause=durable-facts counts=failures=1"));
    assert!(stdout.contains("stage=eligibility cause=open-sessions"));
    assert!(stdout.contains("counts=owners=2,continuations=1,sessions=1"));
    assert!(!stdout.contains("owner-a"));
    assert!(!stdout.contains("closed-session"));
}

#[tokio::test]
async fn preflight_diagnostic_process_exits_within_bound() {
    let root = std::env::temp_dir().join(format!("t050-preflight-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let process_root = root.clone();
    let binary = std::env::var_os("CARGO_BIN_EXE_guigu-agent-bridge").unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking(move || {
            let run = || {
                Command::new(&binary)
                    .args([
                        "prepare-isolated-legacy-fixture-migration-manifest",
                        "--root",
                        process_root.to_str().unwrap(),
                        "--config",
                        "/nonexistent/config",
                        "--database",
                        "/nonexistent/state.db",
                        "--unit",
                        "guigu-codex-test.service",
                        "--manifest",
                        "/nonexistent/manifest",
                        "--evidence",
                        "/nonexistent/evidence",
                        "--operation-id",
                        "00000000-0000-4000-8000-000000000000",
                        "--commit-timestamp",
                        "2026-10-04T00:00:00.000000000Z",
                    ])
                    .env("T050_FIXTURE_MIGRATION_PREFLIGHT_DIAGNOSTIC", "1")
                    .output()
                    .unwrap()
            };
            (run(), run())
        }),
    )
    .await
    .expect("diagnostic process timeout")
    .expect("diagnostic process task failed");
    assert_eq!(result.0.status.code(), Some(3));
    assert_eq!(result.1.status.code(), Some(3));
    assert_eq!(result.0.stdout, result.1.stdout);
    let stdout = String::from_utf8_lossy(&result.0.stdout);
    assert!(stdout.contains("stage=preflight cause=root-contract counts=failures=1"));
    assert!(!stdout.contains("state.db"));
    assert!(!stdout.contains("fingerprint"));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn publication_and_sqlite_open_diagnostics_are_fixed_and_invariant() {
    use std::os::unix::fs::PermissionsExt;

    let publication = Fixture::new().await;
    std::fs::set_permissions(
        &publication.manifest,
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    let publication_tree = fixture_tree(&publication.root);
    let output = publication
        .prepare()
        .env("T050_FIXTURE_MIGRATION_PREFLIGHT_DIAGNOSTIC", "1")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("stage=publication cause=manifest-target counts=failures=1"));
    assert!(!stdout.contains(publication.root.to_str().unwrap()));
    assert_eq!(fixture_tree(&publication.root), publication_tree);

    let sqlite = Fixture::new().await;
    let wal = PathBuf::from(format!("{}-wal", sqlite.db.to_string_lossy()));
    std::fs::create_dir(&wal).unwrap();
    let sqlite_tree = fixture_tree(&sqlite.root);
    let output = sqlite
        .prepare()
        .env("T050_FIXTURE_MIGRATION_PREFLIGHT_DIAGNOSTIC", "1")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("stage=sqlite-open cause=sidecar-auth counts=failures=1"));
    assert!(!stdout.contains(sqlite.root.to_str().unwrap()));
    assert_eq!(fixture_tree(&sqlite.root), sqlite_tree);
}

#[tokio::test]
async fn unit_state_process_matrix_is_fail_closed_redacted_and_repeatable() {
    let fixture = Fixture::new().await;
    for response in [
        "ActiveState=active\nSubState=running\nMainPID=42\n",
        "ActiveState=failed\nSubState=failed\nMainPID=0\n",
        "ActiveState=inactive\nSubState=dead\n",
        "ActiveState=inactive\nSubState=dead\nMainPID=0\nMainPID=0\n",
    ] {
        std::fs::write(
            &fixture.systemctl,
            format!("#!/bin/sh\nprintf '{response}'\n"),
        )
        .unwrap();
        let tree = fixture_tree(&fixture.root);
        let first = fixture
            .prepare()
            .env("T050_FIXTURE_MIGRATION_PREFLIGHT_DIAGNOSTIC", "1")
            .output()
            .unwrap();
        let second = fixture
            .prepare()
            .env("T050_FIXTURE_MIGRATION_PREFLIGHT_DIAGNOSTIC", "1")
            .output()
            .unwrap();
        assert_eq!(first.status.code(), Some(3));
        assert_eq!(second.status.code(), Some(3));
        assert_eq!(first.stdout, second.stdout);
        let stdout = String::from_utf8_lossy(&first.stdout);
        assert!(stdout.contains("stage=preflight cause=unit-state counts=failures=1"));
        assert!(!stdout.contains(fixture.root.to_str().unwrap()));
        assert!(!stdout.contains("MainPID"));
        assert_eq!(fixture_tree(&fixture.root), tree);
    }
}

#[tokio::test]
async fn runtime_unknown_diagnostic_is_bounded_redacted_repeatable_and_invariant() {
    use std::os::unix::fs::PermissionsExt;

    #[derive(Clone, Copy)]
    enum Setup {
        SocketIo,
        SocketParse,
        ProcfsPermission,
        ProcessExecutablePermission,
        CmdlineParse,
        CmdlinePermission,
        FdIo,
    }

    for (setup, cause) in [
        (Setup::SocketIo, "runtime-unknown-socket-io"),
        (Setup::SocketParse, "runtime-unknown-socket-parse"),
        (Setup::ProcfsPermission, "runtime-unknown-procfs-permission"),
        (
            Setup::ProcessExecutablePermission,
            "runtime-unknown-process-executable-permission",
        ),
        (Setup::CmdlineParse, "runtime-unknown-cmdline-parse"),
        (
            Setup::CmdlinePermission,
            "runtime-unknown-cmdline-permission",
        ),
        (Setup::FdIo, "runtime-unknown-fd-io"),
    ] {
        let fixture = Fixture::new().await;
        std::fs::remove_file(&fixture.manifest).unwrap();
        match setup {
            Setup::SocketIo => std::fs::remove_file(fixture.proc_root.join("net/tcp6")).unwrap(),
            Setup::SocketParse => {
                std::fs::write(fixture.proc_root.join("net/tcp"), b"malformed\n").unwrap();
            }
            Setup::ProcfsPermission => {
                std::fs::set_permissions(
                    &fixture.proc_root,
                    std::fs::Permissions::from_mode(0o111),
                )
                .unwrap();
            }
            Setup::ProcessExecutablePermission => {
                let process = fixture.proc_root.join("424242");
                let denied = fixture.proc_root.join("denied-executable");
                std::fs::create_dir(&process).unwrap();
                std::fs::create_dir(&denied).unwrap();
                std::fs::write(denied.join("binary"), b"fixture").unwrap();
                std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o000)).unwrap();
                std::os::unix::fs::symlink(denied.join("binary"), process.join("exe")).unwrap();
            }
            Setup::CmdlineParse | Setup::CmdlinePermission | Setup::FdIo => {
                let process = fixture.proc_root.join("424242");
                std::fs::create_dir(&process).unwrap();
                std::os::unix::fs::symlink(
                    env!("CARGO_BIN_EXE_guigu-agent-bridge"),
                    process.join("exe"),
                )
                .unwrap();
                std::fs::write(
                    process.join("cmdline"),
                    if matches!(setup, Setup::CmdlineParse) {
                        b"not-terminated".as_slice()
                    } else {
                        b"candidate\0".as_slice()
                    },
                )
                .unwrap();
                if matches!(setup, Setup::CmdlinePermission) {
                    std::fs::set_permissions(
                        process.join("cmdline"),
                        std::fs::Permissions::from_mode(0o000),
                    )
                    .unwrap();
                } else if matches!(setup, Setup::FdIo) {
                    std::fs::write(process.join("fd"), b"not-a-directory").unwrap();
                }
            }
        }
        let before_rows = durable_rows(&fixture.db).await;
        let before = fixture_tree(&fixture.root);
        let run = || {
            fixture
                .prepare()
                .env("T050_FIXTURE_MIGRATION_PREFLIGHT_DIAGNOSTIC", "1")
                .output()
                .unwrap()
        };
        let first = run();
        let second = run();
        assert_eq!(first.status.code(), Some(3), "{cause}");
        assert_eq!(second.status.code(), Some(3), "{cause}");
        assert_eq!(first.stdout, second.stdout, "{cause}");
        let stdout = String::from_utf8_lossy(&first.stdout);
        assert!(
            stdout.contains(&format!("stage=preflight cause={cause} counts=failures=1")),
            "{stdout}"
        );
        for forbidden in [
            fixture.root.to_string_lossy().as_ref(),
            "424242",
            "inode",
            "fingerprint",
            "token",
            "row",
            "credential",
            "bearer",
            "manifest.json",
        ] {
            assert!(!stdout.contains(forbidden), "{cause}: {stdout}");
        }
        assert_eq!(durable_rows(&fixture.db).await, before_rows, "{cause}");
        assert_eq!(fixture_tree(&fixture.root), before, "{cause}");
        if matches!(setup, Setup::ProcfsPermission) {
            std::fs::set_permissions(&fixture.proc_root, std::fs::Permissions::from_mode(0o700))
                .unwrap();
        } else if matches!(setup, Setup::ProcessExecutablePermission) {
            std::fs::set_permissions(
                fixture.proc_root.join("denied-executable"),
                std::fs::Permissions::from_mode(0o700),
            )
            .unwrap();
        }
    }
}

#[tokio::test]
async fn unreaped_child_refuses_before_manifest_publication() {
    assert_prepare_refusal(
        "unreaped-child",
        "UPDATE delivery_dispositions SET child_fingerprint='child:legacy',reap_status=NULL,reap_at=NULL WHERE delivery_id='delivery'",
    )
    .await;
}

#[tokio::test]
async fn session_drift_after_cas_rolls_back_rows_receipt_evidence_and_tree() {
    let f = Fixture::new().await;
    let before = durable_rows(&f.db).await;
    let tree = fixture_tree(&f.root);
    let rejected = f
        .run()
        .env("T050_FIXTURE_MIGRATION_TEST_DRIFT_SESSION", "1")
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(3));
    assert_eq!(durable_rows(&f.db).await, before);
    assert_eq!(fixture_tree(&f.root), tree);
    assert!(!f.evidence.exists());
}

#[tokio::test]
async fn crash_before_commit_rolls_back_and_retry_succeeds() {
    let f = Fixture::new().await;
    let before = durable_rows(&f.db).await;
    let pause = f.root.join("precommit");
    let mut child = f
        .run()
        .env("T050_FIXTURE_MIGRATION_TEST_PRECOMMIT", &pause)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !pause.exists() {
        assert!(child.try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert_eq!(durable_rows(&f.db).await, before);
    assert_eq!(f.run().output().unwrap().status.code(), Some(0));
}

#[tokio::test]
async fn hostile_targets_and_live_interlock_fail_closed() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new().await;
    let before = durable_rows(&f.db).await;
    let guard =
        guigu_agent_bridge::runtime_admission::Guard::acquire(&f.db, &f.config, "startup").unwrap();
    assert_eq!(f.run().output().unwrap().status.code(), Some(3));
    assert_eq!(durable_rows(&f.db).await, before);
    drop(guard);
    std::fs::write(&f.evidence, b"hostile").unwrap();
    std::fs::set_permissions(&f.evidence, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(f.run().output().unwrap().status.code(), Some(3));
    assert_eq!(durable_rows(&f.db).await, before);
    let f = Fixture::new().await;
    std::fs::remove_file(&f.manifest).unwrap();
    let outside = f.root.join("outside");
    std::fs::write(&outside, b"unchanged").unwrap();
    std::os::unix::fs::symlink(&outside, &f.manifest).unwrap();
    let before = durable_rows(&f.db).await;
    assert_eq!(f.prepare().output().unwrap().status.code(), Some(3));
    assert_eq!(std::fs::read(&outside).unwrap(), b"unchanged");
    assert_eq!(durable_rows(&f.db).await, before);
    let f = Fixture::new().await;
    std::fs::remove_file(&f.manifest).unwrap();
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", f.db.to_string_lossy()));
    }
    let outside = f.root.join("outside");
    std::fs::write(&outside, b"unchanged").unwrap();
    let before = durable_rows(&f.db).await;
    std::os::unix::fs::symlink(&outside, format!("{}-wal", f.db.to_string_lossy())).unwrap();
    assert_eq!(f.prepare().output().unwrap().status.code(), Some(3));
    assert_eq!(std::fs::read(&outside).unwrap(), b"unchanged");
    std::fs::remove_file(format!("{}-wal", f.db.to_string_lossy())).unwrap();
    assert_eq!(durable_rows(&f.db).await, before);
}
