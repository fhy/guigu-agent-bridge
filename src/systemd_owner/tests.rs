use super::*;
use crate::storage::{ReliabilityStore, connect, migrate};

fn private_root() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("r107-owner-")
        .tempdir_in(std::env::var_os("HOME").unwrap())
        .unwrap()
}

pub(super) fn modeled() -> Capsule {
    Capsule {
        version: 1,
        owner: "owner".into(),
        generation: 1,
        initial_revision: 1,
        fingerprint: "fp".into(),
        pid: 123,
        starttime: 456,
        boot: "boot".into(),
        boottime: 1,
        captured_at: "now".into(),
        database: FileBinding {
            path: "/db".into(),
            device: 1,
            inode: 2,
            digest: "db".into(),
        },
        config: FileBinding {
            path: "/config".into(),
            device: 1,
            inode: 3,
            digest: "config".into(),
        },
        binary: FileBinding {
            path: "/bin".into(),
            device: 1,
            inode: 4,
            digest: "binary".into(),
        },
        manager_pid: 1,
        manager_start: 1,
        manager_uid: 1,
        unit: "r107-test.service".into(),
        invocation: "a".repeat(32),
        domain: "/r107-test.service".into(),
        domain_device: 1,
        domain_inode: 99,
        start_monotonic: "100".into(),
    }
}

pub(super) fn terminal_fields(capsule: &Capsule) -> BTreeMap<String, String> {
    [
        ("Id", capsule.unit.clone()),
        ("InvocationID", capsule.invocation.clone()),
        ("KillMode", "control-group".into()),
        ("Restart", "no".into()),
        ("RemainAfterExit", "yes".into()),
        ("ExecMainPID", capsule.pid.to_string()),
        ("ExecMainStartTimestampMonotonic", "100".into()),
        ("ActiveState", "active".into()),
        ("SubState", "exited".into()),
        ("MainPID", "0".into()),
        ("ControlGroup", "".into()),
        ("ExecMainExitTimestampMonotonic", "200".into()),
        ("ExecMainCode", "1".into()),
        ("ExecMainStatus", "0".into()),
        ("Result", "success".into()),
    ]
    .into_iter()
    .map(|(key, value)| (key.into(), value))
    .collect()
}

#[test]
fn terminal_identity_and_cleanup_fields_are_all_required() {
    let capsule = modeled();
    let fields = terminal_fields(&capsule);
    assert!(terminal(&capsule, &fields).is_ok());
    for (key, value) in [
        ("InvocationID", "different"),
        ("Id", "other.service"),
        ("ExecMainPID", "124"),
        ("ExecMainStartTimestampMonotonic", "101"),
        ("KillMode", "process"),
        ("Restart", "always"),
        ("RemainAfterExit", "no"),
        ("MainPID", "123"),
        ("ControlGroup", "/foreign"),
        ("ActiveState", "inactive"),
        ("SubState", "running"),
        ("ExecMainExitTimestampMonotonic", "0"),
        ("ExecMainCode", "0"),
        ("ExecMainStatus", "-1"),
    ] {
        let mut changed = fields.clone();
        changed.insert(key.into(), value.into());
        assert!(terminal(&capsule, &changed).is_err(), "{key}");
    }
    for key in fields.keys() {
        let mut changed = fields.clone();
        changed.remove(key);
        assert!(terminal(&capsule, &changed).is_err(), "missing {key}");
    }
}

#[test]
fn private_capsules_reject_missing_links_modes_and_overwrite() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let root = private_root();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.path().join("capsule");
    assert!(read_private(&path).is_err());
    write_private(&path, b"original").unwrap();
    assert_eq!(read_private(&path).unwrap(), b"original");
    assert!(write_private(&path, b"replacement").is_err());
    let link = root.path().join("link");
    symlink(&path, &link).unwrap();
    assert!(read_private(&link).is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_private(&path).is_err());
}

async fn fixture() -> (tempfile::TempDir, sqlx::SqlitePool, ReliabilityStore, Proof) {
    let root = private_root();
    let pool = connect(root.path().join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let store = ReliabilityStore::new(pool.clone());
    let evidence = RuntimeOwnerEvidence::capture().unwrap();
    let now = Utc::now().to_rfc3339();
    assert!(
        store
            .begin_runtime_with_evidence("owner", "fp", &now, &evidence)
            .await
            .unwrap()
    );
    let mut capsule = modeled();
    capsule.pid = evidence.pid;
    capsule.starttime = evidence.proc_starttime;
    capsule.boot = evidence.boot_id;
    capsule.boottime = evidence.boottime_ns;
    let binding = "{\"systemd_capsule_v1\":\"test\"}".to_owned();
    sqlx::query("UPDATE runtime_owner_evidence SET child_snapshot=? WHERE instance_token='owner'")
        .bind(&binding)
        .execute(&pool)
        .await
        .unwrap();
    assert!(store.stopping_runtime("owner", &now).await.unwrap());
    assert!(store.stop_runtime("owner", &now).await.unwrap());
    let terminal = terminal(&capsule, &terminal_fields(&capsule)).unwrap();
    let proof = Proof {
        capsule,
        binding,
        terminal,
        observed_at: now,
        boottime: evidence.boottime_ns,
    };
    (root, pool, store, proof)
}

#[tokio::test]
async fn lifecycle_is_cas_bound_and_duplicate_is_read_only() {
    let (_root, pool, store, proof) = fixture().await;
    assert!(!store.finalize_systemd_owner(&proof, 1).await.unwrap());
    assert!(store.finalize_systemd_owner(&proof, 2).await.unwrap());
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(store.finalize_systemd_owner(&proof, 2).await.unwrap());
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(before, after);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence WHERE state='reaped'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn lifecycle_rejects_missing_binding_unknown_and_effects() {
    for case in 0..7 {
        let (_root, pool, store, mut proof) = fixture().await;
        match case {
            0 => proof.binding = "hand-written".into(),
            1 => proof.capsule.starttime += 1,
            2 => {
                sqlx::query("UPDATE runtime_owner_evidence SET state='unknown' WHERE is_current=1")
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            3 => {
                sqlx::query(
                    "UPDATE runtime_owner_evidence SET child_snapshot='{}' WHERE is_current=1",
                )
                .execute(&pool)
                .await
                .unwrap();
            }
            4 => {
                sqlx::query(
                    "INSERT INTO runtime_instances VALUES('foreign','t','t','active','foreign')",
                )
                .execute(&pool)
                .await
                .unwrap();
            }
            5 => proof.observed_at = "2000-01-01T00:00:00Z".into(),
            _ => {
                sqlx::query("INSERT INTO workspace_claims VALUES('claim','owner','task','/workspace','[]',1,1,'active','t','t')").execute(&pool).await.unwrap();
            }
        }
        assert!(
            !store.finalize_systemd_owner(&proof, 2).await.unwrap(),
            "case {case}"
        );
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence WHERE state='reaped'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 0);
    }
}

#[tokio::test]
async fn publication_failure_rolls_back_owner_and_evidence() {
    let (_root, pool, store, proof) = fixture().await;
    sqlx::query("CREATE TRIGGER refuse_reaped BEFORE INSERT ON runtime_owner_evidence WHEN NEW.state='reaped' BEGIN SELECT RAISE(ABORT,'test'); END").execute(&pool).await.unwrap();
    assert!(store.finalize_systemd_owner(&proof, 2).await.is_err());
    let current: (i64, String) = sqlx::query_as(
        "SELECT evidence_revision,state FROM runtime_owner_evidence WHERE is_current=1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(current, (2, "scan_pending".into()));
}

#[tokio::test]
#[ignore = "launched only by the real transient unit parent test"]
async fn transient_child_capture() {
    let root = PathBuf::from(std::env::var_os("R107_CAPSULE_TEST_ROOT").unwrap());
    let pool = connect(root.join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let store = ReliabilityStore::new(pool);
    let evidence = RuntimeOwnerEvidence::capture().unwrap();
    let fingerprint = RuntimeOwnerEvidence::fingerprint().unwrap();
    let now = Utc::now().to_rfc3339();
    assert!(
        store
            .begin_runtime_with_evidence("real-owner", &fingerprint, &now, &evidence)
            .await
            .unwrap()
    );
    if std::env::var_os("R107_CAPSULE_ABNORMAL").is_some() {
        std::process::exit(42);
    }
    assert!(
        store
            .stopping_runtime("real-owner", &Utc::now().to_rfc3339())
            .await
            .unwrap()
    );
    assert!(
        store
            .stop_runtime("real-owner", &Utc::now().to_rfc3339())
            .await
            .unwrap()
    );
    if std::env::var_os("R107_CAPSULE_FOREIGN_CHILD").is_some() {
        let _child = std::process::Command::new("/bin/sleep")
            .arg("60")
            .spawn()
            .unwrap();
    }
}

async fn transient_case(kind: u8) {
    use std::os::unix::fs::PermissionsExt;
    let root = private_root();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = root.path().join("config");
    std::fs::write(&config, "fixture-config").unwrap();
    let capsule = root.path().join("capsule.json");
    let unit = format!("r107-capsule-{}.service", uuid::Uuid::now_v7());
    let mut command = tokio::process::Command::new("/usr/bin/systemd-run");
    command
        .args([
            "--user",
            "--quiet",
            "--property=Type=exec",
            "--property=RemainAfterExit=yes",
            "--property=KillMode=control-group",
            "--property=Restart=no",
        ])
        .arg(format!("--unit={unit}"))
        .arg(format!("--setenv=BRIDGE_OWNER_CONFIG={}", config.display()))
        .arg(format!(
            "--setenv=BRIDGE_OWNER_CAPSULE={}",
            capsule.display()
        ))
        .arg(format!(
            "--setenv=R107_CAPSULE_TEST_ROOT={}",
            root.path().display()
        ));
    if kind == 1 {
        command.arg("--setenv=R107_CAPSULE_FOREIGN_CHILD=1");
    }
    if kind == 2 {
        command.arg("--setenv=R107_CAPSULE_ABNORMAL=1");
    }
    command.arg(std::env::current_exe().unwrap()).args([
        "--exact",
        "systemd_owner::tests::transient_child_capture",
        "--ignored",
    ]);
    assert!(command.status().await.unwrap().success());
    let result = async {
        for _ in 0..100 {
            let fields = properties(&unit).await?;
            if field(&fields, "SubState")? == "exited" || field(&fields, "ActiveState")? == "failed"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        observe(&capsule, &root.path().join("state.db"), &config).await
    }
    .await;
    let stopped = tokio::process::Command::new("/usr/bin/systemctl")
        .args(["--user", "stop", &unit])
        .status()
        .await
        .unwrap();
    assert!(stopped.success());
    if kind == 1 {
        assert!(result.is_err());
        return;
    }
    let proof = result.unwrap();
    let pool = connect(root.path().join("state.db")).await.unwrap();
    let store = ReliabilityStore::new(pool);
    assert!(
        store
            .finalize_systemd_owner(&proof, if kind == 2 { 1 } else { 2 })
            .await
            .unwrap()
    );
}

#[tokio::test]
#[ignore = "requires real user systemd; run explicitly under the R107 build lock"]
async fn real_transient_unit_terminal_record_and_foreign_child() {
    transient_case(0).await;
    transient_case(1).await;
    transient_case(2).await;
}
