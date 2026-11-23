use super::tests::{fixture, owner, stat};
use super::*;
use std::os::unix::fs::symlink;

const CURRENT: &str = "22222222-2222-4222-8222-222222222222";

fn cross_fixture(start: i64, reused: bool) -> tempfile::TempDir {
    let root = fixture();
    fs::write(root.path().join("sys/kernel/random/boot_id"), CURRENT).unwrap();
    fs::write(root.path().join("20/stat"), stat(start)).unwrap();
    if reused {
        fs::rename(root.path().join("20"), root.path().join("10")).unwrap();
    }
    root
}
fn scan(root: &Path) -> Result<(BootObservation, String)> {
    inspect(
        root,
        &owner(),
        &["/private".into(), "/shared".into()],
        &["/private".into()],
        "DEVICE",
        99,
    )
}

#[test]
fn cross_boot_ticks_and_reused_pid_never_skip_current_competitors() {
    for start in [1, 100, 1000] {
        for reused in [false, true] {
            for selector in ["group", "subgroup", "device", "command", "cwd", "fd"] {
                let root = cross_fixture(start, reused);
                let p = root.path().join(if reused { "10" } else { "20" });
                assert!(scan(root.path()).is_ok());
                match selector {
                    "group" => fs::write(p.join("cgroup"), "0::/observer").unwrap(),
                    "subgroup" => fs::write(p.join("cgroup"), "0::/observer/child").unwrap(),
                    "device" => fs::write(p.join("environ"), b"MATRIX_DEVICE_ID=DEVICE\0").unwrap(),
                    "command" => {
                        fs::write(p.join("cmdline"), b"worker\0/private/config\0").unwrap()
                    }
                    "cwd" => {
                        fs::remove_file(p.join("cwd")).unwrap();
                        symlink("/private", p.join("cwd")).unwrap();
                    }
                    "fd" => symlink("/private/state.db", p.join("fd/3")).unwrap(),
                    _ => unreachable!(),
                }
                assert!(scan(root.path()).is_err(), "{start}/{reused}/{selector}");
            }
        }
    }
}

#[test]
fn cross_boot_requires_stable_readable_relevance_anchors() {
    for field in ["stat", "cgroup", "cmdline"] {
        let root = cross_fixture(1, false);
        fs::remove_file(root.path().join("20").join(field)).unwrap();
        assert!(scan(root.path()).is_err(), "{field}");
    }
    for (field, value) in [
        ("stat", "bad"),
        ("cmdline", ""),
        ("cgroup", ""),
        ("cgroup", "0::/a\n0::/b"),
    ] {
        let root = cross_fixture(1, false);
        fs::write(root.path().join("20").join(field), value).unwrap();
        assert!(scan(root.path()).is_err(), "{field}/{value}");
    }
    let root = cross_fixture(1, false);
    let p = root.path().join("20");
    fs::remove_file(p.join("environ")).unwrap();
    fs::remove_dir(p.join("fd")).unwrap();
    fs::remove_file(p.join("cwd")).unwrap();
    symlink("/shared", p.join("cwd")).unwrap();
    // Distinct readable anchors; no universal ptrace or shared-workspace veto.
    assert!(scan(root.path()).is_ok());
    fs::write(p.join("cmdline"), b"worker\0/private/store\0").unwrap();
    assert!(scan(root.path()).is_err());
}

#[test]
fn boot_identity_and_capture_observation_are_not_silently_replaced() {
    let root = cross_fixture(1, true);
    let path = root.path().join("sys/kernel/random/boot_id");
    let old = owner();
    let proof = owner_absent(root.path(), &old).unwrap();
    assert!(proof.different_boot());
    assert!(proof.valid_for(&old));
    for value in [
        "",
        "boot",
        "00000000-0000-0000-0000-000000000000",
        " 22222222-2222-4222-8222-222222222222",
        "22222222-2222-4222-8222-222222222222\n\n",
    ] {
        fs::write(&path, value).unwrap();
        assert!(owner_absent(root.path(), &old).is_err());
    }
    fs::remove_file(&path).unwrap();
    assert!(owner_absent(root.path(), &old).is_err());
    fs::write(&path, "33333333-3333-4333-8333-333333333333").unwrap();
    assert!(proof.revalidate(root.path(), &old).is_err());
    fs::write(&path, CURRENT).unwrap();
    assert!(proof.revalidate(root.path(), &old).is_ok());
    let mut invalid = old.clone();
    invalid.boot = "bad".into();
    assert!(owner_absent(root.path(), &invalid).is_err());
    fs::write(&path, &old.boot).unwrap();
    assert!(owner_absent(root.path(), &old).is_err()); // same-boot reused PID
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn previous_boot_still_refuses_a_real_owned_resource_holder() {
    use std::io::Write;
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("resource");
    fs::write(&file, "fixture").unwrap();
    let mut child = Child(
        std::process::Command::new("bash")
            .args([
                "-c",
                "IFS= read -r file; exec 3<\"$file\"; printf ready; IFS= read -r done",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap(),
    );
    writeln!(child.0.stdin.as_mut().unwrap(), "{}", file.display()).unwrap();
    child.0.stdin.as_mut().unwrap().flush().unwrap();
    let mut ready = [0; 5];
    child
        .0
        .stdout
        .as_mut()
        .unwrap()
        .read_exact(&mut ready)
        .unwrap();
    assert_eq!(&ready, b"ready");
    let proc = PathBuf::from(format!("/proc/{}", child.0.id()));
    assert_eq!(fs::read_link(proc.join("fd/3")).unwrap(), file);
    // Inspect real kernel evidence for the owned helper, without unrelated host
    // processes racing this test's specific fd-selector assertion.
    let view = tempfile::tempdir().unwrap();
    fs::create_dir_all(view.path().join("sys/kernel/random")).unwrap();
    symlink(
        "/proc/sys/kernel/random/boot_id",
        view.path().join("sys/kernel/random/boot_id"),
    )
    .unwrap();
    symlink(&proc, view.path().join(child.0.id().to_string())).unwrap();
    let old = owner();
    let error = inspect(
        view.path(),
        &old,
        &[root.path().into()],
        &[root.path().into()],
        "FIXTURE-DEVICE",
        std::process::id(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("preexisting-open-resource"),
        "{error}"
    );
    // Drop always terminates/reaps the helper even if an assertion fails.
}

pub(super) fn config(root: &Path) -> Config {
    for name in ["crypto", "sessions"] {
        fs::create_dir(root.join(name)).unwrap();
    }
    if !root.join("state.db").exists() {
        fs::write(root.join("state.db"), "fixture").unwrap();
    }
    fs::write(root.join("config.toml"), "immutable fixture").unwrap();
    crate::config::load_from_str_with_env(
        &format!(
            r#"
[bridge]
database = {:?}
session_root = {:?}
[transports.matrix]
enabled = true
homeserver = "https://example.test"
user_id = "@bridge:example.test"
device_id = "DEVICE"
device_trusted = true
access_token = "{{env:R115_FIXTURE_TOKEN}}"
crypto_store_path = {:?}
"#,
            root.join("state.db"),
            root.join("sessions"),
            root.join("crypto")
        ),
        &std::collections::BTreeMap::from([("R115_FIXTURE_TOKEN".into(), "synthetic".into())]),
    )
    .unwrap()
}

#[test]
fn full_exclusion_revalidation_binds_boot_owner_and_physical_identity() {
    let proc = cross_fixture(1, false);
    let root = tempfile::tempdir().unwrap();
    let cfg = config(root.path());
    let path = root.path().join("config.toml");
    let old = owner();
    let proof = capture_at(proc.path(), &old, &cfg, &path).unwrap();
    assert_eq!(proof.owner, old);
    assert_eq!(proof.kind, "current-scoped-exclusion-v2");
    proof.revalidate_at(proc.path(), &cfg, &path).unwrap();
    fs::write(
        proc.path().join("sys/kernel/random/boot_id"),
        "33333333-3333-4333-8333-333333333333",
    )
    .unwrap();
    assert!(proof.revalidate_at(proc.path(), &cfg, &path).is_err());
    fs::write(proc.path().join("sys/kernel/random/boot_id"), CURRENT).unwrap();
    fs::write(&path, "changed").unwrap();
    assert!(proof.revalidate_at(proc.path(), &cfg, &path).is_err());
}

#[tokio::test]
async fn cross_boot_precommit_drift_rolls_back_and_exact_retry_confirms() {
    use crate::native_succession::{
        history_boot_tests as boots, history_fixture as history, state,
    };
    use crate::storage::recovery_eligibility::writer::succession::{confirmed, publish};
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("state.db");
    let pool = history::seed(&database).await;
    boots::previous_boot(&pool).await;
    let cfg = config(root.path());
    let path = root.path().join("config.toml");
    let proc = cross_fixture(1, false);
    let mut c = pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let proof = capture_at(proc.path(), &state.owner, &cfg, &path).unwrap();
    let before = history::snapshot(&pool).await;
    let notices = history::notices(&pool).await;
    let calls = std::cell::Cell::new(0);
    let boot_path = proc.path().join("sys/kernel/random/boot_id");
    let error = publish(&pool, &state, &proof, &[], || {
        calls.set(calls.get() + 1);
        if calls.get() == 2 {
            fs::write(&boot_path, "33333333-3333-4333-8333-333333333333").unwrap();
        }
        proof.revalidate_at(proc.path(), &cfg, &path)
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("boot-proof-drift"), "{error}");
    assert_eq!(history::snapshot(&pool).await, before);
    assert!(!confirmed(&database, &state, &proof).await.unwrap());
    fs::write(&boot_path, CURRENT).unwrap();
    publish(&pool, &state, &proof, &[], || {
        proof.revalidate_at(proc.path(), &cfg, &path)
    })
    .await
    .unwrap();
    assert!(confirmed(&database, &state, &proof).await.unwrap());
    assert!(
        publish(&pool, &state, &proof, &[], || proof.revalidate_at(
            proc.path(),
            &cfg,
            &path
        ))
        .await
        .is_err()
    );
    assert!(confirmed(&database, &state, &proof).await.unwrap());
    boots::check_record(&pool).await;
    assert_eq!(history::notices(&pool).await, notices);
    pool.close().await;
}
