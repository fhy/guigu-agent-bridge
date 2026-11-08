use super::*;
use std::os::unix::fs::symlink;

fn owner() -> Owner {
    Owner {
        token: "old".into(),
        generation: 1,
        revision: 2,
        pid: 10,
        starttime: 100,
        boot: "boot".into(),
        fingerprint: "pid:10;exe:1:2:3:4:5;cgroup:0::/observer".into(),
    }
}
fn stat(start: i64) -> String {
    let mut fields = vec!["0".to_owned(); 20];
    fields[0] = "S".into();
    fields[1] = "1".into();
    fields[19] = start.to_string();
    format!("20 (fixture) {}", fields.join(" "))
}
fn fixture() -> tempfile::TempDir {
    let root = tempfile::Builder::new()
        .prefix("r105-proc-")
        .tempdir_in("/tmp")
        .unwrap();
    fs::create_dir_all(root.path().join("sys/kernel/random")).unwrap();
    fs::write(root.path().join("sys/kernel/random/boot_id"), "boot").unwrap();
    let p = root.path().join("20");
    fs::create_dir(&p).unwrap();
    fs::write(p.join("stat"), stat(110)).unwrap();
    fs::write(p.join("cgroup"), "0::/unrelated").unwrap();
    fs::write(p.join("environ"), b"USER=fixture\0").unwrap();
    fs::write(p.join("cmdline"), b"fixture\0").unwrap();
    symlink("/tmp", p.join("cwd")).unwrap();
    fs::create_dir(p.join("fd")).unwrap();
    root
}
#[test]
fn native_succession_refuses_live_reused_pid_and_unknown_boot() {
    let root = fixture();
    let mut old = owner();
    assert!(
        inspect(
            root.path(),
            &old,
            &["/unique/generation".into()],
            &["/unique/generation".into()],
            "DEVICE",
            99
        )
        .is_ok()
    );
    fs::create_dir(root.path().join("10")).unwrap();
    assert!(inspect(root.path(), &old, &[], &[], "DEVICE", 99).is_err());
    fs::remove_dir(root.path().join("10")).unwrap();
    old.boot = "other".into();
    assert!(inspect(root.path(), &old, &[], &[], "DEVICE", 99).is_err());
    old = owner();
    old.starttime = 0;
    assert!(inspect(root.path(), &old, &[], &[], "DEVICE", 99).is_err());
}
#[test]
fn native_succession_refuses_escaped_children_and_open_resources() {
    for variant in 0..5 {
        let root = fixture();
        let p = root.path().join("20");
        match variant {
            0 => fs::write(p.join("environ"), b"MATRIX_DEVICE_ID=DEVICE\0").unwrap(),
            1 => fs::write(p.join("cmdline"), b"worker\0/unique/generation/state.db\0").unwrap(),
            2 => symlink("/unique/generation/state.db", p.join("fd/3")).unwrap(),
            3 => fs::write(p.join("cgroup"), "0::/observer").unwrap(),
            _ => fs::remove_file(p.join("environ")).unwrap(),
        }
        assert!(
            inspect(
                root.path(),
                &owner(),
                &["/unique/generation".into()],
                &["/unique/generation".into()],
                "DEVICE",
                99
            )
            .is_err(),
            "{variant}"
        );
    }
}
#[test]
fn native_succession_does_not_require_unrelated_preexisting_process_access() {
    let root = fixture();
    let p = root.path().join("20");
    fs::write(p.join("stat"), stat(99)).unwrap();
    fs::remove_file(p.join("environ")).unwrap();
    fs::remove_file(p.join("cwd")).unwrap();
    symlink("/shared/workspace", p.join("cwd")).unwrap();
    assert!(
        inspect(
            root.path(),
            &owner(),
            &["/unique/generation".into(), "/shared/workspace".into()],
            &["/unique/generation".into()],
            "DEVICE",
            99
        )
        .is_ok()
    );
    symlink("/unique/generation/state.db", p.join("fd/3")).unwrap();
    assert!(
        inspect(
            root.path(),
            &owner(),
            &["/unique/generation".into(), "/shared/workspace".into()],
            &["/unique/generation".into()],
            "DEVICE",
            99
        )
        .is_err()
    );
}
#[test]
fn native_succession_observes_real_child_death_without_claiming_join_history() {
    let mut child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    let pid = child.id();
    let p = PathBuf::from(format!("/proc/{pid}"));
    let starttime = fs::read_to_string(p.join("stat"))
        .unwrap()
        .rsplit_once(") ")
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse()
        .unwrap();
    let mut old = owner();
    old.pid = i64::from(pid);
    old.starttime = starttime;
    old.boot = fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .unwrap()
        .trim()
        .into();
    old.fingerprint = format!(
        "pid:{pid};exe:1:2:3:4:5;cgroup:{}",
        fs::read_to_string(p.join("cgroup")).unwrap().trim()
    );
    let live = owner_absent(Path::new("/proc"), &old);
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(live.is_err());
    assert!(owner_absent(Path::new("/proc"), &old).is_ok());
}

#[test]
fn native_succession_physical_config_inode_and_content_drift_refuse() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config");
    fs::write(&path, "original").unwrap();
    let binding = Physical::capture(&path, true).unwrap();
    assert!(binding.verify().is_ok());
    fs::write(&path, "modified").unwrap();
    assert!(binding.verify().is_err());
    fs::remove_file(&path).unwrap();
    fs::write(&path, "original").unwrap();
    assert!(binding.verify().is_err());
    fs::remove_file(&path).unwrap();
    symlink("/dev/null", &path).unwrap();
    assert!(Physical::capture(&path, true).is_err());
}
