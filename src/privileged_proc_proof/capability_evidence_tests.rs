use super::*;
use std::{
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, PermissionsExt},
    },
    process::Command,
};

const CHILD: &str =
    "privileged_proc_proof::capability_evidence_tests::capability_evidence_fixture_child";
const PLANTED: &str = "planted-secret-capability-path";

fn capability_bytes() -> [u8; 20] {
    let mut value = [0u8; 20];
    value[0..4].copy_from_slice(&0x0200_0001u32.to_le_bytes());
    value[4..8].copy_from_slice(&(1u32 << 19).to_le_bytes());
    value
}

fn test_evidence(path: &Path) -> CapabilityEvidence {
    let mut file = match secure_open(path, libc::O_RDONLY) {
        Ok(file) => file,
        Err(())
            if std::fs::symlink_metadata(path)
                .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            return CapabilityEvidence::NoGeneration;
        }
        Err(()) => return CapabilityEvidence::Refused,
    };
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return CapabilityEvidence::Refused,
    };
    let expected_hash = Sha256::digest(b"reviewed-helper");
    let valid = metadata.is_file()
        && metadata.mode() & 0o7777 == 0o750
        && metadata.nlink() == 1
        && fd_hash(&mut file, 4096) == Ok(expected_hash.into())
        && capability_tuple_named(&file, c"user.t050-capability") == Ok((1 << 19, 0, 1));
    if valid {
        CapabilityEvidence::Present { generation: 1 }
    } else {
        CapabilityEvidence::Refused
    }
}

#[test]
#[ignore]
fn capability_evidence_fixture_child() {
    let evidence = if std::env::var_os("T050_CAPABILITY_EVIDENCE_MODE").as_deref()
        == Some(std::ffi::OsStr::new("absence"))
    {
        let etc = std::env::var_os("T050_CAPABILITY_EVIDENCE_ETC_ROOT").unwrap();
        let libexec = std::env::var_os("T050_CAPABILITY_EVIDENCE_LIBEXEC_ROOT").unwrap();
        if clean_no_generation_at(Path::new(&etc), Path::new(&libexec), unsafe {
            libc::geteuid()
        }) {
            CapabilityEvidence::NoGeneration
        } else {
            CapabilityEvidence::Refused
        }
    } else {
        let Some(path) = std::env::var_os("T050_CAPABILITY_EVIDENCE_FIXTURE") else {
            return;
        };
        test_evidence(Path::new(&path))
    };
    println!("{}", evidence.line());
    std::process::exit(if evidence.is_success() { 0 } else { 3 });
}

fn child() -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--ignored", "--exact", CHILD, "--nocapture"]);
    command
}

fn run(path: &Path) -> std::process::Output {
    child()
        .env("T050_CAPABILITY_EVIDENCE_FIXTURE", path)
        .output()
        .unwrap()
}

fn run_absence(etc: &Path, libexec: &Path, inspection_failure: bool) -> std::process::Output {
    let mut command = child();
    command
        .env("T050_CAPABILITY_EVIDENCE_MODE", "absence")
        .env("T050_CAPABILITY_EVIDENCE_ETC_ROOT", etc)
        .env("T050_CAPABILITY_EVIDENCE_LIBEXEC_ROOT", libexec);
    if inspection_failure {
        command.env("T050_CAPABILITY_EVIDENCE_INSPECTION_FAILURE", "1");
    }
    command.output().unwrap()
}

fn helper(path: &Path) {
    std::fs::write(path, b"reviewed-helper").unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o750)).unwrap();
    let file = File::open(path).unwrap();
    let value = capability_bytes();
    assert_eq!(unsafe {
        libc::fsetxattr(
            file.as_raw_fd(),
            c"user.t050-capability".as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    }, 0);
}

fn fixed_line(output: &std::process::Output) -> &str {
    std::str::from_utf8(&output.stdout)
        .unwrap()
        .lines()
        .find(|line| line.starts_with("production-capability-evidence "))
        .unwrap_or_else(|| {
            panic!(
                "child emitted no fixed record: exit={:?} stdout_bytes={} stderr_bytes={}",
                output.status.code(),
                output.stdout.len(),
                output.stderr.len()
            )
        })
}

fn roots() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let etc = temp.path().join("etc");
    let libexec = temp.path().join("libexec");
    for root in [&etc, &libexec] {
        std::fs::create_dir(root).unwrap();
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    (temp, etc, libexec)
}

fn empty_parents(etc: &Path, libexec: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let artifact = etc.join("guigu-agent-bridge/t050-proc-proof");
    let helper = libexec.join("guigu-agent-bridge");
    std::fs::create_dir_all(&artifact).unwrap();
    std::fs::create_dir_all(&helper).unwrap();
    for path in [etc.join("guigu-agent-bridge"), artifact.clone(), helper.clone()] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    (artifact, helper)
}

fn assert_result(output: &std::process::Output, expected: CapabilityEvidence) {
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(fixed_line(output), expected.line());
    assert!(output.stderr.is_empty());
}

#[test]
fn process_capability_success_is_fixed() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("helper");
    helper(&path);
    let output = run(&path);
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(fixed_line(&output), CapabilityEvidence::Present { generation: 1 }.line());
}

#[test]
fn process_capability_refusal_is_fixed() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("helper");
    std::fs::write(&path, b"reviewed-helper").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o750)).unwrap();
    assert_result(&run(&path), CapabilityEvidence::Refused);
}

#[test]
fn process_no_generation_is_distinct() {
    let temp = tempfile::tempdir().unwrap();
    assert_result(&run(&temp.path().join("absent-helper")), CapabilityEvidence::NoGeneration);
}

#[test]
fn process_cleanup_returns_to_no_generation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("helper");
    helper(&path);
    std::fs::remove_file(&path).unwrap();
    assert_result(&run(&path), CapabilityEvidence::NoGeneration);
    assert!(std::fs::read_dir(temp.path()).unwrap().next().is_none());
}

#[test]
fn process_refusal_never_reflects_path_or_content() {
    let temp = tempfile::Builder::new().prefix(PLANTED).tempdir().unwrap();
    let outside = temp.path().join(PLANTED);
    let path = temp.path().join("helper");
    std::fs::write(&outside, PLANTED).unwrap();
    std::os::unix::fs::symlink(&outside, &path).unwrap();
    let output = run(&path);
    assert_result(&output, CapabilityEvidence::Refused);
    assert!(!fixed_line(&output).contains(PLANTED));
    assert_eq!(std::fs::read_to_string(outside).unwrap(), PLANTED);
}

#[test]
fn absence_process_accepts_missing_and_authenticated_empty_parents() {
    let (_temp, etc, libexec) = roots();
    assert_result(&run_absence(&etc, &libexec, false), CapabilityEvidence::NoGeneration);
    empty_parents(&etc, &libexec);
    assert_result(&run_absence(&etc, &libexec, false), CapabilityEvidence::NoGeneration);
}

#[test]
fn absence_process_rejects_symlink_nondirectory_and_wrong_metadata() {
    for side in ["artifact", "helper"] {
        for kind in ["symlink", "nondirectory", "metadata"] {
            let (_temp, etc, libexec) = roots();
            let outside = etc
                .parent()
                .unwrap()
                .join(format!("{PLANTED}-{side}-{kind}"));
            std::fs::create_dir(&outside).unwrap();
            std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o755)).unwrap();
            let bridge = if side == "artifact" {
                etc.join("guigu-agent-bridge")
            } else {
                libexec.join("guigu-agent-bridge")
            };
            match kind {
                "symlink" => std::os::unix::fs::symlink(&outside, &bridge).unwrap(),
                "nondirectory" => std::fs::write(&bridge, PLANTED).unwrap(),
                "metadata" => {
                    std::fs::create_dir(&bridge).unwrap();
                    std::fs::set_permissions(
                        &bridge,
                        std::fs::Permissions::from_mode(0o775),
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            let output = run_absence(&etc, &libexec, false);
            assert_result(&output, CapabilityEvidence::Refused);
            assert!(!fixed_line(&output).contains(PLANTED));
            assert!(outside.is_dir());
        }
    }
}

#[test]
fn absence_process_rejects_inspection_failure() {
    let (_temp, etc, libexec) = roots();
    empty_parents(&etc, &libexec);
    assert_result(&run_absence(&etc, &libexec, true), CapabilityEvidence::Refused);
}

#[test]
fn absence_process_rejects_every_residue_without_mutation() {
    let cases = [
        (true, "artifacts.v1"),
        (true, "policy.v1"),
        (true, ".provision.lock"),
        (true, "artifacts.v1.quarantine.7"),
        (true, "policy.v1.quarantine.7"),
        (false, "t050-proc-proof"),
        (false, "t050-proc-proof.quarantine.7"),
    ];
    for (artifact_side, name) in cases {
        let (_temp, etc, libexec) = roots();
        let (artifact, helper_parent) = empty_parents(&etc, &libexec);
        let residue = if artifact_side { artifact.join(name) } else { helper_parent.join(name) };
        std::fs::write(&residue, PLANTED).unwrap();
        assert_result(&run_absence(&etc, &libexec, false), CapabilityEvidence::Refused);
        assert_eq!(std::fs::read_to_string(residue).unwrap(), PLANTED);
    }
}

#[test]
fn production_lines_are_fixed_and_redacted() {
    assert_eq!(CapabilityEvidence::Present { generation: 7 }.line(), "production-capability-evidence status=0 cause=none generation=7 capability=cap_sys_ptrace_ep");
    assert_eq!(CapabilityEvidence::NoGeneration.line(), "production-capability-evidence status=3 cause=no-generation generation=none capability=none");
    assert_eq!(CapabilityEvidence::Refused.line(), "production-capability-evidence status=3 cause=artifact generation=unknown capability=unknown");
}
