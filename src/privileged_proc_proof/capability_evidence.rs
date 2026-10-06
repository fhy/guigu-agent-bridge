#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityEvidence {
    Present { generation: u64 },
    NoGeneration,
    Refused,
}

impl CapabilityEvidence {
    pub fn is_success(self) -> bool {
        matches!(self, Self::Present { .. })
    }

    pub fn line(self) -> String {
        match self {
            Self::Present { generation } => format!(
                "production-capability-evidence status=0 cause=none generation={generation} capability=cap_sys_ptrace_ep"
            ),
            Self::NoGeneration => {
                "production-capability-evidence status=3 cause=no-generation generation=none capability=none".into()
            }
            Self::Refused => {
                "production-capability-evidence status=3 cause=artifact generation=unknown capability=unknown".into()
            }
        }
    }
}

fn leaf_absent_for_evidence(path: &str) -> bool {
    std::fs::symlink_metadata(path)
        .map(|_| false)
        .unwrap_or_else(|error| error.kind() == std::io::ErrorKind::NotFound)
}

fn directory_has_no_quarantine(path: &str, prefixes: &[&[u8]]) -> bool {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
        Err(_) => return false,
    };
    for entry in entries {
        let Ok(entry) = entry else {
            return false;
        };
        if prefixes
            .iter()
            .any(|prefix| entry.file_name().as_bytes().starts_with(prefix))
        {
            return false;
        }
    }
    true
}

fn clean_no_generation() -> bool {
    [
        MANIFEST_PATH,
        POLICY_PATH,
        HELPER_PATH,
        "/etc/guigu-agent-bridge/t050-proc-proof/.provision.lock",
    ]
    .iter()
    .all(|path| leaf_absent_for_evidence(path))
        && directory_has_no_quarantine(
            "/etc/guigu-agent-bridge/t050-proc-proof",
            &[b"artifacts.v1.quarantine", b"policy.v1.quarantine."],
        )
        && directory_has_no_quarantine(
            "/usr/libexec/guigu-agent-bridge",
            &[b"t050-proc-proof.quarantine."],
        )
}

fn authenticate_capability_generation() -> Result<u64, ()> {
    let artifacts = open_artifacts()?;
    validate_system_path(Path::new(HELPER_PATH))?;
    let mut helper = secure_open(Path::new(HELPER_PATH), libc::O_RDONLY)?;
    let helper_metadata = helper.metadata().map_err(|_| ())?;
    if !exact_metadata(
        &helper_metadata,
        artifacts.manifest.helper_uid,
        artifacts.manifest.helper_gid,
        artifacts.manifest.helper_mode,
    ) || fd_hash(&mut helper, MAX_HELPER_SIZE)? != artifacts.manifest.helper_hash
        || capability_tuple(&helper)?
            != (
                artifacts.manifest.capability_permitted,
                artifacts.manifest.capability_inheritable,
                artifacts.manifest.capability_flags,
            )
    {
        return Err(());
    }
    let manifest_hash = Sha256::digest(artifacts.manifest_bytes);
    reopen_matches(
        Path::new(MANIFEST_PATH),
        &artifacts.manifest_file,
        &artifacts.manifest_metadata,
        512,
        manifest_hash.into(),
    )?;
    let policy_hash = Sha256::digest(&artifacts.policy_bytes);
    reopen_matches(
        Path::new(POLICY_PATH),
        &artifacts.policy_file,
        &artifacts.policy_metadata,
        MAX_POLICY_SIZE as u64,
        policy_hash.into(),
    )?;
    reopen_matches(
        Path::new(HELPER_PATH),
        &helper,
        &helper_metadata,
        MAX_HELPER_SIZE,
        artifacts.manifest.helper_hash,
    )?;
    if capability_tuple(&helper)?
        != (
            artifacts.manifest.capability_permitted,
            artifacts.manifest.capability_inheritable,
            artifacts.manifest.capability_flags,
        )
    {
        return Err(());
    }
    Ok(artifacts.manifest.generation)
}

pub fn production_capability_evidence() -> CapabilityEvidence {
    match authenticate_capability_generation() {
        Ok(generation) => CapabilityEvidence::Present { generation },
        Err(()) if clean_no_generation() => CapabilityEvidence::NoGeneration,
        Err(()) => CapabilityEvidence::Refused,
    }
}

#[cfg(test)]
mod capability_evidence_tests {
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
            Err(()) if std::fs::symlink_metadata(path).is_err_and(|error| {
                error.kind() == std::io::ErrorKind::NotFound
            }) => return CapabilityEvidence::NoGeneration,
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
        let Some(path) = std::env::var_os("T050_CAPABILITY_EVIDENCE_FIXTURE") else {
            return;
        };
        let evidence = test_evidence(Path::new(&path));
        println!("{}", evidence.line());
        std::process::exit(if evidence.is_success() { 0 } else { 3 });
    }

    fn run(path: &Path) -> std::process::Output {
        Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", CHILD, "--nocapture"])
            .env("T050_CAPABILITY_EVIDENCE_FIXTURE", path)
            .output()
            .unwrap()
    }

    fn helper(path: &Path) {
        std::fs::write(path, b"reviewed-helper").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o750)).unwrap();
        let file = File::open(path).unwrap();
        let value = capability_bytes();
        assert_eq!(
            unsafe {
                libc::fsetxattr(
                    file.as_raw_fd(),
                    c"user.t050-capability".as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                )
            },
            0
        );
    }

    fn fixed_line(output: &std::process::Output) -> &str {
        std::str::from_utf8(&output.stdout)
            .unwrap()
            .lines()
            .find(|line| line.starts_with("production-capability-evidence "))
            .unwrap()
    }

    #[test]
    fn process_capability_success_is_fixed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("helper");
        helper(&path);
        let output = run(&path);
        assert_eq!(output.status.code(), Some(0));
        assert_eq!(
            fixed_line(&output),
            CapabilityEvidence::Present { generation: 1 }.line()
        );
    }

    #[test]
    fn process_capability_refusal_is_fixed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("helper");
        std::fs::write(&path, b"reviewed-helper").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o750)).unwrap();
        let output = run(&path);
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(fixed_line(&output), CapabilityEvidence::Refused.line());
    }

    #[test]
    fn process_no_generation_is_distinct() {
        let temp = tempfile::tempdir().unwrap();
        let output = run(&temp.path().join("absent-helper"));
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(fixed_line(&output), CapabilityEvidence::NoGeneration.line());
    }

    #[test]
    fn process_cleanup_returns_to_no_generation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("helper");
        helper(&path);
        std::fs::remove_file(&path).unwrap();
        let output = run(&path);
        assert_eq!(fixed_line(&output), CapabilityEvidence::NoGeneration.line());
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
        let line = fixed_line(&output);
        assert_eq!(line, CapabilityEvidence::Refused.line());
        assert!(!line.contains(PLANTED));
        assert_eq!(std::fs::read_to_string(outside).unwrap(), PLANTED);
    }

    #[test]
    fn production_lines_are_fixed_and_redacted() {
        assert_eq!(
            CapabilityEvidence::Present { generation: 7 }.line(),
            "production-capability-evidence status=0 cause=none generation=7 capability=cap_sys_ptrace_ep"
        );
        assert_eq!(
            CapabilityEvidence::NoGeneration.line(),
            "production-capability-evidence status=3 cause=no-generation generation=none capability=none"
        );
        assert_eq!(
            CapabilityEvidence::Refused.line(),
            "production-capability-evidence status=3 cause=artifact generation=unknown capability=unknown"
        );
    }

}
