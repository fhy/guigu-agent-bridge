use std::{
    fs::File,
    io::{Seek, SeekFrom, Write as _},
    os::fd::{AsRawFd, FromRawFd, RawFd},
    path::Path,
    process::{Command, Stdio},
};

use super::{
    ChildOutcome, PRODUCTION_PROVISIONER, StageCause, classify_child, emit_boundary_result,
    provision_binding, stage_policy,
};

fn anonymous_policy() -> Result<File, StageCause> {
    let fd = unsafe {
        libc::memfd_create(
            c"guigu-t050-policy".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    if fd < 0 {
        Err(StageCause::Output)
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn seal_and_rewind(policy: &mut File) -> Result<(), StageCause> {
    policy
        .seek(SeekFrom::Start(0))
        .map_err(|_| StageCause::Output)?;
    let seals = libc::F_SEAL_SEAL | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_WRITE;
    if unsafe { libc::fcntl(policy.as_raw_fd(), libc::F_ADD_SEALS, seals) } < 0 {
        return Err(StageCause::Output);
    }
    Ok(())
}

fn prepare_policy_with(
    stage: impl FnOnce(RawFd) -> Result<[u8; 32], StageCause>,
) -> Result<(File, [u8; 32]), StageCause> {
    let mut policy = anonymous_policy()?;
    let hash = stage(policy.as_raw_fd())?;
    seal_and_rewind(&mut policy)?;
    Ok((policy, hash))
}

fn encode_hash(hash: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in hash {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn production_identity() -> Result<(), ()> {
    let current = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|_| ())?;
    let expected = std::fs::canonicalize(PRODUCTION_PROVISIONER).map_err(|_| ())?;
    if current == expected { Ok(()) } else { Err(()) }
}

fn run_boundary_with_policy(
    parent: &str,
    helper: &str,
    policy: &str,
    policy_input: File,
) -> Result<(), ()> {
    let binding = provision_binding(parent, helper, policy);
    println!(
        "production-provision-boundary phase=start status=attempting binding_sha256={binding}"
    );
    std::io::stdout().flush().map_err(|_| ())?;
    if production_identity().is_err() {
        emit_boundary_result(&binding, 3, "identity")?;
        return Err(());
    }
    let output = Command::new("/usr/bin/sudo")
        .args(["-n", "/bin/sh", "-c"])
        .arg("exec 3<&0; exec \"$1\" production-provision --parent-sha \"$2\" --helper-sha \"$3\" --policy-sha \"$4\"")
        .arg("sh")
        .arg(PRODUCTION_PROVISIONER)
        .arg(parent)
        .arg(helper)
        .arg(policy)
        .stdin(Stdio::from(policy_input))
        .output()
        .map_err(|_| ())?;
    match classify_child(&output, &binding) {
        ChildOutcome::Success => emit_boundary_result(&binding, 0, "none"),
        ChildOutcome::ProvisionRefusal => {
            emit_boundary_result(&binding, 3, "provision")?;
            Err(())
        }
        ChildOutcome::ChildFailure => {
            emit_boundary_result(&binding, 3, "child")?;
            Err(())
        }
        ChildOutcome::InvalidResult => {
            emit_boundary_result(&binding, 3, "result")?;
            Err(())
        }
    }
}

pub(super) fn run_boundary_fd3(parent: &str, helper: &str, policy: &str) -> Result<(), ()> {
    if production_identity().is_err() {
        let binding = provision_binding(parent, helper, policy);
        println!(
            "production-provision-boundary phase=start status=attempting binding_sha256={binding}"
        );
        std::io::stdout().flush().map_err(|_| ())?;
        emit_boundary_result(&binding, 3, "identity")?;
        return Err(());
    }
    let policy_fd = unsafe { libc::dup(3) };
    if policy_fd < 0 {
        let binding = provision_binding(parent, helper, policy);
        println!(
            "production-provision-boundary phase=start status=attempting binding_sha256={binding}"
        );
        std::io::stdout().flush().map_err(|_| ())?;
        emit_boundary_result(&binding, 3, "policy")?;
        return Err(());
    }
    run_boundary_with_policy(parent, helper, policy, unsafe {
        File::from_raw_fd(policy_fd)
    })
}

fn run(config: &Path, database: &Path, parent: &str, helper: &str) -> Result<(), ()> {
    let (policy, hash) = match prepare_policy_with(|fd| stage_policy(config, database, fd)) {
        Ok(value) => value,
        Err(cause) => {
            eprintln!("stage-policy status=3 cause={}", cause.code());
            return Err(());
        }
    };
    eprintln!("stage-policy status=0 cause=none");
    run_boundary_with_policy(parent, helper, &encode_hash(hash), policy)
}

pub(super) fn run_cli(mut args: impl Iterator<Item = String>) -> Result<(), ()> {
    fn set_once(slot: &mut Option<String>, value: String) -> Result<(), ()> {
        if slot.is_some() {
            Err(())
        } else {
            *slot = Some(value);
            Ok(())
        }
    }

    let mut config = None;
    let mut database = None;
    let mut parent = None;
    let mut helper = None;
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .filter(|value| !value.starts_with('-'))
            .ok_or(())?;
        match flag.as_str() {
            "--config" => set_once(&mut config, value)?,
            "--database-root" => set_once(&mut database, value)?,
            "--parent-sha" => set_once(&mut parent, value)?,
            "--helper-sha" => set_once(&mut helper, value)?,
            _ => return Err(()),
        }
    }
    let (Some(config), Some(database), Some(parent), Some(helper)) =
        (config, database, parent, helper)
    else {
        return Err(());
    };
    run(Path::new(&config), Path::new(&database), &parent, &helper)
}

#[cfg(test)]
mod tests {
    use std::{
        io::Read,
        os::fd::FromRawFd,
        process::{Command, Stdio},
    };

    use super::*;
    use sha2::{Digest, Sha256};

    const POLICY: &[u8] = b"canonical-policy-fixture";

    fn fixture_stage(fd: RawFd) -> Result<[u8; 32], StageCause> {
        let duplicate = unsafe { libc::dup(fd) };
        assert!(duplicate >= 0);
        let mut file = unsafe { File::from_raw_fd(duplicate) };
        std::io::Write::write_all(&mut file, POLICY).unwrap();
        file.sync_all().unwrap();
        Ok(Sha256::digest(POLICY).into())
    }

    #[test]
    fn staged_policy_is_sealed_rewound_and_hash_bound() {
        let (mut policy, hash) = prepare_policy_with(fixture_stage).unwrap();
        let mut bytes = Vec::new();
        policy.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, POLICY);
        assert_eq!(hash, <[u8; 32]>::from(Sha256::digest(POLICY)));
        assert_eq!(
            unsafe { libc::fcntl(policy.as_raw_fd(), libc::F_GET_SEALS) } & 0x0f,
            0x0f
        );
        assert!(std::io::Write::write_all(&mut policy, b"drift").is_err());
    }

    #[test]
    fn staging_refusal_creates_no_usable_policy() {
        assert!(matches!(
            prepare_policy_with(|_| Err(StageCause::Database)),
            Err(StageCause::Database)
        ));
    }

    #[test]
    fn real_child_receives_complete_policy_from_owned_fd() {
        let (policy, _) = prepare_policy_with(fixture_stage).unwrap();
        let output = Command::new("/bin/sh")
            .args(["-c", "bytes=$(wc -c); test \"$bytes\" -eq 24"])
            .stdin(Stdio::from(policy))
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(output.stdout.is_empty());
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn duplicate_or_missing_orchestration_arguments_refuse_before_staging() {
        assert!(run_cli(["--config".into(), "/x".into()].into_iter()).is_err());
        assert!(
            run_cli(
                [
                    ("--config", "/x"),
                    ("--config", "/y"),
                    ("--database-root", "/d"),
                    ("--parent-sha", "00"),
                    ("--helper-sha", "00"),
                ]
                .into_iter()
                .flat_map(|(flag, value)| [flag.to_owned(), value.to_owned()]),
            )
            .is_err()
        );
    }
}
