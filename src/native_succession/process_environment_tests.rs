use super::tests::{fixture, owner, stat};
use super::*;
use std::os::unix::fs::symlink;

fn scan(root: &Path) -> Result<(BootObservation, String)> {
    inspect(
        root,
        &owner(),
        &[
            "/private".into(),
            "/shared/private".into(),
            "/shared".into(),
        ],
        &["/private".into(), "/shared/private".into()],
        "DEVICE",
        99,
    )
}

#[test]
fn same_boot_shared_pwd_and_oldpwd_do_not_establish_ownership() {
    for start in [100, 110] {
        for environment in [
            "PWD=/shared\0",
            "OLDPWD=/shared/project\0",
            "PWD=/shared\0OLDPWD=/shared/project\0USER=fixture\0",
        ] {
            let root = fixture();
            fs::write(root.path().join("20/stat"), stat(start)).unwrap();
            fs::write(root.path().join("20/environ"), environment).unwrap();
            let (observation, _) = scan(root.path()).unwrap();
            assert!(!observation.different_boot());
            assert_eq!(observation.absence, "same-boot-pid-absent");
        }
    }
}

#[test]
fn same_boot_environment_exception_preserves_private_device_and_other_variables() {
    for environment in [
        "PWD=/private/store\0",
        "OLDPWD=/shared/private/store\0",
        "PWD=/shared\0MATRIX_DEVICE_ID=DEVICE\0",
        "PWD=/shared\0CONFIG=/private/config\0",
        "PWD=/shared\0WORKSPACE=/shared\0",
        "NOT_PWD=/shared\0",
        "PWD_SUFFIX=/shared\0",
        "PWD=/shared\0OLDPWD=/private\0",
        "PWD/shared\0",
    ] {
        let root = fixture();
        fs::write(root.path().join("20/environ"), environment).unwrap();
        let error = scan(root.path()).unwrap_err();
        assert!(error.to_string().contains("related-escaped-process"));
    }
}

#[test]
fn shared_pwd_never_masks_domain_command_cwd_or_fd_evidence() {
    for (selector, predicate) in [
        ("domain", "related-domain-member"),
        ("command", "related-escaped-process"),
        ("cwd", "related-workspace"),
        ("fd", "related-open-resource"),
    ] {
        let root = fixture();
        let p = root.path().join("20");
        fs::write(p.join("environ"), b"PWD=/shared\0OLDPWD=/shared/project\0").unwrap();
        match selector {
            "domain" => fs::write(p.join("cgroup"), "0::/observer").unwrap(),
            "command" => fs::write(p.join("cmdline"), b"worker\0/shared/project\0").unwrap(),
            "cwd" => {
                fs::remove_file(p.join("cwd")).unwrap();
                symlink("/shared/project", p.join("cwd")).unwrap();
            }
            "fd" => symlink("/shared/project/file", p.join("fd/3")).unwrap(),
            _ => unreachable!(),
        }
        let error = scan(root.path()).unwrap_err();
        assert!(error.to_string().contains(predicate), "{selector}: {error}");
    }
}

#[test]
fn shared_pwd_does_not_bypass_missing_or_unreadable_evidence() {
    for (field, predicate) in [
        ("stat", "process-stat"),
        ("cgroup", "relevant-domain-unreadable"),
        ("environ", "relevant-environment-unreadable"),
        ("cmdline", "relevant-command-unreadable"),
        ("cwd", "relevant-cwd-unreadable"),
        ("fd", "relevant-resources-unreadable"),
    ] {
        let root = fixture();
        let p = root.path().join("20");
        fs::write(p.join("environ"), b"PWD=/shared\0").unwrap();
        if field == "fd" {
            fs::remove_dir(p.join(field)).unwrap();
        } else {
            fs::remove_file(p.join(field)).unwrap();
        }
        let error = scan(root.path()).unwrap_err();
        assert!(error.to_string().contains(predicate), "{field}: {error}");
    }
    let root = fixture();
    let path = root.path().join("20/environ");
    fs::remove_file(&path).unwrap();
    fs::create_dir(path).unwrap();
    assert!(
        scan(root.path())
            .unwrap_err()
            .to_string()
            .contains("relevant-environment-unreadable")
    );
}

#[test]
fn shared_pwd_still_refuses_a_starttime_change_during_scan() {
    use std::io::Write;
    let root = fixture();
    let p = root.path().join("20");
    fs::write(p.join("environ"), b"PWD=/shared\0").unwrap();
    let command = p.join("cmdline");
    fs::remove_file(&command).unwrap();
    let name = std::ffi::CString::new(command.as_os_str().as_encoded_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    // Opening the writer rendezvous with the scanner's command read, after it
    // captured the first stat. No timing/sleep or detached helper is needed.
    let writer = std::thread::spawn(move || {
        let mut output = fs::OpenOptions::new().write(true).open(command).unwrap();
        fs::write(p.join("stat"), stat(111)).unwrap();
        output.write_all(b"worker\0").unwrap();
    });
    let result = scan(root.path());
    writer.join().unwrap();
    let error = result.unwrap_err();
    assert!(error.to_string().contains("process-raced"), "{error}");
}
