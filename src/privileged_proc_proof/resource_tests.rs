use std::{
    os::unix::process::ExitStatusExt,
    process::{Command, Stdio},
};

const CHILD: &str = "privileged_proc_proof::resource_tests::cpu_limit_fixture_child";

#[test]
#[ignore]
fn cpu_limit_fixture_child() {
    let Some(limit) = std::env::var_os("T050_PROOF_CPU_LIMIT_FIXTURE") else {
        return;
    };
    let limit = limit.to_str().unwrap().parse::<libc::rlim_t>().unwrap();
    let value = libc::rlimit {
        rlim_cur: limit,
        rlim_max: limit,
    };
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_CPU, &value) }, 0);

    let mut digest = sha2::Sha256::default();
    let block = [0x5au8; 64 * 1024];
    loop {
        use sha2::Digest;
        digest.update(block);
        std::hint::black_box(&digest);
    }
}

fn resource_child(seconds: u64) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", CHILD, "--nocapture"])
        .env_remove("RUST_TEST_THREADS")
        .env("T050_PROOF_CPU_LIMIT_FIXTURE", seconds.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap()
}

#[test]
fn helper_cpu_budget_covers_streaming_authentication_and_resource_exit_is_bounded() {
    assert_eq!(super::HELPER_CPU_SECONDS, 60);
    assert_eq!(super::HELPER_SCAN_DEADLINE_MS, 5000);
    assert!(super::HELPER_CPU_SECONDS < super::HELPER_RESPONSE_TIMEOUT.as_secs());

    let output = resource_child(1);
    assert_eq!(output.status.signal(), Some(libc::SIGKILL));
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}
