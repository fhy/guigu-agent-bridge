use std::{
    io::{Read, Write},
    process::{Command, Stdio},
    time::Duration,
};

use matrix_sdk::{
    Client, SessionMeta, SessionTokens, authentication::matrix::MatrixSession,
    ruma::api::MatrixVersion,
};
use serde_json::{Value, json};
use tokio::time::timeout;

fn spawn(nonce: &str) -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args(["offline-identity-collector-v1", nonce])
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn read_frame(input: &mut impl Read) -> Value {
    let mut header = [0; 4];
    input.read_exact(&mut header).unwrap();
    let length = u32::from_be_bytes(header) as usize;
    assert!(length <= 65536);
    let mut bytes = vec![0; length];
    input.read_exact(&mut bytes).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn write_frame(output: &mut impl Write, value: Value) {
    let bytes = serde_json::to_vec(&value).unwrap();
    output
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .unwrap();
    output.write_all(&bytes).unwrap();
    output.flush().unwrap();
}

#[test]
fn real_collector_reaches_filtered_ready_without_credentials() {
    let nonce = "a".repeat(64);
    let mut child = spawn(&nonce);
    let ready = read_frame(child.stdout.as_mut().unwrap());
    assert_eq!(ready["nonce"], nonce);
    assert_eq!(ready["process"], child.id());
    assert_eq!(ready["no_new_privs"], 1);
    assert_eq!(ready["seccomp"], 2);
    assert_eq!(ready["policy"].as_str().unwrap().len(), 64);
    let status = std::fs::read_to_string(format!("/proc/{}/status", child.id())).unwrap();
    assert!(status.lines().any(|line| line == "NoNewPrivs:\t1"));
    assert!(status.lines().any(|line| line == "Seccomp:\t2"));
    drop(child.stdin.take());
    assert_eq!(child.wait().unwrap().code(), Some(5));
}

#[test]
fn collector_rejects_non_private_stdio_before_ready() {
    let output = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args(["offline-identity-collector-v1", &"b".repeat(64)])
        .env_clear()
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(5));
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn collector_rejects_oversized_truncated_and_wrong_nonce_frames_before_store_access() {
    for kind in ["oversized", "truncated", "nonce", "trailing"] {
        let root = tempfile::Builder::new()
            .prefix("r092-protocol-")
            .tempdir_in("/tmp")
            .unwrap();
        let store = root.path().join("must-not-open");
        std::fs::create_dir(&store).unwrap();
        std::fs::write(store.join("sentinel"), b"preserve").unwrap();
        let mut child = spawn(&"d".repeat(64));
        read_frame(child.stdout.as_mut().unwrap());
        match kind {
            "oversized" => child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(&65537u32.to_be_bytes())
                .unwrap(),
            "truncated" => child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(&[0, 0, 0, 10, b'{'])
                .unwrap(),
            _ => {
                write_frame(
                    child.stdin.as_mut().unwrap(),
                    json!({
                        "nonce": if kind == "nonce" { "e".repeat(64) } else { "d".repeat(64) },
                        "homeserver": "http://127.0.0.1:1", "user_id": "@test:example.test",
                        "device_id": "FIXTURE", "access_token": "synthetic-secret",
                        "store": store, "passphrase": null
                    }),
                );
                if kind == "trailing" {
                    child.stdin.as_mut().unwrap().write_all(b"extra").unwrap();
                }
            }
        }
        drop(child.stdin.take());
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(5), "{kind}");
        assert!(output.stdout.is_empty(), "{kind}");
        assert!(output.stderr.is_empty(), "{kind}");
        assert_eq!(std::fs::read_dir(&store).unwrap().count(), 1, "{kind}");
        assert_eq!(
            std::fs::read(store.join("sentinel")).unwrap(),
            b"preserve",
            "{kind}"
        );
    }
}

#[test]
fn collector_closes_an_inherited_descriptor_without_touching_parent_file() {
    use std::os::{fd::AsRawFd, unix::process::CommandExt};
    let root = tempfile::Builder::new()
        .prefix("r092-inherited-")
        .tempdir_in("/tmp")
        .unwrap();
    let path = root.path().join("parent-owned");
    std::fs::write(&path, b"preserve").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let descriptor = file.as_raw_fd();
    let mut command = Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"));
    command
        .args(["offline-identity-collector-v1", &"f".repeat(64)])
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    unsafe {
        command.pre_exec(move || {
            if libc::dup2(descriptor, 198) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().unwrap();
    read_frame(child.stdout.as_mut().unwrap());
    assert!(std::path::Path::new(&format!("/proc/{}/fd/1", child.id())).exists());
    assert!(!std::path::Path::new(&format!("/proc/{}/fd/198", child.id())).exists());
    drop(child.stdin.take());
    assert_eq!(child.wait().unwrap().code(), Some(5));
    assert_eq!(std::fs::read(&path).unwrap(), b"preserve");
    assert!(file.metadata().is_ok());
}

async fn initialize_store(
    store: &std::path::Path,
    server: &wiremock::MockServer,
) -> (String, String) {
    let client = Client::builder()
        .homeserver_url(server.uri())
        .server_versions([MatrixVersion::V1_11])
        .sqlite_store(store, None)
        .build()
        .await
        .unwrap();
    client
        .restore_session(MatrixSession {
            meta: SessionMeta {
                user_id: "@collector:example.test".parse().unwrap(),
                device_id: "FIXTURE".into(),
            },
            tokens: SessionTokens {
                access_token: "synthetic-secret".into(),
                refresh_token: None,
            },
        })
        .await
        .unwrap();
    timeout(
        Duration::from_secs(10),
        client.encryption().wait_for_e2ee_initialization_tasks(),
    )
    .await
    .unwrap();
    let expected_ed25519 = client.encryption().ed25519_key().await.unwrap();
    let expected_curve25519 = client
        .encryption()
        .curve25519_key()
        .await
        .unwrap()
        .to_base64();
    drop(client);
    server.reset().await;
    (expected_ed25519, expected_curve25519)
}

fn capture_existing_store(store: std::path::PathBuf, uri: String) -> Value {
    let nonce = "c".repeat(64);
    let mut child = spawn(&nonce);
    let ready = read_frame(child.stdout.as_mut().unwrap());
    assert_eq!(ready["nonce"], nonce);
    assert_eq!(ready["denied_probes"], json!([1, 1, 1, 1, 1, 1]));
    write_frame(
        child.stdin.as_mut().unwrap(),
        json!({
            "nonce": nonce, "homeserver": uri, "user_id": "@collector:example.test",
            "device_id": "FIXTURE", "access_token": "synthetic-secret", "store": store,
            "passphrase": null
        }),
    );
    drop(child.stdin.take());
    let mut header = [0; 4];
    if let Err(error) = child.stdout.as_mut().unwrap().read_exact(&mut header) {
        let output = child.wait_with_output().unwrap();
        panic!(
            "collector result: {error}; status={}; stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let length = u32::from_be_bytes(header) as usize;
    assert!(length <= 65536);
    let mut bytes = vec![0; length];
    child
        .stdout
        .as_mut()
        .unwrap()
        .read_exact(&mut bytes)
        .unwrap();
    let captured: Value = serde_json::from_slice(&bytes).unwrap();
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0), "{:?}", output.stderr);
    assert!(output.stderr.is_empty());
    assert_eq!(captured["no_new_privs"], 1);
    assert_eq!(captured["seccomp"], 2);
    captured
}

#[tokio::test]
async fn real_sdk_reads_existing_keys_with_egress_denied() {
    let root = tempfile::Builder::new()
        .prefix("r092-collector-")
        .tempdir_in("/tmp")
        .unwrap();
    let store = root.path().join("sdk");
    let server = wiremock::MockServer::start().await;
    let (expected_ed25519, expected_curve25519) = initialize_store(&store, &server).await;
    let uri = server.uri();
    let result = tokio::task::spawn_blocking(move || capture_existing_store(store, uri));
    let captured = timeout(Duration::from_secs(60), result)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(captured["ed25519"], expected_ed25519);
    assert_eq!(captured["curve25519"], expected_curve25519);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn real_parent_validates_handshake_and_reaps_successful_collector() {
    use guigu_agent_bridge::offline_identity::{CollectorRequest, collect_from_executable};
    use sha2::Digest;
    let root = tempfile::Builder::new()
        .prefix("r092-parent-success-")
        .tempdir_in("/tmp")
        .unwrap();
    let original = env!("CARGO_BIN_EXE_guigu-agent-bridge");
    let started = std::time::Instant::now();
    let mut fingerprint = sha2::Sha256::new();
    std::io::copy(
        &mut std::fs::File::open(original).unwrap(),
        &mut fingerprint,
    )
    .unwrap();
    eprintln!("debug executable hash elapsed={:?}", started.elapsed());
    let executable = root.path().join("collector");
    let stripped = Command::new("strip")
        .arg("--strip-debug")
        .arg("-o")
        .arg(&executable)
        .arg(original)
        .output()
        .unwrap();
    assert!(stripped.status.success(), "{stripped:?}");
    let store = root.path().join("sdk");
    let server = wiremock::MockServer::start().await;
    let (ed25519, curve25519) = initialize_store(&store, &server).await;
    let result = collect_from_executable(
        CollectorRequest {
            homeserver: server.uri(),
            user_id: "@collector:example.test".into(),
            device_id: "FIXTURE".into(),
            access_token: "synthetic-secret".into(),
            store,
            passphrase: None,
        },
        executable,
    )
    .await
    .unwrap();
    assert_eq!(result.ed25519, ed25519);
    assert_eq!(result.curve25519, curve25519);
    assert_eq!(result.isolation.no_new_privs, 1);
    assert_eq!(result.isolation.seccomp, 2);
    assert!(!std::path::Path::new(&format!("/proc/{}", result.isolation.process)).exists());
    assert!(server.received_requests().await.unwrap().is_empty());
}
