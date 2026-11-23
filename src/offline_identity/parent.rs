use std::{
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::oneshot,
    time::Instant,
};

use super::{
    executable::{self, PinnedExecutable},
    policy,
    protocol::{Capture, Captured, MAX_FRAME, Ready},
};

pub struct CollectorRequest {
    pub homeserver: String,
    pub user_id: String,
    pub device_id: String,
    pub access_token: String,
    pub store: PathBuf,
    pub passphrase: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IsolationEvidence {
    pub nonce: String,
    pub timeline: CaptureTimeline,
    pub policy: String,
    pub executable: String,
    pub process: u32,
    pub generation: String,
    pub no_new_privs: u8,
    pub seccomp: u8,
    pub denied_probes: [i32; 6],
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureTimeline {
    pub setup_started: DateTime<Utc>,
    pub ready_verified: DateTime<Utc>,
    pub go_sent: DateTime<Utc>,
    pub result_received: DateTime<Utc>,
    pub child_exited: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CollectorResult {
    pub ed25519: String,
    pub curve25519: String,
    pub isolation: IsolationEvidence,
}

#[derive(Debug, thiserror::Error)]
#[error("isolated-collector-refused")]
pub struct CollectorRefused;

pub async fn collect(request: CollectorRequest) -> Result<CollectorResult, CollectorRefused> {
    collect_pinned(request, executable::pin).await
}

#[cfg(feature = "test-support")]
pub async fn collect_from_executable(
    request: CollectorRequest,
    path: PathBuf,
) -> Result<CollectorResult, CollectorRefused> {
    collect_pinned(request, move || executable::pin_path(&path)).await
}

async fn collect_pinned(
    request: CollectorRequest,
    pin: impl FnOnce() -> Result<PinnedExecutable, CollectorRefused> + Send + 'static,
) -> Result<CollectorResult, CollectorRefused> {
    let started = Instant::now();
    let (cancel, cancelled) = oneshot::channel::<()>();
    let (result, received) = oneshot::channel();
    tokio::spawn(async move {
        let outcome = supervise(request, cancelled, started, pin).await;
        let _ = result.send(outcome);
    });
    let outcome = received.await.map_err(|_| CollectorRefused)?;
    drop(cancel);
    outcome
}

async fn supervise(
    request: CollectorRequest,
    mut cancelled: oneshot::Receiver<()>,
    started: Instant,
    pin: impl FnOnce() -> Result<PinnedExecutable, CollectorRefused> + Send + 'static,
) -> Result<CollectorResult, CollectorRefused> {
    let setup = started + Duration::from_secs(5);
    let setup_started = Utc::now();
    let executable = tokio::time::timeout_at(setup, tokio::task::spawn_blocking(pin))
        .await
        .map_err(|_| CollectorRefused)?
        .map_err(|_| CollectorRefused)??;
    let nonce = format!("{:x}", Sha256::digest(uuid::Uuid::now_v7().as_bytes()));
    let path = format!("/proc/self/fd/{}", executable.file.as_raw_fd());
    let mut child = Command::new(path)
        .args(["offline-identity-collector-v1", &nonce])
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| CollectorRefused)?;
    let outcome = tokio::select! {
        biased;
        _ = &mut cancelled => Err(CollectorRefused),
        outcome = tokio::time::timeout_at(started + Duration::from_secs(60), async {
            let mut result = exchange(&mut child, request, &nonce, &executable, setup, setup_started).await?;
            let status = child.wait().await.map_err(|_| CollectorRefused)?;
            if !status.success() { return Err(CollectorRefused); }
            result.isolation.timeline.child_exited = Some(Utc::now());
            Ok(result)
        }) => {
            outcome.unwrap_or(Err(CollectorRefused))
        }
    };
    if outcome.is_err() {
        let _ = child.kill().await;
    }
    outcome
}

async fn exchange(
    child: &mut Child,
    request: CollectorRequest,
    nonce: &str,
    executable: &PinnedExecutable,
    setup: Instant,
    setup_started: DateTime<Utc>,
) -> Result<CollectorResult, CollectorRefused> {
    let process = child.id().ok_or(CollectorRefused)?;
    let mut stdout = child.stdout.take().ok_or(CollectorRefused)?;
    let mut stdin = child.stdin.take().ok_or(CollectorRefused)?;
    let mut stderr = child.stderr.take().ok_or(CollectorRefused)?;
    let ready: Ready = tokio::time::timeout_at(setup, read_frame(&mut stdout))
        .await
        .map_err(|_| CollectorRefused)??;
    validate_ready(&ready, process, nonce, executable)?;
    let ready_verified = Utc::now();
    if Instant::now() >= setup {
        return Err(CollectorRefused);
    }
    send_capture(&mut stdin, request, nonce, setup).await?;
    drop(stdin);
    let go_sent = Utc::now();
    let captured: Captured = read_frame(&mut stdout).await?;
    let result_received = Utc::now();
    let mut extra = [0; 1];
    if stdout
        .read(&mut extra)
        .await
        .map_err(|_| CollectorRefused)?
        != 0
        || stderr
            .read(&mut extra)
            .await
            .map_err(|_| CollectorRefused)?
            != 0
        || captured.no_new_privs != 1
        || captured.seccomp != 2
    {
        return Err(CollectorRefused);
    }
    Ok(CollectorResult {
        ed25519: captured.ed25519,
        curve25519: captured.curve25519,
        isolation: IsolationEvidence {
            nonce: nonce.to_owned(),
            timeline: CaptureTimeline {
                setup_started,
                ready_verified,
                go_sent,
                result_received,
                child_exited: None,
            },
            policy: ready.policy,
            executable: ready.executable,
            process,
            generation: ready.generation,
            no_new_privs: 1,
            seccomp: 2,
            denied_probes: ready.denied_probes,
        },
    })
}

async fn send_capture(
    stdin: &mut tokio::process::ChildStdin,
    request: CollectorRequest,
    nonce: &str,
    setup: Instant,
) -> Result<(), CollectorRefused> {
    let capture = Capture {
        nonce: nonce.to_owned(),
        homeserver: request.homeserver,
        user_id: request.user_id,
        device_id: request.device_id,
        access_token: request.access_token,
        store: request.store,
        passphrase: request.passphrase,
    };
    let mut payload = serde_json::to_vec(&capture).map_err(|_| CollectorRefused)?;
    if payload.len() > MAX_FRAME || Instant::now() >= setup {
        payload.fill(0);
        return Err(CollectorRefused);
    }
    stdin
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .map_err(|_| CollectorRefused)?;
    let sent = stdin.write_all(&payload).await;
    payload.fill(0);
    sent.map_err(|_| CollectorRefused)?;
    stdin.shutdown().await.map_err(|_| CollectorRefused)?;
    Ok(())
}

fn validate_ready(
    ready: &Ready,
    process: u32,
    nonce: &str,
    executable: &PinnedExecutable,
) -> Result<(), CollectorRefused> {
    if ready.nonce != nonce
        || ready.process != process
        || ready.policy != policy::digest()
        || ready.executable != executable.digest
        || ready.no_new_privs != 1
        || ready.seccomp != 2
        || ready.denied_probes != [libc::EPERM; 6]
    {
        return Err(CollectorRefused);
    }
    let root = format!("/proc/{process}");
    let generation = generation(&root)?;
    let metadata = std::fs::metadata(format!("{root}/exe")).map_err(|_| CollectorRefused)?;
    if metadata.dev() != executable.device
        || metadata.ino() != executable.inode
        || generation != ready.generation
    {
        return Err(CollectorRefused);
    }
    let status = std::fs::read_to_string(format!("{root}/status")).map_err(|_| CollectorRefused)?;
    if !status.lines().any(|line| line == "NoNewPrivs:\t1")
        || !status.lines().any(|line| line == "Seccomp:\t2")
        || generation != self::generation(&root)?
    {
        return Err(CollectorRefused);
    }
    Ok(())
}

fn generation(root: &str) -> Result<String, CollectorRefused> {
    let stat = std::fs::read_to_string(format!("{root}/stat")).map_err(|_| CollectorRefused)?;
    let fields = stat
        .rsplit_once(')')
        .ok_or(CollectorRefused)?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let started = fields.get(19).ok_or(CollectorRefused)?;
    let boot =
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(|_| CollectorRefused)?;
    Ok(format!("{}:{}", boot.trim(), started))
}

async fn read_frame<Value: serde::de::DeserializeOwned>(
    input: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<Value, CollectorRefused> {
    let length = input.read_u32().await.map_err(|_| CollectorRefused)? as usize;
    if length == 0 || length > MAX_FRAME {
        return Err(CollectorRefused);
    }
    let mut bytes = vec![0; length];
    input
        .read_exact(&mut bytes)
        .await
        .map_err(|_| CollectorRefused)?;
    serde_json::from_slice(&bytes).map_err(|_| CollectorRefused)
}
