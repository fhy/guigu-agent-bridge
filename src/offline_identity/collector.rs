use std::{ffi::OsString, fs, os::unix::fs::FileTypeExt};

use matrix_sdk::{Client, authentication::matrix::MatrixSession, ruma::api::MatrixVersion};
use sha2::{Digest, Sha256};

use super::{
    policy,
    protocol::{self, Capture, Captured, Ready},
};

pub(super) fn run(args: &[OsString]) -> Result<u8, ()> {
    std::panic::set_hook(Box::new(|_| eprintln!("collector stage=panic")));
    let nonce = args.get(2).and_then(|value| value.to_str()).ok_or(())?;
    if args.len() != 3 || nonce.len() != 64 || !nonce.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(());
    }
    validate_descriptors()?;
    if fs::read_dir("/proc/self/task").map_err(|_| ())?.count() != 1 {
        return Err(());
    }
    if unsafe { libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, 0) } != 0 {
        return Err(());
    }
    policy::install()?;
    policy::verify()?;
    let ready = Ready {
        nonce: nonce.to_owned(),
        policy: policy::digest(),
        executable: format!(
            "{:x}",
            Sha256::digest(fs::read("/proc/self/exe").map_err(|_| ())?)
        ),
        process: std::process::id(),
        generation: generation()?,
        no_new_privs: 1,
        seccomp: 2,
        denied_probes: [libc::EPERM; 6],
    };
    protocol::write_frame(&mut std::io::stdout(), &ready)?;
    let request: Capture = protocol::read_frame(&mut std::io::stdin())?;
    if request.nonce != nonce {
        return Err(());
    }
    if unsafe { libc::close(0) } != 0 {
        return Err(());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(|_| ())
        .inspect_err(|_| eprintln!("collector stage=runtime-create"))?;
    let captured = runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(55), capture(request))
            .await
            .map_err(|_| ())?
    })?;
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    policy::verify()?;
    protocol::write_frame(&mut std::io::stdout(), &captured)?;
    Ok(0)
}

fn validate_descriptors() -> Result<(), ()> {
    for descriptor in 0..=2 {
        let metadata = fs::metadata(format!("/proc/self/fd/{descriptor}")).map_err(|_| ())?;
        if !metadata.file_type().is_fifo() {
            return Err(());
        }
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
        let expected = if descriptor == 0 {
            libc::O_RDONLY
        } else {
            libc::O_WRONLY
        };
        if flags == -1 || flags & libc::O_ACCMODE != expected {
            return Err(());
        }
    }
    Ok(())
}

fn generation() -> Result<String, ()> {
    let stat = fs::read_to_string("/proc/self/stat").map_err(|_| ())?;
    let fields = stat
        .rsplit_once(')')
        .ok_or(())?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let started = fields.get(19).ok_or(())?;
    let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(|_| ())?;
    Ok(format!("{}:{}", boot.trim(), started))
}

async fn capture(request: Capture) -> Result<Captured, ()> {
    if !request.store.is_absolute()
        || !request.store.is_dir()
        || fs::read_dir(&request.store)
            .map_err(|_| ())?
            .next()
            .is_none()
    {
        return Err(());
    }
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(std::time::Duration::from_secs(1))
        .build()
        .map_err(|_| ())
        .inspect_err(|_| eprintln!("collector stage=http-client"))?;
    let client = Client::builder()
        .homeserver_url(&request.homeserver)
        .server_versions([MatrixVersion::V1_11])
        .http_client(http)
        .sqlite_store(&request.store, request.passphrase.as_deref())
        .build()
        .await
        .map_err(|_| ())
        .inspect_err(|_| eprintln!("collector stage=sdk-builder"))?;
    let session = MatrixSession {
        meta: matrix_sdk::SessionMeta {
            user_id: request.user_id.parse().map_err(|_| ())?,
            device_id: request.device_id.into(),
        },
        tokens: matrix_sdk::SessionTokens {
            access_token: request.access_token,
            refresh_token: None,
        },
    };
    client
        .restore_session(session)
        .await
        .map_err(|_| ())
        .inspect_err(|_| eprintln!("collector stage=restore-session"))?;
    let encryption = client.encryption();
    let ed25519 = encryption
        .ed25519_key()
        .await
        .ok_or(())
        .inspect_err(|_| eprintln!("collector stage=ed25519"))?;
    let curve25519 = encryption
        .curve25519_key()
        .await
        .ok_or(())
        .inspect_err(|_| eprintln!("collector stage=curve25519"))?
        .to_base64();
    encryption.wait_for_e2ee_initialization_tasks().await;
    drop(client);
    Ok(Captured {
        ed25519,
        curve25519,
        no_new_privs: 1,
        seccomp: 2,
    })
}
