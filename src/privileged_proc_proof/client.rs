// SPLIT_CLIENT
pub fn prove_runtime_absence(
    guard: &crate::runtime_admission::Guard,
) -> Result<ProofResult, ProofError> {
    prove_runtime_absence_inner(guard).map_err(ProofError::from)
}

fn prove_runtime_absence_inner(guard: &crate::runtime_admission::Guard) -> Result<ProofResult, ()> {
    let binding = guard.proof_binding().map_err(|_| ())?;
    let artifacts = open_artifacts()?;
    verify_parent_artifacts(&artifacts)?;
    guard.verify_proof_identities(
        &identity_from_policy(&artifacts.policy.binary),
        &identity_from_policy(&artifacts.policy.config),
        &identity_from_policy(&artifacts.policy.database_root),
    ).map_err(|_| ())?;
    let (socket, mut child, child_pidfd) = spawn_helper()?;
    let request = parent_request(&artifacts, binding)?;
    if send_packet(socket.as_raw_fd(), &request).is_err() {
        stop_and_reap(&mut child);
        return Err(());
    }
    let response = receive_helper_result(&socket, &mut child, &child_pidfd, binding)?;
    revalidate_parent(guard, &artifacts)?;
    Ok(response.result)
}

fn spawn_helper() -> Result<(OwnedFd, std::process::Child, OwnedFd), ()> {
    let (parent_socket, child_socket) = socketpair()?;
    let child_fd = child_socket.as_raw_fd();
    let mut command = std::process::Command::new(HELPER_PATH);
    command.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null());
    unsafe {
        command.pre_exec(move || {
            if libc::dup2(child_fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::syscall(libc::SYS_close_range, 4u32, u32::MAX, libc::CLOSE_RANGE_CLOEXEC) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().map_err(|_| ())?;
    drop(child_socket);
    match pidfd_open(child.id()) {
        Ok(pidfd) => Ok((parent_socket, child, pidfd)),
        Err(()) => { stop_and_reap(&mut child); Err(()) }
    }
}

fn parent_request(
    artifacts: &Artifacts,
    binding: crate::runtime_admission::ProofBinding,
) -> Result<Vec<u8>, ()> {
    Request {
        manifest: artifacts.manifest_bytes,
        policy: artifacts.policy_bytes.clone(),
        manifest_identity: ArtifactIdentity::from_metadata(&artifacts.manifest_metadata),
        policy_identity: ArtifactIdentity::from_metadata(&artifacts.policy_metadata),
        nonce: binding.nonce,
        mode: binding.mode,
        parent_pid: binding.pid,
        parent_starttime: binding.starttime,
        admission_device: binding.device,
        admission_inode: binding.inode,
        deadline_ms: 2000,
    }.encode()
}

fn receive_helper_result(
    socket: &OwnedFd,
    child: &mut std::process::Child,
    child_pidfd: &OwnedFd,
    binding: crate::runtime_admission::ProofBinding,
) -> Result<Response, ()> {
    let timeout = libc::timeval { tv_sec:3, tv_usec:0 };
    if unsafe { libc::setsockopt(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_RCVTIMEO, (&timeout as *const libc::timeval).cast(), std::mem::size_of_val(&timeout) as libc::socklen_t) } != 0 {
        stop_and_reap(child);
        return Err(());
    }
    let response = recv_packet(socket.as_raw_fd()).and_then(|(bytes, credentials)| {
        if credentials.pid != child.id() as i32 || credentials.uid != unsafe { libc::getuid() } { return Err(()); }
        Response::decode(&bytes)
    });
    let Ok(response) = response else { stop_and_reap(child); return Err(()); };
    let status = bounded_wait(child, Instant::now() + Duration::from_secs(1))?;
    let mut extra = [0u8; 1];
    let extra_len = unsafe { libc::recv(socket.as_raw_fd(), extra.as_mut_ptr().cast(), 1, libc::MSG_DONTWAIT) };
    if !status.success() || response.nonce != binding.nonce || extra_len != 0 || pidfd_live(child_pidfd) {
        return Err(());
    }
    Ok(response)
}

fn revalidate_parent(
    guard: &crate::runtime_admission::Guard,
    artifacts: &Artifacts,
) -> Result<(), ()> {
    guard.revalidate().map_err(|_| ())?;
    reopen_matches(Path::new(MANIFEST_PATH), &artifacts.manifest_file,
        &artifacts.manifest_metadata, 512, Sha256::digest(artifacts.manifest_bytes).into())?;
    reopen_matches(Path::new(POLICY_PATH), &artifacts.policy_file,
        &artifacts.policy_metadata, MAX_POLICY_SIZE, artifacts.manifest.policy_hash)?;
    verify_parent_artifacts(artifacts)
}
