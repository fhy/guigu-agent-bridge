// SPLIT_HELPER
fn identity_from_policy(value: &FileIdentity) -> crate::runtime_admission::ProofIdentity {
    crate::runtime_admission::ProofIdentity {
        path: value.path.clone(),
        device: value.device,
        inode: value.inode,
    }
}

fn verify_policy_object(value: &FileIdentity, directory: bool) -> Result<(), ()> {
    let file = secure_open(
        Path::new(std::str::from_utf8(&value.path).map_err(|_| ())?),
        libc::O_PATH | if directory { libc::O_DIRECTORY } else { 0 },
    )?;
    let metadata = file.metadata().map_err(|_| ())?;
    if metadata.dev() != value.device
        || metadata.ino() != value.inode
        || metadata.uid() != value.uid
        || metadata.gid() != value.gid
        || metadata.mode() & 0o7777 != value.mode
        || file_type(metadata.mode()) != value.file_type
    {
        Err(())
    } else {
        Ok(())
    }
}

fn verify_helper_artifacts(
    artifacts: &Artifacts,
    request: &Request,
    parent_pid: u32,
) -> Result<(), ()> {
    if artifacts.manifest_bytes != request.manifest
        || artifacts.policy_bytes != request.policy
        || ArtifactIdentity::from_metadata(&artifacts.manifest_metadata)
            != request.manifest_identity
        || ArtifactIdentity::from_metadata(&artifacts.policy_metadata) != request.policy_identity
    {
        return Err(());
    }
    let mut own = secure_open(Path::new(HELPER_PATH), libc::O_RDONLY)?;
    let own_meta = own.metadata().map_err(|_| ())?;
    let live_own_meta = std::fs::metadata("/proc/self/exe").map_err(|_| ())?;
    if !same_metadata(&own_meta, &live_own_meta)
        || fd_hash(&mut own, 64 * 1024 * 1024)? != artifacts.manifest.helper_hash
        || capability_tuple(&own)?
            != (
                artifacts.manifest.capability_permitted,
                artifacts.manifest.capability_inheritable,
                artifacts.manifest.capability_flags,
            )
    {
        return Err(());
    }
    let parent_path =
        Path::new(std::str::from_utf8(&artifacts.policy.binary.path).map_err(|_| ())?);
    let mut parent = secure_open(parent_path, libc::O_RDONLY)?;
    let parent_meta = parent.metadata().map_err(|_| ())?;
    let live_parent_meta = std::fs::metadata(format!("/proc/{parent_pid}/exe")).map_err(|_| ())?;
    if !exact_metadata(
        &parent_meta,
        artifacts.manifest.parent_uid,
        artifacts.manifest.parent_gid,
        artifacts.manifest.parent_mode,
    ) || !same_metadata(&parent_meta, &live_parent_meta)
        || fd_hash(&mut parent, 128 * 1024 * 1024)? != artifacts.manifest.parent_hash
    {
        return Err(());
    }
    verify_policy_object(&artifacts.policy.binary, false)?;
    verify_policy_object(&artifacts.policy.config, false)?;
    verify_policy_object(&artifacts.policy.database_root, true)?;
    Ok(())
}

fn verify_parent_command(pid: u32, mode: u8) -> Result<(), ()> {
    let bytes = read_bounded(&PathBuf::from(format!("/proc/{pid}/cmdline")), MAX_CMDLINE)?;
    if bytes.is_empty() || bytes.last() != Some(&0) {
        return Err(());
    }
    let args = bytes
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if command_shape(&args, mode) { Ok(()) } else { Err(()) }
}

fn command_shape(args: &[&[u8]], mode: u8) -> bool {
    let flags_match = |expected: &[&[u8]]| {
        args.len() == 2 + expected.len() * 2
            && expected.iter().enumerate().all(|(index, flag)| args.get(2 + index * 2).copied() == Some(*flag))
    };
    let prepare_flags: &[&[u8]] = &[b"--root", b"--config", b"--database", b"--unit", b"--manifest", b"--evidence", b"--operation-id"];
    match mode {
        1 => args.get(1).copied() == Some(b"prepare-isolated-legacy-fixture-migration-manifest")
            && flags_match(&[prepare_flags, &[b"--commit-timestamp"]].concat()),
        2 => args.get(1).copied() == Some(b"migrate-isolated-legacy-fixture")
            && flags_match(&[b"--manifest"]),
        _ => false,
    }
}

struct ParentGeneration {
    pidfd: OwnedFd,
    device: u64,
    inode: u64,
    starttime: u64,
}

fn authenticate_parent_generation(pid: u32, starttime: u64) -> Result<ParentGeneration, ()> {
    let process = PathBuf::from(format!("/proc/{pid}"));
    let metadata = std::fs::metadata(&process).map_err(|_| ())?;
    let pidfd = pidfd_open(pid)?;
    if !pidfd_live(&pidfd) || process_starttime(&process)? != starttime {
        return Err(());
    }
    Ok(ParentGeneration { pidfd, device:metadata.dev(), inode:metadata.ino(), starttime })
}

fn revalidate_parent_generation(pid: u32, generation: &ParentGeneration) -> Result<(), ()> {
    let process = PathBuf::from(format!("/proc/{pid}"));
    let metadata = std::fs::metadata(&process).map_err(|_| ())?;
    if !pidfd_live(&generation.pidfd) || metadata.dev() != generation.device
        || metadata.ino() != generation.inode || process_starttime(&process)? != generation.starttime
    { Err(()) } else { Ok(()) }
}

pub fn helper_main() -> Result<(), ProofError> {
    helper_main_inner().map_err(ProofError::from)
}

fn helper_main_inner() -> Result<(), ()> {
    if !entry_fd_table_is_exact()
        || unsafe { libc::getuid() } == 0
        || unsafe { libc::geteuid() } == 0
    {
        return Err(());
    }
    verify_initial_capability()?;
    set_resource_limits()?;
    let parent_pid = u32::try_from(unsafe { libc::getppid() }).map_err(|_| ())?;
    let (request_bytes, peer) = recv_packet(3)?;
    if peer.pid != parent_pid as i32 || peer.uid != unsafe { libc::getuid() } {
        return Err(());
    }
    let request = Request::decode(&request_bytes)?;
    let mut extra = [0u8; 1];
    let extra_result = unsafe {
        libc::recv(
            3,
            extra.as_mut_ptr().cast(),
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    if extra_result > 0 || request.parent_pid != parent_pid || request.deadline_ms == 0 {
        return Err(());
    }
    let artifacts = open_artifacts()?;
    if peer.uid != artifacts.policy.operator_uid || peer.gid != artifacts.policy.operator_gid {
        return Err(());
    }
    let parent_generation = authenticate_parent_generation(parent_pid, request.parent_starttime)?;
    verify_helper_artifacts(&artifacts, &request, parent_pid)?;
    verify_parent_command(parent_pid, request.mode)?;
    let lock =
        PathBuf::from(std::str::from_utf8(&artifacts.policy.database_root.path).map_err(|_| ())?)
            .join(".runtime-admission.lock");
    let binding = crate::runtime_admission::ProofBinding {
        nonce: request.nonce,
        mode: request.mode,
        pid: request.parent_pid,
        starttime: request.parent_starttime,
        device: request.admission_device,
        inode: request.admission_inode,
    };
    crate::runtime_admission::verify_proof_binding(&lock, binding).map_err(|_| ())?;
    crate::runtime_admission::verify_proof_identities(
        &lock,
        &identity_from_policy(&artifacts.policy.binary),
        &identity_from_policy(&artifacts.policy.config),
        &identity_from_policy(&artifacts.policy.database_root),
    )
    .map_err(|_| ())?;
    install_deny_seccomp()?;
    finish_helper(&artifacts, &request, parent_pid, &parent_generation, &lock, binding)
}

fn finish_helper(
    artifacts: &Artifacts,
    request: &Request,
    parent_pid: u32,
    parent_generation: &ParentGeneration,
    lock: &Path,
    binding: crate::runtime_admission::ProofBinding,
) -> Result<(), ()> {
    let result = stable_scan(
        Path::new("/proc"),
        &artifacts.policy,
        parent_pid,
        helper_deadline(request.deadline_ms),
    );
    revalidate_parent_generation(parent_pid, parent_generation)?;
    verify_helper_artifacts(artifacts, request, parent_pid)?;
    crate::runtime_admission::verify_proof_binding(lock, binding).map_err(|_| ())?;
    reopen_matches(
        Path::new(MANIFEST_PATH),
        &artifacts.manifest_file,
        &artifacts.manifest_metadata,
        512,
        Sha256::digest(artifacts.manifest_bytes).into(),
    )?;
    reopen_matches(
        Path::new(POLICY_PATH),
        &artifacts.policy_file,
        &artifacts.policy_metadata,
        MAX_POLICY_SIZE,
        artifacts.manifest.policy_hash,
    )?;
    drop_capabilities()?;
    install_output_seccomp()?;
    let sent = send_packet(
        3,
        &Response {
            nonce: request.nonce,
            result,
            cause: if result == ProofResult::Unknown { 1 } else { 0 },
            count: if result == ProofResult::Unknown { 1 } else { 0 },
        }
        .encode(),
    );
    unsafe { libc::_exit(if sent.is_ok() { 0 } else { 3 }) }
}
