// SPLIT_ARTIFACT
#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}
const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
const RESOLVE_NO_SYMLINKS: u64 = 0x04;
const RESOLVE_BENEATH: u64 = 0x08;

fn secure_open(path: &Path, flags: i32) -> Result<File, ()> {
    let relative = path.strip_prefix("/").map_err(|_| ())?;
    let name = CString::new(relative.as_os_str().as_bytes()).map_err(|_| ())?;
    let root = OpenOptions::new().read(true).open("/").map_err(|_| ())?;
    let how = OpenHow {
        flags: u64::try_from(flags | libc::O_CLOEXEC).map_err(|_| ())?,
        mode: 0,
        resolve: RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS,
    };
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root.as_raw_fd(),
            name.as_ptr(),
            &how,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd < 0 {
        return Err(());
    }
    Ok(unsafe { File::from_raw_fd(fd as RawFd) })
}

fn validate_system_path(path: &Path) -> Result<(), ()> {
    let parent = path.parent().ok_or(())?;
    let mut current = PathBuf::from("/");
    for component in parent.strip_prefix("/").map_err(|_| ())?.components() {
        current.push(component);
        let metadata = std::fs::symlink_metadata(&current).map_err(|_| ())?;
        if !metadata.is_dir() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
            return Err(());
        }
    }
    let leaf_parent = std::fs::symlink_metadata(parent).map_err(|_| ())?;
    if leaf_parent.mode() & 0o7777 != 0o755 {
        return Err(());
    }
    Ok(())
}

fn stable_file_bytes(file: &mut File, max: usize) -> Result<(Vec<u8>, std::fs::Metadata), ()> {
    let before = file.metadata().map_err(|_| ())?;
    if !before.is_file() || before.len() > max as u64 {
        return Err(());
    }
    let mut bytes = Vec::new();
    file.take((max + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    let after = file.metadata().map_err(|_| ())?;
    if bytes.len() as u64 != before.len()
        || before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.mode() != after.mode()
        || before.uid() != after.uid()
        || before.gid() != after.gid()
        || before.nlink() != after.nlink()
    {
        return Err(());
    }
    Ok((bytes, before))
}

fn exact_metadata(metadata: &std::fs::Metadata, uid: u32, gid: u32, mode: u32) -> bool {
    metadata.is_file()
        && metadata.uid() == uid
        && metadata.gid() == gid
        && metadata.mode() & 0o7777 == mode
        && metadata.nlink() == 1
}

fn same_metadata(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.mode() == right.mode()
        && left.uid() == right.uid()
        && left.gid() == right.gid()
        && left.nlink() == right.nlink()
        && left.len() == right.len()
}

fn fd_hash(file: &mut File, max: u64) -> Result<[u8; 32], ()> {
    fd_hash_observed(file, max, |_, _| {})
}

fn fd_hash_observed(
    file: &mut File,
    max: u64,
    mut observed: impl FnMut(u64, usize),
) -> Result<[u8; 32], ()> {
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(0)).map_err(|_| ())?;
    let before = file.metadata().map_err(|_| ())?;
    if !before.is_file() || before.len() > max {
        return Err(());
    }
    let mut digest = Sha256::new();
    let mut buffer = [0u8; HASH_BUFFER_SIZE];
    let mut total = 0u64;
    loop {
        let count = file.read(&mut buffer).map_err(|_| ())?;
        if count == 0 {
            break;
        }
        total = total.checked_add(count as u64).ok_or(())?;
        if total > max || total > before.len() {
            return Err(());
        }
        digest.update(&buffer[..count]);
        observed(total, count);
    }
    let after = file.metadata().map_err(|_| ())?;
    if total != before.len() || !same_metadata(&before, &after) {
        return Err(());
    }
    Ok(digest.finalize().into())
}

fn capability_tuple(file: &File) -> Result<(u64, u64, u32), ()> {
    capability_tuple_named(file, c"security.capability")
}

fn capability_tuple_named(
    file: &File,
    name: &std::ffi::CStr,
) -> Result<(u64, u64, u32), ()> {
    let mut bytes = [0u8; 24];
    let size = unsafe {
        libc::fgetxattr(
            file.as_raw_fd(),
            name.as_ptr(),
            bytes.as_mut_ptr().cast(),
            bytes.len(),
        )
    };
    if size != 20 {
        return Err(());
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().map_err(|_| ())?);
    if magic != 0x0200_0001 {
        return Err(());
    }
    let permitted = u64::from(u32::from_le_bytes(bytes[4..8].try_into().map_err(|_| ())?))
        | (u64::from(u32::from_le_bytes(
            bytes[12..16].try_into().map_err(|_| ())?,
        )) << 32);
    let inheritable = u64::from(u32::from_le_bytes(bytes[8..12].try_into().map_err(|_| ())?))
        | (u64::from(u32::from_le_bytes(
            bytes[16..20].try_into().map_err(|_| ())?,
        )) << 32);
    Ok((permitted, inheritable, 1))
}

fn has_no_capability(file: &File) -> bool {
    let name = c"security.capability";
    let result =
        unsafe { libc::fgetxattr(file.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
    result < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ENODATA)
}

struct Artifacts {
    manifest_file: File,
    manifest_metadata: std::fs::Metadata,
    manifest_bytes: [u8; 256],
    manifest: ArtifactManifest,
    policy_file: File,
    policy_metadata: std::fs::Metadata,
    policy_bytes: Vec<u8>,
    policy: Policy,
}

fn open_artifacts() -> Result<Artifacts, ()> {
    validate_system_path(Path::new(MANIFEST_PATH))?;
    validate_system_path(Path::new(POLICY_PATH))?;
    let mut manifest_file = secure_open(Path::new(MANIFEST_PATH), libc::O_RDONLY)?;
    let (manifest_bytes, manifest_metadata) = stable_file_bytes(&mut manifest_file, 512)?;
    if !exact_metadata(&manifest_metadata, 0, 0, 0o644) {
        return Err(());
    }
    let manifest_bytes: [u8; 256] = manifest_bytes.try_into().map_err(|_| ())?;
    let manifest = ArtifactManifest::decode(&manifest_bytes)?;
    if SOURCE_DIRTY
        || manifest.source_commit != SOURCE_COMMIT
        || manifest.build_identity != BUILD_IDENTITY
    {
        return Err(());
    }
    let mut policy_file = secure_open(Path::new(POLICY_PATH), libc::O_RDONLY)?;
    let (policy_bytes, policy_metadata) = stable_file_bytes(&mut policy_file, MAX_POLICY_SIZE)?;
    if !exact_metadata(
        &policy_metadata,
        manifest.policy_uid,
        manifest.policy_gid,
        manifest.policy_mode,
    ) || <[u8; 32]>::from(Sha256::digest(&policy_bytes)) != manifest.policy_hash
    {
        return Err(());
    }
    let policy = Policy::decode(&policy_bytes)?;
    if manifest.parent_uid != policy.operator_uid
        || manifest.parent_gid != policy.operator_gid
        || manifest.helper_gid != policy.operator_gid
    {
        return Err(());
    }
    Ok(Artifacts {
        manifest_file,
        manifest_metadata,
        manifest_bytes,
        manifest,
        policy_file,
        policy_metadata,
        policy_bytes,
        policy,
    })
}

fn reopen_matches(
    path: &Path,
    retained: &File,
    expected: &std::fs::Metadata,
    max: u64,
    hash: [u8; 32],
) -> Result<(), ()> {
    let current_retained = retained.metadata().map_err(|_| ())?;
    if !same_metadata(&current_retained, expected) {
        return Err(());
    }
    let mut reopened = secure_open(path, libc::O_RDONLY)?;
    let reopened_metadata = reopened.metadata().map_err(|_| ())?;
    if !same_metadata(&reopened_metadata, expected) || fd_hash(&mut reopened, max)? != hash {
        return Err(());
    }
    Ok(())
}

fn verify_parent_artifacts(artifacts: &Artifacts) -> Result<(), ()> {
    let current = std::env::current_exe().map_err(|_| ())?;
    let mut parent = secure_open(&current, libc::O_RDONLY)?;
    let metadata = parent.metadata().map_err(|_| ())?;
    if !exact_metadata(
        &metadata,
        artifacts.manifest.parent_uid,
        artifacts.manifest.parent_gid,
        artifacts.manifest.parent_mode,
    ) || !has_no_capability(&parent)
        || fd_hash(&mut parent, MAX_HELPER_SIZE)? != artifacts.manifest.parent_hash
    {
        return Err(());
    }
    let mut helper = secure_open(Path::new(HELPER_PATH), libc::O_RDONLY)?;
    validate_system_path(Path::new(HELPER_PATH))?;
    let metadata = helper.metadata().map_err(|_| ())?;
    if !exact_metadata(
        &metadata,
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
    Ok(())
}

fn socketpair() -> Result<(OwnedFd, OwnedFd), ()> {
    let mut fds = [-1; 2];
    let result = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
            0,
            fds.as_mut_ptr(),
        )
    };
    if result != 0 {
        return Err(());
    }
    for fd in fds {
        let enabled: libc::c_int = 1;
        if unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PASSCRED,
                (&enabled as *const libc::c_int).cast(),
                std::mem::size_of_val(&enabled) as libc::socklen_t,
            )
        } != 0
        {
            unsafe {
                libc::close(fds[0]);
                libc::close(fds[1]);
            }
            return Err(());
        }
    }
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

fn send_packet(fd: RawFd, bytes: &[u8]) -> Result<(), ()> {
    let sent = unsafe { libc::send(fd, bytes.as_ptr().cast(), bytes.len(), libc::MSG_NOSIGNAL) };
    if sent == bytes.len() as isize {
        Ok(())
    } else {
        Err(())
    }
}

fn recv_packet(fd: RawFd) -> Result<(Vec<u8>, libc::ucred), ()> {
    let mut bytes = vec![0u8; MAX_PACKET_SIZE + 1];
    let mut control = [0u8; 64];
    let mut iov = libc::iovec {
        iov_base: bytes.as_mut_ptr().cast(),
        iov_len: bytes.len(),
    };
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = control.len();
    let received = unsafe { libc::recvmsg(fd, &mut message, libc::MSG_TRUNC) };
    if received <= 0 || received as usize > MAX_PACKET_SIZE {
        return Err(());
    }
    if message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
        return Err(());
    }
    let header = unsafe { libc::CMSG_FIRSTHDR(&message) };
    if header.is_null()
        || unsafe { (*header).cmsg_level } != libc::SOL_SOCKET
        || unsafe { (*header).cmsg_type } != libc::SCM_CREDENTIALS
        || unsafe { (*header).cmsg_len }
            < unsafe { libc::CMSG_LEN(std::mem::size_of::<libc::ucred>() as u32) } as usize
        || !unsafe { libc::CMSG_NXTHDR(&message, header) }.is_null()
    {
        return Err(());
    }
    let credentials = unsafe { *libc::CMSG_DATA(header).cast::<libc::ucred>() };
    bytes.truncate(received as usize);
    Ok((bytes, credentials))
}

fn stop_and_reap(child: &mut std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn bounded_wait(
    child: &mut std::process::Child,
    deadline: Instant,
) -> Result<std::process::ExitStatus, ()> {
    loop {
        if let Some(status) = child.try_wait().map_err(|_| ())? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            stop_and_reap(child);
            return Err(());
        }
        std::thread::yield_now();
    }
}
