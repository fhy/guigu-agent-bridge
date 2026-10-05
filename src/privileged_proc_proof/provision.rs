use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;

fn fixture_path(root: &Path, suffix: &str) -> Result<PathBuf, ()> {
    if !root.is_absolute() || root == Path::new("/") || suffix.contains("..") {
        return Err(());
    }
    Ok(root.join(suffix))
}

pub(super) fn metadata_identity(path: &Path) -> Result<FileIdentity, ()> {
    let identity = raw_identity(path)?;
    if identity.mode & 0o022 != 0 { return Err(()); }
    Ok(identity)
}

pub(super) fn raw_identity(path: &Path) -> Result<FileIdentity, ()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path).map_err(|_| ())?;
    if metadata.is_file() && metadata.nlink() != 1 { return Err(()); }
    Ok(FileIdentity {
        path: path.as_os_str().as_bytes().to_vec(),
        device: metadata.dev(),
        inode: metadata.ino(),
        file_type: metadata.mode() & libc::S_IFMT,
        mode: metadata.mode() & 0o7777,
        uid: metadata.uid(),
        gid: metadata.gid(),
    })
}

fn hash_file(path: &Path) -> Result<[u8; 32], ()> {
    let mut file = secure_open(path, libc::O_RDONLY)?;
    fd_hash(&mut file, MAX_HELPER_SIZE)
}

const R4_PARENT_HASH: &str = "e1acaf12d51f67cf5f0c4767aefa4a287d6680b042d72bd1728cdcb52a26d6a9";
const R4_HELPER_HASH: &str = "213f14f32754f8f16714daf0aeeb89faf95c27223978cd46492d6e4531ce778f";
const R4_POLICY_HASH: &str = "340f5896eb2e99e7cbcc056ed9634894b639dddc71ed766119b6e77bec52ecd1";

fn parse_hash(value: &str) -> Result<[u8; 32], ()> {
    if value.len() != 64 { return Err(()); }
    let mut out = [0u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).map_err(|_| ())?;
    }
    Ok(out)
}

fn capability_bytes() -> [u8; 20] {
    let mut value = [0u8; 20];
    value[0..4].copy_from_slice(&0x0200_0001u32.to_le_bytes());
    value[4..8].copy_from_slice(&(1u32 << 19).to_le_bytes());
    value
}

fn set_capability(file: &File) -> Result<(), ()> {
    let value = capability_bytes();
    if unsafe { libc::fsetxattr(file.as_raw_fd(), c"security.capability".as_ptr(), value.as_ptr().cast(), value.len(), 0) } != 0 { return Err(()); }
    if capability_tuple(file)? != (1 << 19, 0, 1) { return Err(()); }
    Ok(())
}

fn read_policy_fd(fd: RawFd, expected: [u8; 32]) -> Result<(Policy, Vec<u8>), ()> {
    let duplicate = unsafe { libc::dup(fd) };
    if duplicate < 0 { return Err(()); }
    let file = unsafe { File::from_raw_fd(duplicate) };
    let seals = unsafe { libc::fcntl(file.as_raw_fd(), 1034) };
    if seals >= 0 && (seals & 0x0f) != 0x0f { return Err(()); }
    if seals < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::EINVAL) { return Err(()); }
    let before = file.metadata().map_err(|_| ())?;
    if !before.is_file() || before.len() > MAX_POLICY_SIZE as u64 { return Err(()); }
    let mut bytes = Vec::new();
    (&file).take((MAX_POLICY_SIZE + 1) as u64).read_to_end(&mut bytes).map_err(|_| ())?;
    let after = file.metadata().map_err(|_| ())?;
    if before.dev() != after.dev() || before.ino() != after.ino() || before.len() != bytes.len() as u64 { return Err(()); }
    if <[u8; 32]>::from(Sha256::digest(&bytes)) != expected { return Err(()); }
    Ok((Policy::decode(&bytes)?, bytes))
}

#[allow(clippy::result_unit_err)]
pub fn provision_production(parent_hash: &str, helper_hash: &str, policy_hash: &str, policy_fd: RawFd) -> Result<(), ()> {
    if unsafe { libc::geteuid() } != 0 || policy_fd < 0 { return Err(()); }
    let layout = bootstrap::ProvisionLayout::production()?;
    let mut cleanup_ambiguous = false;
    let result = provision_production_inner(
        parent_hash,
        helper_hash,
        policy_hash,
        policy_fd,
        layout.artifact_dir(),
        layout.helper_dir(),
        &mut cleanup_ambiguous,
    );
    let cleanup = layout.finish(result.is_ok() || cleanup_ambiguous);
    result.and(cleanup)
}

fn leaf_absent(path: &Path) -> Result<(), ()> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err(()),
    }
}

fn same_leaf(path: &Path, expected: &FileIdentity) -> Result<(), ()> {
    let actual = metadata_identity(path)?;
    if actual.device == expected.device && actual.inode == expected.inode && actual.file_type == libc::S_IFREG { Ok(()) } else { Err(()) }
}

fn open_leaf_fd(parent: &File, name: &str, write: bool) -> Result<File, ()> {
    let name = std::ffi::CString::new(name).map_err(|_| ())?;
    #[repr(C)] struct T050OpenHow { flags: u64, mode: u64, resolve: u64 }
    let how = T050OpenHow { flags: (if write { libc::O_RDWR } else { libc::O_RDONLY } | libc::O_CLOEXEC) as u64, mode: 0, resolve: 0x08 | 0x04 | 0x02 };
    let fd = unsafe { libc::syscall(libc::SYS_openat2, parent.as_raw_fd(), name.as_ptr(), &how, std::mem::size_of::<OpenHow>()) };
    if fd < 0 { return Err(()); }
    Ok(unsafe { File::from_raw_fd(fd as RawFd) })
}

fn fd_bytes(file: &mut File, max: usize) -> Result<Vec<u8>, ()> {
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(0)).map_err(|_| ())?;
    let before = file.metadata().map_err(|_| ())?;
    if !before.is_file() || before.len() > max as u64 { return Err(()); }
    let mut bytes = Vec::new();
    file.take((max + 1) as u64).read_to_end(&mut bytes).map_err(|_| ())?;
    let after = file.metadata().map_err(|_| ())?;
    if bytes.len() as u64 != before.len() || before.dev() != after.dev() || before.ino() != after.ino() { return Err(()); }
    Ok(bytes)
}

fn stream_copy_retained(
    source: &mut File,
    target: &mut File,
    max: u64,
    mut observed: impl FnMut(u64, usize),
) -> Result<[u8; 32], ()> {
    source.seek(SeekFrom::Start(0)).map_err(|_| ())?;
    let before = source.metadata().map_err(|_| ())?;
    if !before.is_file() || before.len() > max {
        return Err(());
    }
    let mut digest = Sha256::new();
    let mut buffer = [0u8; HASH_BUFFER_SIZE];
    let mut total = 0u64;
    loop {
        let count = source.read(&mut buffer).map_err(|_| ())?;
        if count == 0 {
            break;
        }
        total = total.checked_add(count as u64).ok_or(())?;
        if total > max || total > before.len() {
            return Err(());
        }
        target.write_all(&buffer[..count]).map_err(|_| ())?;
        digest.update(&buffer[..count]);
        observed(total, count);
    }
    let after = source.metadata().map_err(|_| ())?;
    let target_metadata = target.metadata().map_err(|_| ())?;
    if total != before.len()
        || !same_metadata(&before, &after)
        || !target_metadata.is_file()
        || target_metadata.len() != total
    {
        return Err(());
    }
    Ok(digest.finalize().into())
}

fn r4_teardown_manifest(manifest: &ArtifactManifest) -> Result<bool, ()> {
    Ok(manifest.generation == 1
        && manifest.parent_hash == parse_hash(R4_PARENT_HASH)?
        && manifest.helper_hash == parse_hash(R4_HELPER_HASH)?
        && manifest.policy_hash == parse_hash(R4_POLICY_HASH)?)
}

fn teardown_helper_metadata_allowed(
    manifest: &ArtifactManifest,
    metadata: &std::fs::Metadata,
) -> Result<bool, ()> {
    teardown_helper_shape_allowed(
        manifest,
        metadata.is_file(),
        metadata.nlink(),
        metadata.uid(),
        metadata.gid(),
        metadata.mode() & 0o7777,
    )
}

fn teardown_helper_shape_allowed(
    manifest: &ArtifactManifest,
    is_file: bool,
    nlink: u64,
    uid: u32,
    gid: u32,
    mode: u32,
) -> Result<bool, ()> {
    let exact = is_file
        && nlink == 1
        && uid == manifest.helper_uid
        && gid == manifest.helper_gid
        && mode == manifest.helper_mode;
    let r4 = is_file
        && nlink == 1
        && uid == 0
        && gid == 0
        && mode == 0o750
        && r4_teardown_manifest(manifest)?;
    Ok(exact || r4)
}

fn verify_published_helper(
    helper: &mut File,
    operator_gid: u32,
    expected_hash: [u8; 32],
) -> Result<(), ()> {
    let metadata = helper.metadata().map_err(|_| ())?;
    if !exact_metadata(&metadata, 0, operator_gid, 0o750)
        || fd_hash(helper, MAX_HELPER_SIZE)? != expected_hash
        || capability_tuple(helper)? != (1 << 19, 0, 1)
    {
        return Err(());
    }
    Ok(())
}

fn replacement_hook(stage: &str) -> Result<(), ()> {
    if std::env::var("T050_PRODUCTION_REPLACE_AT").ok().as_deref() == Some(stage) { Err(()) } else { Ok(()) }
}

fn rename_noreplace(dir: &File, from: &str, to: &str) -> Result<(), ()> {
    let from = std::ffi::CString::new(from).map_err(|_| ())?;
    let to = std::ffi::CString::new(to).map_err(|_| ())?;
    let rc = unsafe { libc::syscall(libc::SYS_renameat2, dir.as_raw_fd(), from.as_ptr(), dir.as_raw_fd(), to.as_ptr(), 1u32) };
    if rc == 0 { Ok(()) } else { Err(()) }
}
fn unlinkat_leaf(dir: &File, name: &str) -> Result<(), ()> {
    let name = std::ffi::CString::new(name).map_err(|_| ())?;
    if unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) } == 0 { Ok(()) } else { Err(()) }
}
fn fsync_dir(dir: &File) -> Result<(), ()> { dir.sync_all().map_err(|_| ()) }

fn provision_production_inner(
    parent_hash: &str,
    helper_hash: &str,
    policy_hash: &str,
    policy_fd: RawFd,
    artifact_dir: &File,
    helper_dir: &File,
    cleanup_ambiguous: &mut bool,
) -> Result<(), ()> {
    let expected_parent = parse_hash(parent_hash)?;
    let expected_helper = parse_hash(helper_hash)?;
    let expected_policy = parse_hash(policy_hash)?;
    let parent = Path::new(PRODUCTION_PARENT_SOURCE);
    let helper_source = Path::new(PRODUCTION_HELPER_SOURCE);
    if hash_file(parent)? != expected_parent { return Err(()); }
    let mut helper_source_file = secure_open(helper_source, libc::O_RDONLY)?;
    if fd_hash(&mut helper_source_file, MAX_HELPER_SIZE)? != expected_helper { return Err(()); }
    let parent_identity = metadata_identity(parent)?;
    if parent_identity.uid == 0 || parent_identity.mode != 0o755 { return Err(()); }
    let (policy, policy_bytes) = read_policy_fd(policy_fd, expected_policy)?;
    if policy.binary.path != parent_identity.path { return Err(()); }
    leaf_absent(Path::new(HELPER_PATH))?;
    leaf_absent(Path::new(POLICY_PATH))?;
    leaf_absent(Path::new(MANIFEST_PATH))?;
    validate_system_path(Path::new(HELPER_PATH))?;
    validate_system_path(Path::new(POLICY_PATH))?;
    use publication::{LeafKind, PublicationTransaction};
    let mut publication = PublicationTransaction::new(artifact_dir, helper_dir);
    let result = (|| {
        let policy_leaf = publication.create(LeafKind::Policy, 0o644)?;
        publication.file_mut(policy_leaf)?.write_all(&policy_bytes).map_err(|_| ())?;
        publication.boundary("policy:after-write")?;
        publication.sync_file(policy_leaf, "policy:after-file-fsync")?;
        publication.promote(policy_leaf, 0, 0, 0o644)?;

        let helper_leaf = publication.create(LeafKind::Helper, 0o750)?;
        let copied_hash = stream_copy_retained(
            &mut helper_source_file,
            publication.file_mut(helper_leaf)?,
            MAX_HELPER_SIZE,
            |_, _| {},
        )?;
        if copied_hash != expected_helper {
            return Err(());
        }
        publication.boundary("helper:after-copy")?;
        if unsafe {
            libc::fchown(
                publication.file_mut(helper_leaf)?.as_raw_fd(),
                0,
                policy.operator_gid,
            )
        } != 0
        {
            return Err(());
        }
        publication.boundary("helper:after-fchown")?;
        if unsafe { libc::fchmod(publication.file_mut(helper_leaf)?.as_raw_fd(), 0o750) } != 0 {
            return Err(());
        }
        publication.boundary("helper:after-fchmod")?;
        set_capability(publication.file_mut(helper_leaf)?)?;
        publication.boundary("helper:after-capability")?;
        publication.sync_file(helper_leaf, "helper:after-file-fsync")?;
        publication.promote(helper_leaf, 0, policy.operator_gid, 0o750)?;
        verify_published_helper(
            publication.file_mut(helper_leaf)?,
            policy.operator_gid,
            expected_helper,
        )?;
        publication.boundary("helper:after-revalidation")?;
        publication.sync_parent(LeafKind::Helper, "helper:after-parent-fsync")?;
        publication.sync_parent(LeafKind::Policy, "artifacts:before-manifest-fsync")?;

        let manifest = ArtifactManifest { source_commit: SOURCE_COMMIT, build_identity: BUILD_IDENTITY, parent_hash: expected_parent, helper_hash: expected_helper, policy_hash: expected_policy, parent_uid: parent_identity.uid, parent_gid: parent_identity.gid, parent_mode: 0o755, helper_uid: 0, helper_gid: policy.operator_gid, helper_mode: 0o750, capability_permitted: 1 << 19, capability_inheritable: 0, capability_flags: 1, policy_uid: 0, policy_gid: 0, policy_mode: 0o644, generation: 1 };
        let manifest_leaf = publication.create(LeafKind::Manifest, 0o644)?;
        publication.file_mut(manifest_leaf)?.write_all(&manifest.encode()).map_err(|_| ())?;
        publication.boundary("manifest:after-write")?;
        publication.sync_file(manifest_leaf, "manifest:after-file-fsync")?;
        publication.promote(manifest_leaf, 0, 0, 0o644)?;
        publication.sync_parent(LeafKind::Manifest, "manifest:after-parent-fsync")?;
        publication.commit(manifest_leaf)
    })();
    let cleanup = publication.finish(result.is_ok());
    if cleanup.is_err() {
        *cleanup_ambiguous = true;
    }
    result.and(cleanup)
}

#[allow(clippy::result_unit_err)]
pub fn teardown_production(parent_hash: &str, helper_hash: &str, policy_hash: &str, generation: u64) -> Result<(), ()> {
    if unsafe { libc::geteuid() } != 0 { return Err(()); }
    let layout = bootstrap::ProvisionLayout::production()?;
    let result = teardown_production_inner(parent_hash, helper_hash, policy_hash, generation);
    let cleanup = layout.finish(result.is_ok());
    result.and(cleanup)
}

fn teardown_production_inner(parent_hash: &str, helper_hash: &str, policy_hash: &str, generation: u64) -> Result<(), ()> {
    let parent_hash = parse_hash(parent_hash)?;
    let helper_hash = parse_hash(helper_hash)?;
    let policy_hash = parse_hash(policy_hash)?;
    let artifact_root = OpenOptions::new().read(true).open("/etc/guigu-agent-bridge/t050-proc-proof").map_err(|_| ())?;
    let helper_root = OpenOptions::new().read(true).open("/usr/libexec/guigu-agent-bridge").map_err(|_| ())?;
    let mut manifest_file = open_leaf_fd(&artifact_root, "artifacts.v1", false)?;
    let mut policy_file = open_leaf_fd(&artifact_root, "policy.v1", false)?;
    let mut helper_file = open_leaf_fd(&helper_root, "t050-proc-proof", true)?;
    replacement_hook("pre-hash")?;
    let manifest_bytes = fd_bytes(&mut manifest_file, 4096)?;
    let manifest = ArtifactManifest::decode(&manifest_bytes)?;
    if manifest.parent_hash != parent_hash || manifest.helper_hash != helper_hash || manifest.policy_hash != policy_hash || manifest.generation != generation { return Err(()); }
    let policy_bytes = fd_bytes(&mut policy_file, MAX_POLICY_SIZE)?;
    if <[u8; 32]>::from(Sha256::digest(&policy_bytes)) != policy_hash { return Err(()); }
    if fd_hash(&mut helper_file, MAX_HELPER_SIZE)? != helper_hash { return Err(()); }
    let manifest_metadata = manifest_file.metadata().map_err(|_| ())?;
    let policy_metadata = policy_file.metadata().map_err(|_| ())?;
    let helper_metadata = helper_file.metadata().map_err(|_| ())?;
    if !exact_metadata(&manifest_metadata, 0, 0, 0o644)
        || !exact_metadata(&policy_metadata, 0, 0, 0o644)
        || !teardown_helper_metadata_allowed(&manifest, &helper_metadata)?
        || capability_tuple(&helper_file)?
            != (
                manifest.capability_permitted,
                manifest.capability_inheritable,
                manifest.capability_flags,
            )
    {
        return Err(());
    }
    let quarantine = Path::new("/etc/guigu-agent-bridge/t050-proc-proof/artifacts.v1.quarantine");
    leaf_absent(quarantine)?;
    let manifest_identity = metadata_identity(Path::new(MANIFEST_PATH))?;
    let policy_identity = metadata_identity(Path::new(POLICY_PATH))?;
    let helper_identity = metadata_identity(Path::new(HELPER_PATH))?;
    replacement_hook("pre-rename")?;
    same_leaf(Path::new(MANIFEST_PATH), &manifest_identity)?;
    let artifact_dir = OpenOptions::new().read(true).open("/etc/guigu-agent-bridge/t050-proc-proof").map_err(|_| ())?;
    let quarantine_name = format!("artifacts.v1.quarantine.{}", std::process::id());
    rename_noreplace(&artifact_dir, "artifacts.v1", &quarantine_name)?;
    fsync_dir(&artifact_dir)?;
    let quarantine = Path::new("/etc/guigu-agent-bridge/t050-proc-proof").join(&quarantine_name);
    if same_leaf(&quarantine, &manifest_identity).is_err() { return Err(()); }
    replacement_hook("pre-helper-rename")?;
    same_leaf(Path::new(HELPER_PATH), &helper_identity)?;
    rename_noreplace(&helper_root, "t050-proc-proof", &format!("t050-proc-proof.quarantine.{}", std::process::id()))?;
    fsync_dir(&helper_root)?;
    replacement_hook("pre-capability-removal")?;
    let name = c"security.capability";
    let size = unsafe { libc::fgetxattr(helper_file.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if size >= 0 && unsafe { libc::fremovexattr(helper_file.as_raw_fd(), name.as_ptr()) } != 0 { return Err(()); }
    if !has_no_capability(&helper_file) { return Err(()); }
    helper_file.sync_all().map_err(|_| ())?;
    drop(helper_file);
    replacement_hook("pre-policy-rename")?;
    same_leaf(Path::new(POLICY_PATH), &policy_identity)?;
    rename_noreplace(&artifact_dir, "policy.v1", &format!("policy.v1.quarantine.{}", std::process::id()))?;
    fsync_dir(&artifact_dir)?;
    let policy_quarantine = Path::new("/etc/guigu-agent-bridge/t050-proc-proof").join(format!("policy.v1.quarantine.{}", std::process::id()));
    let helper_quarantine = Path::new("/usr/libexec/guigu-agent-bridge").join(format!("t050-proc-proof.quarantine.{}", std::process::id()));
    let _policy_qfd = open_leaf_fd(&artifact_dir, &format!("policy.v1.quarantine.{}", std::process::id()), false)?;
    let _helper_qfd = open_leaf_fd(&helper_root, &format!("t050-proc-proof.quarantine.{}", std::process::id()), false)?;
    same_leaf(&policy_quarantine, &policy_identity)?;
    same_leaf(&helper_quarantine, &helper_identity)?;
    replacement_hook("pre-unlink")?;
    same_leaf(&policy_quarantine, &policy_identity)?;
    same_leaf(&helper_quarantine, &helper_identity)?;
    same_leaf(&quarantine, &manifest_identity)?;
    unlinkat_leaf(&artifact_dir, &format!("policy.v1.quarantine.{}", std::process::id()))?;
    fsync_dir(&artifact_dir)?;
    unlinkat_leaf(&helper_root, &format!("t050-proc-proof.quarantine.{}", std::process::id()))?;
    fsync_dir(&helper_root)?;
    unlinkat_leaf(&artifact_dir, &quarantine_name)?;
    fsync_dir(&artifact_dir)?;
    OpenOptions::new().read(true).open(Path::new("/etc/guigu-agent-bridge/t050-proc-proof")).map_err(|_| ())?.sync_all().map_err(|_| ())?;
    if let Some(parent) = Path::new(HELPER_PATH).parent() {
        OpenOptions::new().read(true).open(parent).map_err(|_| ())?.sync_all().map_err(|_| ())?;
    }
    if Path::new(MANIFEST_PATH).exists() || Path::new(POLICY_PATH).exists() || Path::new(HELPER_PATH).exists() { return Err(()); }
    Ok(())
}

fn write_exclusive(path: &Path, bytes: &[u8], mode: u32) -> Result<(), ()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)
        .map_err(|_| ())?;
    file.write_all(bytes).map_err(|_| ())?;
    file.sync_all().map_err(|_| ())
}
