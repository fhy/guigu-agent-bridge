// SPLIT_PROVISION
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;

pub const PRODUCTION_PARENT_SOURCE: &str = "/home/fhy/.local/lib/guigu-codex-test/guigu-agent-bridge";
pub const PRODUCTION_HELPER_SOURCE: &str = "/home/fhy/.cache/cargo-target/guigu-agent-bridge/debug/guigu-t050-proc-proof";

#[derive(Debug, Clone)]
pub struct FixturePaths {
    pub root: PathBuf,
    pub parent: PathBuf,
    pub helper: PathBuf,
    pub config: PathBuf,
    pub database_root: PathBuf,
}

fn fixture_path(root: &Path, suffix: &str) -> Result<PathBuf, ()> {
    if !root.is_absolute() || root == Path::new("/") || suffix.contains("..") {
        return Err(());
    }
    Ok(root.join(suffix))
}

fn metadata_identity(path: &Path) -> Result<FileIdentity, ()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path).map_err(|_| ())?;
    if (metadata.is_file() && metadata.nlink() != 1) || metadata.mode() & 0o022 != 0 {
        return Err(());
    }
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
    let bytes = std::fs::read(path).map_err(|_| ())?;
    Ok(Sha256::digest(bytes).into())
}

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
    let result = provision_production_inner(parent_hash, helper_hash, policy_hash, policy_fd);
    if result.is_err() { let _ = teardown_production(); }
    result
}

fn provision_production_inner(parent_hash: &str, helper_hash: &str, policy_hash: &str, policy_fd: RawFd) -> Result<(), ()> {
    if unsafe { libc::geteuid() } != 0 || policy_fd < 0 { return Err(()); }
    let expected_parent = parse_hash(parent_hash)?;
    let expected_helper = parse_hash(helper_hash)?;
    let expected_policy = parse_hash(policy_hash)?;
    let parent = Path::new(PRODUCTION_PARENT_SOURCE);
    let helper_source = Path::new(PRODUCTION_HELPER_SOURCE);
    if hash_file(parent)? != expected_parent || hash_file(helper_source)? != expected_helper { return Err(()); }
    let parent_identity = metadata_identity(parent)?;
    if parent_identity.uid == 0 || parent_identity.mode != 0o755 { return Err(()); }
    let (policy, policy_bytes) = read_policy_fd(policy_fd, expected_policy)?;
    if policy.binary.path != parent_identity.path { return Err(()); }
    let artifact_root = Path::new(POLICY_PATH).parent().ok_or(())?;
    if Path::new(HELPER_PATH).exists() || Path::new(POLICY_PATH).exists() || Path::new(MANIFEST_PATH).exists() { return Err(()); }
    validate_system_path(Path::new(HELPER_PATH))?;
    validate_system_path(Path::new(POLICY_PATH))?;
    let helper_bytes = std::fs::read(helper_source).map_err(|_| ())?;
    std::fs::create_dir_all(artifact_root).map_err(|_| ())?;
    write_exclusive(Path::new(POLICY_PATH), &policy_bytes, 0o644)?;
    let mut helper = OpenOptions::new().write(true).create_new(true).mode(0o750).open(HELPER_PATH).map_err(|_| ())?;
    helper.write_all(&helper_bytes).map_err(|_| ())?;
    helper.sync_all().map_err(|_| ())?;
    std::fs::set_permissions(HELPER_PATH, std::os::unix::fs::PermissionsExt::from_mode(0o750)).map_err(|_| ())?;
    set_capability(&helper)?;
    OpenOptions::new().read(true).open(artifact_root).map_err(|_| ())?.sync_all().map_err(|_| ())?;
    let manifest = ArtifactManifest { source_commit: SOURCE_COMMIT, build_identity: BUILD_IDENTITY, parent_hash: expected_parent, helper_hash: expected_helper, policy_hash: expected_policy, parent_uid: parent_identity.uid, parent_gid: parent_identity.gid, parent_mode: 0o755, helper_uid: 0, helper_gid: policy.operator_gid, helper_mode: 0o750, capability_permitted: 1 << 19, capability_inheritable: 0, capability_flags: 1, policy_uid: 0, policy_gid: 0, policy_mode: 0o644, generation: 1 };
    write_exclusive(Path::new(MANIFEST_PATH), &manifest.encode(), 0o644)?;
    OpenOptions::new().read(true).open(artifact_root).map_err(|_| ())?.sync_all().map_err(|_| ())
}

#[allow(clippy::result_unit_err)]
pub fn teardown_production() -> Result<(), ()> {
    if unsafe { libc::geteuid() } != 0 { return Err(()); }
    let artifact_root = Path::new(POLICY_PATH).parent().ok_or(())?;
    let quarantine = artifact_root.join("artifacts.v1.quarantine");
    if Path::new(MANIFEST_PATH).exists() { std::fs::rename(MANIFEST_PATH, &quarantine).map_err(|_| ())?; }
    if Path::new(POLICY_PATH).exists() { std::fs::remove_file(POLICY_PATH).map_err(|_| ())?; }
    if Path::new(HELPER_PATH).exists() {
        let helper = OpenOptions::new().read(true).write(true).open(HELPER_PATH).map_err(|_| ())?;
        let name = c"security.capability";
        let size = unsafe { libc::fgetxattr(helper.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
        if size >= 0 && unsafe { libc::fremovexattr(helper.as_raw_fd(), name.as_ptr()) } != 0 { return Err(()); }
        if capability_tuple(&helper)? != (0, 0, 0) { return Err(()); }
        helper.sync_all().map_err(|_| ())?;
        drop(helper);
        std::fs::remove_file(HELPER_PATH).map_err(|_| ())?;
    }
    if quarantine.exists() { std::fs::remove_file(quarantine).map_err(|_| ())?; }
    OpenOptions::new().read(true).open(artifact_root).map_err(|_| ())?.sync_all().map_err(|_| ())?;
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

#[allow(clippy::result_unit_err)]
pub fn provision_temp_fixture(paths: &FixturePaths, generation: u64) -> Result<(), ()> {
    let result = provision_temp_fixture_inner(paths, generation);
    if result.is_err() {
        let _ = teardown_temp_fixture(&paths.root);
    }
    result
}

fn provision_temp_fixture_inner(paths: &FixturePaths, generation: u64) -> Result<(), ()> {
    if generation == 0 || paths.root.starts_with("/etc") || paths.root.starts_with("/usr") {
        return Err(());
    }
    let artifact_root = fixture_path(&paths.root, "etc/guigu-agent-bridge/t050-proc-proof")?;
    let helper_target = fixture_path(&paths.root, "usr/libexec/guigu-agent-bridge/t050-proc-proof")?;
    let manifest_target = artifact_root.join("artifacts.v1");
    let policy_target = artifact_root.join("policy.v1");
    if manifest_target.exists() || policy_target.exists() || helper_target.exists() {
        return Err(());
    }
    std::fs::create_dir_all(&artifact_root).map_err(|_| ())?;
    std::fs::create_dir_all(helper_target.parent().ok_or(())?).map_err(|_| ())?;
    let parent = metadata_identity(&paths.parent)?;
    let config = metadata_identity(&paths.config)?;
    let database_root = metadata_identity(&paths.database_root)?;
    let helper_source = metadata_identity(&paths.helper)?;
    let operator_uid = parent.uid;
    let operator_gid = parent.gid;
    if parent.mode != 0o755 || helper_source.mode != 0o755 || database_root.file_type != libc::S_IFDIR {
        return Err(());
    }
    let policy = Policy {
        operator_uid,
        operator_gid,
        binary: FileIdentity { path: parent.path.clone(), ..parent.clone() },
        config: FileIdentity { path: config.path.clone(), ..config.clone() },
        database_root: FileIdentity { path: database_root.path.clone(), ..database_root.clone() },
        admission_lock: b".runtime-admission.lock".to_vec(),
        unit: b"guigu-codex-test.service".to_vec(),
        ports: [9291, 9292],
    };
    let policy_bytes = policy.encode();
    let helper_hash = hash_file(&paths.helper)?;
    let policy_hash: [u8; 32] = Sha256::digest(&policy_bytes).into();
    let manifest = ArtifactManifest {
        source_commit: SOURCE_COMMIT,
        build_identity: BUILD_IDENTITY,
        parent_hash: hash_file(&paths.parent)?,
        helper_hash,
        policy_hash,
        parent_uid: parent.uid,
        parent_gid: parent.gid,
        parent_mode: parent.mode,
        helper_uid: 0,
        helper_gid: operator_gid,
        helper_mode: 0o750,
        capability_permitted: 1 << 19,
        capability_inheritable: 0,
        capability_flags: 1,
        policy_uid: 0,
        policy_gid: 0,
        policy_mode: 0o644,
        generation,
    };
    let helper_bytes = std::fs::read(&paths.helper).map_err(|_| ())?;
    write_exclusive(&policy_target, &policy_bytes, 0o644)?;
    failpoint("after-policy")?;
    write_exclusive(&helper_target, &helper_bytes, 0o750)?;
    std::fs::set_permissions(&helper_target, std::os::unix::fs::PermissionsExt::from_mode(0o750)).map_err(|_| ())?;
    failpoint("after-helper")?;
    let directory = OpenOptions::new().read(true).open(&artifact_root).map_err(|_| ())?;
    directory.sync_all().map_err(|_| ())?;
    failpoint("before-manifest")?;
    write_exclusive(&manifest_target, &manifest.encode(), 0o644)?;
    directory.sync_all().map_err(|_| ())?;
    failpoint("after-manifest")
}

fn failpoint(stage: &str) -> Result<(), ()> {
    #[cfg(debug_assertions)]
    if std::env::var("T050_PROVISION_TEST_FAIL").ok().as_deref() == Some(stage) { return Err(()); }
    let _ = stage;
    Ok(())
}

#[allow(clippy::result_unit_err)]
pub fn teardown_temp_fixture(root: &Path) -> Result<(), ()> {
    let artifact_root = fixture_path(root, "etc/guigu-agent-bridge/t050-proc-proof")?;
    let helper = fixture_path(root, "usr/libexec/guigu-agent-bridge/t050-proc-proof")?;
    let manifest = artifact_root.join("artifacts.v1");
    let policy = artifact_root.join("policy.v1");
    if manifest.exists() { std::fs::rename(&manifest, artifact_root.join("artifacts.v1.quarantine")).map_err(|_| ())?; }
    if artifact_root.exists() { OpenOptions::new().read(true).open(&artifact_root).map_err(|_| ())?.sync_all().map_err(|_| ())?; }
    if policy.exists() { std::fs::remove_file(&policy).map_err(|_| ())?; }
    if helper.exists() {
        let file = OpenOptions::new().read(true).write(true).open(&helper).map_err(|_| ())?;
        let name = c"security.capability";
        let size = unsafe { libc::fgetxattr(file.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
        if size >= 0 {
            if unsafe { libc::fremovexattr(file.as_raw_fd(), name.as_ptr()) } != 0 { return Err(()); }
        } else if std::io::Error::last_os_error().raw_os_error() != Some(libc::ENODATA) {
            return Err(())
        }
        file.sync_all().map_err(|_| ())?;
        drop(file);
        std::fs::remove_file(&helper).map_err(|_| ())?;
    }
    if artifact_root.join("artifacts.v1.quarantine").exists() { std::fs::remove_file(artifact_root.join("artifacts.v1.quarantine")).map_err(|_| ())?; }
    if manifest.exists() || policy.exists() || helper.exists() { return Err(()); }
    if artifact_root.exists() { OpenOptions::new().read(true).open(&artifact_root).map_err(|_| ())?.sync_all().map_err(|_| ())?; }
    Ok(())
}
