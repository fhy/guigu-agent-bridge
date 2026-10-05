// SPLIT_PROVISION
use std::io::Write;

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
