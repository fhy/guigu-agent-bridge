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
