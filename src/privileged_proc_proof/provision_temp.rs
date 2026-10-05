#[allow(clippy::result_unit_err)]
pub fn provision_temp_fixture(paths: &FixturePaths, generation: u64) -> Result<(), ()> {
    let artifact_root = fixture_path(&paths.root, "etc/guigu-agent-bridge/t050-proc-proof")?;
    let targets = [
        artifact_root.join("artifacts.v1"),
        artifact_root.join("policy.v1"),
        fixture_path(&paths.root, "usr/libexec/guigu-agent-bridge/t050-proc-proof")?,
    ];
    if targets
        .iter()
        .any(|path| std::fs::symlink_metadata(path).is_ok())
    {
        return Err(());
    }
    let (etc, libexec) = prepare_temp_roots(&paths.root)?;
    let uid = unsafe { libc::geteuid() };
    let layout = bootstrap::ProvisionLayout::at_roots(&etc, &libexec, uid)?;
    let result = provision_temp_fixture_inner(paths, generation);
    if result.is_err() {
        let _ = teardown_temp_fixture_inner(&paths.root);
    }
    let cleanup = layout.finish(result.is_ok());
    result.and(cleanup)
}

fn prepare_temp_roots(root: &Path) -> Result<(PathBuf, PathBuf), ()> {
    let etc = fixture_path(root, "etc")?;
    let libexec = fixture_path(root, "usr/libexec")?;
    std::fs::create_dir_all(&etc).map_err(|_| ())?;
    std::fs::create_dir_all(&libexec).map_err(|_| ())?;
    std::fs::set_permissions(
        &etc,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .map_err(|_| ())?;
    std::fs::set_permissions(
        &libexec,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .map_err(|_| ())?;
    Ok((etc, libexec))
}

fn provision_temp_fixture_inner(paths: &FixturePaths, generation: u64) -> Result<(), ()> {
    if generation == 0 || paths.root.starts_with("/etc") || paths.root.starts_with("/usr") {
        return Err(());
    }
    let artifact_root = fixture_path(&paths.root, "etc/guigu-agent-bridge/t050-proc-proof")?;
    let helper_target = fixture_path(
        &paths.root,
        "usr/libexec/guigu-agent-bridge/t050-proc-proof",
    )?;
    let manifest_target = artifact_root.join("artifacts.v1");
    let policy_target = artifact_root.join("policy.v1");
    if manifest_target.exists() || policy_target.exists() || helper_target.exists() {
        return Err(());
    }
    let parent = metadata_identity(&paths.parent)?;
    let config = metadata_identity(&paths.config)?;
    let database_root = metadata_identity(&paths.database_root)?;
    let helper_source = metadata_identity(&paths.helper)?;
    let operator_uid = parent.uid;
    let operator_gid = parent.gid;
    if parent.mode != 0o755
        || helper_source.mode != 0o755
        || database_root.file_type != libc::S_IFDIR
    {
        return Err(());
    }
    let policy = Policy {
        operator_uid,
        operator_gid,
        binary: FileIdentity {
            path: parent.path.clone(),
            ..parent.clone()
        },
        config: FileIdentity {
            path: config.path.clone(),
            ..config.clone()
        },
        database_root: FileIdentity {
            path: database_root.path.clone(),
            ..database_root.clone()
        },
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
    std::fs::set_permissions(
        &helper_target,
        std::os::unix::fs::PermissionsExt::from_mode(0o750),
    )
    .map_err(|_| ())?;
    failpoint("after-helper")?;
    let directory = OpenOptions::new()
        .read(true)
        .open(&artifact_root)
        .map_err(|_| ())?;
    directory.sync_all().map_err(|_| ())?;
    failpoint("before-manifest")?;
    write_exclusive(&manifest_target, &manifest.encode(), 0o644)?;
    directory.sync_all().map_err(|_| ())?;
    failpoint("after-manifest")
}

fn failpoint(stage: &str) -> Result<(), ()> {
    #[cfg(debug_assertions)]
    if std::env::var("T050_PROVISION_TEST_FAIL")
        .ok()
        .as_deref()
        == Some(stage)
    {
        return Err(());
    }
    let _ = stage;
    Ok(())
}

#[allow(clippy::result_unit_err)]
pub fn teardown_temp_fixture(root: &Path) -> Result<(), ()> {
    let (etc, libexec) = prepare_temp_roots(root)?;
    let layout =
        bootstrap::ProvisionLayout::at_roots(&etc, &libexec, unsafe { libc::geteuid() })?;
    let result = teardown_temp_fixture_inner(root);
    let cleanup = layout.finish(result.is_ok());
    result.and(cleanup)
}

fn teardown_temp_fixture_inner(root: &Path) -> Result<(), ()> {
    let artifact_root = fixture_path(root, "etc/guigu-agent-bridge/t050-proc-proof")?;
    let helper = fixture_path(
        root,
        "usr/libexec/guigu-agent-bridge/t050-proc-proof",
    )?;
    let manifest = artifact_root.join("artifacts.v1");
    let policy = artifact_root.join("policy.v1");
    if manifest.exists() {
        std::fs::rename(
            &manifest,
            artifact_root.join("artifacts.v1.quarantine"),
        )
        .map_err(|_| ())?;
    }
    if artifact_root.exists() {
        OpenOptions::new()
            .read(true)
            .open(&artifact_root)
            .map_err(|_| ())?
            .sync_all()
            .map_err(|_| ())?;
    }
    if policy.exists() {
        std::fs::remove_file(&policy).map_err(|_| ())?;
    }
    if helper.exists() {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&helper)
            .map_err(|_| ())?;
        let name = c"security.capability";
        let size = unsafe {
            libc::fgetxattr(
                file.as_raw_fd(),
                name.as_ptr(),
                std::ptr::null_mut(),
                0,
            )
        };
        if size >= 0 {
            if unsafe { libc::fremovexattr(file.as_raw_fd(), name.as_ptr()) } != 0 {
                return Err(());
            }
        } else if std::io::Error::last_os_error().raw_os_error() != Some(libc::ENODATA) {
            return Err(());
        }
        file.sync_all().map_err(|_| ())?;
        drop(file);
        std::fs::remove_file(&helper).map_err(|_| ())?;
    }
    let quarantine = artifact_root.join("artifacts.v1.quarantine");
    if quarantine.exists() {
        std::fs::remove_file(quarantine).map_err(|_| ())?;
    }
    if manifest.exists() || policy.exists() || helper.exists() {
        return Err(());
    }
    if artifact_root.exists() {
        OpenOptions::new()
            .read(true)
            .open(&artifact_root)
            .map_err(|_| ())?
            .sync_all()
            .map_err(|_| ())?;
    }
    Ok(())
}
