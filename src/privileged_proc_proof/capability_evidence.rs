#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityEvidence {
    Present { generation: u64 },
    NoGeneration,
    Refused,
}

impl CapabilityEvidence {
    pub fn is_success(self) -> bool {
        matches!(self, Self::Present { .. })
    }

    pub fn line(self) -> String {
        match self {
            Self::Present { generation } => format!(
                "production-capability-evidence status=0 cause=none generation={generation} capability=cap_sys_ptrace_ep"
            ),
            Self::NoGeneration => {
                "production-capability-evidence status=3 cause=no-generation generation=none capability=none".into()
            }
            Self::Refused => {
                "production-capability-evidence status=3 cause=artifact generation=unknown capability=unknown".into()
            }
        }
    }
}

enum ParentState {
    Missing,
    Present(File),
}

fn evidence_trusted_root(path: &Path, expected_uid: u32) -> Result<File, ()> {
    use std::os::unix::fs::OpenOptionsExt;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| ())?;
    validate_evidence_directory(&file, expected_uid)?;
    Ok(file)
}

fn validate_evidence_directory(file: &File, expected_uid: u32) -> Result<(), ()> {
    let metadata = file.metadata().map_err(|_| ())?;
    if !metadata.is_dir()
        || metadata.uid() != expected_uid
        || metadata.gid() != expected_uid
        || metadata.mode() & 0o7777 != 0o755
    {
        return Err(());
    }
    Ok(())
}

fn open_directory_optional(parent: &File, name: &std::ffi::CStr) -> Result<ParentState, ()> {
    let how = OpenHow {
        flags: (libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) as u64,
        mode: 0,
        resolve: RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS,
    };
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            parent.as_raw_fd(),
            name.as_ptr(),
            &how,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd >= 0 {
        return Ok(ParentState::Present(unsafe {
            File::from_raw_fd(fd as RawFd)
        }));
    }
    if std::io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT) {
        Ok(ParentState::Missing)
    } else {
        Err(())
    }
}

fn retained_parent(
    root: &Path,
    components: &[&str],
    expected_uid: u32,
) -> Result<ParentState, ()> {
    let mut current = evidence_trusted_root(root, expected_uid)?;
    for component in components {
        let name = CString::new(*component).map_err(|_| ())?;
        match open_directory_optional(&current, &name)? {
            ParentState::Missing => return Ok(ParentState::Missing),
            ParentState::Present(next) => {
                validate_evidence_directory(&next, expected_uid)?;
                current = next;
            }
        }
    }
    Ok(ParentState::Present(current))
}

fn leaf_absent_at(parent: &File, name: &str) -> bool {
    let Ok(name) = CString::new(name) else {
        return false;
    };
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    result < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT)
}

fn directory_has_no_prefixes(parent: &File, prefixes: &[&[u8]]) -> bool {
    let duplicate = unsafe { libc::dup(parent.as_raw_fd()) };
    if duplicate < 0 {
        return false;
    }
    let directory = unsafe { libc::fdopendir(duplicate) };
    if directory.is_null() {
        unsafe { libc::close(duplicate) };
        return false;
    }
    loop {
        unsafe { *libc::__errno_location() = 0 };
        let entry = unsafe { libc::readdir(directory) };
        if entry.is_null() {
            let clean = unsafe { *libc::__errno_location() == 0 };
            unsafe { libc::closedir(directory) };
            return clean;
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if prefixes.iter().any(|prefix| name.starts_with(prefix)) {
            unsafe { libc::closedir(directory) };
            return false;
        }
    }
}

fn clean_no_generation_at(etc_root: &Path, libexec_root: &Path, expected_uid: u32) -> bool {
    #[cfg(test)]
    if std::env::var_os("T050_CAPABILITY_EVIDENCE_INSPECTION_FAILURE").is_some() {
        return false;
    }
    let artifact = match retained_parent(
        etc_root,
        &["guigu-agent-bridge", "t050-proc-proof"],
        expected_uid,
    ) {
        Ok(parent) => parent,
        Err(()) => return false,
    };
    let helper = match retained_parent(libexec_root, &["guigu-agent-bridge"], expected_uid) {
        Ok(parent) => parent,
        Err(()) => return false,
    };
    let artifact_clean = match artifact {
        ParentState::Missing => true,
        ParentState::Present(parent) => {
            ["artifacts.v1", "policy.v1", ".provision.lock"]
                .iter()
                .all(|name| leaf_absent_at(&parent, name))
                && directory_has_no_prefixes(
                    &parent,
                    &[b"artifacts.v1.quarantine", b"policy.v1.quarantine."],
                )
        }
    };
    let helper_clean = match helper {
        ParentState::Missing => true,
        ParentState::Present(parent) => {
            leaf_absent_at(&parent, "t050-proc-proof")
                && directory_has_no_prefixes(&parent, &[b"t050-proc-proof.quarantine."])
        }
    };
    artifact_clean && helper_clean
}

fn clean_no_generation() -> bool {
    clean_no_generation_at(Path::new("/etc"), Path::new("/usr/libexec"), 0)
}

fn authenticate_capability_generation() -> Result<u64, ()> {
    let artifacts = open_artifacts()?;
    validate_system_path(Path::new(HELPER_PATH))?;
    let mut helper = secure_open(Path::new(HELPER_PATH), libc::O_RDONLY)?;
    let helper_metadata = helper.metadata().map_err(|_| ())?;
    if !exact_metadata(
        &helper_metadata,
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
    let manifest_hash = Sha256::digest(artifacts.manifest_bytes);
    reopen_matches(
        Path::new(MANIFEST_PATH),
        &artifacts.manifest_file,
        &artifacts.manifest_metadata,
        512,
        manifest_hash.into(),
    )?;
    let policy_hash = Sha256::digest(&artifacts.policy_bytes);
    reopen_matches(
        Path::new(POLICY_PATH),
        &artifacts.policy_file,
        &artifacts.policy_metadata,
        MAX_POLICY_SIZE as u64,
        policy_hash.into(),
    )?;
    reopen_matches(
        Path::new(HELPER_PATH),
        &helper,
        &helper_metadata,
        MAX_HELPER_SIZE,
        artifacts.manifest.helper_hash,
    )?;
    if capability_tuple(&helper)?
        != (
            artifacts.manifest.capability_permitted,
            artifacts.manifest.capability_inheritable,
            artifacts.manifest.capability_flags,
        )
    {
        return Err(());
    }
    Ok(artifacts.manifest.generation)
}

pub fn production_capability_evidence() -> CapabilityEvidence {
    match authenticate_capability_generation() {
        Ok(generation) => CapabilityEvidence::Present { generation },
        Err(()) if clean_no_generation() => CapabilityEvidence::NoGeneration,
        Err(()) => CapabilityEvidence::Refused,
    }
}

#[cfg(test)]
#[path = "capability_evidence_tests.rs"]
mod capability_evidence_tests;
