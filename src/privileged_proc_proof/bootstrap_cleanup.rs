fn remove_created_lock(parent: &File, lock: &File) -> Result<(), ()> {
    construction_fault("lock-cleanup-inspect").map_err(|_| ())?;
    let retained = Identity::from(&lock.metadata().map_err(|_| ())?);
    let current = current_identity(parent, c".provision.lock")?;
    if current.device != retained.device || current.inode != retained.inode {
        return Err(());
    }
    construction_fault("lock-cleanup-unlink").map_err(|_| ())?;
    if unsafe { libc::unlinkat(parent.as_raw_fd(), c".provision.lock".as_ptr(), 0) } != 0 {
        return Err(());
    }
    construction_fault("lock-cleanup-fsync").map_err(|_| ())?;
    sync_parent(parent, "lock-create-rollback")
}

fn current_identity(parent: &File, name: &std::ffi::CStr) -> Result<Identity, ()> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(());
    }
    let metadata = unsafe { metadata.assume_init() };
    Ok(Identity {
        device: metadata.st_dev,
        inode: metadata.st_ino,
    })
}

fn cleanup_lock(parent: &File, lock: &File, expected: Identity) -> Result<(), ()> {
    let retained = lock.metadata().map_err(|_| ())?;
    let current = current_identity(parent, c".provision.lock")?;
    if !expected.matches(&retained)
        || current.device != expected.device
        || current.inode != expected.inode
    {
        return Err(());
    }
    if unsafe { libc::unlinkat(parent.as_raw_fd(), c".provision.lock".as_ptr(), 0) } != 0 {
        return Err(());
    }
    sync_parent(parent, "lock-unlink")
}

fn cleanup_directories(created: &mut Vec<CreatedDirectory>) -> Result<(), ()> {
    while let Some(directory) = created.pop() {
        remove_created_directory(&directory.parent, &directory.name, directory.identity)?;
    }
    Ok(())
}

fn remove_created_directory(
    parent: &File,
    name: &std::ffi::CStr,
    identity: Identity,
) -> Result<(), ()> {
    let current = current_identity(parent, name)?;
    if current.device != identity.device || current.inode != identity.inode {
        return Err(());
    }
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) } != 0 {
        return Err(());
    }
    construction_fault("directory-cleanup-fsync").map_err(|_| ())?;
    sync_parent(parent, "directory-unlink")
}

fn sync_parent(parent: &File, event: &'static str) -> Result<(), ()> {
    parent.sync_all().map_err(|_| ())?;
    #[cfg(test)]
    FSYNC_EVENTS.with(|events| events.borrow_mut().push(event));
    let _ = event;
    Ok(())
}

fn construction_fault(stage: &str) -> Result<(), ConstructionFailure> {
    #[cfg(test)]
    if CONSTRUCTION_FAULT.with(|fault| {
        fault
            .borrow()
            .as_deref()
            .is_some_and(|faults| faults.split(',').any(|fault| fault == stage))
    }) {
        return Err(ConstructionFailure::Clean);
    }
    let _ = stage;
    Ok(())
}

#[cfg(test)]
pub(super) fn take_fsync_events() -> Vec<&'static str> {
    FSYNC_EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

#[cfg(test)]
pub(super) fn set_construction_fault(stage: Option<&str>) {
    CONSTRUCTION_FAULT.with(|fault| *fault.borrow_mut() = stage.map(str::to_owned));
}
