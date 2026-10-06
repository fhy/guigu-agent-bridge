use std::{fs::File, os::fd::AsRawFd};

use super::{codec::Identity, fs, fs::OwnershipFailure};

pub(crate) struct OperationLock {
    _file: File,
    dir: File,
    identity: Identity,
    armed: bool,
}

impl OperationLock {
    pub(crate) fn acquire(dir: &File) -> Result<Self, OwnershipFailure> {
        let file =
            fs::create_leaf(dir, super::codec::LOCK, 0o600).map_err(|_| OwnershipFailure::Clean)?;
        super::fault::hit("before-lock-metadata").map_err(|_| OwnershipFailure::Ambiguous)?;
        let pending = fs::identity(&file.metadata().map_err(|_| OwnershipFailure::Ambiguous)?);
        let outcome = pending.and_then(|identity| {
            super::fault::hit("after-lock-create")?;
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(());
            }
            super::fault::hit("after-lock-flock")?;
            file.sync_all().map_err(|_| ())?;
            super::fault::hit("after-lock-file-fsync")?;
            dir.sync_all().map_err(|_| ())?;
            super::fault::hit("after-lock-parent-fsync")?;
            Ok(identity)
        });
        let identity = match outcome {
            Ok(identity) => identity,
            Err(()) => {
                let observed =
                    fs::identity(&file.metadata().map_err(|_| OwnershipFailure::Ambiguous)?)
                        .map_err(|_| OwnershipFailure::Ambiguous)?;
                let current = fs::open_leaf(dir, super::codec::LOCK, false)
                    .map_err(|_| OwnershipFailure::Ambiguous)?;
                let current_identity = fs::identity(
                    &current
                        .metadata()
                        .map_err(|_| OwnershipFailure::Ambiguous)?,
                )
                .map_err(|_| OwnershipFailure::Ambiguous)?;
                if !fs::same_object(observed, current_identity) {
                    return Err(OwnershipFailure::Ambiguous);
                }
                fs::unlink(dir, super::codec::LOCK)
                    .and_then(|()| dir.sync_all().map_err(|_| ()))
                    .map_err(|_| OwnershipFailure::Ambiguous)?;
                return Err(OwnershipFailure::Clean);
            }
        };
        Ok(Self {
            _file: file,
            dir: dir.try_clone().map_err(|_| OwnershipFailure::Ambiguous)?,
            identity,
            armed: true,
        })
    }

    pub(crate) fn acquire_resume(dir: &File) -> Result<Self, ()> {
        let file = fs::open_leaf(dir, super::codec::LOCK, true)?;
        let identity = fs::identity(&file.metadata().map_err(|_| ())?)?;
        let uid = unsafe { libc::geteuid() };
        let gid = unsafe { libc::getegid() };
        if identity.uid != uid || identity.gid != gid || identity.mode != 0o600 {
            return Err(());
        }
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(());
        }
        Ok(Self {
            _file: file,
            dir: dir.try_clone().map_err(|_| ())?,
            identity,
            armed: true,
        })
    }

    pub(crate) fn preserve(mut self) {
        self.armed = false;
    }

    pub(crate) fn identity(&self) -> Identity {
        self.identity
    }

    pub(crate) fn finish(mut self) -> Result<(), ()> {
        self.remove()?;
        self.armed = false;
        Ok(())
    }

    fn remove(&self) -> Result<(), ()> {
        super::fault::hit("cleanup-before-lock-delete")?;
        let mut current = fs::open_leaf(&self.dir, super::codec::LOCK, false)?;
        let (_, observed) = fs::hash(&mut current)?;
        if !fs::same(self.identity, observed) {
            return Err(());
        }
        fs::unlink(&self.dir, super::codec::LOCK)?;
        self.dir.sync_all().map_err(|_| ())
    }
}

impl Drop for OperationLock {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.remove();
        }
    }
}
