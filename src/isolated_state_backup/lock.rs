use std::{fs::File, os::fd::AsRawFd};

use super::{codec::Identity, fs};

pub(crate) struct OperationLock {
    _file: File,
    dir: File,
    identity: Identity,
    armed: bool,
}

impl OperationLock {
    pub(crate) fn acquire(dir: &File) -> Result<Self, ()> {
        let file = fs::create_leaf(dir, super::codec::LOCK, 0o600)?;
        let pending = fs::identity(&file.metadata().map_err(|_| ())?);
        let outcome = pending.and_then(|identity| {
            super::fault::hit("after-lock-create")?;
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(());
            }
            super::fault::hit("after-lock-flock")?;
            dir.sync_all().map_err(|_| ())?;
            super::fault::hit("after-lock-fsync")?;
            Ok(identity)
        });
        let identity = match outcome {
            Ok(identity) => identity,
            Err(()) => {
                let observed = fs::identity(&file.metadata().map_err(|_| ())?)?;
                let current = fs::open_leaf(dir, super::codec::LOCK, false)?;
                let current_identity = fs::identity(&current.metadata().map_err(|_| ())?)?;
                if !fs::same_object(observed, current_identity) {
                    return Err(());
                }
                fs::unlink(dir, super::codec::LOCK)?;
                dir.sync_all().map_err(|_| ())?;
                return Err(());
            }
        };
        Ok(Self {
            _file: file,
            dir: dir.try_clone().map_err(|_| ())?,
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
