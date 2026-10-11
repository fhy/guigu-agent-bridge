use crate::{
    Refusal, Result,
    identity::{FileId, ObjectId},
};
use std::{
    ffi::CString,
    fs::File,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::MetadataExt,
    },
    path::{Component, Path},
};

pub fn component(name: &str) -> Result<CString> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\0') {
        return Err(Refusal::Path);
    }
    CString::new(name).map_err(|_| Refusal::Path)
}
pub struct OwnedDir {
    file: File,
    identity: ObjectId,
    anchors: Vec<Anchor>,
}
struct Anchor {
    parent: File,
    parent_id: ObjectId,
    name: CString,
    child_id: ObjectId,
}
impl OwnedDir {
    pub fn authenticate(file: File, expected: &ObjectId) -> Result<Self> {
        expected.validate_dir(expected.uid, expected.gid)?;
        if ObjectId::capture(&file)? != *expected {
            return Err(Refusal::Identity);
        }
        Ok(Self {
            file,
            identity: expected.clone(),
            anchors: Vec::new(),
        })
    }
    pub fn open_absolute(path: &Path, uid: u32, gid: u32) -> Result<Self> {
        let text = path.to_str().ok_or(Refusal::Path)?;
        if !path.is_absolute()
            || text.split('/').any(|part| part == "." || part == "..")
            || text.contains("//")
            || text.ends_with('/')
            || path
                .components()
                .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
        {
            return Err(Refusal::Path);
        }
        let raw = unsafe {
            libc::open(
                c"/".as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        if raw < 0 {
            return Err(Refusal::Path);
        }
        let mut current = unsafe { File::from_raw_fd(raw) };
        let mut anchors = Vec::new();
        for part in path.components().skip(1) {
            let name = component(part.as_os_str().to_str().ok_or(Refusal::Path)?)?;
            let next = open_at(&current, &name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
            anchors.push(Anchor {
                parent: current.try_clone().map_err(|_| Refusal::Identity)?,
                parent_id: ObjectId::capture(&current)?,
                name,
                child_id: ObjectId::capture(&next)?,
            });
            current = next;
            let meta = current.metadata().map_err(|_| Refusal::Identity)?;
            if ![0, uid].contains(&meta.uid()) || meta.mode() & 0o022 != 0 {
                return Err(Refusal::Permission);
            }
        }
        let identity = ObjectId::capture(&current)?;
        identity.validate_dir(uid, gid)?;
        let mut owned = Self::authenticate(current, &identity)?;
        owned.anchors = anchors;
        owned.recheck()?;
        Ok(owned)
    }
    pub fn file(&self) -> &File {
        &self.file
    }
    pub fn identity(&self) -> &ObjectId {
        &self.identity
    }
    pub fn recheck(&self) -> Result<()> {
        if ObjectId::capture(&self.file)? != self.identity {
            return Err(Refusal::Identity);
        }
        for anchor in &self.anchors {
            if ObjectId::capture(&anchor.parent)? != anchor.parent_id {
                return Err(Refusal::Identity);
            }
            let child = open_at(
                &anchor.parent,
                &anchor.name,
                libc::O_RDONLY | libc::O_DIRECTORY,
                0,
            )?;
            if ObjectId::capture(&child)? != anchor.child_id {
                return Err(Refusal::Identity);
            }
        }
        Ok(())
    }
    pub fn open(&self, name: &str, flags: i32, mode: u32) -> Result<File> {
        let name = component(name)?;
        self.recheck()?;
        open_at(&self.file, &name, flags, mode)
    }
    pub fn subdir(&self, name: &str) -> Result<Self> {
        let file = self.open(name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        let id = ObjectId::capture(&file)?;
        id.validate_dir(self.identity.uid, self.identity.gid)?;
        let mut owned = Self::authenticate(file, &id)?;
        for anchor in &self.anchors {
            owned.anchors.push(Anchor {
                parent: anchor.parent.try_clone().map_err(|_| Refusal::Identity)?,
                parent_id: anchor.parent_id.clone(),
                name: anchor.name.clone(),
                child_id: anchor.child_id.clone(),
            });
        }
        owned.anchors.push(Anchor {
            parent: self.file.try_clone().map_err(|_| Refusal::Identity)?,
            parent_id: self.identity.clone(),
            name: component(name)?,
            child_id: id,
        });
        owned.recheck()?;
        Ok(owned)
    }
    pub fn mkdir(&self, name: &str) -> Result<Self> {
        self.recheck()?;
        let name = component(name)?;
        if unsafe { libc::mkdirat(self.file.as_raw_fd(), name.as_ptr(), 0o700) } != 0 {
            return Err(Refusal::Publication);
        }
        self.sync()?;
        self.subdir(name.to_str().map_err(|_| Refusal::Path)?)
    }
    pub fn sync(&self) -> Result<()> {
        self.file.sync_all().map_err(|_| Refusal::Publication)
    }
    pub fn verify_leaf(&self, name: &str, expected: &FileId) -> Result<File> {
        let file = self.open(name, libc::O_RDONLY, 0)?;
        expected.recheck(&file)?;
        Ok(file)
    }
}
pub fn open_at(parent: &File, name: &CString, flags: i32, mode: u32) -> Result<File> {
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            mode,
        )
    };
    if fd < 0 {
        return Err(Refusal::Path);
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}
pub struct NamespaceLock {
    file: File,
    identity: FileId,
    name: String,
    epoch: String,
}
impl NamespaceLock {
    pub fn create(dir: &OwnedDir, name: &str, epoch: &str) -> Result<Self> {
        if !crate::contract::hex(epoch, 64) {
            return Err(Refusal::Lock);
        }
        let mut file = dir.open(name, libc::O_RDWR | libc::O_CREAT | libc::O_EXCL, 0o600)?;
        use std::io::Write;
        file.write_all(epoch.as_bytes())
            .map_err(|_| Refusal::Lock)?;
        file.sync_all().map_err(|_| Refusal::Lock)?;
        dir.sync()?;
        let identity = FileId::capture(&file)?;
        Self::acquire(dir, name, file, &identity, epoch)
    }
    pub fn acquire(
        dir: &OwnedDir,
        name: &str,
        file: File,
        expected: &FileId,
        epoch: &str,
    ) -> Result<Self> {
        expected.validate(dir.identity.uid, dir.identity.gid, 0o600)?;
        if expected.sha256 != crate::digest(epoch.as_bytes()) {
            return Err(Refusal::Lock);
        }
        expected.recheck(&file)?;
        dir.verify_leaf(name, expected)?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(Refusal::Lock);
        }
        let lock = Self {
            file,
            identity: expected.clone(),
            name: name.into(),
            epoch: epoch.into(),
        };
        lock.recheck(dir)?;
        Ok(lock)
    }
    pub fn recheck(&self, dir: &OwnedDir) -> Result<()> {
        self.identity
            .recheck(&self.file)
            .map_err(|_| Refusal::Lock)?;
        dir.verify_leaf(&self.name, &self.identity)
            .map_err(|_| Refusal::Lock)?;
        Ok(())
    }
    pub fn identity(&self) -> &FileId {
        &self.identity
    }
    pub fn epoch(&self) -> &str {
        &self.epoch
    }
}
