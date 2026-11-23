use sha2::{Digest as _, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read as _, Seek as _},
    os::{
        fd::{AsRawFd as _, FromRawFd as _},
        unix::fs::{FileExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
    },
    path::Path,
};

pub(super) const UNIT_PARENT: &str = "/home/fhy/.config/systemd/user";
pub(super) const UNIT_NAME: &str = "guigu-codex-test.service";
pub(super) const FORENSIC_NAME: &str = ".guigu-codex-test.service.t050-forensic";
const EXPECTED_SIZE: u64 = 698;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Cause {
    Arguments,
    Parent,
    Artifact(Stage),
    Reload,
    Verify,
    Rollback,
}

impl Cause {
    pub(super) const fn code(self) -> &'static str {
        match self {
            Self::Arguments => "arguments",
            Self::Parent => "parent",
            Self::Artifact(_) => "artifact",
            Self::Reload => "reload",
            Self::Verify => "verify",
            Self::Rollback => "rollback",
        }
    }

    pub(super) const fn substage(self) -> &'static str {
        match self {
            Self::Artifact(stage) => stage.code(),
            Self::Arguments | Self::Parent | Self::Reload | Self::Verify | Self::Rollback => "none",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Stage {
    Inspect,
    Authenticate,
    Move,
    Chmod,
    FileSync,
    ParentSync,
    ParentBinding,
}

impl Stage {
    const fn code(self) -> &'static str {
        match self {
            Self::Inspect => "inspect",
            Self::Authenticate => "authenticate",
            Self::Move => "move",
            Self::Chmod => "chmod",
            Self::FileSync => "file-sync",
            Self::ParentSync => "parent-sync",
            Self::ParentBinding => "parent-binding",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Identity {
    dev: u64,
    ino: u64,
    uid: u32,
    gid: u32,
}

impl Identity {
    fn from(metadata: &std::fs::Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            uid: metadata.uid(),
            gid: metadata.gid(),
        }
    }

    fn matches(self, metadata: &std::fs::Metadata) -> bool {
        self == Self::from(metadata)
    }
}

fn authenticated_parent(path: &Path) -> Result<File, Cause> {
    let parent = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| Cause::Parent)?;
    let metadata = parent.metadata().map_err(|_| Cause::Parent)?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.gid() != unsafe { libc::getegid() }
        || metadata.mode() & 0o022 != 0
    {
        return Err(Cause::Parent);
    }
    Ok(parent)
}

fn c_name(name: &str) -> std::ffi::CString {
    std::ffi::CString::new(name).expect("fixed leaf name contains no NUL")
}

fn open_leaf(parent: &File, name: &str) -> std::io::Result<File> {
    let name = c_name(name);
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn leaf_absent(parent: &File, name: &str) -> Result<bool, Cause> {
    match open_leaf(parent, name) {
        Ok(_) => Ok(false),
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => Ok(true),
        Err(_) => Err(Cause::Artifact(Stage::Inspect)),
    }
}

fn open_active(parent: &File, expected: [u8; 32]) -> Result<(File, Identity), Cause> {
    let mut file = open_leaf(parent, UNIT_NAME).map_err(|_| Cause::Artifact(Stage::Inspect))?;
    let before = file
        .metadata()
        .map_err(|_| Cause::Artifact(Stage::Authenticate))?;
    if !before.is_file()
        || before.uid() != unsafe { libc::geteuid() }
        || before.gid() != unsafe { libc::getegid() }
        || before.mode() & 0o7777 != 0o644
        || before.len() != EXPECTED_SIZE
    {
        return Err(Cause::Artifact(Stage::Authenticate));
    }
    let identity = Identity::from(&before);
    file.rewind()
        .map_err(|_| Cause::Artifact(Stage::Authenticate))?;
    let mut digest = Sha256::new();
    let mut bytes = [0_u8; 4096];
    let mut total = 0_u64;
    loop {
        let count = file
            .read(&mut bytes)
            .map_err(|_| Cause::Artifact(Stage::Authenticate))?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .ok_or(Cause::Artifact(Stage::Authenticate))?;
        if total > EXPECTED_SIZE {
            return Err(Cause::Artifact(Stage::Authenticate));
        }
        digest.update(&bytes[..count]);
    }
    let after = file
        .metadata()
        .map_err(|_| Cause::Artifact(Stage::Authenticate))?;
    if total != EXPECTED_SIZE
        || digest.finalize().as_slice() != expected
        || !identity.matches(&after)
        || (before.mode(), before.len()) != (after.mode(), after.len())
    {
        return Err(Cause::Artifact(Stage::Authenticate));
    }
    Ok((file, identity))
}

fn named_identity(parent: &File, name: &str, identity: Identity, mode: u32) -> bool {
    let Ok(file) = open_leaf(parent, name) else {
        return false;
    };
    let Ok(metadata) = file.metadata() else {
        return false;
    };
    metadata.is_file()
        && identity.matches(&metadata)
        && metadata.mode() & 0o7777 == mode
        && metadata.len() == EXPECTED_SIZE
}

fn fd_matches(file: &File, identity: Identity, mode: u32, expected: [u8; 32]) -> bool {
    let Ok(before) = file.metadata() else {
        return false;
    };
    if !before.is_file()
        || !identity.matches(&before)
        || before.mode() & 0o7777 != mode
        || before.len() != EXPECTED_SIZE
    {
        return false;
    }
    let mut digest = Sha256::new();
    let mut bytes = [0_u8; 4096];
    let mut total = 0_u64;
    loop {
        let Ok(count) = file.read_at(&mut bytes, total) else {
            return false;
        };
        if count == 0 {
            break;
        }
        let Some(next) = total.checked_add(count as u64) else {
            return false;
        };
        if next > EXPECTED_SIZE {
            return false;
        }
        digest.update(&bytes[..count]);
        total = next;
    }
    let Ok(after) = file.metadata() else {
        return false;
    };
    total == EXPECTED_SIZE
        && digest.finalize().as_slice() == expected
        && identity.matches(&after)
        && (before.mode(), before.len()) == (after.mode(), after.len())
}

fn rename_noreplace(parent: &File, from: &str, to: &str) -> bool {
    let from = c_name(from);
    let to = c_name(to);
    unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            from.as_ptr(),
            parent.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        ) == 0
    }
}

fn parent_still_bound(path: &Path, parent: &File) -> bool {
    let Ok(current) = authenticated_parent(path) else {
        return false;
    };
    let (Ok(expected), Ok(actual)) = (parent.metadata(), current.metadata()) else {
        return false;
    };
    (expected.dev(), expected.ino()) == (actual.dev(), actual.ino())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HookPoint {
    ParentAuthenticated,
    ActiveAuthenticated,
    Moved,
    ModeTightened,
    BeforeRollback,
}

struct Retained<'a> {
    parent_path: &'a Path,
    parent: &'a File,
    file: &'a File,
    identity: Identity,
    expected: [u8; 32],
}

impl Retained<'_> {
    fn rollback(
        &self,
        hook: &mut impl FnMut(HookPoint),
        reload: &mut impl FnMut() -> bool,
        forensic_mode: u32,
    ) -> Result<(), Cause> {
        hook(HookPoint::BeforeRollback);
        if !parent_still_bound(self.parent_path, self.parent)
            || !named_identity(self.parent, FORENSIC_NAME, self.identity, forensic_mode)
            || !fd_matches(self.file, self.identity, forensic_mode, self.expected)
        {
            return Err(Cause::Rollback);
        }
        self.file
            .set_permissions(std::fs::Permissions::from_mode(0o644))
            .map_err(|_| Cause::Rollback)?;
        self.file.sync_all().map_err(|_| Cause::Rollback)?;
        if !named_identity(self.parent, FORENSIC_NAME, self.identity, 0o644)
            || !fd_matches(self.file, self.identity, 0o644, self.expected)
            || !rename_noreplace(self.parent, FORENSIC_NAME, UNIT_NAME)
        {
            return Err(Cause::Rollback);
        }
        self.parent.sync_all().map_err(|_| Cause::Rollback)?;
        if !parent_still_bound(self.parent_path, self.parent)
            || !named_identity(self.parent, UNIT_NAME, self.identity, 0o644)
            || !reload()
        {
            return Err(Cause::Rollback);
        }
        Ok(())
    }
}

pub(super) fn run_with_hooks(
    parent_path: &Path,
    expected: [u8; 32],
    mut hook: impl FnMut(HookPoint),
    mut reload: impl FnMut() -> bool,
    mut verify_state: impl FnMut() -> bool,
) -> Result<(), Cause> {
    let parent = authenticated_parent(parent_path)?;
    hook(HookPoint::ParentAuthenticated);
    if !leaf_absent(&parent, FORENSIC_NAME)? {
        return Err(Cause::Artifact(Stage::Inspect));
    }
    let (file, identity) = open_active(&parent, expected)?;
    let retained = Retained {
        parent_path,
        parent: &parent,
        file: &file,
        identity,
        expected,
    };
    hook(HookPoint::ActiveAuthenticated);
    if !parent_still_bound(parent_path, &parent) {
        return Err(Cause::Artifact(Stage::ParentBinding));
    }
    if !rename_noreplace(&parent, UNIT_NAME, FORENSIC_NAME) {
        return Err(Cause::Artifact(Stage::Move));
    }
    hook(HookPoint::Moved);
    if !named_identity(&parent, FORENSIC_NAME, identity, 0o644) {
        return Err(Cause::Rollback);
    }
    if file
        .set_permissions(std::fs::Permissions::from_mode(0o600))
        .is_err()
    {
        retained.rollback(&mut hook, &mut reload, 0o644)?;
        return Err(Cause::Artifact(Stage::Chmod));
    }
    hook(HookPoint::ModeTightened);
    if file.sync_all().is_err() {
        retained.rollback(&mut hook, &mut reload, 0o600)?;
        return Err(Cause::Artifact(Stage::FileSync));
    }
    if !named_identity(&parent, FORENSIC_NAME, identity, 0o600)
        || !fd_matches(&file, identity, 0o600, expected)
    {
        return Err(Cause::Rollback);
    }
    if parent.sync_all().is_err() {
        retained.rollback(&mut hook, &mut reload, 0o600)?;
        return Err(Cause::Artifact(Stage::ParentSync));
    }
    if !parent_still_bound(parent_path, &parent) {
        return Err(Cause::Rollback);
    }
    if !leaf_absent(&parent, UNIT_NAME).unwrap_or(false) {
        retained.rollback(&mut hook, &mut reload, 0o600)?;
        return Err(Cause::Verify);
    }
    if !reload() {
        retained.rollback(&mut hook, &mut reload, 0o600)?;
        return Err(Cause::Reload);
    }
    if !parent_still_bound(parent_path, &parent)
        || !leaf_absent(&parent, UNIT_NAME).unwrap_or(false)
        || !named_identity(&parent, FORENSIC_NAME, identity, 0o600)
        || !fd_matches(&file, identity, 0o600, expected)
        || !verify_state()
    {
        retained.rollback(&mut hook, &mut reload, 0o600)?;
        return Err(Cause::Verify);
    }
    Ok(())
}

#[cfg(test)]
#[path = "unit_residue_tests.rs"]
mod tests;
