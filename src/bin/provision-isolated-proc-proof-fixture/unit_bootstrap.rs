use std::{
    fs::{File, OpenOptions},
    io::{Read as _, Seek as _, Write as _},
    os::{
        fd::{AsRawFd as _, FromRawFd as _},
        unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
    },
    path::Path,
    process::Command,
};

const UNIT_PARENT: &str = "/home/fhy/.config/systemd/user";
const UNIT_NAME: &str = "guigu-codex-test.service";
const TEMP_NAME: &str = ".guigu-codex-test.service.t050-new";
const QUARANTINE_NAME: &str = ".guigu-codex-test.service.t050-rollback";

const UNIT: &[u8] = b"[Unit]\nDescription=Guigu Agent Bridge isolated test\n\n[Service]\nType=simple\nExecStart=/home/fhy/.local/lib/guigu-codex-test/guigu-agent-bridge /home/fhy/.config/guigu-codex-test/config.toml\nRestart=no\nKillMode=control-group\nTimeoutStopSec=30s\n";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Outcome {
    Installed,
    AlreadyPresent,
}

impl Outcome {
    pub(super) const fn code(self) -> &'static str {
        match self {
            Self::Installed => "installed",
            Self::AlreadyPresent => "already-present",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Cause {
    Parent,
    Artifact(ArtifactStage),
    Reload,
    Rollback,
}

impl Cause {
    pub(super) const fn code(self) -> &'static str {
        match self {
            Self::Parent => "parent",
            Self::Artifact(_) => "artifact",
            Self::Reload => "reload",
            Self::Rollback => "rollback",
        }
    }

    pub(super) const fn substage(self) -> &'static str {
        match self {
            Self::Artifact(stage) => stage.code(),
            Self::Parent | Self::Reload | Self::Rollback => "none",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ArtifactStage {
    Inspect,
    Create,
    Write,
    FileSync,
    Chmod,
    Publish,
    ParentSync,
    ParentBinding,
}

impl ArtifactStage {
    const fn code(self) -> &'static str {
        match self {
            Self::Inspect => "inspect",
            Self::Create => "create",
            Self::Write => "write",
            Self::FileSync => "file-sync",
            Self::Chmod => "chmod",
            Self::Publish => "publish",
            Self::ParentSync => "parent-sync",
            Self::ParentBinding => "parent-binding",
        }
    }
}

#[derive(Clone, Copy)]
struct Identity {
    dev: u64,
    ino: u64,
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

fn open_leaf(
    parent: &File,
    name: &str,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> std::io::Result<File> {
    let name = c_name(name);
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags, mode) };
    if fd < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn inspect_leaf(parent: &File, name: &str, cause: Cause) -> Result<Option<File>, Cause> {
    match open_leaf(
        parent,
        name,
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0,
    ) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => Ok(None),
        Err(_) => Err(cause),
    }
}

fn exact_unit(parent: &File) -> Result<bool, Cause> {
    let inspect = Cause::Artifact(ArtifactStage::Inspect);
    let Some(mut file) = inspect_leaf(parent, UNIT_NAME, inspect)? else {
        return Ok(false);
    };
    let metadata = file.metadata().map_err(|_| inspect)?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.gid() != unsafe { libc::getegid() }
        || metadata.mode() & 0o7777 != 0o644
    {
        return Err(inspect);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|_| inspect)?;
    Ok(bytes == UNIT)
}

fn verify_staged(file: &mut File, identity: Identity) -> Result<(), Cause> {
    let write = Cause::Artifact(ArtifactStage::Write);
    let before = file.metadata().map_err(|_| write)?;
    if !before.is_file()
        || before.dev() != identity.dev
        || before.ino() != identity.ino
        || before.uid() != unsafe { libc::geteuid() }
        || before.gid() != unsafe { libc::getegid() }
        || before.mode() & 0o7777 != 0o644
        || before.len() != UNIT.len() as u64
    {
        return Err(write);
    }
    file.rewind().map_err(|_| write)?;
    let mut bytes = Vec::with_capacity(UNIT.len() + 1);
    (&mut *file)
        .take(UNIT.len() as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| write)?;
    let after = file.metadata().map_err(|_| write)?;
    if bytes != UNIT
        || (before.dev(), before.ino(), before.mode(), before.len())
            != (after.dev(), after.ino(), after.mode(), after.len())
    {
        return Err(write);
    }
    Ok(())
}

fn verify_quarantine(
    parent: &File,
    identity: Identity,
    require_canonical: bool,
) -> Result<(), Cause> {
    let mut file =
        inspect_leaf(parent, QUARANTINE_NAME, Cause::Rollback)?.ok_or(Cause::Rollback)?;
    let metadata = file.metadata().map_err(|_| Cause::Rollback)?;
    if !metadata.is_file()
        || metadata.dev() != identity.dev
        || metadata.ino() != identity.ino
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.gid() != unsafe { libc::getegid() }
    {
        return Err(Cause::Rollback);
    }
    if require_canonical {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|_| Cause::Rollback)?;
        if metadata.mode() & 0o7777 != 0o644 || bytes != UNIT {
            return Err(Cause::Rollback);
        }
    }
    Ok(())
}

fn rename_noreplace(parent: &File, from: &str, to: &str, cause: Cause) -> Result<(), Cause> {
    let from = c_name(from);
    let to = c_name(to);
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            from.as_ptr(),
            parent.as_raw_fd(),
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 { Ok(()) } else { Err(cause) }
}

fn quarantine(
    parent: &File,
    name: &str,
    identity: Identity,
    require_canonical: bool,
) -> Result<(), Cause> {
    rename_noreplace(parent, name, QUARANTINE_NAME, Cause::Rollback)?;
    parent.sync_all().map_err(|_| Cause::Rollback)?;
    if verify_quarantine(parent, identity, require_canonical).is_ok() {
        return Ok(());
    }
    // The atomically isolated leaf was not ours. Restore it without replacing
    // anything that appeared at the original name; otherwise retain both residues.
    if rename_noreplace(parent, QUARANTINE_NAME, name, Cause::Rollback).is_ok() {
        parent.sync_all().map_err(|_| Cause::Rollback)?;
    }
    Err(Cause::Rollback)
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

#[derive(Clone, Copy)]
enum HookPoint {
    ParentAuthenticated,
    BeforePublish,
}

fn run_with_reload_and_hook(
    parent_path: &Path,
    mut hook: impl FnMut(HookPoint),
    mut reload: impl FnMut() -> bool,
    fault: Option<ArtifactStage>,
) -> Result<Outcome, Cause> {
    let parent = authenticated_parent(parent_path)?;
    hook(HookPoint::ParentAuthenticated);
    let inspect = Cause::Artifact(ArtifactStage::Inspect);
    if fault == Some(ArtifactStage::Inspect)
        || inspect_leaf(&parent, QUARANTINE_NAME, inspect)?.is_some()
    {
        return Err(inspect);
    }
    if exact_unit(&parent)? {
        if !parent_still_bound(parent_path, &parent) {
            return Err(Cause::Artifact(ArtifactStage::ParentBinding));
        }
        return if reload() {
            Ok(Outcome::AlreadyPresent)
        } else {
            Err(Cause::Reload)
        };
    }
    if inspect_leaf(&parent, UNIT_NAME, inspect)?.is_some() {
        return Err(inspect);
    }
    if fault == Some(ArtifactStage::Create) {
        return Err(Cause::Artifact(ArtifactStage::Create));
    }
    let mut file = open_leaf(
        &parent,
        TEMP_NAME,
        libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        0o600,
    )
    .map_err(|_| Cause::Artifact(ArtifactStage::Create))?;
    let metadata = file
        .metadata()
        .map_err(|_| Cause::Artifact(ArtifactStage::Create))?;
    let identity = Identity {
        dev: metadata.dev(),
        ino: metadata.ino(),
    };
    let staged = (|| {
        if fault == Some(ArtifactStage::Write) {
            return Err(Cause::Artifact(ArtifactStage::Write));
        }
        file.write_all(UNIT)
            .map_err(|_| Cause::Artifact(ArtifactStage::Write))?;
        if fault == Some(ArtifactStage::FileSync) {
            return Err(Cause::Artifact(ArtifactStage::FileSync));
        }
        file.sync_all()
            .map_err(|_| Cause::Artifact(ArtifactStage::FileSync))?;
        if fault == Some(ArtifactStage::Chmod) {
            return Err(Cause::Artifact(ArtifactStage::Chmod));
        }
        file.set_permissions(std::fs::Permissions::from_mode(0o644))
            .map_err(|_| Cause::Artifact(ArtifactStage::Chmod))?;
        file.sync_all()
            .map_err(|_| Cause::Artifact(ArtifactStage::FileSync))?;
        hook(HookPoint::BeforePublish);
        verify_staged(&mut file, identity)?;
        drop(file);
        if fault == Some(ArtifactStage::Publish) {
            return Err(Cause::Artifact(ArtifactStage::Publish));
        }
        rename_noreplace(
            &parent,
            TEMP_NAME,
            UNIT_NAME,
            Cause::Artifact(ArtifactStage::Publish),
        )?;
        Ok(())
    })();
    match staged {
        Ok(()) => {}
        Err(cause) => {
            quarantine(&parent, TEMP_NAME, identity, false)?;
            return Err(cause);
        }
    }
    if fault == Some(ArtifactStage::ParentSync) || parent.sync_all().is_err() {
        quarantine(&parent, UNIT_NAME, identity, true)?;
        return Err(Cause::Artifact(ArtifactStage::ParentSync));
    }
    if fault == Some(ArtifactStage::ParentBinding) || !parent_still_bound(parent_path, &parent) {
        quarantine(&parent, UNIT_NAME, identity, true)?;
        return Err(Cause::Artifact(ArtifactStage::ParentBinding));
    }
    if reload() {
        return Ok(Outcome::Installed);
    }
    quarantine(&parent, UNIT_NAME, identity, true)?;
    if !parent_still_bound(parent_path, &parent) {
        return Err(Cause::Rollback);
    }
    if !reload() {
        return Err(Cause::Rollback);
    }
    Err(Cause::Reload)
}

fn run_with_reload(parent_path: &Path, reload: impl FnMut() -> bool) -> Result<Outcome, Cause> {
    run_with_reload_and_hook(parent_path, |_| {}, reload, None)
}

pub(super) fn run_production() -> Result<Outcome, Cause> {
    run_with_reload(Path::new(UNIT_PARENT), || {
        Command::new("/usr/bin/systemctl")
            .args(["--user", "daemon-reload"])
            .status()
            .is_ok_and(|status| status.success())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parent() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        temp
    }

    #[test]
    fn install_and_repeat_are_canonical_and_residue_free() {
        let temp = parent();
        let mut calls = 0;
        assert_eq!(
            run_with_reload(temp.path(), || {
                calls += 1;
                true
            }),
            Ok(Outcome::Installed)
        );
        let unit = temp.path().join(UNIT_NAME);
        assert_eq!(std::fs::read(&unit).unwrap(), UNIT);
        assert_eq!(std::fs::metadata(&unit).unwrap().mode() & 0o7777, 0o644);
        assert!(!temp.path().join(TEMP_NAME).exists());
        assert_eq!(
            run_with_reload(temp.path(), || true),
            Ok(Outcome::AlreadyPresent)
        );
        assert_eq!(calls, 1);
    }

    #[test]
    fn hostile_unit_and_temp_are_invariant() {
        for name in [UNIT_NAME, TEMP_NAME] {
            let temp = parent();
            let path = temp.path().join(name);
            std::fs::write(&path, b"foreign").unwrap();
            let before = std::fs::read(&path).unwrap();
            let mut called = false;
            assert_eq!(
                run_with_reload(temp.path(), || {
                    called = true;
                    true
                }),
                Err(Cause::Artifact(if name == UNIT_NAME {
                    ArtifactStage::Inspect
                } else {
                    ArtifactStage::Create
                }))
            );
            assert!(!called);
            assert_eq!(std::fs::read(path).unwrap(), before);
        }
    }

    #[test]
    fn hostile_parent_shapes_are_rejected() {
        let root = parent();
        let file = root.path().join("file");
        std::fs::write(&file, b"not a directory").unwrap();
        assert_eq!(run_with_reload(&file, || true), Err(Cause::Parent));

        let target = root.path().join("target");
        std::fs::create_dir(&target).unwrap();
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert_eq!(run_with_reload(&link, || true), Err(Cause::Parent));

        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o722)).unwrap();
        assert_eq!(run_with_reload(&target, || true), Err(Cause::Parent));
    }

    #[test]
    fn noncanonical_existing_unit_shapes_are_invariant() {
        for shape in ["content", "mode", "symlink"] {
            let temp = parent();
            let unit = temp.path().join(UNIT_NAME);
            match shape {
                "content" => std::fs::write(&unit, b"foreign").unwrap(),
                "mode" => {
                    std::fs::write(&unit, UNIT).unwrap();
                    std::fs::set_permissions(&unit, std::fs::Permissions::from_mode(0o600))
                        .unwrap();
                }
                "symlink" => std::os::unix::fs::symlink("outside", &unit).unwrap(),
                _ => unreachable!(),
            }
            let before = std::fs::symlink_metadata(&unit).unwrap();
            assert_eq!(
                run_with_reload(temp.path(), || true),
                Err(Cause::Artifact(ArtifactStage::Inspect))
            );
            let after = std::fs::symlink_metadata(&unit).unwrap();
            assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
            assert!(!temp.path().join(TEMP_NAME).exists());
        }
    }

    #[test]
    fn reload_failure_restores_absence_and_reloads_again() {
        let temp = parent();
        let mut calls = 0;
        assert_eq!(
            run_with_reload(temp.path(), || {
                calls += 1;
                calls == 2
            }),
            Err(Cause::Reload)
        );
        assert_eq!(calls, 2);
        assert!(!temp.path().join(UNIT_NAME).exists());
        assert!(!temp.path().join(TEMP_NAME).exists());
        assert_eq!(
            std::fs::read(temp.path().join(QUARANTINE_NAME)).unwrap(),
            UNIT
        );
    }

    #[test]
    fn rollback_reload_failure_is_distinct_and_absent() {
        let temp = parent();
        let mut calls = 0;
        assert_eq!(
            run_with_reload(temp.path(), || {
                calls += 1;
                false
            }),
            Err(Cause::Rollback)
        );
        assert_eq!(calls, 2);
        assert!(!temp.path().join(UNIT_NAME).exists());
        assert!(!temp.path().join(TEMP_NAME).exists());
        assert_eq!(
            std::fs::read(temp.path().join(QUARANTINE_NAME)).unwrap(),
            UNIT
        );
    }

    #[test]
    fn parent_replacement_cannot_redirect_leaf_operations() {
        let root = parent();
        let active = root.path().join("active");
        let retained = root.path().join("retained");
        std::fs::create_dir(&active).unwrap();
        std::fs::set_permissions(&active, std::fs::Permissions::from_mode(0o700)).unwrap();
        let foreign = b"foreign-unit";
        let mut reloaded = false;
        assert_eq!(
            run_with_reload_and_hook(
                &active,
                |point| {
                    if matches!(point, HookPoint::ParentAuthenticated) {
                        std::fs::rename(&active, &retained).unwrap();
                        std::fs::create_dir(&active).unwrap();
                        std::fs::set_permissions(&active, std::fs::Permissions::from_mode(0o700))
                            .unwrap();
                        std::fs::write(active.join(UNIT_NAME), foreign).unwrap();
                    }
                },
                || {
                    reloaded = true;
                    true
                },
                None,
            ),
            Err(Cause::Artifact(ArtifactStage::ParentBinding))
        );
        assert!(!reloaded);
        assert_eq!(std::fs::read(active.join(UNIT_NAME)).unwrap(), foreign);
        assert!(!retained.join(UNIT_NAME).exists());
        assert_eq!(std::fs::read(retained.join(QUARANTINE_NAME)).unwrap(), UNIT);
    }

    #[test]
    fn publication_collision_preserves_foreign_leaf_and_quarantines_staged_inode() {
        let temp = parent();
        let foreign = b"foreign-unit";
        assert_eq!(
            run_with_reload_and_hook(
                temp.path(),
                |point| {
                    if matches!(point, HookPoint::BeforePublish) {
                        std::fs::write(temp.path().join(UNIT_NAME), foreign).unwrap();
                    }
                },
                || true,
                None,
            ),
            Err(Cause::Artifact(ArtifactStage::Publish))
        );
        assert_eq!(std::fs::read(temp.path().join(UNIT_NAME)).unwrap(), foreign);
        assert_eq!(
            std::fs::read(temp.path().join(QUARANTINE_NAME)).unwrap(),
            UNIT
        );
        assert!(!temp.path().join(TEMP_NAME).exists());
    }

    #[test]
    fn staged_698_byte_mutation_is_rejected_before_publication() {
        let temp = parent();
        let mut reloaded = false;
        assert_eq!(
            run_with_reload_and_hook(
                temp.path(),
                |point| {
                    if matches!(point, HookPoint::BeforePublish) {
                        let mut staged = OpenOptions::new()
                            .append(true)
                            .open(temp.path().join(TEMP_NAME))
                            .unwrap();
                        staged.write_all(&vec![b'x'; 698 - UNIT.len()]).unwrap();
                        staged.sync_all().unwrap();
                    }
                },
                || {
                    reloaded = true;
                    true
                },
                None,
            ),
            Err(Cause::Artifact(ArtifactStage::Write))
        );
        assert!(!reloaded);
        assert!(!temp.path().join(UNIT_NAME).exists());
        assert!(!temp.path().join(TEMP_NAME).exists());
        assert_eq!(
            std::fs::metadata(temp.path().join(QUARANTINE_NAME))
                .unwrap()
                .len(),
            698
        );
    }

    #[test]
    fn rollback_replacement_is_restored_without_deletion() {
        let temp = parent();
        let saved = temp.path().join("saved-canonical");
        let foreign = b"foreign-unit";
        let mut calls = 0;
        assert_eq!(
            run_with_reload(temp.path(), || {
                calls += 1;
                if calls == 1 {
                    std::fs::rename(temp.path().join(UNIT_NAME), &saved).unwrap();
                    std::fs::write(temp.path().join(UNIT_NAME), foreign).unwrap();
                }
                false
            }),
            Err(Cause::Rollback)
        );
        assert_eq!(calls, 1);
        assert_eq!(std::fs::read(saved).unwrap(), UNIT);
        assert_eq!(std::fs::read(temp.path().join(UNIT_NAME)).unwrap(), foreign);
        assert!(!temp.path().join(QUARANTINE_NAME).exists());
    }

    #[test]
    fn injected_pre_reload_artifact_stages_are_exact_and_retain_owned_residue() {
        for stage in [
            ArtifactStage::Write,
            ArtifactStage::FileSync,
            ArtifactStage::Chmod,
            ArtifactStage::ParentSync,
        ] {
            let temp = parent();
            let mut reloaded = false;
            assert_eq!(
                run_with_reload_and_hook(
                    temp.path(),
                    |_| {},
                    || {
                        reloaded = true;
                        true
                    },
                    Some(stage),
                ),
                Err(Cause::Artifact(stage))
            );
            assert!(!reloaded);
            assert!(!temp.path().join(UNIT_NAME).exists());
            assert!(!temp.path().join(TEMP_NAME).exists());
            assert!(temp.path().join(QUARANTINE_NAME).is_file());
        }
    }

    #[test]
    fn artifact_substage_codes_are_fixed_and_complete() {
        let cases = [
            (ArtifactStage::Inspect, "inspect"),
            (ArtifactStage::Create, "create"),
            (ArtifactStage::Write, "write"),
            (ArtifactStage::FileSync, "file-sync"),
            (ArtifactStage::Chmod, "chmod"),
            (ArtifactStage::Publish, "publish"),
            (ArtifactStage::ParentSync, "parent-sync"),
            (ArtifactStage::ParentBinding, "parent-binding"),
        ];
        for (stage, code) in cases {
            let cause = Cause::Artifact(stage);
            assert_eq!(cause.code(), "artifact");
            assert_eq!(cause.substage(), code);
        }
        for cause in [Cause::Parent, Cause::Reload, Cause::Rollback] {
            assert_eq!(cause.substage(), "none");
        }
    }
}
