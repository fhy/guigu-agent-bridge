use std::{
    fs::{File, OpenOptions},
    io::Write as _,
    os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
    path::Path,
    process::Command,
};

const UNIT_PARENT: &str = "/home/fhy/.config/systemd/user";
const UNIT_NAME: &str = "guigu-codex-test.service";
const TEMP_NAME: &str = ".guigu-codex-test.service.t050-new";

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
    Artifact,
    Reload,
    Rollback,
}

impl Cause {
    pub(super) const fn code(self) -> &'static str {
        match self {
            Self::Parent => "parent",
            Self::Artifact => "artifact",
            Self::Reload => "reload",
            Self::Rollback => "rollback",
        }
    }
}

#[derive(Clone, Copy)]
struct Identity {
    dev: u64,
    ino: u64,
}

fn authenticated_parent(path: &Path) -> Result<File, Cause> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| Cause::Parent)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.gid() != unsafe { libc::getegid() }
        || metadata.mode() & 0o022 != 0
    {
        return Err(Cause::Parent);
    }
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| Cause::Parent)
}

fn exact_unit(path: &Path) -> Result<bool, Cause> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(Cause::Artifact),
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.gid() != unsafe { libc::getegid() }
        || metadata.mode() & 0o7777 != 0o644
    {
        return Err(Cause::Artifact);
    }
    Ok(std::fs::read(path).map_err(|_| Cause::Artifact)? == UNIT)
}

fn remove_identity(path: &Path, identity: Identity, require_canonical: bool) -> Result<(), Cause> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| Cause::Rollback)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.dev() != identity.dev
        || metadata.ino() != identity.ino
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.gid() != unsafe { libc::getegid() }
        || (require_canonical
            && (metadata.mode() & 0o7777 != 0o644
                || std::fs::read(path).map_err(|_| Cause::Rollback)? != UNIT))
    {
        return Err(Cause::Rollback);
    }
    std::fs::remove_file(path).map_err(|_| Cause::Rollback)
}

fn rename_noreplace(from: &Path, to: &Path) -> Result<(), Cause> {
    use std::os::unix::ffi::OsStrExt as _;
    let from = std::ffi::CString::new(from.as_os_str().as_bytes()).map_err(|_| Cause::Artifact)?;
    let to = std::ffi::CString::new(to.as_os_str().as_bytes()).map_err(|_| Cause::Artifact)?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(Cause::Artifact)
    }
}

fn run_with_reload(parent_path: &Path, mut reload: impl FnMut() -> bool) -> Result<Outcome, Cause> {
    let parent = authenticated_parent(parent_path)?;
    let unit = parent_path.join(UNIT_NAME);
    if exact_unit(&unit)? {
        return if reload() {
            Ok(Outcome::AlreadyPresent)
        } else {
            Err(Cause::Reload)
        };
    }
    if std::fs::symlink_metadata(&unit).is_ok() {
        return Err(Cause::Artifact);
    }
    let temp = parent_path.join(TEMP_NAME);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temp)
        .map_err(|_| Cause::Artifact)?;
    let metadata = file.metadata().map_err(|_| Cause::Artifact)?;
    let identity = Identity {
        dev: metadata.dev(),
        ino: metadata.ino(),
    };
    let staged = (|| {
        file.write_all(UNIT).map_err(|_| Cause::Artifact)?;
        file.sync_all().map_err(|_| Cause::Artifact)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o644))
            .map_err(|_| Cause::Artifact)?;
        file.sync_all().map_err(|_| Cause::Artifact)?;
        drop(file);
        rename_noreplace(&temp, &unit)?;
        Ok(())
    })();
    match staged {
        Ok(()) => {}
        Err(cause) => {
            remove_identity(&temp, identity, false)?;
            parent.sync_all().map_err(|_| Cause::Rollback)?;
            return Err(cause);
        }
    }
    if parent.sync_all().is_err() {
        remove_identity(&unit, identity, true)?;
        parent.sync_all().map_err(|_| Cause::Rollback)?;
        return Err(Cause::Artifact);
    }
    if reload() {
        return Ok(Outcome::Installed);
    }
    remove_identity(&unit, identity, true)?;
    parent.sync_all().map_err(|_| Cause::Rollback)?;
    if !reload() {
        return Err(Cause::Rollback);
    }
    Err(Cause::Reload)
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
                Err(Cause::Artifact)
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
            assert_eq!(run_with_reload(temp.path(), || true), Err(Cause::Artifact));
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
    }
}
