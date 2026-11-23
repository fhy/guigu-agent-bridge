use std::{
    ffi::CString,
    fs::File,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
    path::{Component, Path},
};

use super::{Failure, UNIT};

pub(super) fn validate(fragment: &str) -> Result<(), Failure> {
    if fragment == "/dev/null" {
        return Ok(());
    }
    let runtime = std::env::var_os("XDG_RUNTIME_DIR").ok_or(Failure::Busy)?;
    authenticate(Path::new(fragment), Path::new(&runtime))
}

fn open(parent: &File, name: &std::ffi::OsStr, flags: i32) -> Result<File, Failure> {
    let name = CString::new(name.as_bytes()).map_err(|_| Failure::Busy)?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if descriptor < 0 {
        return Err(Failure::Busy);
    }
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn authenticate(fragment: &Path, runtime: &Path) -> Result<(), Failure> {
    if !runtime.is_absolute()
        || fragment != runtime.join("systemd/user").join(UNIT)
        || runtime
            .components()
            .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(Failure::Busy);
    }
    let owner = unsafe { libc::geteuid() };
    let mut parent = File::open("/").map_err(|_| Failure::Busy)?;
    let directory = fragment.parent().ok_or(Failure::Busy)?;
    let mut current = std::path::PathBuf::from("/");
    let mut directories = Vec::new();
    for component in directory.components().skip(1) {
        let Component::Normal(name) = component else {
            return Err(Failure::Busy);
        };
        parent = open(&parent, name, libc::O_PATH | libc::O_DIRECTORY)?;
        current.push(name);
        let metadata = parent.metadata().map_err(|_| Failure::Busy)?;
        if metadata.mode() & 0o022 != 0
            || (metadata.uid() != 0 && metadata.uid() != owner)
            || (current.starts_with(runtime) && metadata.uid() != owner)
        {
            return Err(Failure::Busy);
        }
        directories.push((
            metadata.dev(),
            metadata.ino(),
            metadata.uid(),
            metadata.mode(),
        ));
    }
    let link = open(&parent, std::ffi::OsStr::new(UNIT), libc::O_PATH)?;
    let before = link.metadata().map_err(|_| Failure::Busy)?;
    trusted_link(&before, owner)?;
    let mut target = [0u8; 64];
    let length = unsafe {
        libc::readlinkat(
            link.as_raw_fd(),
            c"".as_ptr(),
            target.as_mut_ptr().cast(),
            target.len(),
        )
    };
    if length != 9 || &target[..9] != b"/dev/null" {
        return Err(Failure::Busy);
    }
    let after = open(&parent, std::ffi::OsStr::new(UNIT), libc::O_PATH)?
        .metadata()
        .map_err(|_| Failure::Busy)?;
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.ctime_nsec() != after.ctime_nsec()
    {
        return Err(Failure::Busy);
    }
    let mut recheck = File::open("/").map_err(|_| Failure::Busy)?;
    for (component, expected) in directory.components().skip(1).zip(directories) {
        recheck = open(
            &recheck,
            component.as_os_str(),
            libc::O_PATH | libc::O_DIRECTORY,
        )?;
        let actual = recheck.metadata().map_err(|_| Failure::Busy)?;
        if (actual.dev(), actual.ino(), actual.uid(), actual.mode()) != expected {
            return Err(Failure::Busy);
        }
    }
    Ok(())
}

fn trusted_link(metadata: &std::fs::Metadata, owner: u32) -> Result<(), Failure> {
    if !metadata.file_type().is_symlink() || metadata.uid() != owner || metadata.nlink() != 1 {
        return Err(Failure::Busy);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn authenticated_runtime_mask_rejects_untrusted_forms() {
        let root = tempfile::Builder::new()
            .prefix("r092-mask-")
            .tempdir_in(std::env::var_os("HOME").unwrap())
            .unwrap();
        let runtime = root.path();
        std::fs::set_permissions(runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
        let units = runtime.join("systemd/user");
        std::fs::create_dir_all(&units).unwrap();
        for directory in [runtime.join("systemd"), units.clone()] {
            std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let fragment = units.join(UNIT);
        symlink("/dev/null", &fragment).unwrap();
        let metadata = std::fs::symlink_metadata(&fragment).unwrap();
        assert!(trusted_link(&metadata, metadata.uid().wrapping_add(1)).is_err());
        authenticate(&fragment, runtime).unwrap();
        validate("/dev/null").unwrap();
        assert!(authenticate(&fragment, &runtime.join("other")).is_err());
        let alias = runtime.join("alias");
        symlink(&units, &alias).unwrap();
        assert!(authenticate(&alias.join(UNIT), runtime).is_err());
        let duplicate = units.join("duplicate");
        std::fs::hard_link(&fragment, &duplicate).unwrap();
        assert!(authenticate(&fragment, runtime).is_err());
        std::fs::remove_file(duplicate).unwrap();
        std::fs::set_permissions(&units, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(authenticate(&fragment, runtime).is_err());
        std::fs::set_permissions(&units, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::remove_file(&fragment).unwrap();
        symlink("/dev/zero", &fragment).unwrap();
        assert!(authenticate(&fragment, runtime).is_err());
        std::fs::remove_file(&fragment).unwrap();
        std::fs::write(&fragment, b"/dev/null").unwrap();
        assert!(authenticate(&fragment, runtime).is_err());
    }
}
