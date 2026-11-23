use std::{
    fs,
    path::{Component, Path},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StoreEvidence {
    Empty,
    Nonempty,
    Indeterminate,
}

pub(super) fn inspect(root: Option<&Path>) -> StoreEvidence {
    let Some(root) = root else {
        return StoreEvidence::Empty;
    };
    if !root.is_absolute() || root.components().any(|part| part == Component::ParentDir) {
        return StoreEvidence::Indeterminate;
    }
    use std::os::unix::fs::MetadataExt;
    for ancestor in root.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return StoreEvidence::Indeterminate;
            }
            Ok(metadata) => {
                let forbidden = if ancestor == root { 0o077 } else { 0o022 };
                if metadata.mode() & forbidden != 0 {
                    return StoreEvidence::Indeterminate;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return StoreEvidence::Indeterminate,
        }
    }
    let before = match fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return StoreEvidence::Empty,
        Err(_) => return StoreEvidence::Indeterminate,
    };
    let mut entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return StoreEvidence::Indeterminate,
    };
    let evidence = match entries.next() {
        Some(Ok(_)) => StoreEvidence::Nonempty,
        Some(Err(_)) => StoreEvidence::Indeterminate,
        None => StoreEvidence::Empty,
    };
    match fs::symlink_metadata(root) {
        Ok(after)
            if !after.file_type().is_symlink()
                && before.dev() == after.dev()
                && before.ino() == after.ino()
                && before.mtime() == after.mtime()
                && before.mtime_nsec() == after.mtime_nsec()
                && before.ctime() == after.ctime()
                && before.ctime_nsec() == after.ctime_nsec() =>
        {
            evidence
        }
        _ => StoreEvidence::Indeterminate,
    }
}
