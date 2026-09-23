use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use url::Url;

use super::MatrixError;

const MANIFEST: &str = "identity-v1.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreIdentity {
    version: u8,
    homeserver_origin: String,
    user_id: String,
    device_id: String,
}

impl StoreIdentity {
    fn new(homeserver: &str, user_id: &str, device_id: &str) -> Result<Self, MatrixError> {
        Ok(Self {
            version: 1,
            homeserver_origin: normalized_origin(homeserver)?,
            user_id: user_id.to_owned(),
            device_id: device_id.to_owned(),
        })
    }
}

pub(super) fn bind(
    root: &Path,
    homeserver: &str,
    user_id: &str,
    device_id: &str,
) -> Result<(), MatrixError> {
    let expected = StoreIdentity::new(homeserver, user_id, device_id)?;
    let manifest = root.join(MANIFEST);
    if manifest.exists() {
        return validate(&manifest, &expected);
    }
    if root
        .read_dir()
        .map_err(|_| MatrixError::UnsafeStorePath)?
        .next()
        .is_some()
    {
        return Err(MatrixError::StoreBindingMismatch);
    }

    let temporary = temporary_path(root);
    let bytes = serde_json::to_vec(&expected).map_err(|_| MatrixError::StoreBindingMismatch)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|_| MatrixError::StoreBindingMismatch)?;
    set_private(&temporary)?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| MatrixError::StoreBindingMismatch)?;
    drop(file);
    match fs::hard_link(&temporary, &manifest) {
        Ok(()) => {
            fs::remove_file(&temporary).map_err(|_| MatrixError::StoreBindingMismatch)?;
            sync_directory(root)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temporary);
            validate(&manifest, &expected)
        }
        Err(_) => {
            let _ = fs::remove_file(&temporary);
            Err(MatrixError::StoreBindingMismatch)
        }
    }
}

fn validate(path: &Path, expected: &StoreIdentity) -> Result<(), MatrixError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| MatrixError::StoreBindingMismatch)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(MatrixError::StoreBindingMismatch);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(MatrixError::StoreBindingMismatch);
        }
    }
    let bytes = fs::read(path).map_err(|_| MatrixError::StoreBindingMismatch)?;
    let actual: StoreIdentity =
        serde_json::from_slice(&bytes).map_err(|_| MatrixError::StoreBindingMismatch)?;
    if &actual != expected {
        return Err(MatrixError::StoreBindingMismatch);
    }
    Ok(())
}

fn normalized_origin(value: &str) -> Result<String, MatrixError> {
    let url = Url::parse(value).map_err(|_| MatrixError::Configuration)?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(MatrixError::Configuration);
    }
    let host = url
        .host_str()
        .ok_or(MatrixError::Configuration)?
        .to_ascii_lowercase();
    let port = url
        .port_or_known_default()
        .ok_or(MatrixError::Configuration)?;
    Ok(format!(
        "{}://{host}:{port}",
        url.scheme().to_ascii_lowercase()
    ))
}

fn temporary_path(root: &Path) -> PathBuf {
    root.join(format!(".identity-v1-{}.tmp", uuid::Uuid::now_v7()))
}

#[cfg(unix)]
fn set_private(path: &Path) -> Result<(), MatrixError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|_| MatrixError::StoreBindingMismatch)
}

#[cfg(not(unix))]
fn set_private(_: &Path) -> Result<(), MatrixError> {
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), MatrixError> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| MatrixError::StoreBindingMismatch)
}

#[cfg(not(unix))]
fn sync_directory(_: &Path) -> Result<(), MatrixError> {
    Ok(())
}
