//! Authenticated matrix-sdk client construction.

use matrix_sdk::{
    Client, SessionTokens,
    authentication::matrix::MatrixSession,
    ruma::{OwnedDeviceId, OwnedUserId},
};

const DEVICE_ID: &str = "GUIGU_BRIDGE";

use crate::config::MatrixTransportConfig;

use super::MatrixError;

/// Authenticated Matrix client. SDK types do not leave `src/matrix`.
#[derive(Clone)]
pub struct MatrixClient {
    pub(crate) inner: Client,
    pub(crate) user_id: String,
}

impl std::fmt::Debug for MatrixClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatrixClient").finish_non_exhaustive()
    }
}

impl MatrixClient {
    pub fn device_id(&self) -> &str {
        DEVICE_ID
    }

    /// Build a persistent E2EE client and restore the configured access-token session.
    pub async fn restore(config: &MatrixTransportConfig) -> Result<Self, MatrixError> {
        let user_id: OwnedUserId = config
            .user_id
            .parse()
            .map_err(|_| MatrixError::Configuration)?;
        let store = config
            .crypto_store_path
            .as_ref()
            .ok_or(MatrixError::Configuration)?;
        if !config.device_trusted {
            return Err(MatrixError::DeviceUntrusted);
        }
        validate_store_ancestors(store)?;
        validate_store_path(store)?;
        create_store_path(store)?;
        validate_store_files(store)?;
        validate_store_databases(store)?;
        let inner = Client::builder()
            .homeserver_url(&config.homeserver)
            .sqlite_store(store, None)
            .build()
            .await
            .map_err(|_| MatrixError::Configuration)?;
        tighten_store_files(store)?;
        let session = MatrixSession {
            meta: matrix_sdk::SessionMeta {
                user_id,
                device_id: OwnedDeviceId::from(DEVICE_ID),
            },
            tokens: SessionTokens {
                access_token: config.access_token.expose().to_owned(),
                refresh_token: None,
            },
        };
        inner
            .restore_session(session)
            .await
            .map_err(|_| MatrixError::Authentication)?;
        Ok(Self {
            inner,
            user_id: config.user_id.clone(),
        })
    }
}

fn validate_store_path(path: &std::path::Path) -> Result<(), MatrixError> {
    if path.as_os_str().is_empty() || path == std::path::Path::new("/") {
        return Err(MatrixError::Configuration);
    }
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(MatrixError::Configuration),
    };
    if let Some(metadata) = metadata {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(MatrixError::Configuration);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(MatrixError::Configuration);
            }
        }
    }
    Ok(())
}

fn create_store_path(path: &std::path::Path) -> Result<(), MatrixError> {
    if !path.exists() {
        std::fs::create_dir_all(path).map_err(|_| MatrixError::Configuration)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
                .map_err(|_| MatrixError::Configuration)?;
        }
    }
    validate_store_path(path)
}

fn validate_store_ancestors(path: &std::path::Path) -> Result<(), MatrixError> {
    let absolute;
    let path = if path.is_absolute() {
        path
    } else {
        absolute = std::env::current_dir()
            .map_err(|_| MatrixError::Configuration)?
            .join(path);
        &absolute
    };
    let mut current = path.parent();
    while let Some(ancestor) = current {
        if ancestor.as_os_str().is_empty() {
            break;
        }
        match std::fs::symlink_metadata(ancestor) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(MatrixError::Configuration);
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if metadata.permissions().mode() & 0o022 != 0 {
                        return Err(MatrixError::Configuration);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(MatrixError::Configuration),
        }
        current = ancestor.parent();
    }
    Ok(())
}

fn validate_store_files(path: &std::path::Path) -> Result<(), MatrixError> {
    for database in [
        "matrix-sdk-state.sqlite3",
        "matrix-sdk-crypto.sqlite3",
        "matrix-sdk-event-cache.sqlite3",
    ] {
        for suffix in ["", "-wal", "-shm"] {
            let file = path.join(format!("{database}{suffix}"));
            let metadata = match std::fs::symlink_metadata(file) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => return Err(MatrixError::Configuration),
            };
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(MatrixError::Configuration);
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o077 != 0 {
                    return Err(MatrixError::Configuration);
                }
            }
        }
    }
    Ok(())
}

fn tighten_store_files(path: &std::path::Path) -> Result<(), MatrixError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for database in [
            "matrix-sdk-state.sqlite3",
            "matrix-sdk-crypto.sqlite3",
            "matrix-sdk-event-cache.sqlite3",
        ] {
            for suffix in ["", "-wal", "-shm"] {
                let file = path.join(format!("{database}{suffix}"));
                let metadata = match std::fs::symlink_metadata(&file) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(_) => return Err(MatrixError::Configuration),
                };
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(MatrixError::Configuration);
                }
                std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600))
                    .map_err(|_| MatrixError::Configuration)?;
            }
        }
    }
    Ok(())
}

fn validate_store_databases(path: &std::path::Path) -> Result<(), MatrixError> {
    const SQLITE_HEADER: &[u8] = b"SQLite format 3\0";
    for name in [
        "matrix-sdk-state.sqlite3",
        "matrix-sdk-crypto.sqlite3",
        "matrix-sdk-event-cache.sqlite3",
    ] {
        let database = path.join(name);
        if !database.exists() {
            continue;
        }
        let mut file = std::fs::File::open(database).map_err(|_| MatrixError::Configuration)?;
        let mut header = [0_u8; 16];
        use std::io::Read;
        file.read_exact(&mut header)
            .map_err(|_| MatrixError::Configuration)?;
        if header != SQLITE_HEADER {
            return Err(MatrixError::Configuration);
        }
    }
    Ok(())
}
