//! Authenticated matrix-sdk client construction.

use matrix_sdk::{
    Client, SessionTokens,
    authentication::matrix::MatrixSession,
    config::SyncSettings,
    ruma::{
        DeviceKeyAlgorithm, DeviceKeyId, OwnedDeviceId, OwnedUserId, api::client::keys::get_keys,
    },
};
use std::collections::BTreeMap;

use crate::config::MatrixTransportConfig;

use super::MatrixError;
use super::store_identity;

/// Authenticated Matrix client. SDK types do not leave `src/matrix`.
#[derive(Clone)]
pub struct MatrixClient {
    pub(crate) inner: Client,
    pub(crate) user_id: String,
    device_id: String,
}

impl std::fmt::Debug for MatrixClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatrixClient").finish_non_exhaustive()
    }
}

impl MatrixClient {
    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// Build a persistent E2EE client and restore the configured access-token session.
    pub async fn restore(config: &MatrixTransportConfig) -> Result<Self, MatrixError> {
        Self::restore_with_identity(config, || {}).await
    }

    pub(crate) async fn restore_with_identity(
        config: &MatrixTransportConfig,
        identity_matched: impl FnOnce(),
    ) -> Result<Self, MatrixError> {
        let user_id: OwnedUserId = config
            .user_id
            .parse()
            .map_err(|_| MatrixError::Configuration)?;
        let device_id: OwnedDeviceId = config.device_id.as_str().into();
        let transient = Client::builder()
            .homeserver_url(&config.homeserver)
            .build()
            .await
            .map_err(|_| MatrixError::Configuration)?;
        transient
            .restore_session(session(config, user_id.clone(), device_id.clone()))
            .await
            .map_err(|_| MatrixError::Authentication)?;
        let authenticated = transient
            .whoami()
            .await
            .map_err(|_| MatrixError::Authentication)?;
        if authenticated.is_guest {
            return Err(MatrixError::Authentication);
        }
        if authenticated.user_id != user_id {
            return Err(MatrixError::UserMismatch);
        }
        let authenticated_device = authenticated.device_id.ok_or(MatrixError::DeviceMissing)?;
        if authenticated_device != device_id {
            return Err(MatrixError::DeviceMismatch);
        }
        identity_matched();
        drop(transient);
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
        store_identity::bind(
            store,
            &config.homeserver,
            user_id.as_str(),
            device_id.as_str(),
        )?;
        validate_store_databases(store)?;
        let inner = Client::builder()
            .homeserver_url(&config.homeserver)
            .sqlite_store(store, None)
            .build()
            .await
            .map_err(|_| MatrixError::StoreCorrupt)?;
        tighten_store_files(store)?;
        inner
            .restore_session(session(config, user_id, device_id))
            .await
            .map_err(|_| MatrixError::Authentication)?;
        Ok(Self {
            inner,
            user_id: config.user_id.clone(),
            device_id: config.device_id.clone(),
        })
    }

    /// Prove that the homeserver currently publishes this store's own device key.
    pub async fn prove_server_device_key(&self) -> Result<(), MatrixError> {
        self.inner
            .encryption()
            .wait_for_e2ee_initialization_tasks()
            .await;
        let fingerprint = self
            .inner
            .encryption()
            .ed25519_key()
            .await
            .ok_or(MatrixError::CryptoInitialization)?;
        let user_id: OwnedUserId = self
            .user_id
            .parse()
            .map_err(|_| MatrixError::Configuration)?;
        let device_id: OwnedDeviceId = self.device_id.as_str().into();
        let mut request = get_keys::v3::Request::new();
        request.device_keys = BTreeMap::from([(user_id.clone(), vec![device_id.clone()])]);
        request.timeout = Some(std::time::Duration::from_secs(10));
        let response = self
            .inner
            .send(request)
            .await
            .map_err(|_| MatrixError::DeviceKeyUpload)?;
        if !response.failures.is_empty()
            || response.device_keys.len() != 1
            || response
                .device_keys
                .get(&user_id)
                .is_none_or(|devices| devices.len() != 1)
        {
            return Err(MatrixError::DeviceKeyUpload);
        }
        let raw = response
            .device_keys
            .get(&user_id)
            .and_then(|devices| devices.get(&device_id))
            .ok_or(MatrixError::DeviceKeyUpload)?;
        let keys = raw
            .deserialize()
            .map_err(|_| MatrixError::DeviceKeyUpload)?;
        if keys.user_id != user_id || keys.device_id != device_id {
            return Err(MatrixError::DeviceKeyUpload);
        }
        let key_id = DeviceKeyId::from_parts(DeviceKeyAlgorithm::Ed25519, &device_id);
        if keys.keys.get(&key_id) != Some(&fingerprint) {
            return Err(MatrixError::DeviceKeyUpload);
        }
        Ok(())
    }

    pub(crate) async fn initialize_and_prove(&self) -> Result<(), MatrixError> {
        self.inner
            .sync_once(SyncSettings::new().timeout(std::time::Duration::from_secs(10)))
            .await
            .map_err(|_| MatrixError::CryptoInitialization)?;
        self.prove_server_device_key().await
    }
}

fn session(
    config: &MatrixTransportConfig,
    user_id: OwnedUserId,
    device_id: OwnedDeviceId,
) -> MatrixSession {
    MatrixSession {
        meta: matrix_sdk::SessionMeta { user_id, device_id },
        tokens: SessionTokens {
            access_token: config.access_token.expose().to_owned(),
            refresh_token: None,
        },
    }
}

fn validate_store_path(path: &std::path::Path) -> Result<(), MatrixError> {
    if path.as_os_str().is_empty() || path == std::path::Path::new("/") {
        return Err(MatrixError::UnsafeStorePath);
    }
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(MatrixError::UnsafeStorePath),
    };
    if let Some(metadata) = metadata {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(MatrixError::UnsafeStorePath);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(MatrixError::UnsafeStorePath);
            }
        }
    }
    Ok(())
}

fn create_store_path(path: &std::path::Path) -> Result<(), MatrixError> {
    if !path.exists() {
        std::fs::create_dir_all(path).map_err(|_| MatrixError::UnsafeStorePath)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
                .map_err(|_| MatrixError::UnsafeStorePath)?;
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
            .map_err(|_| MatrixError::UnsafeStoreAncestor)?
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
                    return Err(MatrixError::UnsafeStoreAncestor);
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if metadata.permissions().mode() & 0o022 != 0 {
                        return Err(MatrixError::UnsafeStoreAncestor);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(MatrixError::UnsafeStoreAncestor),
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
                Err(_) => return Err(MatrixError::UnsafeStorePath),
            };
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(MatrixError::UnsafeStorePath);
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o077 != 0 {
                    return Err(MatrixError::UnsafeStorePath);
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
                    Err(_) => return Err(MatrixError::UnsafeStorePath),
                };
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(MatrixError::UnsafeStorePath);
                }
                std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600))
                    .map_err(|_| MatrixError::UnsafeStorePath)?;
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
        let mut file = std::fs::File::open(database).map_err(|_| MatrixError::StoreCorrupt)?;
        let mut header = [0_u8; 16];
        use std::io::Read;
        file.read_exact(&mut header)
            .map_err(|_| MatrixError::StoreCorrupt)?;
        if header != SQLITE_HEADER {
            return Err(MatrixError::StoreCorrupt);
        }
    }
    Ok(())
}
