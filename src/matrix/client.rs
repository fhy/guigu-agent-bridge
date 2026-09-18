//! Authenticated matrix-sdk client construction.

use matrix_sdk::{
    Client, SessionTokens,
    authentication::matrix::MatrixSession,
    ruma::{OwnedDeviceId, OwnedUserId},
};

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
    /// Build a no-E2EE client and restore the configured access-token session.
    pub async fn restore(config: &MatrixTransportConfig) -> Result<Self, MatrixError> {
        let user_id: OwnedUserId = config
            .user_id
            .parse()
            .map_err(|_| MatrixError::Configuration)?;
        let inner = Client::builder()
            .homeserver_url(&config.homeserver)
            .build()
            .await
            .map_err(|_| MatrixError::Configuration)?;
        let session = MatrixSession {
            meta: matrix_sdk::SessionMeta {
                user_id,
                device_id: OwnedDeviceId::from("GUIGU_BRIDGE"),
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
