use std::time::Duration;

use matrix_sdk::{config::SyncSettings, store::StateStoreDataKey, sync::SyncResponse};

use super::{MatrixClient, MatrixError};
use crate::storage::{CutoverState, CutoverStore, cutover_digest};

#[derive(Clone)]
pub(crate) struct ActiveCutover {
    pub store: CutoverStore,
    pub baseline: String,
}

impl MatrixClient {
    pub async fn cutover_binding(
        &self,
        config: &crate::config::Config,
    ) -> Result<String, MatrixError> {
        use std::os::unix::fs::MetadataExt;
        self.inner
            .encryption()
            .wait_for_e2ee_initialization_tasks()
            .await;
        let key = self
            .inner
            .encryption()
            .ed25519_key()
            .await
            .ok_or(MatrixError::CutoverRefused)?;
        let database = tokio::fs::metadata(&config.bridge.database)
            .await
            .map_err(|_| MatrixError::Storage)?;
        let crypto_path = config
            .transports
            .matrix
            .crypto_store_path
            .as_ref()
            .ok_or(MatrixError::Configuration)?;
        let crypto = tokio::fs::metadata(crypto_path)
            .await
            .map_err(|_| MatrixError::Storage)?;
        let executable = std::env::current_exe().map_err(|_| MatrixError::Storage)?;
        let artifact = tokio::fs::read(executable)
            .await
            .map_err(|_| MatrixError::Storage)?;
        let context = format!(
            "cutover-v1:{config:?}:{}:{}:{}:{}:{}:{}:{}:{}",
            database.dev(),
            database.ino(),
            crypto.dev(),
            crypto.ino(),
            self.user_id,
            self.device_id,
            key,
            cutover_digest(&artifact)
        );
        Ok(cutover_digest(context.as_bytes()))
    }

    pub async fn stored_sync_token(&self) -> Result<Option<String>, MatrixError> {
        self.inner
            .state_store()
            .get_kv_data(StateStoreDataKey::SyncToken)
            .await
            .map_err(|_| MatrixError::Storage)?
            .map(|value| value.into_sync_token().ok_or(MatrixError::CutoverRefused))
            .transpose()
    }

    pub async fn prepare_cutover(&self, store: &CutoverStore) -> Result<Self, MatrixError> {
        let mut state = store.state().await?;
        if state.baseline.is_none() {
            let previous = self.stored_sync_token().await?;
            let response = self
                .inner
                .sync_once(SyncSettings::new().timeout(Duration::from_secs(10)))
                .await
                .map_err(|_| MatrixError::CryptoInitialization)?;
            validate_response(&response, previous.as_deref())?;
            self.prove_server_device_key().await?;
            store.activate(&response.next_batch).await?;
            state = store.state().await?;
        } else {
            self.prove_server_device_key().await?;
        }
        self.active_cutover_client(&state).await
    }

    pub async fn active_cutover_client(&self, state: &CutoverState) -> Result<Self, MatrixError> {
        let baseline = state
            .baseline
            .as_deref()
            .filter(|token| !token.is_empty())
            .ok_or(MatrixError::CutoverRefused)?;
        if self.stored_sync_token().await?.as_deref() != Some(baseline)
            || state
                .watermark
                .as_ref()
                .is_none_or(|token| token.is_empty())
        {
            return Err(MatrixError::CutoverRefused);
        }
        let inner = self
            .inner
            .notification_client(format!("bridge-cutover-{}", uuid::Uuid::new_v4()))
            .await
            .map_err(|_| MatrixError::CutoverRefused)?;
        let active = Self {
            inner,
            user_id: self.user_id.clone(),
            device_id: self.device_id.clone(),
            cutover_baseline: Some(baseline.to_owned()),
        };
        if active.stored_sync_token().await?.as_deref() != Some(baseline)
            || active.inner.user_id() != self.inner.user_id()
            || active.inner.device_id() != self.inner.device_id()
        {
            return Err(MatrixError::CutoverRefused);
        }
        Ok(active)
    }
}

pub(crate) fn validate_response(
    response: &SyncResponse,
    previous: Option<&str>,
) -> Result<(), MatrixError> {
    if response.next_batch.is_empty()
        || previous == Some(response.next_batch.as_str())
        || response
            .rooms
            .joined
            .values()
            .any(|room| room.timeline.limited)
        || response
            .rooms
            .left
            .values()
            .any(|room| room.timeline.limited)
    {
        return Err(MatrixError::CutoverRefused);
    }
    Ok(())
}
