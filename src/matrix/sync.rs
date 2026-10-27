//! Bounded, cancellable Matrix sync ownership.

#[path = "sync_owner.rs"]
mod owner;
use owner::classify_sdk_error;

#[path = "sync_batch.rs"]
mod batch;

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use matrix_sdk::config::SyncSettings;
use matrix_sdk::ruma::api::client::error::ErrorKind;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use uuid::Uuid;

use super::{InboundMatrixEvent, MatrixClient, MatrixError, event::decode_event};
use crate::storage::{BatchAdmission, MatrixRecoveryStore};

/// Boxed future returned by [`SyncTokenStore`].
pub(crate) type FailureObserver = Arc<dyn Fn(&MatrixError) + Send + Sync>;
pub(super) type Disposition = Result<(), MatrixError>;

struct RequestOwner<'a>(&'a AtomicBool);
impl Drop for RequestOwner<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

pub type SyncTokenFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait RawMatrixEventConsumer: Send + Sync {
    fn consume<'a>(
        &'a self,
        raw: &'a str,
        room_id: &'a str,
    ) -> SyncTokenFuture<'a, Result<bool, MatrixError>>;
    fn admit<'a>(
        &'a self,
        _raw: &'a str,
        _room: &'a str,
    ) -> SyncTokenFuture<'a, Result<(), MatrixError>> {
        Box::pin(async { Ok(()) })
    }
}

pub trait MissingRoomKeyObserver: Send + Sync {
    fn record_missing_room_key(&self);
}

/// Adapter-owned checkpoint storage. T017 supplies durable production storage.
pub trait SyncTokenStore: Send + Sync {
    /// Load the last token committed after successful event delivery.
    fn load<'a>(&'a self) -> SyncTokenFuture<'a, Result<Option<String>, MatrixError>>;
    /// Commit a token after all events in its batch have been delivered.
    fn save<'a>(&'a self, token: &'a str) -> SyncTokenFuture<'a, Result<(), MatrixError>>;
}

/// Process-local checkpoint store used until T017 wires durable storage.
#[derive(Debug, Default)]
pub struct MemorySyncTokenStore {
    token: Mutex<Option<String>>,
}

#[derive(Clone)]
pub struct SqliteSyncTokenStore {
    pool: SqlitePool,
}

impl SqliteSyncTokenStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

impl SyncTokenStore for SqliteSyncTokenStore {
    fn load<'a>(&'a self) -> SyncTokenFuture<'a, Result<Option<String>, MatrixError>> {
        Box::pin(async move {
            sqlx::query_scalar("SELECT watermark FROM matrix_recovery_control WHERE control_id=1 AND phase='active'")
                .fetch_optional(&self.pool)
                .await
                .map_err(|_| MatrixError::Storage)
        })
    }

    fn save<'a>(&'a self, _token: &'a str) -> SyncTokenFuture<'a, Result<(), MatrixError>> {
        Box::pin(async { Err(MatrixError::Storage) })
    }
}

impl SyncTokenStore for MemorySyncTokenStore {
    fn load<'a>(&'a self) -> SyncTokenFuture<'a, Result<Option<String>, MatrixError>> {
        Box::pin(async move {
            self.token
                .lock()
                .map(|token| token.clone())
                .map_err(|_| MatrixError::Storage)
        })
    }

    fn save<'a>(&'a self, token: &'a str) -> SyncTokenFuture<'a, Result<(), MatrixError>> {
        Box::pin(async move {
            *self.token.lock().map_err(|_| MatrixError::Storage)? = Some(token.to_owned());
            Ok(())
        })
    }
}

/// Configuration and dependencies for one Matrix sync owner.
pub struct MatrixSync {
    client: MatrixClient,
    tokens: Arc<dyn SyncTokenStore>,
    capacity: usize,
    server_timeout: Duration,
    raw_consumer: Option<Arc<dyn RawMatrixEventConsumer>>,
    missing_key_observer: Option<Arc<dyn MissingRoomKeyObserver>>,
    recovery: Option<RecoveryBinding>,
    initial_response: Mutex<Option<matrix_sdk::sync::SyncResponse>>,
    initial_token: Mutex<Option<String>>,
    in_flight: AtomicBool,
    first_sync: AtomicBool,
    failure_observer: Option<FailureObserver>,
    #[cfg(test)]
    test_failure: Mutex<Option<MatrixError>>,
}

#[derive(Clone)]
pub(super) struct RecoveryBinding {
    owner_token: String,
    store: MatrixRecoveryStore,
    account_id: String,
    operation_id: String,
    owner_epoch: i64,
}

impl std::fmt::Debug for MatrixSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatrixSync")
            .field("capacity", &self.capacity)
            .field("server_timeout", &self.server_timeout)
            .finish_non_exhaustive()
    }
}

impl MatrixSync {
    /// Create a sync owner with a bounded output channel.
    pub fn new(
        client: MatrixClient,
        tokens: Arc<dyn SyncTokenStore>,
        capacity: usize,
    ) -> Result<Self, MatrixError> {
        if capacity == 0 {
            return Err(MatrixError::Configuration);
        }
        Ok(Self {
            client,
            tokens,
            capacity,
            server_timeout: Duration::from_secs(30),
            raw_consumer: None,
            missing_key_observer: None,
            recovery: None,
            initial_response: Mutex::new(None),
            initial_token: Mutex::new(None),
            in_flight: AtomicBool::new(false),
            first_sync: AtomicBool::new(true),
            failure_observer: None,
            #[cfg(test)]
            test_failure: Mutex::new(None),
        })
    }

    pub fn with_raw_consumer(mut self, consumer: Arc<dyn RawMatrixEventConsumer>) -> Self {
        self.raw_consumer = Some(consumer);
        self
    }

    pub fn with_missing_key_observer(mut self, observer: Arc<dyn MissingRoomKeyObserver>) -> Self {
        self.missing_key_observer = Some(observer);
        self
    }

    pub fn with_recovery(
        mut self,
        store: MatrixRecoveryStore,
        account_id: impl Into<String>,
        operation_id: impl Into<String>,
        owner_epoch: i64,
    ) -> Self {
        let operation_id = operation_id.into();
        self.recovery = Some(RecoveryBinding {
            owner_token: operation_id.clone(),
            store,
            account_id: account_id.into(),
            operation_id,
            owner_epoch,
        });
        self
    }

    pub(crate) fn with_failure_observer(mut self, observer: FailureObserver) -> Self {
        self.failure_observer = Some(observer);
        self
    }

    pub fn with_initial_response(self, response: matrix_sdk::sync::SyncResponse) -> Self {
        *self.initial_response.lock().expect("initial response lock") = Some(response);
        self
    }

    pub fn with_component_owner(mut self, owner: String) -> Self {
        if let Some(recovery) = &mut self.recovery {
            recovery.owner_token = owner;
        }
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_failure(mut self, error: MatrixError) -> Self {
        self.test_failure = Mutex::new(Some(error));
        self
    }

    pub fn start(self) -> (MatrixSyncHandle, mpsc::Receiver<InboundMatrixEvent>) {
        let (handle, events, _) = self.spawn(false);
        (handle, events)
    }

    pub fn start_with_dispositions(
        self,
    ) -> (
        MatrixSyncHandle,
        mpsc::Receiver<InboundMatrixEvent>,
        mpsc::Sender<Disposition>,
    ) {
        self.spawn(true)
    }

    pub async fn sync_once(
        &self,
        events: &mpsc::Sender<InboundMatrixEvent>,
    ) -> Result<usize, MatrixError> {
        self.sync_with_dispositions(events, &mut None).await
    }

    async fn response_for(
        &self,
        token: Option<&str>,
    ) -> Result<matrix_sdk::sync::SyncResponse, MatrixError> {
        let initial = self
            .initial_response
            .lock()
            .map_err(|_| MatrixError::Storage)?
            .take();
        let response = if let Some(response) = initial {
            if self.recovery.is_some()
                && self
                    .initial_token
                    .lock()
                    .map_err(|_| MatrixError::Storage)?
                    .take()
                    .as_deref()
                    != token
            {
                return Err(MatrixError::CutoverRefused);
            }
            response
        } else {
            let mut settings = SyncSettings::new()
                .timeout(self.server_timeout)
                .full_state(self.recovery.is_some() && self.first_sync.load(Ordering::Acquire));
            if let Some(token) = token {
                settings = settings.token(token);
            }
            self.client
                .inner
                .sync_once(settings)
                .await
                .map_err(classify_sdk_error)?
        };
        Ok(response)
    }

    pub(super) async fn fence_fatal_response(
        &self,
        error: &MatrixError,
    ) -> Result<(), MatrixError> {
        if matches!(
            error,
            MatrixError::CutoverRefused
                | MatrixError::Protocol { .. }
                | MatrixError::CryptoInitialization
                | MatrixError::DeviceKicked
        ) && let Some(recovery) = &self.recovery
        {
            recovery
                .store
                .refuse_response(
                    &recovery.operation_id,
                    &recovery.owner_token,
                    recovery.owner_epoch,
                    "invalid-or-unrecoverable-response",
                    &timestamp(),
                )
                .await
                .map_err(|_| MatrixError::Storage)?;
        }
        Ok(())
    }

    pub(super) async fn sync_with_dispositions(
        &self,
        events: &mpsc::Sender<InboundMatrixEvent>,
        dispositions: &mut Option<mpsc::Receiver<Disposition>>,
    ) -> Result<usize, MatrixError> {
        if self
            .in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(MatrixError::Protocol {
                detail: "concurrent sync owner",
            });
        }
        let _owner = RequestOwner(&self.in_flight);
        #[cfg(test)]
        if let Some(error) = self
            .test_failure
            .lock()
            .map_err(|_| MatrixError::Storage)?
            .take()
        {
            return Err(error);
        }
        let token = self.tokens.load().await?;
        if let Some(recovery) = &self.recovery {
            recovery
                .store
                .renew_component_lease(
                    "MatrixSync",
                    &recovery.owner_token,
                    recovery.owner_epoch,
                    &timestamp(),
                )
                .await
                .map_err(|_| MatrixError::Storage)?;
        }
        let response = match self.response_for(token.as_deref()).await {
            Ok(response) => response,
            Err(error) => {
                self.fence_fatal_response(&error).await?;
                return Err(error);
            }
        };
        let result = self
            .process_batch(response, token.as_deref(), events, dispositions)
            .await;
        if let Err(error) = &result {
            self.fence_fatal_response(error).await?;
        }
        result
    }
}

/// Shutdown and join handle for a running [`MatrixSync`].
pub struct MatrixSyncHandle {
    shutdown_tx: watch::Sender<bool>,
    join: JoinHandle<Result<(), MatrixError>>,
    alive: Arc<AtomicBool>,
}

impl std::fmt::Debug for MatrixSyncHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatrixSyncHandle").finish_non_exhaustive()
    }
}

impl MatrixSyncHandle {
    pub fn alive(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.alive)
    }
    /// Request shutdown and wait until the owner has released its request/task.
    pub async fn shutdown(self) -> Result<(), MatrixError> {
        let _ = self.shutdown_tx.send(true);
        self.join.await.map_err(|_| MatrixError::Join)?
    }

    pub async fn shutdown_bounded(mut self, timeout: Duration) -> Result<(), MatrixError> {
        let _ = self.shutdown_tx.send(true);
        match tokio::time::timeout(timeout, &mut self.join).await {
            Ok(result) => result.map_err(|_| MatrixError::Join)?,
            Err(_) => {
                self.join.abort();
                let _ = self.join.await;
                Err(MatrixError::Join)
            }
        }
    }

    /// Wait for a terminal sync failure without requesting shutdown.
    pub async fn wait(self) -> Result<(), MatrixError> {
        self.join.await.map_err(|_| MatrixError::Join)?
    }
}
