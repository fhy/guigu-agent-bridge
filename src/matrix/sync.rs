//! Bounded, cancellable Matrix sync ownership.

#[path = "sync_owner.rs"]
mod owner;
#[path = "sync_shutdown.rs"]
mod shutdown;
use owner::classify_sdk_error;
pub(crate) use shutdown::MatrixSyncLease;

#[path = "sync_batch.rs"]
mod batch;
#[cfg(test)]
#[path = "succession_runloop_tests.rs"]
mod succession_tests;

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
    processing: AtomicBool,
    first_sync: AtomicBool,
    failure_observer: Option<FailureObserver>,
    #[cfg(test)]
    test_failure: Mutex<Option<MatrixError>>,
    #[cfg(test)]
    before_receipt: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
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
            processing: AtomicBool::new(false),
            first_sync: AtomicBool::new(true),
            failure_observer: None,
            #[cfg(test)]
            test_failure: Mutex::new(None),
            #[cfg(test)]
            before_receipt: None,
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
        self.processing.store(true, Ordering::Release);
        let _processing = RequestOwner(&self.processing);
        let maintain = async {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            loop {
                tick.tick().await;
                if let Some(recovery) = &self.recovery
                    && recovery
                        .store
                        .renew_component_lease(
                            "MatrixSync",
                            &recovery.owner_token,
                            recovery.owner_epoch,
                            &timestamp(),
                        )
                        .await
                        .is_err()
                {
                    break MatrixError::Storage;
                }
            }
        };
        // Both futures remain polled while either waits for the shared writer
        // lock. Awaiting renewal inside a timer branch would deadlock a paused
        // batch that already owns that lock. No detached task survives shutdown.
        let result = tokio::select! {
            result = self.process_batch(response, token.as_deref(), events, dispositions) => result,
            error = maintain, if self.recovery.is_some() => Err(error),
        };
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
    recovery_lease: Option<MatrixSyncLease>,
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
        self.join.await.map_err(|_| MatrixError::Join)??;
        if let Some(lease) = self.recovery_lease {
            lease.release().await?;
        }
        Ok(())
    }

    pub async fn shutdown_bounded(self, timeout: Duration) -> Result<(), MatrixError> {
        if let Some(lease) = self.shutdown_bounded_deferred(timeout).await? {
            lease.release().await?;
        }
        Ok(())
    }

    pub(crate) async fn shutdown_bounded_deferred(
        self,
        timeout: Duration,
    ) -> Result<Option<MatrixSyncLease>, MatrixError> {
        let (result, lease) = self.shutdown_joined(timeout).await?;
        result?;
        Ok(lease)
    }

    pub(crate) async fn shutdown_joined(
        mut self,
        timeout: Duration,
    ) -> Result<(Result<(), MatrixError>, Option<MatrixSyncLease>), MatrixError> {
        let _ = self.shutdown_tx.send(true);
        match tokio::time::timeout(timeout, &mut self.join).await {
            Ok(result) => {
                let result = result.map_err(|_| MatrixError::Join)?;
                tracing::debug!(
                    stage = "matrix-sync-cleanup",
                    joined = true,
                    processing_failed = result.is_err(),
                    lease_deferred = self.recovery_lease.is_some(),
                    "sync task joined; processing result is separate from cleanup"
                );
                Ok((result, self.recovery_lease))
            }
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
