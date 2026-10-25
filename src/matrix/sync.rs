//! Bounded, cancellable Matrix sync ownership.

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
use matrix_sdk::sync::SyncResponse;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use super::cutover::{ActiveCutover, validate_response};
use super::{InboundMatrixEvent, MatrixClient, MatrixError};
use crate::storage::CutoverStore;

#[path = "sync_processing.rs"]
mod processing;

#[cfg(test)]
#[path = "cutover_tests.rs"]
mod cutover_tests;

/// Boxed future returned by [`SyncTokenStore`].
pub type SyncTokenFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type Disposition = Result<(), MatrixError>;
pub(crate) type FailureObserver = Arc<dyn Fn(&MatrixError) + Send + Sync>;

enum ProcessResult {
    Complete(usize),
    Cancelled,
}

pub trait RawMatrixEventConsumer: Send + Sync {
    fn consume<'a>(
        &'a self,
        raw: &'a str,
        room_id: &'a str,
    ) -> SyncTokenFuture<'a, Result<bool, MatrixError>>;
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
    initial_response: Option<SyncResponse>,
    failure_observer: Option<FailureObserver>,
    cutover: Option<ActiveCutover>,
    batch_lock: tokio::sync::Mutex<()>,
    #[cfg(test)]
    test_failure: Option<MatrixError>,
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
            initial_response: None,
            failure_observer: None,
            cutover: None,
            batch_lock: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            test_failure: None,
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

    pub fn with_initial_response(mut self, response: SyncResponse) -> Self {
        self.initial_response = Some(response);
        self
    }

    pub async fn with_cutover(mut self, store: CutoverStore) -> Result<Self, MatrixError> {
        let state = store.state().await?;
        let baseline = state.baseline.ok_or(MatrixError::CutoverRefused)?;
        if self.initial_response.is_some()
            || self.client.cutover_baseline.as_deref() != Some(baseline.as_str())
            || self.client.stored_sync_token().await?.as_deref() != Some(baseline.as_str())
        {
            return Err(MatrixError::CutoverRefused);
        }
        self.cutover = Some(ActiveCutover { store, baseline });
        Ok(self)
    }

    async fn request_response(&self) -> Result<SyncResponse, MatrixError> {
        let (token, previous) = if let Some(cutover) = &self.cutover {
            let state = cutover.store.state().await?;
            if state.baseline.as_deref() != Some(cutover.baseline.as_str()) {
                return Err(MatrixError::CutoverRefused);
            }
            (
                Some(cutover.baseline.clone()),
                self.client.stored_sync_token().await?,
            )
        } else {
            (self.tokens.load().await?, None)
        };
        let mut settings = SyncSettings::new().timeout(self.server_timeout);
        if let Some(token) = token {
            settings = settings.token(token);
        }
        let response = self
            .client
            .inner
            .sync_once(settings)
            .await
            .map_err(classify_sdk_error)?;
        if self.cutover.is_some() {
            validate_response(&response, previous.as_deref())?;
        }
        Ok(response)
    }

    pub(crate) fn with_failure_observer(mut self, observer: FailureObserver) -> Self {
        self.failure_observer = Some(observer);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_failure(mut self, error: MatrixError) -> Self {
        self.test_failure = Some(error);
        self
    }

    /// Spawn the single sync owner task.
    pub fn start(self) -> (MatrixSyncHandle, mpsc::Receiver<InboundMatrixEvent>) {
        let (handle, events, _) = self.start_inner(false);
        (handle, events)
    }

    #[doc(hidden)]
    pub fn start_with_dispositions(
        self,
    ) -> (
        MatrixSyncHandle,
        mpsc::Receiver<InboundMatrixEvent>,
        mpsc::Sender<Disposition>,
    ) {
        self.start_inner(true)
    }

    fn start_inner(
        self,
        durable_dispositions: bool,
    ) -> (
        MatrixSyncHandle,
        mpsc::Receiver<InboundMatrixEvent>,
        mpsc::Sender<Disposition>,
    ) {
        let (events_tx, events_rx) = mpsc::channel(self.capacity);
        let (dispositions_tx, dispositions_rx) = mpsc::channel(self.capacity);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let alive = Arc::new(AtomicBool::new(true));
        let task_alive = Arc::clone(&alive);
        let failure_observer = self.failure_observer.clone();
        let join = tokio::spawn(async move {
            let dispositions = durable_dispositions.then_some(dispositions_rx);
            let result = self.run(events_tx, dispositions, shutdown_rx).await;
            if let Err(error) = &result
                && let Some(observer) = &failure_observer
            {
                observer(error);
            }
            task_alive.store(false, Ordering::Release);
            result
        });
        (
            MatrixSyncHandle {
                shutdown_tx,
                join,
                alive,
            },
            events_rx,
            dispositions_tx,
        )
    }

    pub async fn sync_once(
        &self,
        events: &mpsc::Sender<InboundMatrixEvent>,
    ) -> Result<usize, MatrixError> {
        self.sync_once_with_dispositions(events, &mut None).await
    }

    async fn sync_once_with_dispositions(
        &self,
        events: &mpsc::Sender<InboundMatrixEvent>,
        dispositions: &mut Option<mpsc::Receiver<Disposition>>,
    ) -> Result<usize, MatrixError> {
        let _batch = self.batch_lock.lock().await;
        if self.cutover.is_some() {
            return Err(MatrixError::CutoverRefused);
        }
        let response = self.request_response().await?;
        match self
            .process_response(events, dispositions, None, response)
            .await?
        {
            ProcessResult::Complete(delivered) => Ok(delivered),
            ProcessResult::Cancelled => unreachable!("sync_once has no cancellation receiver"),
        }
    }

    async fn run(
        mut self,
        events: mpsc::Sender<InboundMatrixEvent>,
        mut dispositions: Option<mpsc::Receiver<Disposition>>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), MatrixError> {
        #[cfg(test)]
        if let Some(error) = self.test_failure.take() {
            return Err(error);
        }
        if self.cutover.is_some() && (self.initial_response.is_some() || dispositions.is_none()) {
            return Err(MatrixError::CutoverRefused);
        }
        if let Some(response) = self.initial_response.take()
            && matches!(
                self.process_response(&events, &mut dispositions, Some(&mut shutdown), response,)
                    .await?,
                ProcessResult::Cancelled
            )
        {
            return Ok(());
        }
        let mut failures = 0_u8;
        loop {
            let response = self
                .sync_once_with_shutdown(&events, &mut dispositions, &mut shutdown)
                .await;
            match response {
                Ok(ProcessResult::Complete(_)) => {
                    failures = 0;
                }
                Ok(ProcessResult::Cancelled) => return Ok(()),
                Err(MatrixError::Transport) if failures < 2 => {
                    failures += 1;
                    tokio::select! {
                        biased;
                        changed = shutdown.changed() => {
                            let _ = changed;
                            return Ok(());
                        }
                        _ = tokio::time::sleep(Duration::from_millis(100 * u64::from(failures))) => {}
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn sync_once_with_shutdown(
        &self,
        events: &mpsc::Sender<InboundMatrixEvent>,
        dispositions: &mut Option<mpsc::Receiver<Disposition>>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<ProcessResult, MatrixError> {
        let _batch = self.batch_lock.lock().await;
        let response = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                return Ok(ProcessResult::Cancelled);
            }
            response = self.request_response() => {
                response?
            }
        };
        self.process_response(events, dispositions, Some(shutdown), response)
            .await
    }
}

fn classify_sdk_error(error: matrix_sdk::Error) -> MatrixError {
    match error.client_api_error_kind() {
        Some(
            ErrorKind::UnknownToken { .. } | ErrorKind::MissingToken | ErrorKind::Forbidden { .. },
        ) => MatrixError::DeviceKicked,
        _ => MatrixError::Transport,
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
