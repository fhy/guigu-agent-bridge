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

use super::{InboundMatrixEvent, MatrixClient, MatrixError, event::decode_event};

/// Boxed future returned by [`SyncTokenStore`].
pub type SyncTokenFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type Disposition = Result<(), MatrixError>;
pub(crate) type FailureObserver = Arc<dyn Fn(&MatrixError) + Send + Sync>;

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

    pub(crate) fn with_failure_observer(mut self, observer: FailureObserver) -> Self {
        self.failure_observer = Some(observer);
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

    /// Run one initial or incremental sync and commit its checkpoint.
    async fn process_response(
        &self,
        events: &mpsc::Sender<InboundMatrixEvent>,
        dispositions: &mut Option<mpsc::Receiver<Disposition>>,
        response: SyncResponse,
    ) -> Result<usize, MatrixError> {
        let mut delivered = 0;
        for (room_id, update) in response.rooms.joined {
            for timeline in update.timeline.events {
                let raw = timeline.kind.raw().json().get();
                if let Some(consumer) = &self.raw_consumer
                    && consumer.consume(raw, room_id.as_str()).await?
                {
                    delivered += 1;
                    continue;
                }
                match decode_event(raw, room_id.as_str(), &self.client.user_id) {
                    Ok(Some(event)) => {
                        events.try_send(event).map_err(|error| match error {
                            mpsc::error::TrySendError::Full(_) => MatrixError::Backpressure,
                            mpsc::error::TrySendError::Closed(_) => MatrixError::ConsumerClosed,
                        })?;
                        if let Some(dispositions) = dispositions {
                            dispositions
                                .recv()
                                .await
                                .ok_or(MatrixError::ConsumerClosed)??;
                        }
                        delivered += 1;
                    }
                    Ok(None) => {}
                    Err(MatrixError::Protocol { .. }) => {
                        // Malformed and unauthorized timeline items have a terminal
                        // disposition. They are consumed from this batch without
                        // advancing the bounded event channel.
                    }
                    Err(error) => return Err(error),
                }
                let missing_decryption = serde_json::from_str::<serde_json::Value>(raw)
                    .ok()
                    .and_then(|event| {
                        event
                            .get("type")
                            .and_then(|value| value.as_str())
                            .map(str::to_owned)
                    })
                    .as_deref()
                    == Some("m.room.encrypted");
                if missing_decryption {
                    if let Some(observer) = &self.missing_key_observer {
                        observer.record_missing_room_key();
                    }
                    return Err(MatrixError::CryptoInitialization);
                }
            }
        }
        self.tokens.save(&response.next_batch).await?;
        Ok(delivered)
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
        let token = self.tokens.load().await?;
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
        self.process_response(events, dispositions, response).await
    }

    async fn run(
        mut self,
        events: mpsc::Sender<InboundMatrixEvent>,
        mut dispositions: Option<mpsc::Receiver<Disposition>>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), MatrixError> {
        if let Some(response) = self.initial_response.take() {
            self.process_response(&events, &mut dispositions, response)
                .await?;
        }
        let mut failures = 0_u8;
        loop {
            let response = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    let _ = changed;
                    return Ok(());
                }
                response = self.sync_once_with_dispositions(&events, &mut dispositions) => response,
            };
            match response {
                Ok(_) => {
                    failures = 0;
                }
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

    /// Wait for a terminal sync failure without requesting shutdown.
    pub async fn wait(self) -> Result<(), MatrixError> {
        self.join.await.map_err(|_| MatrixError::Join)?
    }
}
