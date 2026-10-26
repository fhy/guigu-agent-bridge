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
pub type SyncTokenFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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

    fn save<'a>(&'a self, token: &'a str) -> SyncTokenFuture<'a, Result<(), MatrixError>> {
        Box::pin(async move {
            let result = sqlx::query("UPDATE matrix_recovery_control SET watermark=?,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE control_id=1 AND phase='active'")
                .bind(token)
                .execute(&self.pool)
                .await
                .map_err(|_| MatrixError::Storage)?;
            if result.rows_affected() != 1 {
                return Err(MatrixError::Storage);
            }
            Ok(())
        })
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
}

#[derive(Clone)]
struct RecoveryBinding {
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
        self.recovery = Some(RecoveryBinding {
            store,
            account_id: account_id.into(),
            operation_id: operation_id.into(),
            owner_epoch,
        });
        self
    }

    /// Spawn the single sync owner task.
    pub fn start(self) -> (MatrixSyncHandle, mpsc::Receiver<InboundMatrixEvent>) {
        let (events_tx, events_rx) = mpsc::channel(self.capacity);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let alive = Arc::new(AtomicBool::new(true));
        let task_alive = Arc::clone(&alive);
        let join = tokio::spawn(async move {
            let result = self.run(events_tx, shutdown_rx).await;
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
        )
    }

    /// Run one initial or incremental sync and commit its checkpoint.
    pub async fn sync_once(
        &self,
        events: &mpsc::Sender<InboundMatrixEvent>,
    ) -> Result<usize, MatrixError> {
        let token = self.tokens.load().await?;
        let mut settings = SyncSettings::new().timeout(self.server_timeout);
        if let Some(ref token) = token {
            settings = settings.token(token);
        }
        let response = self
            .client
            .inner
            .sync_once(settings)
            .await
            .map_err(classify_sdk_error)?;
        let recovery = self.recovery.clone();
        let batch_id = Uuid::now_v7().to_string();
        if let Some(recovery) = &recovery {
            let expected_events = response
                .rooms
                .joined
                .values()
                .flat_map(|update| update.timeline.events.iter())
                .filter_map(|event| {
                    serde_json::from_str::<serde_json::Value>(event.kind.raw().json().get())
                        .ok()
                        .and_then(|value| {
                            value
                                .get("event_id")
                                .and_then(|id| id.as_str())
                                .map(str::to_owned)
                        })
                })
                .collect::<Vec<_>>();
            let mut digest = Sha256::new();
            digest.update(response.next_batch.as_str());
            digest.update(serde_json::to_vec(&expected_events).map_err(|_| MatrixError::Storage)?);
            recovery
                .store
                .begin_batch(
                    &batch_id,
                    token.as_deref(),
                    &response.next_batch,
                    &format!("{:x}", digest.finalize()),
                    token.as_deref().unwrap_or_default(),
                    &recovery.operation_id,
                    recovery.owner_epoch,
                    &expected_events,
                    "sync",
                )
                .await
                .map_err(|_| MatrixError::Storage)?;
        }
        let mut delivered = 0;
        for (room_id, update) in response.rooms.joined {
            for timeline in update.timeline.events {
                let raw = timeline.kind.raw().json().get();
                let receipt_event = serde_json::from_str::<serde_json::Value>(raw)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("event_id")
                            .and_then(|id| id.as_str())
                            .map(str::to_owned)
                    });
                if let (Some(recovery), Some(event_id)) = (&recovery, &receipt_event) {
                    let mut digest = Sha256::new();
                    digest.update(raw.as_bytes());
                    let admission = recovery
                        .store
                        .reserve_event(
                            &recovery.account_id,
                            room_id.as_str(),
                            event_id,
                            &recovery.operation_id,
                            &format!("{:x}", digest.finalize()),
                            &batch_id,
                            "sync",
                        )
                        .await
                        .map_err(|_| MatrixError::Storage)?;
                    if admission == BatchAdmission::Duplicate {
                        continue;
                    }
                }
                if let Some(consumer) = &self.raw_consumer
                    && consumer.consume(raw, room_id.as_str()).await?
                {
                    if let (Some(recovery), Some(event_id)) = (&recovery, &receipt_event) {
                        recovery
                            .store
                            .finish_event(
                                &recovery.account_id,
                                room_id.as_str(),
                                event_id,
                                &recovery.operation_id,
                                &batch_id,
                                &recovery.operation_id,
                                recovery.owner_epoch,
                                "sync",
                            )
                            .await
                            .map_err(|_| MatrixError::Storage)?;
                    }
                    delivered += 1;
                    continue;
                }
                if let Some(event) = decode_event(raw, room_id.as_str(), &self.client.user_id)? {
                    events.try_send(event).map_err(|error| match error {
                        mpsc::error::TrySendError::Full(_) => MatrixError::Backpressure,
                        mpsc::error::TrySendError::Closed(_) => MatrixError::ConsumerClosed,
                    })?;
                    delivered += 1;
                    if let (Some(recovery), Some(event_id)) = (&recovery, &receipt_event) {
                        recovery
                            .store
                            .finish_event(
                                &recovery.account_id,
                                room_id.as_str(),
                                event_id,
                                &recovery.operation_id,
                                &batch_id,
                                &recovery.operation_id,
                                recovery.owner_epoch,
                                "sync",
                            )
                            .await
                            .map_err(|_| MatrixError::Storage)?;
                    }
                } else if serde_json::from_str::<serde_json::Value>(raw)
                    .ok()
                    .and_then(|event| {
                        event
                            .get("type")
                            .and_then(|value| value.as_str())
                            .map(str::to_owned)
                    })
                    .as_deref()
                    == Some("m.room.encrypted")
                    && let Some(observer) = &self.missing_key_observer
                {
                    observer.record_missing_room_key();
                    if let (Some(recovery), Some(event_id)) = (&recovery, &receipt_event) {
                        recovery
                            .store
                            .refuse_event(
                                &recovery.account_id,
                                room_id.as_str(),
                                event_id,
                                &recovery.operation_id,
                                &batch_id,
                                &recovery.operation_id,
                                recovery.owner_epoch,
                                "missing_room_key",
                                "sync",
                            )
                            .await
                            .map_err(|_| MatrixError::Storage)?;
                    }
                }
            }
        }
        if let Some(recovery) = &recovery {
            recovery
                .store
                .complete_batch(
                    &batch_id,
                    &response.next_batch,
                    &recovery.operation_id,
                    recovery.owner_epoch,
                    "sync",
                )
                .await
                .map_err(|_| MatrixError::Storage)?;
        } else {
            self.tokens.save(&response.next_batch).await?;
        }
        Ok(delivered)
    }

    async fn run(
        self,
        events: mpsc::Sender<InboundMatrixEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), MatrixError> {
        let mut failures = 0_u8;
        loop {
            let response = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    let _ = changed;
                    return Ok(());
                }
                response = self.sync_once(&events) => response,
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
