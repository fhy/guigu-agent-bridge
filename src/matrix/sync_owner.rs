use super::*;

impl MatrixSync {
    pub(crate) async fn rebuild_state(&self) -> Result<(), MatrixError> {
        let recovery = self.recovery.as_ref().ok_or(MatrixError::Configuration)?;
        let token = self
            .tokens
            .load()
            .await?
            .ok_or(MatrixError::CutoverRefused)?;
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
        let response = self
            .client
            .inner
            .sync_once(
                SyncSettings::new()
                    .token(&token)
                    .full_state(true)
                    .timeout(self.server_timeout),
            )
            .await
            .map_err(classify_sdk_error)?;
        if response.next_batch.is_empty()
            || response
                .rooms
                .joined
                .values()
                .any(|room| room.timeline.limited)
        {
            recovery
                .store
                .refuse_response(
                    &recovery.operation_id,
                    &recovery.owner_token,
                    recovery.owner_epoch,
                    "rebuild-history-gap",
                    &timestamp(),
                )
                .await
                .map_err(|_| MatrixError::Storage)?;
            return Err(MatrixError::CutoverRefused);
        }
        *self
            .initial_token
            .lock()
            .map_err(|_| MatrixError::Storage)? = Some(token);
        *self
            .initial_response
            .lock()
            .map_err(|_| MatrixError::Storage)? = Some(response);
        Ok(())
    }

    pub(super) fn spawn(
        self,
        acknowledged: bool,
    ) -> (
        MatrixSyncHandle,
        mpsc::Receiver<InboundMatrixEvent>,
        mpsc::Sender<Disposition>,
    ) {
        let (events_tx, events_rx) = mpsc::channel(self.capacity);
        let (dispositions_tx, dispositions_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let alive = Arc::new(AtomicBool::new(true));
        let task_alive = Arc::clone(&alive);
        let failure_observer = self.failure_observer.clone();
        let join = tokio::spawn(async move {
            let result = self
                .run(
                    events_tx,
                    shutdown_rx,
                    acknowledged.then_some(dispositions_rx),
                )
                .await;
            if let (Err(error), Some(observer)) = (&result, failure_observer) {
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

    async fn run(
        self,
        events: mpsc::Sender<InboundMatrixEvent>,
        mut shutdown: watch::Receiver<bool>,
        mut dispositions: Option<mpsc::Receiver<Disposition>>,
    ) -> Result<(), MatrixError> {
        if let Some(recovery) = &self.recovery {
            recovery
                .store
                .reconcile_restart()
                .await
                .map_err(|_| MatrixError::Storage)?;
        }
        let mut failures = 0_u8;
        loop {
            let response = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    let _ = changed;
                    return Ok(());
                }
                response = self.sync_with_dispositions(&events, &mut dispositions) => response,
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

pub(super) fn classify_sdk_error(error: matrix_sdk::Error) -> MatrixError {
    match error.client_api_error_kind() {
        Some(
            ErrorKind::UnknownToken { .. } | ErrorKind::MissingToken | ErrorKind::Forbidden { .. },
        ) => MatrixError::DeviceKicked,
        Some(_) => MatrixError::CutoverRefused,
        None => MatrixError::Transport,
    }
}
