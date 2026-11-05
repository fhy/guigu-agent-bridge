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
        let recovery_lease = self.recovery.clone().map(MatrixSyncLease::new);
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
                recovery_lease,
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
            let request = self.sync_with_dispositions(&events, &mut dispositions);
            tokio::pin!(request);
            let response = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    let _ = changed;
                    return Ok(());
                }
                response = &mut request => response,
            };
            match response {
                Ok(_) => {
                    if failures != 0 {
                        report_recovered(failures);
                    }
                    failures = 0;
                }
                Err(MatrixError::Transport) if failures < 2 => {
                    failures += 1;
                    report_retry(failures);
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
                Err(error) => {
                    tracing::error!(
                        stage = "incremental-sync",
                        category = "sync-owner-stopped",
                        failures,
                        "required sync owner stopped; inspect the correlated first failure"
                    );
                    return Err(error);
                }
            }
        }
    }
}

fn report_retry(failures: u8) {
    if failures == 1 {
        tracing::warn!(
            stage = "incremental-sync",
            category = "transport",
            failures,
            retry_limit = 2,
            "sync failed; bounded retry pending"
        );
    } else {
        tracing::debug!(
            stage = "incremental-sync",
            category = "transport",
            failures,
            "repeated sync failure within retry budget"
        );
    }
}

fn report_recovered(failures: u8) {
    tracing::info!(
        stage = "incremental-sync",
        category = "transport-recovered",
        failures,
        "sync recovered within retry budget"
    );
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn retries_preserve_first_warning_count_repeats_and_report_recovery() {
        let captured = crate::test_diagnostics::Capture::default();
        tracing::subscriber::with_default(captured.subscriber(), || {
            report_retry(1);
            report_retry(2);
            report_recovered(2);
        });
        let output = captured.output();
        assert_eq!(output.matches("WARN").count(), 1);
        assert_eq!(output.matches("DEBUG").count(), 1);
        assert_eq!(output.matches("INFO").count(), 1);
        assert!(output.contains("failures=2"));
        assert!(output.find("WARN").unwrap() < output.find("transport-recovered").unwrap());
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
