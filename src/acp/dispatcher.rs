//! The ACP dispatcher: the adapter's [`TaskDispatcher`] implementation.
//!
//! # The two stages
//!
//! ACP has no "task accepted" message: `session/prompt` only answers when the whole
//! turn is over, and progress arrives as `session/update` notifications. The
//! adapter therefore maps the frozen two-stage contract onto the handshakes ACP
//! *does* offer:
//!
//! | stage | what it does | what it means |
//! |-------|--------------|---------------|
//! | [`deliver`](TaskDispatcher::deliver) | spawns/initialises the backend if needed, then `session/new` or `session/resume`, then records and acknowledges the delivery | an **explicit** agent acknowledgement: every delivery gets at least one answer from the agent, and the acceptance is durable in `deliveries` |
//! | [`execute`](TaskDispatcher::execute) | sends `session/prompt` and waits for the terminal `PromptResponse` | the turn ran, with its answer collected from `session/update` |
//!
//! That split is what makes the failure mapping truthful: anything that fails before
//! acceptance is [`DispatchError::NotAccepted`], so the worker never writes
//! `Running`; anything that fails afterwards is [`DispatchError::ExecutionFailed`].
//!
//! The caller must have persisted the task through the submission path before
//! calling `deliver` (normally `Repository::insert_task_and_event` with the
//! initial `Queued` event). `deliver` records its acceptance via
//! `record_delivery`, whose `deliveries.task_id` foreign key requires that task
//! row to already exist.
//!
//! # Terminal mapping
//!
//! | answer | outcome |
//! |--------|---------|
//! | `stopReason: end_turn` | `Ok(Completed { output })` — `output` is the accumulated turn text |
//! | `stopReason: refusal`, `max_tokens`, `max_turn_requests`, anything unknown | `Ok(Failed { .. })` — a deterministic verdict, deliberately **not** retried, because the turn already ran and retrying would repeat its side effects |
//! | prompt timeout, transport exit, framing/protocol failure, agent error object | `Err(ExecutionFailed { .. })` — no terminal result was produced |
//!
//! # What this stage does not do
//!
//! `execute` never starts a process: the session the delivery was accepted on lives
//! in one child, so a dead transport is reported honestly instead of being replaced
//! behind the task's back. Recovering in-flight work is T015; streaming and
//! `session/cancel` are T015; permission policy is T011; execution leases are T016.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::acp::client::{AcpClient, AcpLimits};
use crate::acp::error::{AcpError, Phase};
use crate::acp::session::{SessionState, SqliteSessionStore, StoredSession};
use crate::acp::{bounded, client};
use crate::bus::RegisteredEndpoint;
use crate::bus::{
    BusFuture, Clock, DispatchError, DispatchOutcome, DispatchRequest, TaskDispatcher,
};
use crate::storage::{Delivery, Repository};

/// Builds an [`AcpDispatcher`].
///
/// The working directory and the pool-backed session store are required; everything
/// else has a documented default. `build` reports what is missing instead of
/// panicking.
pub struct AcpDispatcherBuilder {
    cwd: Option<PathBuf>,
    additional_directories: Vec<String>,
    limits: AcpLimits,
    clock: Clock,
    store: Option<SqliteSessionStore>,
    repository: Option<Arc<dyn Repository>>,
    reliability: Option<crate::storage::ReliabilityStore>,
    legacy_results: bool,
}

impl Default for AcpDispatcherBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl AcpDispatcherBuilder {
    /// A builder with default limits and the system clock.
    pub fn new() -> Self {
        Self {
            cwd: None,
            additional_directories: Vec::new(),
            limits: AcpLimits::default(),
            clock: Clock::system(),
            store: None,
            repository: None,
            reliability: None,
            legacy_results: false,
        }
    }

    /// The working directory for backend processes and ACP sessions.
    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn additional_directories(mut self, directories: Vec<String>) -> Self {
        self.additional_directories = directories;
        self
    }

    /// Override the protocol bounds.
    pub fn limits(mut self, limits: AcpLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Override the clock used for delivery and session timestamps.
    pub fn clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// The durable session store (shares the repository's pool).
    pub fn sessions(mut self, store: SqliteSessionStore) -> Self {
        self.store = Some(store);
        self
    }

    /// The repository used to record and acknowledge deliveries.
    pub fn repository(mut self, repository: Arc<dyn Repository>) -> Self {
        self.repository = Some(repository);
        self
    }

    pub fn reliability(mut self, store: crate::storage::ReliabilityStore) -> Self {
        self.reliability = Some(store);
        self
    }

    pub fn legacy_results(mut self, enabled: bool) -> Self {
        self.legacy_results = enabled;
        self
    }

    /// The configured dispatcher.
    ///
    /// # Errors
    ///
    /// A message naming the first missing or unusable setting.
    pub fn build(self) -> Result<AcpDispatcher, &'static str> {
        let Self {
            cwd,
            additional_directories,
            limits,
            clock,
            store,
            repository,
            reliability,
            legacy_results,
        } = self;
        let cwd = cwd.ok_or("a working directory is required")?;
        let cwd = cwd
            .to_str()
            .ok_or("the working directory must be valid UTF-8")?
            .to_owned();
        let store = store.ok_or("a session store is required")?;
        let repository = repository.ok_or("a repository is required")?;
        Ok(AcpDispatcher {
            inner: Arc::new(DispatcherInner {
                cwd,
                additional_directories,
                limits,
                clock,
                store,
                repository,
                reliability,
                client: Mutex::new(None),
                legacy_results,
            }),
        })
    }
}

impl std::fmt::Debug for AcpDispatcherBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcpDispatcherBuilder")
            .field("cwd", &self.cwd)
            .field("limits", &self.limits)
            .field("has_sessions", &self.store.is_some())
            .field("has_repository", &self.repository.is_some())
            .finish()
    }
}

/// The ACP adapter as the worker sees it.
#[derive(Clone)]
pub struct AcpDispatcher {
    inner: Arc<DispatcherInner>,
}

struct DispatcherInner {
    cwd: String,
    additional_directories: Vec<String>,
    limits: AcpLimits,
    clock: Clock,
    store: SqliteSessionStore,
    repository: Arc<dyn Repository>,
    reliability: Option<crate::storage::ReliabilityStore>,
    /// The negotiated backend, if one is running.
    ///
    /// The lock exists only to clone or replace an `Arc`; it is never held across
    /// an `.await`, and the transport it guards holds no lock at all. A dead
    /// client is simply dropped when it is replaced, which closes its outgoing
    /// channel, tears its loop down and reaps the child.
    client: Mutex<Option<Arc<AcpClient>>>,
    legacy_results: bool,
}

pub struct AcpStructuredTurn {
    dispatcher: AcpDispatcher,
    target: RegisteredEndpoint,
    endpoint: crate::models::EndpointId,
    session_id: String,
    handle: crate::acp::AcpTurnHandle,
}

impl AcpStructuredTurn {
    pub async fn recv(&mut self) -> Option<crate::acp::TurnUpdate> {
        self.handle.recv().await
    }

    pub async fn finish(self) -> Result<crate::acp::TurnResult, AcpError> {
        let completed = self.handle.finish().await;
        if let Err(error) = self
            .dispatcher
            .inner
            .store
            .set_state(
                &self.session_id,
                self.endpoint,
                SessionState::Ready,
                self.dispatcher.inner.clock.now(),
            )
            .await
        {
            tracing::warn!(%error,"could not mark the acp session ready after a streamed turn");
        }
        match completed {
            Ok(completed) => AcpClient::decode_prompt_completion(completed)?
                .task_result
                .ok_or_else(|| AcpError::Schema {
                    method: crate::acp::schema::METHOD_SESSION_PROMPT.to_owned(),
                    detail: "missing taskResult in strict mode".to_owned(),
                }),
            Err(AcpError::Exited { .. } | AcpError::TransportClosed) => {
                let recovered = self.dispatcher.ensure_client(&self.target).await?;
                recovered
                    .resume_session_with_workspaces(
                        &self.session_id,
                        &self.dispatcher.inner.cwd,
                        &self.dispatcher.inner.additional_directories,
                    )
                    .await?;
                Err(AcpError::RecoveryNeeded)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn cancel_and_shutdown(self) -> Result<(), AcpError> {
        let client = self
            .dispatcher
            .take_client()
            .ok_or(AcpError::TransportClosed)?;
        client.cancel_and_reap(&self.session_id).await
    }
}

impl AcpDispatcher {
    /// Start building an adapter.
    pub fn builder() -> AcpDispatcherBuilder {
        AcpDispatcherBuilder::new()
    }

    /// Shut the current backend down, if one is running.
    ///
    /// Returns the child's exit code when the platform reports one. T017 calls
    /// this during assembly shutdown; tests call it to prove that no child is left
    /// behind.
    pub async fn shutdown(&self) -> Option<i32> {
        match self.take_client() {
            Some(client) => client.shutdown().await,
            None => None,
        }
    }

    /// Shut the current backend down and report whether its owner loop joined.
    pub async fn shutdown_reaped(&self) -> bool {
        match self.take_client() {
            Some(client) => client.shutdown_reaped().await,
            None => false,
        }
    }

    /// The negotiated backend identity, when a backend is running.
    pub fn backend_id(&self) -> Option<String> {
        self.client_slot()
            .map(|client| client.backend_id().to_owned())
    }

    /// Execute exactly one strict structured turn on the accepted session.
    ///
    /// T016 uses this additive entry point to coordinate bounded continuation.
    /// A missing structured result is a schema error; `end_turn` and legacy
    /// mappings are deliberately not accepted here.
    pub async fn execute_turn(
        &self,
        request: &DispatchRequest<'_>,
        prompt: &str,
    ) -> Result<crate::acp::TurnResult, AcpError> {
        let turn = self.prompt_inner(request, prompt).await?;
        turn.task_result.ok_or_else(|| AcpError::Schema {
            method: crate::acp::schema::METHOD_SESSION_PROMPT.to_owned(),
            detail: "missing taskResult in strict mode".to_owned(),
        })
    }

    pub async fn start_turn(
        &self,
        request: &DispatchRequest<'_>,
        prompt: &str,
        capacity: usize,
    ) -> Result<AcpStructuredTurn, AcpError> {
        let endpoint = request.target.id();
        let lookup_backend = self
            .client_slot()
            .filter(|client| client.is_alive())
            .map(|client| client.backend_id().to_owned())
            .unwrap_or_default();
        let stored = self
            .inner
            .store
            .live_session_with_workspaces(
                endpoint,
                request.task.conversation_id,
                &self.inner.cwd,
                &self.inner.additional_directories,
                &lookup_backend,
            )
            .await?
            .ok_or(AcpError::SessionId {
                detail: "no live session for this delivery".to_owned(),
            })?;
        let session_id = stored.session_id().to_owned();
        let client = match self.client_slot().filter(|client| client.is_alive()) {
            Some(client) => client,
            None => {
                let client = self.ensure_client(request.target).await?;
                client
                    .resume_session_with_workspaces(
                        &session_id,
                        &self.inner.cwd,
                        &self.inner.additional_directories,
                    )
                    .await?;
                return Err(AcpError::RecoveryNeeded);
            }
        };
        self.inner
            .store
            .set_state(
                &session_id,
                endpoint,
                SessionState::Prompting,
                self.inner.clock.now(),
            )
            .await?;
        let handle = client.prompt_stream(&session_id, prompt, capacity).await?;
        Ok(AcpStructuredTurn {
            dispatcher: self.clone(),
            target: request.target.clone(),
            endpoint,
            session_id,
            handle,
        })
    }

    /// Stage one: make the session ready and record the acceptance.
    async fn deliver_inner(&self, request: &DispatchRequest<'_>) -> Result<(), AcpError> {
        let endpoint = request.target.id();
        let conversation = request.task.conversation_id;
        let prepared_at = self.inner.clock.now();
        if let Some(reliability) = &self.inner.reliability {
            reliability
                .prepare_delivery(
                    &request.delivery_id.to_string(),
                    &request.task.task_id.to_string(),
                    request.attempt,
                    &endpoint.to_string(),
                    &prepared_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                )
                .await
                .map_err(|_| AcpError::Storage {
                    source: crate::storage::StorageError::Malformed {
                        field: "delivery_dispositions",
                        detail: "delivery preparation failed".into(),
                    },
                })?;
        }
        let client = self.ensure_client(request.target).await?;

        let existing = self
            .inner
            .store
            .live_session_with_workspaces(
                endpoint,
                conversation,
                &self.inner.cwd,
                &self.inner.additional_directories,
                client.backend_id(),
            )
            .await?;
        let session_id = match existing {
            Some(stored) => {
                // A live session is re-confirmed with the agent rather than assumed:
                // `resume` is this attempt's acknowledgement, and it is what makes a
                // second task in one conversation still have an explicit round trip.
                let session_id = client
                    .resume_session_with_workspaces(
                        stored.session_id(),
                        &self.inner.cwd,
                        &self.inner.additional_directories,
                    )
                    .await?;
                let refreshed = StoredSession::new_with_workspaces(
                    session_id.clone(),
                    endpoint,
                    conversation,
                    &self.inner.cwd,
                    &self.inner.additional_directories,
                    client.backend_id(),
                    self.inner.clock.now(),
                )?;
                self.inner.store.upsert_session(&refreshed).await?;
                session_id
            }
            None => {
                let session_id = client
                    .new_session_with_workspaces(
                        &self.inner.cwd,
                        &self.inner.additional_directories,
                    )
                    .await?;
                let stored = StoredSession::new_with_workspaces(
                    session_id.clone(),
                    endpoint,
                    conversation,
                    &self.inner.cwd,
                    &self.inner.additional_directories,
                    client.backend_id(),
                    self.inner.clock.now(),
                )?;
                self.inner.store.upsert_session(&stored).await?;
                session_id
            }
        };

        // The delivery becomes durable in the same order the worker records it:
        // dispatched, then acknowledged. A replay of the same attempt is a no-op
        // (T009's policy), while a conflicting attempt is reported.
        let now = self.inner.clock.now();
        if let Some(reliability) = &self.inner.reliability {
            if !reliability
                .acknowledge_delivery(
                    &request.delivery_id.to_string(),
                    &session_id,
                    &now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                )
                .await
                .map_err(|_| AcpError::Storage {
                    source: crate::storage::StorageError::Malformed {
                        field: "delivery_dispositions",
                        detail: "delivery acknowledgement failed".into(),
                    },
                })?
            {
                return Err(AcpError::Storage {
                    source: crate::storage::StorageError::Malformed {
                        field: "delivery_dispositions",
                        detail: "delivery acknowledgement was fenced".into(),
                    },
                });
            }
        } else {
            let delivery = Delivery::new(
                request.delivery_id,
                request.task.task_id,
                request.attempt,
                endpoint,
                now,
            );
            self.inner.repository.record_delivery(&delivery).await?;
            let _outcome = self
                .inner
                .repository
                .acknowledge_delivery(request.delivery_id, now)
                .await?;
        }

        tracing::debug!(
            session_id = %session_id,
            backend = %client.backend_id(),
            attempt = request.attempt,
            "acp delivery accepted"
        );
        Ok(())
    }

    /// Stage two: run the turn to its terminal answer.
    async fn execute_inner(
        &self,
        request: &DispatchRequest<'_>,
    ) -> Result<DispatchOutcome, AcpError> {
        let turn = self.prompt_inner(request, &request.task.text).await?;
        if turn.task_result.is_none() && !self.inner.legacy_results {
            return Err(AcpError::Schema {
                method: crate::acp::schema::METHOD_SESSION_PROMPT.to_owned(),
                detail: "missing taskResult in strict mode".to_owned(),
            });
        }
        Ok(map_turn(turn, self.inner.legacy_results))
    }

    async fn prompt_inner(
        &self,
        request: &DispatchRequest<'_>,
        prompt: &str,
    ) -> Result<client::PromptTurn, AcpError> {
        let endpoint = request.target.id();
        let conversation = request.task.conversation_id;
        let lookup_backend = self
            .client_slot()
            .filter(|client| client.is_alive())
            .map(|client| client.backend_id().to_owned())
            .unwrap_or_default();
        let stored = self
            .inner
            .store
            .live_session_with_workspaces(
                endpoint,
                conversation,
                &self.inner.cwd,
                &self.inner.additional_directories,
                &lookup_backend,
            )
            .await?
            .ok_or(AcpError::SessionId {
                detail: "no live session for this delivery".to_owned(),
            })?;
        let session_id = stored.session_id().to_owned();
        let client = match self.client_slot().filter(|client| client.is_alive()) {
            Some(client) => client,
            None => {
                let client = self.ensure_client(request.target).await?;
                client
                    .resume_session_with_workspaces(
                        &session_id,
                        &self.inner.cwd,
                        &self.inner.additional_directories,
                    )
                    .await?;
                return Err(AcpError::RecoveryNeeded);
            }
        };

        self.inner
            .store
            .set_state(
                &session_id,
                endpoint,
                SessionState::Prompting,
                self.inner.clock.now(),
            )
            .await?;

        let turn = client.prompt(&session_id, prompt).await;

        // The turn's own answer is authoritative: a bookkeeping failure must not
        // turn a finished turn into a task failure, so it is logged and the outcome
        // still reflects what the agent said.
        if let Err(error) = self
            .inner
            .store
            .set_state(
                &session_id,
                endpoint,
                SessionState::Ready,
                self.inner.clock.now(),
            )
            .await
        {
            tracing::warn!(%error, "could not mark the acp session ready after a turn");
        }

        match turn {
            Ok(turn) => Ok(turn),
            Err(AcpError::Exited { .. } | AcpError::TransportClosed) => {
                let recovered = self.ensure_client(request.target).await?;
                recovered
                    .resume_session_with_workspaces(
                        &session_id,
                        &self.inner.cwd,
                        &self.inner.additional_directories,
                    )
                    .await?;
                Err(AcpError::RecoveryNeeded)
            }
            Err(error) => Err(error),
        }
    }

    /// The running backend, spawning and negotiating one when needed.
    async fn ensure_client(&self, target: &RegisteredEndpoint) -> Result<Arc<AcpClient>, AcpError> {
        if let Some(client) = self.client_slot()
            && client.is_alive()
        {
            return Ok(client);
        }
        let address = target.address().ok_or(AcpError::UnsupportedAddress {
            phase: Phase::Spawn,
        })?;
        let client = Arc::new(
            AcpClient::connect(
                address,
                target.agent_id(),
                Some(std::path::Path::new(&self.inner.cwd)),
                self.inner.limits,
            )
            .await?,
        );
        let mut slot = self
            .inner
            .client
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *slot = Some(Arc::clone(&client));
        Ok(client)
    }

    fn client_slot(&self) -> Option<Arc<AcpClient>> {
        self.inner.client.lock().ok().and_then(|slot| slot.clone())
    }

    fn take_client(&self) -> Option<Arc<AcpClient>> {
        self.inner
            .client
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
    }
}

impl std::fmt::Debug for AcpDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The working directory is not rendered: it is a host path, and the
        // backend identity is all a diagnostic needs.
        f.debug_struct("AcpDispatcher")
            .field("backend_id", &self.backend_id())
            .field("running", &self.client_slot().is_some())
            .finish()
    }
}

impl TaskDispatcher for AcpDispatcher {
    /// Stage one: prepare the session, then record and acknowledge the delivery.
    ///
    /// Returns `Ok(())` only after the agent has answered this attempt (an
    /// `initialize` handshake and a `session/new` or `session/resume` reply).
    fn deliver<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move {
            self.deliver_inner(&request).await.map_err(|error| {
                tracing::warn!(%error, "acp delivery was not accepted");
                DispatchError::NotAccepted {
                    reason: reason_of(&error),
                }
            })
        })
    }

    /// Stage two: prompt the session and map the turn's answer onto an outcome.
    fn execute<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
        Box::pin(async move {
            self.execute_inner(&request).await.map_err(|error| {
                tracing::warn!(%error, "acp execution failed");
                DispatchError::ExecutionFailed {
                    reason: reason_of(&error),
                }
            })
        })
    }
}

/// Map a finished turn onto the frozen outcome.
fn map_turn(turn: client::PromptTurn, legacy_results: bool) -> DispatchOutcome {
    match turn.task_result {
        Some(crate::acp::TurnResult::Completed { output }) => DispatchOutcome::Completed { output },
        Some(crate::acp::TurnResult::Failed { reason }) => DispatchOutcome::Failed {
            error: bounded(&reason),
        },
        Some(crate::acp::TurnResult::Blocked { reason }) => DispatchOutcome::Failed {
            error: bounded(&format!("blocked: {reason}")),
        },
        Some(crate::acp::TurnResult::Continue { reason, .. }) => DispatchOutcome::Failed {
            error: bounded(&format!("continue: {reason}")),
        },
        None if legacy_results && turn.stop_reason.is_end_turn() => {
            DispatchOutcome::Completed { output: turn.text }
        }
        None => DispatchOutcome::Failed {
            error: bounded("legacy turn did not end"),
        },
    }
}

/// A bounded, credential-free reason for a failed stage.
fn reason_of(error: &AcpError) -> String {
    bounded(&error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acp::schema::StopReason;

    #[test]
    fn an_end_turn_becomes_a_completed_outcome_with_the_turn_text() {
        let outcome = map_turn(
            client::PromptTurn {
                stop_reason: StopReason::EndTurn,
                text: "the answer".to_owned(),
                task_result: None,
            },
            true,
        );
        assert_eq!(
            outcome,
            DispatchOutcome::Completed {
                output: "the answer".to_owned()
            }
        );
    }

    #[test]
    fn every_other_stop_reason_is_a_terminal_failure() {
        for reason in [
            StopReason::Refusal,
            StopReason::MaxTokens,
            StopReason::MaxTurnRequests,
            StopReason::Other("something_new".to_owned()),
        ] {
            let outcome = map_turn(
                client::PromptTurn {
                    stop_reason: reason.clone(),
                    text: "partial".to_owned(),
                    task_result: Some(crate::acp::TurnResult::Failed {
                        reason: format!("agent stopped: {}", reason.as_str()),
                    }),
                },
                false,
            );
            match outcome {
                DispatchOutcome::Failed { error } => {
                    assert!(error.contains(reason.as_str()), "got: {error}");
                    assert!(error.len() <= crate::acp::MAX_DETAIL_BYTES);
                }
                other => panic!("expected a failure for {reason:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_reason_is_bounded_and_carries_no_task_body() {
        let secret = "SECRET-BODY-DO-NOT-LEAK";
        let long = "z".repeat(4096);
        let error = AcpError::Schema {
            method: "session/prompt".to_owned(),
            detail: format!("{long}{secret}"),
        };
        let reason = reason_of(&error);
        assert!(reason.len() <= crate::acp::MAX_DETAIL_BYTES);
        assert!(!reason.contains(secret));
    }

    #[test]
    fn a_builder_reports_what_is_missing() {
        assert_eq!(
            AcpDispatcher::builder().build().err(),
            Some("a working directory is required")
        );
    }

    #[test]
    fn the_dispatcher_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AcpDispatcher>();
    }
}
