use std::sync::Arc;
use std::time::Duration;

use crate::bus::{BusFuture, DispatchError, DispatchOutcome, DispatchRequest, TaskDispatcher};
use crate::models::EndpointAddress;

use super::wire::{Message, PROTOCOL_VERSION, Part, Role, SendParams, TaskState};
use super::{A2aClient, A2aStore, InboundReservation};

/// Polling A2A dispatcher. Accepted remote task bindings are kept separate from
/// wire task identities; production assembly supplies one instance per peer.
pub struct A2aDispatcher {
    client: Arc<A2aClient>,
    store: A2aStore,
    peer_id: String,
    poll_interval: Duration,
    max_polls: u32,
}

impl A2aDispatcher {
    pub fn new(
        client: Arc<A2aClient>,
        store: A2aStore,
        peer_id: String,
        poll_interval: Duration,
        max_polls: u32,
    ) -> Self {
        Self {
            client,
            store,
            peer_id,
            poll_interval,
            max_polls,
        }
    }
}

impl TaskDispatcher for A2aDispatcher {
    fn deliver<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move {
            let local_id = request.task.task_id.to_string();
            let reservation = self
                .store
                .reserve_outbound(
                    &self.peer_id,
                    &local_id,
                    &local_id,
                    &request.task.conversation_id.to_string(),
                    chrono::Utc::now(),
                )
                .await
                .map_err(|error| DispatchError::NotAccepted {
                    reason: error.to_string(),
                })?;
            let exchange = match reservation {
                InboundReservation::Replay(row) if row.external_task_id.is_some() => return Ok(()),
                InboundReservation::Replay(_) => return Err(DispatchError::AcceptanceUnknown),
                InboundReservation::New(row) => row,
            };
            let snapshot = self
                .client
                .send(SendParams {
                    protocol_version: PROTOCOL_VERSION.into(),
                    request_id: local_id.clone(),
                    context_id: request.task.conversation_id.to_string(),
                    message: Message {
                        message_id: local_id.clone(),
                        role: Role::User,
                        parts: vec![Part::Text {
                            text: request.task.text.clone(),
                        }],
                    },
                })
                .await;
            let snapshot = match snapshot {
                Ok(snapshot) => snapshot,
                Err(super::A2aError::Unauthorized) => {
                    return Err(DispatchError::NotAccepted {
                        reason: "A2A authentication failed".into(),
                    });
                }
                Err(_) => {
                    self.store
                        .mark_acceptance_unknown(
                            &exchange.exchange_id,
                            exchange.revision,
                            chrono::Utc::now(),
                        )
                        .await
                        .map_err(|_| DispatchError::AcceptanceUnknown)?;
                    return Err(DispatchError::AcceptanceUnknown);
                }
            };
            if !matches!(snapshot.status, TaskState::Submitted | TaskState::Working) {
                return Err(DispatchError::NotAccepted {
                    reason: "remote did not accept task".into(),
                });
            }
            self.store
                .acknowledge_outbound(
                    &exchange.exchange_id,
                    &snapshot.id,
                    if snapshot.status == TaskState::Working {
                        "working"
                    } else {
                        "submitted"
                    },
                    chrono::Utc::now(),
                )
                .await
                .map_err(|_| DispatchError::AcceptanceUnknown)?;
            Ok(())
        })
    }

    fn execute<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
        Box::pin(async move {
            let exchange = self
                .store
                .outbound_for_task(&self.peer_id, &request.task.task_id.to_string())
                .await
                .map_err(|error| DispatchError::ExecutionFailed {
                    reason: error.to_string(),
                })?
                .ok_or_else(|| DispatchError::ExecutionFailed {
                    reason: "remote task binding missing".into(),
                })?;
            let remote_id = exchange.external_task_id.clone().ok_or_else(|| {
                DispatchError::ExecutionFailed {
                    reason: "remote task binding missing".into(),
                }
            })?;
            for _ in 0..self.max_polls {
                let snapshot = self.client.get(&remote_id).await.map_err(|error| {
                    DispatchError::ExecutionFailed {
                        reason: error.to_string(),
                    }
                })?;
                match snapshot.status {
                    TaskState::Submitted | TaskState::Working => {
                        tokio::time::sleep(self.poll_interval).await
                    }
                    TaskState::Completed => {
                        self.store
                            .store_artifacts(&exchange.exchange_id, &snapshot.artifacts)
                            .await
                            .map_err(|error| DispatchError::ExecutionFailed {
                                reason: error.to_string(),
                            })?;
                        return Ok(DispatchOutcome::Completed {
                            output: render_artifacts(&snapshot.artifacts),
                        });
                    }
                    TaskState::Failed => {
                        return Ok(DispatchOutcome::Failed {
                            error: "remote_failed".into(),
                        });
                    }
                    TaskState::Canceled => {
                        return Ok(DispatchOutcome::Failed {
                            error: "remote_cancelled".into(),
                        });
                    }
                    TaskState::Rejected => {
                        return Err(DispatchError::NotAccepted {
                            reason: "remote rejected task".into(),
                        });
                    }
                    TaskState::InputRequired | TaskState::AuthRequired => {
                        return Err(DispatchError::ExecutionFailed {
                            reason: "unsupported remote task state".into(),
                        });
                    }
                }
            }
            Err(DispatchError::RecoveryNeeded)
        })
    }
}

pub struct A2aDispatcherRouter {
    peers: std::collections::HashMap<String, Arc<A2aDispatcher>>,
}

impl A2aDispatcherRouter {
    pub fn new(peers: std::collections::HashMap<String, Arc<A2aDispatcher>>) -> Self {
        Self { peers }
    }
    fn peer(&self, request: &DispatchRequest<'_>) -> Result<&Arc<A2aDispatcher>, DispatchError> {
        let Some(EndpointAddress::A2a { peer }) = request.target.address() else {
            return Err(DispatchError::NotAccepted {
                reason: "A2A endpoint address missing".into(),
            });
        };
        self.peers
            .get(peer)
            .ok_or_else(|| DispatchError::NotAccepted {
                reason: "A2A peer unavailable".into(),
            })
    }
}

impl TaskDispatcher for A2aDispatcherRouter {
    fn deliver<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move { self.peer(&request)?.deliver(request).await })
    }
    fn execute<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
        Box::pin(async move { self.peer(&request)?.execute(request).await })
    }
}

fn render_artifacts(artifacts: &[super::wire::Artifact]) -> String {
    artifacts
        .iter()
        .flat_map(|artifact| &artifact.parts)
        .filter_map(|part| match part {
            Part::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
