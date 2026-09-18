use std::{collections::BTreeMap, sync::Arc};

use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    a2a::{
        server::{A2aHandler, HandlerFuture},
        wire::{
            JsonRpcRequest, PROTOCOL_VERSION, Part, SendParams, TaskQuery, TaskSnapshot, TaskState,
        },
    },
    bus::{AdmissionContext, Bus, Cancellation},
    models::{
        AgentEndpoint, AgentTask, Conversation, ConversationId, EndpointAddress, ExternalRef,
        Priority, TaskId, TaskStatus, TransportType,
    },
    storage::{Repository, SqliteRepository},
};

use super::{A2aError, A2aStore, ExchangeRecord, InboundReservation, derive_peer_endpoint_id};

pub struct A2aIngress {
    store: A2aStore,
    repository: Arc<dyn Repository>,
    concrete: SqliteRepository,
    bus: Arc<dyn Bus>,
    cancellation: Cancellation,
    endpoints: BTreeMap<String, crate::models::EndpointId>,
}

impl A2aIngress {
    pub fn new(
        store: A2aStore,
        repository: Arc<dyn Repository>,
        concrete: SqliteRepository,
        bus: Arc<dyn Bus>,
        cancellation: Cancellation,
        endpoints: BTreeMap<String, crate::models::EndpointId>,
    ) -> Self {
        Self {
            store,
            repository,
            concrete,
            bus,
            cancellation,
            endpoints,
        }
    }

    async fn dispatch(
        &self,
        peer: &str,
        endpoint: &str,
        request: JsonRpcRequest,
    ) -> Result<Value, A2aError> {
        match request.method.as_str() {
            "message/send" => self.send(peer, endpoint, request.params).await,
            "tasks/get" => self.query(peer, request.params, false).await,
            "tasks/cancel" => self.query(peer, request.params, true).await,
            _ => Err(A2aError::Unsupported),
        }
    }

    async fn send(&self, peer: &str, endpoint: &str, value: Value) -> Result<Value, A2aError> {
        let params: SendParams = serde_json::from_value(value)
            .map_err(|_| A2aError::Protocol("invalid message/send params"))?;
        require_version(&params.protocol_version)?;
        let canonical =
            serde_json::to_vec(&params).map_err(|_| A2aError::Protocol("invalid request"))?;
        if canonical.len() > 1_048_576 || params.message.parts.len() > 64 {
            return Err(A2aError::TooLarge);
        }
        let hash = Uuid::new_v5(&Uuid::NAMESPACE_OID, &canonical).to_string();
        let reservation = self
            .store
            .reserve_inbound(
                peer,
                &params.request_id,
                &hash,
                &params.context_id,
                chrono::Utc::now(),
            )
            .await?;
        if let InboundReservation::Replay(row) = &reservation {
            return to_value(self.snapshot(row).await?);
        }
        let new_row = match &reservation {
            InboundReservation::New(row) => row,
            InboundReservation::Replay(_) => unreachable!(),
        };
        let content_bytes = self
            .store
            .store_message(&new_row.exchange_id, &params.message, chrono::Utc::now())
            .await?;
        let to_agent = *self
            .endpoints
            .get(endpoint)
            .ok_or(A2aError::Protocol("unknown endpoint"))?;
        let from_agent = derive_peer_endpoint_id(peer);
        self.concrete
            .upsert_agent(
                &format!("a2a-peer-{peer}"),
                &AgentEndpoint {
                    id: from_agent,
                    transport: TransportType::A2a,
                    address: EndpointAddress::A2a { peer: peer.into() },
                    enabled: true,
                    capabilities: vec![],
                },
            )
            .await
            .map_err(|_| A2aError::Protocol("peer persistence failed"))?;
        let external = ExternalRef {
            transport: TransportType::A2a,
            external_id: format!("{peer}/{}", params.context_id),
            thread_ref: None,
        };
        let conversation = self.conversation(external, from_agent, to_agent).await?;
        let task_id = TaskId::generate();
        let task = AgentTask {
            task_id,
            root_task_id: task_id,
            parent_task_id: None,
            from_agent,
            to_agent,
            conversation_id: conversation.id,
            reply_to: None,
            text: text_parts(&params.message.parts)?,
            priority: Priority::DEFAULT,
            depth: 0,
            hops: 0,
            deadline: None,
            version: 0,
        };
        self.bus
            .submit_with_context(
                task,
                AdmissionContext {
                    transport: "a2a".into(),
                    external_event_id: params.request_id.clone(),
                    room_id: params.context_id.clone(),
                    thread_root: None,
                    reply_event_id: params.message.message_id,
                    monitor_room: None,
                    monitor_generation: 0,
                },
            )
            .await
            .map_err(|_| A2aError::Protocol("task admission failed"))?;
        let row = match reservation {
            InboundReservation::New(row) => row,
            _ => unreachable!(),
        };
        let external_task = Uuid::now_v7().to_string();
        if !self
            .store
            .bind_task(
                &row.exchange_id,
                &task_id.to_string(),
                &external_task,
                content_bytes,
                chrono::Utc::now(),
            )
            .await?
        {
            return Err(A2aError::RecoveryNeeded);
        }
        to_value(TaskSnapshot {
            id: external_task,
            context_id: params.context_id,
            status: TaskState::Submitted,
            artifacts: vec![],
            metadata: None,
        })
    }

    async fn conversation(
        &self,
        external: ExternalRef,
        from: crate::models::EndpointId,
        to: crate::models::EndpointId,
    ) -> Result<Conversation, A2aError> {
        if let Some(value) = self
            .repository
            .conversation_by_external_ref(&external)
            .await
            .map_err(|_| A2aError::Protocol("conversation lookup failed"))?
        {
            return Ok(value);
        }
        let value = Conversation {
            id: ConversationId::generate(),
            participants: vec![from, to],
            external_ref: Some(external.clone()),
        };
        if self.repository.insert_conversation(&value).await.is_ok() {
            return Ok(value);
        }
        self.repository
            .conversation_by_external_ref(&external)
            .await
            .map_err(|_| A2aError::Protocol("conversation race failed"))?
            .ok_or(A2aError::Protocol("conversation race failed"))
    }

    async fn query(&self, peer: &str, value: Value, cancel: bool) -> Result<Value, A2aError> {
        let query: TaskQuery =
            serde_json::from_value(value).map_err(|_| A2aError::Protocol("invalid task query"))?;
        require_version(&query.protocol_version)?;
        let row = self
            .store
            .by_external_task(peer, "inbound", &query.task_id)
            .await?
            .ok_or(A2aError::Protocol("unknown task"))?;
        if cancel {
            let id = parse_task(&row)?;
            self.cancellation.cancel(id, "remote_a2a_cancel");
        }
        to_value(self.snapshot(&row).await?)
    }

    async fn snapshot(&self, row: &ExchangeRecord) -> Result<TaskSnapshot, A2aError> {
        let event = self
            .repository
            .latest_event(parse_task(row)?)
            .await
            .map_err(|_| A2aError::Protocol("task lookup failed"))?;
        Ok(TaskSnapshot {
            id: row
                .external_task_id
                .clone()
                .ok_or(A2aError::RecoveryNeeded)?,
            context_id: String::new(),
            status: event
                .map(|event| project(event.status))
                .unwrap_or(TaskState::Submitted),
            artifacts: vec![],
            metadata: row.content_cleaned.then(|| json!({"contentExpired": true})),
        })
    }
}

impl A2aHandler for A2aIngress {
    fn handle<'a>(
        &'a self,
        peer: &'a str,
        endpoint: &'a str,
        request: JsonRpcRequest,
    ) -> HandlerFuture<'a> {
        Box::pin(async move { self.dispatch(peer, endpoint, request).await })
    }
}

fn require_version(version: &str) -> Result<(), A2aError> {
    (version == PROTOCOL_VERSION)
        .then_some(())
        .ok_or(A2aError::Unsupported)
}
fn parse_task(row: &ExchangeRecord) -> Result<TaskId, A2aError> {
    row.internal_task_id
        .as_deref()
        .ok_or(A2aError::RecoveryNeeded)?
        .parse()
        .map_err(|_| A2aError::RecoveryNeeded)
}
fn text_parts(parts: &[Part]) -> Result<String, A2aError> {
    let mut values = Vec::new();
    for part in parts {
        match part {
            Part::Text { text } => values.push(text.as_str()),
            Part::Data { .. } | Part::FileInline { .. } => {}
            Part::FileUri { .. } => return Err(A2aError::Unsupported),
        }
    }
    Ok(values.join("\n"))
}
fn project(status: TaskStatus) -> TaskState {
    match status {
        TaskStatus::Queued => TaskState::Submitted,
        TaskStatus::Dispatched | TaskStatus::Running => TaskState::Working,
        TaskStatus::Completed => TaskState::Completed,
        TaskStatus::Failed | TaskStatus::TimedOut => TaskState::Failed,
        TaskStatus::Cancelled => TaskState::Canceled,
    }
}
fn to_value<T: serde::Serialize>(value: T) -> Result<Value, A2aError> {
    serde_json::to_value(value).map_err(|_| A2aError::Protocol("response encoding failed"))
}
