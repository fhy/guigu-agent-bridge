use std::{collections::BTreeMap, sync::Arc};

use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    a2a::{
        server::{A2aHandler, HandlerFuture},
        wire::{
            JsonRpcRequest, NormalizedSend, TaskSnapshot, TaskState, normalize_send,
            normalize_task_query,
        },
    },
    bus::{AdmissionContext, Bus, Cancellation},
    models::{
        AgentEndpoint, AgentTask, Conversation, ConversationId, EndpointAddress, ExternalRef,
        Priority, TaskId, TransportType,
    },
    storage::{Repository, SqliteRepository},
};

use super::{
    A2aError, A2aStore, ExchangeRecord, FaultPoint, InboundReservation, IngressFaultInjector,
    IngressStage, NoFault, derive_peer_endpoint_id, ingress_support::*,
};

pub struct A2aIngress {
    store: A2aStore,
    repository: Arc<dyn Repository>,
    concrete: SqliteRepository,
    bus: Arc<dyn Bus>,
    cancellation: Cancellation,
    endpoints: BTreeMap<String, crate::models::EndpointId>,
    faults: Arc<dyn IngressFaultInjector>,
}

struct PreparedSend {
    params: NormalizedSend,
    row: ExchangeRecord,
    to_agent: crate::models::EndpointId,
    task_text: String,
    content_bytes: i64,
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
            faults: Arc::new(NoFault),
        }
    }

    pub(crate) fn with_faults(mut self, faults: Arc<dyn IngressFaultInjector>) -> Self {
        self.faults = faults;
        self
    }

    async fn dispatch(
        &self,
        peer: &str,
        endpoint: &str,
        request: JsonRpcRequest,
    ) -> Result<Value, A2aError> {
        match request.method.as_str() {
            "message/send" => self.send(peer, endpoint, request.id, request.params).await,
            "tasks/get" => self.query(peer, request.params, false).await,
            "tasks/cancel" => self.query(peer, request.params, true).await,
            _ => Err(A2aError::Unsupported),
        }
    }

    async fn send(
        &self,
        peer: &str,
        endpoint: &str,
        rpc_id: Value,
        value: Value,
    ) -> Result<Value, A2aError> {
        let prepared = self.prepare_send(peer, endpoint, rpc_id, value).await?;
        let (from_agent, conversation) = self.prepare_identity(peer, &prepared).await?;
        let winner = self
            .admit(peer, &prepared, from_agent, &conversation)
            .await?;
        self.bind_and_snapshot(&prepared, &winner).await
    }

    async fn prepare_send(
        &self,
        peer: &str,
        endpoint: &str,
        rpc_id: Value,
        value: Value,
    ) -> Result<PreparedSend, A2aError> {
        reject_bad_version(&value)?;
        let params =
            normalize_send(value, &rpc_id).map_err(|_| invalid(IngressStage::ParamsDecode))?;
        let task_text = text_parts(&params.message.parts)?;
        let to_agent = *self
            .endpoints
            .get(endpoint)
            .ok_or_else(|| target_unavailable(IngressStage::EndpointResolve))?;
        let hash = Uuid::new_v5(&Uuid::NAMESPACE_OID, &params.canonical).to_string();
        self.faults.inject(FaultPoint::BeforeReservation)?;
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
        self.faults.inject(FaultPoint::AfterReservation)?;
        let row = match reservation {
            InboundReservation::New(row) | InboundReservation::Replay(row) => row,
        };
        let content_bytes = if self
            .store
            .message_matches(&row.exchange_id, &params.message)
            .await?
        {
            row.content_bytes
        } else if row.content_bytes == 0 {
            self.faults.inject(FaultPoint::BeforeContent)?;
            let bytes = self
                .store
                .store_message(&row.exchange_id, &params.message, chrono::Utc::now())
                .await?;
            self.faults.inject(FaultPoint::AfterContent)?;
            bytes
        } else {
            return Err(recovery(IngressStage::ContentPersist));
        };
        Ok(PreparedSend {
            params,
            row,
            to_agent,
            task_text,
            content_bytes,
        })
    }

    async fn prepare_identity(
        &self,
        peer: &str,
        prepared: &PreparedSend,
    ) -> Result<(crate::models::EndpointId, Conversation), A2aError> {
        self.faults.inject(FaultPoint::BeforePrerequisites)?;
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
            .map_err(|_| storage(IngressStage::PeerPersist))?;
        let external = ExternalRef {
            transport: TransportType::A2a,
            external_id: format!("{peer}/{}", prepared.params.context_id),
            thread_ref: None,
        };
        let conversation = self
            .conversation(external, from_agent, prepared.to_agent)
            .await?;
        self.faults.inject(FaultPoint::AfterPrerequisites)?;
        Ok((from_agent, conversation))
    }

    async fn admit(
        &self,
        peer: &str,
        prepared: &PreparedSend,
        from_agent: crate::models::EndpointId,
        conversation: &Conversation,
    ) -> Result<String, A2aError> {
        let key = admission_key(peer, &prepared.params.request_id);
        let existing = self.store.admitted_task(&key).await?;
        let proposed_id = TaskId::generate();
        let task = AgentTask {
            task_id: proposed_id,
            root_task_id: proposed_id,
            parent_task_id: None,
            from_agent,
            to_agent: prepared.to_agent,
            conversation_id: conversation.id,
            reply_to: None,
            text: prepared.task_text.clone(),
            priority: Priority::DEFAULT,
            depth: 0,
            hops: 0,
            deadline: None,
            version: 0,
        };
        if existing.is_none() {
            self.faults.inject(FaultPoint::AdmissionBusy)?;
            self.faults.inject(FaultPoint::BeforeAdmission)?;
            self.bus
                .submit_with_context(
                    task.clone(),
                    AdmissionContext {
                        transport: "a2a".into(),
                        external_event_id: key.clone(),
                        room_id: prepared.params.context_id.clone(),
                        thread_root: None,
                        reply_event_id: prepared.params.message.message_id.clone(),
                        monitor_room: None,
                        monitor_generation: 0,
                    },
                )
                .await
                .map_err(admission_error)?;
            self.faults.inject(FaultPoint::AfterAdmission)?;
        }
        let winner = self
            .store
            .admitted_task(&key)
            .await?
            .ok_or_else(|| recovery(IngressStage::Admission))?;
        let winner_id: TaskId = winner
            .parse()
            .map_err(|_| recovery(IngressStage::Admission))?;
        let winner_task = self
            .repository
            .get_task(winner_id)
            .await
            .map_err(|_| storage(IngressStage::Admission))?
            .ok_or_else(|| recovery(IngressStage::Admission))?;
        if winner_task.from_agent != from_agent
            || winner_task.to_agent != prepared.to_agent
            || winner_task.conversation_id != conversation.id
        {
            return Err(recovery(IngressStage::Admission));
        }
        Ok(winner)
    }

    async fn bind_and_snapshot(
        &self,
        prepared: &PreparedSend,
        winner: &str,
    ) -> Result<Value, A2aError> {
        let external_task = external_task_id(&prepared.row.exchange_id);
        if prepared.row.state == "submitted" {
            if prepared.row.internal_task_id.as_deref() != Some(winner)
                || prepared.row.external_task_id.as_deref() != Some(&external_task)
            {
                return Err(recovery(IngressStage::Binding));
            }
        } else {
            self.faults.inject(FaultPoint::BeforeBinding)?;
            if !self
                .store
                .bind_task(
                    &prepared.row.exchange_id,
                    winner,
                    &external_task,
                    prepared.content_bytes,
                    chrono::Utc::now(),
                )
                .await?
            {
                let rebound = self
                    .store
                    .exchange(&prepared.row.exchange_id)
                    .await?
                    .ok_or_else(|| recovery(IngressStage::Binding))?;
                if rebound.internal_task_id.as_deref() != Some(winner)
                    || rebound.external_task_id.as_deref() != Some(&external_task)
                {
                    return Err(recovery(IngressStage::Binding));
                }
            }
            self.faults.inject(FaultPoint::AfterBinding)?;
        }
        self.faults.inject(FaultPoint::BeforeSnapshot)?;
        let bound = self
            .store
            .exchange(&prepared.row.exchange_id)
            .await?
            .ok_or_else(|| recovery(IngressStage::Snapshot))?;
        to_value(self.snapshot(&bound).await?)
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
            .map_err(|_| storage(IngressStage::Conversation))?
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
            .map_err(|_| storage(IngressStage::Conversation))?
            .ok_or_else(|| recovery(IngressStage::Conversation))
    }

    async fn query(&self, peer: &str, value: Value, cancel: bool) -> Result<Value, A2aError> {
        reject_bad_version(&value)?;
        let query = normalize_task_query(value).map_err(|_| invalid(IngressStage::ParamsDecode))?;
        let row = self
            .store
            .by_external_task(peer, "inbound", &query.task_id)
            .await?
            .ok_or_else(|| target_unavailable(IngressStage::Snapshot))?;
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
            .map_err(|_| storage(IngressStage::Snapshot))?;
        Ok(TaskSnapshot {
            id: row
                .external_task_id
                .clone()
                .ok_or(A2aError::RecoveryNeeded)?,
            context_id: row.external_context_id.clone().unwrap_or_default(),
            status: event
                .as_ref()
                .map(|event| project(event.status))
                .unwrap_or(TaskState::Submitted),
            artifacts: output_artifacts(row, event.as_ref()),
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
