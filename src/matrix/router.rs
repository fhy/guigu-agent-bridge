use std::collections::BTreeMap;

use super::{EventDedup, InboundMatrixEvent, PermissionPolicy, derive_matrix_user_id};
use crate::{
    bus::{AdmissionContext, Bus, BusError, EndpointRegistry},
    models::{AgentTask, ConversationId, Priority, TaskId},
};

pub type MatrixAdmissionFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Production Matrix admission boundary. Unlike [`Bus`], this contract can
/// return an immutable durable winner before mutable routing is evaluated.
pub trait DurableMatrixAdmission: Send + Sync {
    fn winner<'a>(
        &'a self,
        external_event_id: &'a str,
    ) -> MatrixAdmissionFuture<'a, Result<Option<AgentTask>, BusError>>;

    fn admit<'a>(
        &'a self,
        task: AgentTask,
        context: AdmissionContext,
    ) -> MatrixAdmissionFuture<'a, Result<AgentTask, BusError>>;
}

#[derive(Debug, Clone, Default)]
pub struct RoutePolicy {
    aliases: BTreeMap<String, String>,
    room_targets: BTreeMap<String, String>,
    direct_targets: BTreeMap<String, Vec<String>>,
}

impl RoutePolicy {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn alias(mut self, alias: impl Into<String>, agent_id: impl Into<String>) -> Self {
        self.aliases.insert(alias.into(), agent_id.into());
        self
    }
    pub fn bind_room(mut self, room_id: impl Into<String>, agent_id: impl Into<String>) -> Self {
        self.room_targets.insert(room_id.into(), agent_id.into());
        self
    }
    pub fn direct_room(
        mut self,
        room_id: impl Into<String>,
        candidates: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.direct_targets.insert(
            room_id.into(),
            candidates.into_iter().map(Into::into).collect(),
        );
        self
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RouteError {
    #[error("no route")]
    NoRoute,
    #[error("ambiguous route")]
    Ambiguous,
    #[error("permission denied")]
    Forbidden,
    #[error("duplicate event")]
    Duplicate,
    #[error("target rejected")]
    Target,
    #[error("bus submission failed")]
    Bus,
    #[error("invalid sender identity")]
    Identity,
}

pub async fn route_event(
    event: &InboundMatrixEvent,
    conversation_id: ConversationId,
    policy: &RoutePolicy,
    permissions: &PermissionPolicy,
    registry: &EndpointRegistry,
    bus: &dyn Bus,
    dedup: &mut EventDedup,
) -> Result<AgentTask, RouteError> {
    route_event_with_monitor(
        event,
        conversation_id,
        policy,
        permissions,
        registry,
        bus,
        dedup,
        None,
        0,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn route_event_with_monitor(
    event: &InboundMatrixEvent,
    conversation_id: ConversationId,
    policy: &RoutePolicy,
    permissions: &PermissionPolicy,
    registry: &EndpointRegistry,
    bus: &dyn Bus,
    dedup: &mut EventDedup,
    monitor_room: Option<String>,
    monitor_generation: u64,
) -> Result<AgentTask, RouteError> {
    if dedup.contains(&event.room_id, &event.event_id) {
        return Err(RouteError::Duplicate);
    }
    let (agent_name, text) = select_target(event, policy)?;
    let target = registry
        .resolve_agent_id(agent_name)
        .ok_or(RouteError::Target)?;
    registry
        .validate_target(target)
        .map_err(|_| RouteError::Target)?;
    let sender = derive_matrix_user_id(&event.sender).map_err(|_| RouteError::Identity)?;
    if !permissions.permits(&event.sender, target) {
        return Err(RouteError::Forbidden);
    }
    let task_id = TaskId::generate();
    let task = AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: None,
        from_agent: sender,
        to_agent: target,
        conversation_id,
        reply_to: None,
        text,
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    };
    let context = AdmissionContext {
        transport: "matrix".into(),
        external_event_id: event.event_id.clone(),
        room_id: event.room_id.clone(),
        thread_root: event.thread_root.clone(),
        reply_event_id: event.event_id.clone(),
        monitor_room,
        monitor_generation,
    };
    match bus.submit_with_context(task.clone(), context).await {
        Ok(()) => {}
        Err(error) => {
            if matches!(error, BusError::EventBufferFull | BusError::EventSinkClosed) {
                dedup.mark(&event.room_id, &event.event_id);
            }
            return Err(RouteError::Bus);
        }
    }
    dedup.mark(&event.room_id, &event.event_id);
    Ok(task)
}

#[allow(clippy::too_many_arguments)]
pub async fn route_event_durable(
    event: &InboundMatrixEvent,
    conversation_id: ConversationId,
    policy: &RoutePolicy,
    permissions: &PermissionPolicy,
    registry: &EndpointRegistry,
    admission: &dyn DurableMatrixAdmission,
    dedup: &mut EventDedup,
    monitor_room: Option<String>,
    monitor_generation: u64,
) -> Result<AgentTask, RouteError> {
    if let Some(task) = admission
        .winner(&event.event_id)
        .await
        .map_err(|_| RouteError::Bus)?
    {
        dedup.mark(&event.room_id, &event.event_id);
        return Ok(task);
    }
    if dedup.contains(&event.room_id, &event.event_id) {
        return Err(RouteError::Duplicate);
    }
    let (agent_name, text) = select_target(event, policy)?;
    let target = registry
        .resolve_agent_id(agent_name)
        .ok_or(RouteError::Target)?;
    registry
        .validate_target(target)
        .map_err(|_| RouteError::Target)?;
    let sender = derive_matrix_user_id(&event.sender).map_err(|_| RouteError::Identity)?;
    if !permissions.permits(&event.sender, target) {
        return Err(RouteError::Forbidden);
    }
    let task_id = TaskId::generate();
    let task = AgentTask {
        task_id,
        root_task_id: task_id,
        parent_task_id: None,
        from_agent: sender,
        to_agent: target,
        conversation_id,
        reply_to: None,
        text,
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    };
    let context = AdmissionContext {
        transport: "matrix".into(),
        external_event_id: event.event_id.clone(),
        room_id: event.room_id.clone(),
        thread_root: event.thread_root.clone(),
        reply_event_id: event.event_id.clone(),
        monitor_room,
        monitor_generation,
    };
    let task = admission
        .admit(task, context)
        .await
        .map_err(|_| RouteError::Bus)?;
    dedup.mark(&event.room_id, &event.event_id);
    Ok(task)
}

fn select_target<'a>(
    event: &InboundMatrixEvent,
    policy: &'a RoutePolicy,
) -> Result<(&'a str, String), RouteError> {
    if let Some((prefix, text)) = event.body.split_once(':') {
        let prefix = prefix.trim();
        if let Some(alias) = prefix.strip_prefix('@') {
            return policy
                .aliases
                .get(alias)
                .map(|agent_id| (agent_id.as_str(), text.trim_start().to_owned()))
                .ok_or(RouteError::NoRoute);
        }
    }
    if let Some(agent_id) = policy.room_targets.get(&event.room_id) {
        return Ok((agent_id, event.body.clone()));
    }
    match policy.direct_targets.get(&event.room_id).map(Vec::as_slice) {
        Some([agent_id]) => Ok((agent_id, event.body.clone())),
        Some([]) | None => Err(RouteError::NoRoute),
        Some(_) => Err(RouteError::Ambiguous),
    }
}
