use std::sync::Arc;

use tokio::sync::mpsc;

use crate::{
    bus::{AdmissionContext, Bus, BusError, BusFuture, Clock, EndpointRegistry, EventSink},
    matrix::{DurableMatrixAdmission, MatrixAdmissionFuture},
    models::{AgentTask, EventId, TaskEvent, TaskEventPayload, TaskStatus},
    storage::{ReliabilityStore, Repository, SqliteRepository},
};

/// Repository-first bounded task admission used by the production Matrix path.
#[derive(Clone)]
pub struct PersistingBus {
    registry: Arc<EndpointRegistry>,
    tasks: mpsc::Sender<AgentTask>,
    events: Arc<dyn EventSink>,
    repository: SqliteRepository,
    clock: Clock,
    default_timeout: Option<chrono::Duration>,
    reliability: Option<(ReliabilityStore, String)>,
}

impl PersistingBus {
    pub fn new(
        registry: Arc<EndpointRegistry>,
        capacity: usize,
        events: Arc<dyn EventSink>,
        repository: SqliteRepository,
        clock: Clock,
    ) -> (Self, mpsc::Receiver<AgentTask>) {
        assert!(capacity > 0);
        let (tasks, receiver) = mpsc::channel(capacity);
        (
            Self {
                registry,
                tasks,
                events,
                repository,
                clock,
                default_timeout: None,
                reliability: None,
            },
            receiver,
        )
    }

    pub fn with_default_timeout(mut self, timeout: chrono::Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    pub fn with_reliability(mut self, store: ReliabilityStore, runtime_instance: String) -> Self {
        self.reliability = Some((store, runtime_instance));
        self
    }

    pub async fn enqueue_persisted(&self, task: AgentTask) -> Result<(), BusError> {
        self.registry.validate_target(task.to_agent)?;
        self.tasks.try_send(task).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => BusError::QueueFull,
            mpsc::error::TrySendError::Closed(_) => BusError::TaskChannelClosed,
        })
    }

    fn submit_inner<'a>(
        &'a self,
        mut task: AgentTask,
        context: Option<AdmissionContext>,
    ) -> BusFuture<'a, Result<AgentTask, BusError>> {
        Box::pin(async move {
            self.registry.validate_target(task.to_agent)?;
            if task.deadline.is_none() {
                task.deadline = self
                    .default_timeout
                    .and_then(|timeout| self.clock.now().checked_add_signed(timeout));
            }
            if let (Some((store, _)), Some(context)) = (&self.reliability, context.as_ref())
                && let Some(task_id) = store
                    .ordinary_receipt_task(&context.transport, &context.external_event_id)
                    .await
                    .map_err(|_| BusError::TaskChannelClosed)?
            {
                let task_id = task_id.parse().map_err(|_| BusError::TaskChannelClosed)?;
                return self
                    .repository
                    .get_task(task_id)
                    .await
                    .map_err(|_| BusError::TaskChannelClosed)?
                    .ok_or(BusError::TaskChannelClosed);
            }
            let permit = self.tasks.try_reserve().map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => BusError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => BusError::TaskChannelClosed,
            })?;
            let event = TaskEvent {
                id: EventId::generate(),
                task_id: task.task_id,
                seq: 1,
                status: TaskStatus::Queued,
                timestamp: self.clock.now(),
                payload: TaskEventPayload::Queued,
            };
            if let Some((store, runtime_instance)) = &self.reliability {
                let outcome = store
                    .admit_task(&task, &event, runtime_instance, context.as_ref())
                    .await
                    .map_err(|_| BusError::TaskChannelClosed)?;
                if let crate::storage::ReceiptOutcome::Replay { task_id, .. } = outcome {
                    let task_id = task_id
                        .ok_or(BusError::TaskChannelClosed)?
                        .parse()
                        .map_err(|_| BusError::TaskChannelClosed)?;
                    return self
                        .repository
                        .get_task(task_id)
                        .await
                        .map_err(|_| BusError::TaskChannelClosed)?
                        .ok_or(BusError::TaskChannelClosed);
                }
            } else {
                self.repository
                    .insert_task_and_event(&task, &event)
                    .await
                    .map_err(|_| BusError::TaskChannelClosed)?;
            }
            permit.send(task.clone());
            self.events.emit(event).await?;
            Ok(task)
        })
    }
}

impl Bus for PersistingBus {
    fn submit<'a>(&'a self, task: AgentTask) -> BusFuture<'a, Result<(), BusError>> {
        Box::pin(async move { self.submit_inner(task, None).await.map(|_| ()) })
    }

    fn submit_with_context<'a>(
        &'a self,
        task: AgentTask,
        context: AdmissionContext,
    ) -> BusFuture<'a, Result<(), BusError>> {
        Box::pin(async move { self.submit_inner(task, Some(context)).await.map(|_| ()) })
    }
}

impl DurableMatrixAdmission for PersistingBus {
    fn winner<'a>(
        &'a self,
        external_event_id: &'a str,
    ) -> MatrixAdmissionFuture<'a, Result<Option<AgentTask>, BusError>> {
        Box::pin(async move {
            let Some((store, _)) = &self.reliability else {
                return Err(BusError::TaskChannelClosed);
            };
            let Some(task_id) = store
                .ordinary_receipt_task("matrix", external_event_id)
                .await
                .map_err(|_| BusError::TaskChannelClosed)?
            else {
                return Ok(None);
            };
            let task_id = task_id.parse().map_err(|_| BusError::TaskChannelClosed)?;
            self.repository
                .get_task(task_id)
                .await
                .map_err(|_| BusError::TaskChannelClosed)
        })
    }

    fn admit<'a>(
        &'a self,
        task: AgentTask,
        context: AdmissionContext,
    ) -> MatrixAdmissionFuture<'a, Result<AgentTask, BusError>> {
        self.submit_inner(task, Some(context))
    }
}
