use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::{mpsc, watch};

use crate::{
    bus::{Cancellation, ConsumerError, EndpointRegistry, EventConsumer},
    matrix::{
        AdminHandler, AdminResult, CommandLedger, DurableMatrixAdmission, EventDedup,
        InboundMatrixEvent, MatrixSender, ReplyContext, RetryAdmission, RouteError,
        resolve_conversation, route_event_durable, send_permission_denied,
        send_task_terminal_reply,
    },
    models::{TaskEvent, TaskId},
    observer::{MonitorSender, ObserverFuture, ObserverMessage, ObserverSendError},
    storage::Repository,
};

use super::ReloadController;

const DEFAULT_REPLY_CAPACITY: usize = 4096;

pub struct ReplyRegistry {
    capacity: usize,
    state: Mutex<(HashMap<TaskId, ReplyContext>, VecDeque<TaskId>)>,
}

impl ReplyRegistry {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            capacity,
            state: Mutex::new((HashMap::new(), VecDeque::new())),
        }
    }

    pub fn insert(&self, task: TaskId, context: ReplyContext) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.0.contains_key(&task) {
            state.1.push_back(task);
        }
        state.0.insert(task, context);
        while state.0.len() > self.capacity {
            if let Some(old) = state.1.pop_front() {
                state.0.remove(&old);
            }
        }
    }

    fn take(&self, task: TaskId) -> Option<ReplyContext> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .0
            .remove(&task)
    }

    fn get(&self, task: TaskId) -> Option<ReplyContext> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .0
            .get(&task)
            .cloned()
    }
}

impl Default for ReplyRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_REPLY_CAPACITY)
    }
}

pub struct TerminalReplyConsumer {
    sender: Arc<dyn MatrixSender>,
    replies: Arc<ReplyRegistry>,
}

impl TerminalReplyConsumer {
    pub fn new(sender: Arc<dyn MatrixSender>, replies: Arc<ReplyRegistry>) -> Self {
        Self { sender, replies }
    }
}

impl EventConsumer for TerminalReplyConsumer {
    fn consume<'a>(
        &'a self,
        event: &'a TaskEvent,
    ) -> crate::bus::BusFuture<'a, Result<(), ConsumerError>> {
        Box::pin(async move {
            let Some(context) = self.replies.get(event.task_id) else {
                return Ok(());
            };
            match send_task_terminal_reply(self.sender.as_ref(), &context, &event.payload).await {
                Ok(true) => {
                    self.replies.take(event.task_id);
                    Ok(())
                }
                Ok(false) => Ok(()),
                Err(_) => {
                    self.replies.take(event.task_id);
                    Err(ConsumerError::Failed {
                        reason: "terminal Matrix reply failed".into(),
                    })
                }
            }
        })
    }
}

pub struct ReloadingMonitorSender {
    sender: Arc<dyn MonitorSender>,
    reload: Arc<ReloadController>,
}

impl ReloadingMonitorSender {
    pub fn new(sender: Arc<dyn MonitorSender>, reload: Arc<ReloadController>) -> Self {
        Self { sender, reload }
    }
}

impl MonitorSender for ReloadingMonitorSender {
    fn send<'a>(&'a self, _room_id: &'a str, message: &'a ObserverMessage) -> ObserverFuture<'a> {
        Box::pin(async move {
            let snapshot = self.reload.snapshot();
            let Some(room) = &snapshot.hot.monitor_room else {
                return Ok(());
            };
            self.sender
                .send(room, message)
                .await
                .map_err(|_| ObserverSendError)
        })
    }
}

pub struct MatrixIngress {
    receiver: mpsc::Receiver<InboundMatrixEvent>,
    repository: Arc<dyn Repository>,
    registry: Arc<EndpointRegistry>,
    admission: Arc<dyn DurableMatrixAdmission>,
    sender: Arc<dyn MatrixSender>,
    reload: Arc<ReloadController>,
    cancellation: Cancellation,
    ledger: Arc<CommandLedger>,
    replies: Arc<ReplyRegistry>,
    dedup: EventDedup,
    retry: Option<Arc<dyn RetryAdmission>>,
    queue: Option<Arc<crate::storage::ReliabilityStore>>,
    reap: Option<Arc<dyn crate::matrix::ReapControl>>,
    #[cfg(test)]
    test_inject: Option<mpsc::Sender<InboundMatrixEvent>>,
}

impl MatrixIngress {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        receiver: mpsc::Receiver<InboundMatrixEvent>,
        repository: Arc<dyn Repository>,
        registry: Arc<EndpointRegistry>,
        admission: Arc<dyn DurableMatrixAdmission>,
        sender: Arc<dyn MatrixSender>,
        reload: Arc<ReloadController>,
        cancellation: Cancellation,
        ledger: Arc<CommandLedger>,
        replies: Arc<ReplyRegistry>,
        dedup_capacity: usize,
    ) -> Self {
        assert!(dedup_capacity > 0);
        Self {
            receiver,
            repository,
            registry,
            admission,
            sender,
            reload,
            cancellation,
            ledger,
            replies,
            dedup: EventDedup::new(dedup_capacity).expect("positive dedup capacity"),
            retry: None,
            queue: None,
            reap: None,
            #[cfg(test)]
            test_inject: None,
        }
    }

    #[cfg(test)]
    pub fn new_with_test_channel(
        receiver: mpsc::Receiver<InboundMatrixEvent>,
        repository: Arc<dyn Repository>,
        registry: Arc<EndpointRegistry>,
        admission: Arc<dyn DurableMatrixAdmission>,
        sender: Arc<dyn MatrixSender>,
        reload: Arc<ReloadController>,
        cancellation: Cancellation,
        ledger: Arc<CommandLedger>,
        replies: Arc<ReplyRegistry>,
        dedup_capacity: usize,
        inject: mpsc::Sender<InboundMatrixEvent>,
    ) -> Self {
        let mut ingress = Self::new(
            receiver,
            repository,
            registry,
            admission,
            sender,
            reload,
            cancellation,
            ledger,
            replies,
            dedup_capacity,
        );
        ingress.test_inject = Some(inject);
        ingress
    }

    pub fn with_queue_control_store(
        mut self,
        queue: Arc<crate::storage::ReliabilityStore>,
    ) -> Self {
        self.queue = Some(queue);
        self
    }

    pub fn with_reap_control(mut self, reap: Arc<dyn crate::matrix::ReapControl>) -> Self {
        self.reap = Some(reap);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_admin_controls(
        self,
        queue: Arc<crate::storage::ReliabilityStore>,
        reap: Arc<dyn crate::matrix::ReapControl>,
    ) -> Self {
        self.with_queue_control_store(queue).with_reap_control(reap)
    }

    pub fn with_retry_admission(mut self, retry: Arc<dyn RetryAdmission>) -> Self {
        self.retry = Some(retry);
        self
    }

    pub fn start(self) -> MatrixIngressHandle {
        #[cfg(test)]
        let test_inject = self.test_inject.clone();
        let (shutdown, receiver) = watch::channel(false);
        let alive = Arc::new(AtomicBool::new(true));
        let task_alive = Arc::clone(&alive);
        let join = tokio::spawn(async move {
            self.run(receiver).await;
            task_alive.store(false, Ordering::Release);
        });
        MatrixIngressHandle {
            shutdown,
            join,
            alive,
            #[cfg(test)]
            test_inject,
        }
    }

    async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        loop {
            let event = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    let _ = changed;
                    return;
                }
                event = self.receiver.recv() => event,
            };
            let Some(event) = event else { return };
            let snapshot = self.reload.snapshot();
            let mut admin = AdminHandler::new(
                Arc::clone(&self.repository),
                self.cancellation.clone(),
                Arc::clone(&self.sender),
                snapshot.hot.admin.clone(),
                Arc::clone(&self.ledger),
            );
            if let Some(retry) = &self.retry {
                admin = admin.with_retry_admission(Arc::clone(retry));
            }
            if let Some(queue) = &self.queue {
                admin = admin.with_queue_control(Arc::clone(queue));
            }
            if let Some(reap) = &self.reap {
                admin = admin.with_reap_control(Arc::clone(reap));
            }
            match admin.handle(&event).await {
                Ok(AdminResult::Replied) => continue,
                Err(_) => continue,
                Ok(AdminResult::Ordinary) => {}
            }
            let Ok(conversation) = resolve_conversation(
                self.repository.as_ref(),
                &event.room_id,
                event.thread_root.as_deref(),
            )
            .await
            else {
                continue;
            };
            let context = ReplyContext {
                room_id: event.room_id.clone(),
                thread_root: event.thread_root.clone(),
                event_id: event.event_id.clone(),
            };
            match route_event_durable(
                &event,
                conversation.id,
                &snapshot.hot.route,
                &snapshot.hot.permissions,
                &self.registry,
                self.admission.as_ref(),
                &mut self.dedup,
                snapshot.hot.monitor_room.clone(),
                snapshot.generation,
            )
            .await
            {
                Ok(task) => self.replies.insert(task.task_id, context),
                Err(RouteError::Forbidden) => {
                    let _ = send_permission_denied(self.sender.as_ref(), &context).await;
                }
                Err(_) => {}
            }
        }
    }
}

pub struct MatrixIngressHandle {
    shutdown: watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
    alive: Arc<AtomicBool>,
    #[cfg(test)]
    test_inject: Option<mpsc::Sender<InboundMatrixEvent>>,
}

impl MatrixIngressHandle {
    pub fn alive(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.alive)
    }
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.join.await;
    }

    #[cfg(test)]
    pub async fn inject(&self, event: InboundMatrixEvent) -> Result<(), InboundMatrixEvent> {
        match &self.test_inject {
            Some(sender) => sender.send(event).await.map_err(|error| error.0),
            None => Err(event),
        }
    }
}
