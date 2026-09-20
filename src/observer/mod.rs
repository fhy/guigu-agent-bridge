//! Read-only task event projection for an operator Matrix room.

use crate::{
    bus::{BusFuture, ConsumerError, EventConsumer},
    models::{AgentTask, EventId, TaskEvent, TaskEventPayload, TaskId, TaskStatus},
    storage::Repository,
};
use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub const MAX_BODY_BYTES: usize = 2048;
pub const DEFAULT_MAX_CHAIN: usize = 16;
pub const DEFAULT_DEDUP_CAPACITY: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageCategory {
    Summary,
    Alert,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
    Error,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverMessage {
    pub category: MessageCategory,
    pub severity: Severity,
    pub body: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObserverSendError;
impl std::fmt::Display for ObserverSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("monitor send failed")
    }
}
impl std::error::Error for ObserverSendError {}
pub type ObserverFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ObserverSendError>> + Send + 'a>>;
pub trait MonitorSender: Send + Sync {
    fn send<'a>(&'a self, room_id: &'a str, message: &'a ObserverMessage) -> ObserverFuture<'a>;
}

#[derive(Debug)]
struct Dedup {
    seen: HashSet<EventId>,
    order: VecDeque<EventId>,
    cap: usize,
}
impl Dedup {
    fn new(cap: usize) -> Self {
        assert!(cap > 0);
        Self {
            seen: HashSet::new(),
            order: VecDeque::new(),
            cap,
        }
    }
    fn contains(&self, id: EventId) -> bool {
        self.seen.contains(&id)
    }
    fn mark(&mut self, id: EventId) {
        if self.seen.insert(id) {
            self.order.push_back(id);
            if self.order.len() > self.cap
                && let Some(old) = self.order.pop_front()
            {
                self.seen.remove(&old);
            }
        }
    }
}

pub struct MatrixObserver {
    repository: Arc<dyn Repository>,
    sender: Arc<dyn MonitorSender>,
    room_id: String,
    max_chain: usize,
    dedup: Mutex<Dedup>,
}
impl MatrixObserver {
    pub fn new(
        repository: Arc<dyn Repository>,
        sender: Arc<dyn MonitorSender>,
        room_id: impl Into<String>,
    ) -> Self {
        Self {
            repository,
            sender,
            room_id: room_id.into(),
            max_chain: DEFAULT_MAX_CHAIN,
            dedup: Mutex::new(Dedup::new(DEFAULT_DEDUP_CAPACITY)),
        }
    }
    pub fn with_limits(mut self, max_chain: usize, dedup_capacity: usize) -> Self {
        assert!(max_chain > 0);
        self.max_chain = max_chain;
        self.dedup = Mutex::new(Dedup::new(dedup_capacity));
        self
    }
    async fn project(&self, event: &TaskEvent) -> Result<(), ConsumerError> {
        {
            let d = self
                .dedup
                .lock()
                .map_err(|_| failed("observer dedup unavailable"))?;
            if d.contains(event.id) {
                return Ok(());
            }
        }
        let task = self
            .repository
            .get_task(event.task_id)
            .await
            .map_err(|_| failed("observer task lookup failed"))?
            .ok_or_else(|| failed("observer task missing"))?;
        if !payload_matches_status(event) {
            return Err(failed("observer event shape invalid"));
        }
        let chain = task_trace(self.repository.as_ref(), &task, self.max_chain).await?;
        let (category, severity) = match event.status {
            TaskStatus::Failed | TaskStatus::TimedOut => (MessageCategory::Alert, Severity::Error),
            TaskStatus::Cancelled => (MessageCategory::Alert, Severity::Warning),
            _ => (MessageCategory::Summary, Severity::Info),
        };
        let body = cap(format!(
            "category={} severity={} status={} task_id={} root_task_id={} parent_task_id={} from_agent={} to_agent={} seq={} timestamp={} chain_state={} chain_boundary={} chain={}",
            category_label(category),
            severity_label(severity),
            status_label(event.status),
            task.task_id,
            task.root_task_id,
            fmt_opt(task.parent_task_id),
            task.from_agent,
            task.to_agent,
            event.seq,
            event.timestamp.to_rfc3339(),
            chain.state,
            fmt_opt(chain.boundary),
            chain
                .ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(">")
        ));
        let message = ObserverMessage {
            category,
            severity,
            body,
        };
        self.sender
            .send(&self.room_id, &message)
            .await
            .map_err(|_| failed("observer send failed"))?;
        self.dedup
            .lock()
            .map_err(|_| failed("observer dedup unavailable"))?
            .mark(event.id);
        Ok(())
    }
}
impl EventConsumer for MatrixObserver {
    fn consume<'a>(&'a self, event: &'a TaskEvent) -> BusFuture<'a, Result<(), ConsumerError>> {
        Box::pin(self.project(event))
    }
}

fn failed(reason: &'static str) -> ConsumerError {
    ConsumerError::Failed {
        reason: reason.to_owned(),
    }
}
fn fmt_opt(id: Option<TaskId>) -> String {
    id.map(|x| x.to_string()).unwrap_or_else(|| "none".into())
}
fn cap(mut s: String) -> String {
    if s.len() > MAX_BODY_BYTES {
        s.truncate(MAX_BODY_BYTES.saturating_sub(3));
        s.push_str("...");
    }
    s
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskTrace {
    pub ids: Vec<TaskId>,
    pub state: &'static str,
    pub boundary: Option<TaskId>,
}

pub async fn task_trace(
    repository: &dyn Repository,
    task: &AgentTask,
    max_chain: usize,
) -> Result<TaskTrace, ConsumerError> {
    assert!(max_chain > 0);
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    let mut cur = task.clone();
    let mut state = "complete";
    let mut boundary = None;
    loop {
        if !seen.insert(cur.task_id) {
            state = "cycle";
            boundary = Some(cur.task_id);
            break;
        }
        ids.push(cur.task_id);
        if ids.len() >= max_chain {
            if cur.parent_task_id.is_some() {
                state = "truncated";
                boundary = cur.parent_task_id;
            }
            break;
        }
        let Some(parent) = cur.parent_task_id else {
            break;
        };
        match repository.get_task(parent).await {
            Ok(Some(next)) => cur = next,
            Ok(None) => {
                state = "missing_parent";
                boundary = Some(parent);
                break;
            }
            Err(_) => return Err(failed("observer parent lookup failed")),
        }
    }
    ids.reverse();
    if state == "complete" && ids.first().copied() != Some(task.root_task_id) {
        state = "root_mismatch";
        boundary = Some(task.root_task_id);
    }
    Ok(TaskTrace {
        ids,
        state,
        boundary,
    })
}

fn payload_matches_status(event: &TaskEvent) -> bool {
    matches!(
        (&event.status, &event.payload),
        (TaskStatus::Queued, TaskEventPayload::Queued)
            | (TaskStatus::Dispatched, TaskEventPayload::Dispatched { .. })
            | (TaskStatus::Running, TaskEventPayload::Running { .. })
            | (TaskStatus::Completed, TaskEventPayload::Completed { .. })
            | (TaskStatus::Failed, TaskEventPayload::Failed { .. })
            | (TaskStatus::TimedOut, TaskEventPayload::TimedOut { .. })
            | (TaskStatus::Cancelled, TaskEventPayload::Cancelled { .. })
    )
}

fn category_label(value: MessageCategory) -> &'static str {
    match value {
        MessageCategory::Summary => "summary",
        MessageCategory::Alert => "alert",
    }
}

fn severity_label(value: Severity) -> &'static str {
    match value {
        Severity::Info => "info",
        Severity::Warning => "warning",
        Severity::Error => "error",
    }
}

fn status_label(value: TaskStatus) -> &'static str {
    match value {
        TaskStatus::Queued => "queued",
        TaskStatus::Dispatched => "dispatched",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::TimedOut => "timed_out",
        TaskStatus::Cancelled => "cancelled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dedup_evicts_oldest() {
        let a = EventId::generate();
        let b = EventId::generate();
        let mut d = Dedup::new(1);
        d.mark(a);
        d.mark(b);
        assert!(!d.contains(a));
        assert!(d.contains(b));
    }
}
