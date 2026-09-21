use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    str::FromStr,
    sync::{Arc, Mutex},
};

use crate::{
    bus::{BusFuture, Cancellation},
    models::{AgentTask, TaskId, TaskStatus},
    observer::{DEFAULT_MAX_CHAIN, MAX_BODY_BYTES, task_trace},
    storage::{ReliabilityStore, Repository},
};

use super::{InboundMatrixEvent, MatrixSender, ReplyContext, ReplyError, derive_matrix_user_id};

#[derive(Debug, Clone, Default)]
pub struct AdminPermissionPolicy {
    users: BTreeSet<String>,
    rooms: Option<BTreeSet<String>>,
}

impl AdminPermissionPolicy {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn allow_user(mut self, user: impl Into<String>) -> Self {
        self.users.insert(user.into());
        self
    }
    pub fn restrict_rooms(mut self, rooms: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.rooms = Some(rooms.into_iter().map(Into::into).collect());
        self
    }
    fn permits(&self, user: &str, room: &str) -> bool {
        self.users.contains(user) && self.rooms.as_ref().is_none_or(|rooms| rooms.contains(room))
    }
}

#[derive(Debug)]
struct LedgerState {
    entries: HashMap<(String, String), Option<CachedReply>>,
    completed: VecDeque<(String, String)>,
}

#[derive(Debug, Clone)]
enum CachedReply {
    Body(String),
    OutboxOwned,
}

#[derive(Debug)]
pub struct CommandLedger {
    capacity: usize,
    state: Mutex<LedgerState>,
}

impl CommandLedger {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            capacity,
            state: Mutex::new(LedgerState {
                entries: HashMap::new(),
                completed: VecDeque::new(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminResult {
    Ordinary,
    Replied,
}

pub enum RetryReply {
    Immediate(String),
    OutboxOwned,
}

pub trait RetryAdmission: Send + Sync {
    fn admit<'a>(
        &'a self,
        event: &'a InboundMatrixEvent,
        source: &'a AgentTask,
    ) -> BusFuture<'a, Result<RetryReply, ()>>;
}

pub(crate) mod sealed {
    pub trait ReapIssuer {}
}

pub trait ReapControl: sealed::ReapIssuer + Send + Sync {
    fn reap<'a>(
        &'a self,
        target: crate::models::EndpointId,
        task: TaskId,
    ) -> BusFuture<'a, Result<Option<ReapAck>, ()>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReapAck {
    pub(crate) task_id: TaskId,
    pub(crate) resource_key: String,
    pub(crate) owner: String,
    pub(crate) fence: i64,
}

pub struct AdminHandler {
    repository: Arc<dyn Repository>,
    cancellation: Cancellation,
    sender: Arc<dyn MatrixSender>,
    permissions: AdminPermissionPolicy,
    ledger: Arc<CommandLedger>,
    max_chain: usize,
    retry: Option<Arc<dyn RetryAdmission>>,
    queue: Option<Arc<ReliabilityStore>>,
    reap: Option<Arc<dyn ReapControl>>,
}

impl AdminHandler {
    pub fn new(
        repository: Arc<dyn Repository>,
        cancellation: Cancellation,
        sender: Arc<dyn MatrixSender>,
        permissions: AdminPermissionPolicy,
        ledger: Arc<CommandLedger>,
    ) -> Self {
        Self {
            repository,
            cancellation,
            sender,
            permissions,
            ledger,
            max_chain: DEFAULT_MAX_CHAIN,
            retry: None,
            queue: None,
            reap: None,
        }
    }

    pub fn with_retry_admission(mut self, retry: Arc<dyn RetryAdmission>) -> Self {
        self.retry = Some(retry);
        self
    }

    pub fn with_queue_control(mut self, queue: Arc<ReliabilityStore>) -> Self {
        self.queue = Some(queue);
        self
    }

    pub fn with_reap_control(mut self, reap: Arc<dyn ReapControl>) -> Self {
        self.reap = Some(reap);
        self
    }

    pub async fn handle(&self, event: &InboundMatrixEvent) -> Result<AdminResult, ReplyError> {
        if !event
            .body
            .split_ascii_whitespace()
            .next()
            .is_some_and(|token| token.starts_with('/'))
        {
            return Ok(AdminResult::Ordinary);
        }
        let context = ReplyContext {
            room_id: event.room_id.clone(),
            thread_root: event.thread_root.clone(),
            event_id: event.event_id.clone(),
        };
        let key = (event.room_id.clone(), event.event_id.clone());
        let mut reservation = match Reservation::begin(Arc::clone(&self.ledger), key) {
            Begin::Cached(CachedReply::Body(body)) => {
                self.sender.send_reply(&context, &body).await?;
                return Ok(AdminResult::Replied);
            }
            Begin::Cached(CachedReply::OutboxOwned) => return Ok(AdminResult::Replied),
            Begin::InFlight => {
                self.sender
                    .send_reply(&context, "command=in_progress")
                    .await?;
                return Ok(AdminResult::Replied);
            }
            Begin::Reserved(reservation) => reservation,
        };
        let body = match self.execute(event).await {
            Ok(body) => body,
            Err(()) => {
                reservation.release();
                self.sender
                    .send_reply(&context, "command=temporarily_unavailable")
                    .await?;
                return Ok(AdminResult::Replied);
            }
        };
        reservation.commit(if body.is_empty() {
            CachedReply::OutboxOwned
        } else {
            CachedReply::Body(body.clone())
        });
        if body.is_empty() {
            return Ok(AdminResult::Replied);
        }
        self.sender.send_reply(&context, &body).await?;
        Ok(AdminResult::Replied)
    }

    async fn execute(&self, event: &InboundMatrixEvent) -> Result<String, ()> {
        if derive_matrix_user_id(&event.sender).is_err()
            || !self.permissions.permits(&event.sender, &event.room_id)
        {
            return Ok("command=forbidden".into());
        }
        let (command, task_id) = match parse(&event.body) {
            Ok(value) => value,
            Err(body) => return Ok(body.into()),
        };
        let Some(task) = self.repository.get_task(task_id).await.map_err(|_| ())? else {
            return Ok("task=not_found".into());
        };
        match command {
            Command::Status => {
                let latest = self
                    .repository
                    .latest_event(task_id)
                    .await
                    .map_err(|_| ())?;
                Ok(cap(match latest {
                    Some(event) => format!(
                        "command=status task_id={} root_task_id={} parent_task_id={} from_agent={} to_agent={} status={} seq={} timestamp={}",
                        task.task_id,
                        task.root_task_id,
                        task.parent_task_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "none".into()),
                        task.from_agent,
                        task.to_agent,
                        status(event.status),
                        event.seq,
                        event.timestamp.to_rfc3339()
                    ),
                    None => format!(
                        "command=status task_id={} root_task_id={} parent_task_id={} from_agent={} to_agent={} status=unknown",
                        task.task_id,
                        task.root_task_id,
                        task.parent_task_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "none".into()),
                        task.from_agent,
                        task.to_agent
                    ),
                }))
            }
            Command::Trace => {
                let trace = task_trace(self.repository.as_ref(), &task, self.max_chain)
                    .await
                    .map_err(|_| ())?;
                Ok(cap(format!(
                    "command=trace task_id={} chain_state={} chain_boundary={} chain={}",
                    task.task_id,
                    trace.state,
                    trace
                        .boundary
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "none".into()),
                    trace
                        .ids
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(">")
                )))
            }
            Command::Cancel => {
                let latest = self
                    .repository
                    .latest_event(task_id)
                    .await
                    .map_err(|_| ())?;
                if latest.is_some_and(|event| terminal(event.status)) {
                    return Ok("cancel_request=already_terminal".into());
                }
                self.cancellation
                    .cancel(task_id, "operator requested cancellation");
                Ok("cancel_request=registered".into())
            }
            Command::Pause => {
                let Some(queue) = &self.queue else {
                    return Ok("pause=unsupported".into());
                };
                self.cancellation
                    .cancel(task_id, "operator requested pause");
                let reaped = match &self.reap {
                    Some(control) => control.reap(task.to_agent, task_id).await.unwrap_or(None),
                    None => None,
                };
                let Some(reaped) = reaped else {
                    let result = queue
                        .mark_pause_recovery_needed(
                            &task_id.to_string(),
                            &chrono::Utc::now().to_rfc3339(),
                        )
                        .await
                        .map_err(|_| ())?;
                    return Ok(format!("pause={result}"));
                };
                let result = queue
                    .pause_task_after_reap(
                        &reaped.task_id.to_string(),
                        &reaped.resource_key,
                        &reaped.owner,
                        reaped.fence,
                        &chrono::Utc::now().to_rfc3339(),
                    )
                    .await
                    .map_err(|_| ())?;
                Ok(format!("pause={result}"))
            }
            Command::Resume => {
                let Some(queue) = &self.queue else {
                    return Ok("resume=unsupported".into());
                };
                let changed = queue
                    .resume_task(&task_id.to_string(), &chrono::Utc::now().to_rfc3339())
                    .await
                    .map_err(|_| ())?;
                Ok(format!(
                    "resume={}",
                    if changed > 0 { "queued" } else { "conflict" }
                ))
            }
            Command::Retry => {
                let latest = self
                    .repository
                    .latest_event(task_id)
                    .await
                    .map_err(|_| ())?;
                if !latest.is_some_and(|event| terminal(event.status)) {
                    return Ok("retry=not_terminal".into());
                }
                match &self.retry {
                    Some(retry) => match retry.admit(event, &task).await? {
                        RetryReply::Immediate(body) => Ok(body),
                        RetryReply::OutboxOwned => Ok(String::new()),
                    },
                    None => Ok("retry=unsupported".into()),
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Command {
    Status,
    Trace,
    Cancel,
    Retry,
    Pause,
    Resume,
}
fn parse(body: &str) -> Result<(Command, TaskId), &'static str> {
    if body.len() > 256 || !body.is_ascii() {
        return Err("command=invalid");
    }
    let mut fields = body.split_ascii_whitespace();
    let command = match fields.next() {
        Some("/status") => Command::Status,
        Some("/trace") => Command::Trace,
        Some("/cancel") => Command::Cancel,
        Some("/retry") => Command::Retry,
        Some("/pause") => Command::Pause,
        Some("/resume") => Command::Resume,
        _ => return Err("command=unknown"),
    };
    let Some(id) = fields.next() else {
        return Err("command=invalid");
    };
    if fields.next().is_some() {
        return Err("command=invalid");
    }
    let parsed = TaskId::from_str(id).map_err(|_| "command=invalid")?;
    if parsed.to_string() != id {
        return Err("command=invalid");
    }
    Ok((command, parsed))
}
fn terminal(status: TaskStatus) -> bool {
    matches!(
        status,
        TaskStatus::Completed | TaskStatus::Failed | TaskStatus::TimedOut | TaskStatus::Cancelled
    )
}
fn status(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Queued => "queued",
        TaskStatus::Dispatched => "dispatched",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::TimedOut => "timed_out",
        TaskStatus::Cancelled => "cancelled",
    }
}
fn cap(mut body: String) -> String {
    if body.len() > MAX_BODY_BYTES {
        body.truncate(MAX_BODY_BYTES - 3);
        body.push_str("...");
    }
    body
}

enum Begin {
    Cached(CachedReply),
    InFlight,
    Reserved(Reservation),
}
struct Reservation {
    ledger: Arc<CommandLedger>,
    key: (String, String),
    active: bool,
}
impl Reservation {
    fn begin(ledger: Arc<CommandLedger>, key: (String, String)) -> Begin {
        {
            let mut state = ledger.state.lock().unwrap_or_else(|p| p.into_inner());
            match state.entries.get(&key) {
                Some(Some(body)) => return Begin::Cached(body.clone()),
                Some(None) => return Begin::InFlight,
                None => {
                    state.entries.insert(key.clone(), None);
                }
            }
            Begin::Reserved(Reservation {
                ledger: Arc::clone(&ledger),
                key,
                active: true,
            })
        }
    }
    fn commit(&mut self, reply: CachedReply) {
        let mut state = self.ledger.state.lock().unwrap_or_else(|p| p.into_inner());
        state.entries.insert(self.key.clone(), Some(reply));
        state.completed.push_back(self.key.clone());
        while state.completed.len() > self.ledger.capacity {
            if let Some(old) = state.completed.pop_front() {
                state.entries.remove(&old);
            }
        }
        self.active = false;
    }
    fn release(&mut self) {
        self.remove();
    }
    fn remove(&mut self) {
        if self.active {
            self.ledger
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .entries
                .remove(&self.key);
            self.active = false;
        }
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.remove();
    }
}
