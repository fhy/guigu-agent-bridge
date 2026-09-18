use std::future::Future;
use std::pin::Pin;

use crate::models::TaskEventPayload;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyContext {
    pub room_id: String,
    pub thread_root: Option<String>,
    pub event_id: String,
}

#[derive(Debug, thiserror::Error)]
#[error("matrix reply failed")]
pub struct ReplyError;

pub type ReplyFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ReplyError>> + Send + 'a>>;

pub trait MatrixSender: Send + Sync {
    fn send_reply<'a>(&'a self, context: &'a ReplyContext, body: &'a str) -> ReplyFuture<'a>;
}

pub const PERMISSION_DENIED_REPLY: &str = "This request is not permitted.";

pub fn send_permission_denied<'a>(
    sender: &'a dyn MatrixSender,
    context: &'a ReplyContext,
) -> ReplyFuture<'a> {
    send_terminal_reply(sender, context, PERMISSION_DENIED_REPLY)
}

pub fn send_terminal_reply<'a>(
    sender: &'a dyn MatrixSender,
    context: &'a ReplyContext,
    body: &'a str,
) -> ReplyFuture<'a> {
    Box::pin(async move { sender.send_reply(context, body).await })
}

pub async fn send_task_terminal_reply(
    sender: &dyn MatrixSender,
    context: &ReplyContext,
    payload: &TaskEventPayload,
) -> Result<bool, ReplyError> {
    let body = match payload {
        TaskEventPayload::Completed { output } => output.as_str(),
        TaskEventPayload::Failed { error } => error.as_str(),
        TaskEventPayload::TimedOut { .. } => "The task timed out.",
        TaskEventPayload::Cancelled { reason } => reason.as_str(),
        TaskEventPayload::Queued
        | TaskEventPayload::Dispatched { .. }
        | TaskEventPayload::Running { .. } => return Ok(false),
    };
    sender.send_reply(context, body).await?;
    Ok(true)
}
