//! Matrix adapter boundary.
//!
//! Matrix SDK types remain private to this module. Downstream routing consumes
//! [`InboundMatrixEvent`] and bridge-owned conversation models.

mod admin;
mod client;
mod conversation;
mod dedup;
mod error;
mod event;
mod identity;
mod outbound;
mod permission;
mod reply;
mod router;
mod sync;

pub use admin::{
    AdminHandler, AdminPermissionPolicy, AdminResult, CommandLedger, RetryAdmission, RetryReply,
};
pub use client::MatrixClient;
pub use conversation::resolve_conversation;
pub use dedup::EventDedup;
pub use error::MatrixError;
pub use event::InboundMatrixEvent;
pub use identity::{derive_matrix_user_id, with_matrix_participant};
pub use outbound::{MatrixOutboxSender, SdkMatrixSender};
pub use permission::PermissionPolicy;
pub use reply::{
    MatrixSender, PERMISSION_DENIED_REPLY, ReplyContext, ReplyError, ReplyFuture,
    send_permission_denied, send_task_terminal_reply, send_terminal_reply,
};
pub use router::{
    DurableMatrixAdmission, MatrixAdmissionFuture, RouteError, RoutePolicy, route_event,
    route_event_durable, route_event_with_monitor, route_workflow,
};
pub use sync::{
    MatrixSync, MatrixSyncHandle, MemorySyncTokenStore, RawMatrixEventConsumer, SyncTokenFuture,
    SyncTokenStore,
};
