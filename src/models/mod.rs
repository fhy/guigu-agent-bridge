//! Core domain models shared by the Agent Bus, storage, and adapters.
//!
//! These are **pure data structures** with serde contracts. They depend only on
//! `serde` / `uuid` / `chrono` and [`crate::error`] — never on transport,
//! storage, or scheduling modules. Field names and types are a public contract
//! consumed by T004 (Bus), T008 (Storage), T010 (Matrix), and T014 (ACP); changes
//! require re-evaluating all consumers.
//!
//! Internal identifiers are strongly typed; external protocol identifiers remain opaque
//! strings at adapter boundaries.

pub mod agent;
pub mod conversation;
pub mod event;
pub mod ids;
pub mod message;
pub mod task;
pub mod workflow;

pub use agent::{AgentEndpoint, Capability, EndpointAddress, TransportType};
pub use conversation::{Conversation, ExternalRef};
pub use event::{TaskEvent, TaskEventPayload};
pub use ids::{ConversationId, DeliveryId, EndpointId, EventId, MessageId, TaskId};
pub use message::Message;
pub use task::{AgentTask, Priority, TaskStatus};
pub use workflow::{
    WorkflowEnvelope, WorkflowError, WorkflowKind, WorkflowState, canonical_metadata,
    parse_workflow,
};
