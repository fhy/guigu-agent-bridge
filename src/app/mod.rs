//! Production assembly primitives and lifecycle ownership.

mod admission;
mod dispatcher;
mod health;
mod ingress;
mod outbox;
mod projection;
mod reload;
mod retry;
mod runtime;

pub use admission::PersistingBus;
pub use dispatcher::AcpDispatcherRouter;
pub use health::{HealthServer, HealthState, OwnerState};
pub use ingress::{
    MatrixIngress, MatrixIngressHandle, ReloadingMonitorSender, ReplyRegistry,
    TerminalReplyConsumer,
};
pub use outbox::{OutboxDrain, OutboxDrainHandle};
pub use projection::{PersistenceFirstProjection, ProjectionMetrics};
pub use reload::{
    ActiveSnapshot, ConfigSource, FileConfigSource, HotConfig, ReloadController, ReloadError,
    ReloadOutcome,
};
pub use retry::DurableRetryAdmission;
pub use runtime::{AppError, AppRuntime, run_config_with_shutdown};
