use std::sync::Arc;

use crate::{bus::BusError, gateway::GatewayStore, models::AgentTask};

use super::PersistingBus;

/// Gateway-private durable handoff used by both live ingress and startup recovery.
#[derive(Clone)]
pub struct GatewayHandoff {
    store: GatewayStore,
    bus: Arc<PersistingBus>,
    runtime: String,
}

impl GatewayHandoff {
    pub fn new(store: GatewayStore, bus: Arc<PersistingBus>, runtime: String) -> Self {
        Self {
            store,
            bus,
            runtime,
        }
    }

    pub async fn handoff(&self, task: AgentTask, revision: i64) -> Result<(), BusError> {
        let task_id = task.task_id.to_string();
        if !self
            .store
            .enqueue_ready(&task_id, &self.runtime, revision)
            .await
            .map_err(|_| BusError::TaskChannelClosed)?
        {
            return Err(BusError::TaskChannelClosed);
        }
        match self.bus.enqueue_persisted(task).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = self
                    .store
                    .restore_ready(&task_id, &self.runtime, revision + 1)
                    .await;
                Err(error)
            }
        }
    }
}
