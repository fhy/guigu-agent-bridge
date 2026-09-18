use crate::{
    bus::{BusFuture, ConsumerError, EventConsumer},
    models::{TaskEvent, TaskStatus},
};

use super::A2aStore;

pub struct A2aTerminalProjection {
    store: A2aStore,
}

impl A2aTerminalProjection {
    pub fn new(store: A2aStore) -> Self {
        Self { store }
    }
}

impl EventConsumer for A2aTerminalProjection {
    fn consume<'a>(&'a self, event: &'a TaskEvent) -> BusFuture<'a, Result<(), ConsumerError>> {
        Box::pin(async move {
            if matches!(
                event.status,
                TaskStatus::Completed
                    | TaskStatus::Failed
                    | TaskStatus::TimedOut
                    | TaskStatus::Cancelled
            ) {
                self.store
                    .project_task_terminal(&event.task_id.to_string())
                    .await
                    .map_err(|_| ConsumerError::Failed {
                        reason: "A2A terminal projection failed".into(),
                    })?;
            }
            Ok(())
        })
    }
}
