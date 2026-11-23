use crate::{
    acp::output::DurableOutputSpool,
    bus::{BusFuture, ConsumerError, EventConsumer},
    models::{TaskEvent, TaskEventPayload},
    storage::OutputSpoolStore,
};

pub(crate) struct DurableOutputProjection {
    store: OutputSpoolStore,
    spool: DurableOutputSpool,
    owner: String,
}

impl DurableOutputProjection {
    pub(crate) fn new(store: OutputSpoolStore, spool: DurableOutputSpool, owner: String) -> Self {
        Self {
            store,
            spool,
            owner,
        }
    }
}

impl EventConsumer for DurableOutputProjection {
    fn consume<'a>(&'a self, event: &'a TaskEvent) -> BusFuture<'a, Result<(), ConsumerError>> {
        Box::pin(async move {
            let TaskEventPayload::Completed { output } = &event.payload else {
                return Ok(());
            };
            let task = event.task_id.to_string();
            let Some(set_id) = self
                .store
                .assembling_set(&task)
                .await
                .map_err(|_| failure())?
            else {
                return Ok(());
            };
            let now = event
                .timestamp
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
            self.spool
                .publish(&set_id, &self.owner, output.as_bytes(), &now)
                .await
                .map_err(|_| failure())?;
            if !self
                .store
                .activate_prepared(&set_id, &now)
                .await
                .map_err(|_| failure())?
            {
                return Err(failure());
            }
            Ok(())
        })
    }
}

fn failure() -> ConsumerError {
    ConsumerError::Failed {
        reason: "durable output projection failed".into(),
    }
}
