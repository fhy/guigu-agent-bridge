use super::Repository;
use crate::{
    bus::{BusFuture, ConsumerError, EventConsumer},
    models::TaskEvent,
};
use std::sync::Arc;

pub struct RepositoryEventConsumer {
    repository: Arc<dyn Repository>,
}
impl RepositoryEventConsumer {
    pub fn new(repository: Arc<dyn Repository>) -> Self {
        Self { repository }
    }
}
impl EventConsumer for RepositoryEventConsumer {
    fn consume<'a>(&'a self, event: &'a TaskEvent) -> BusFuture<'a, Result<(), ConsumerError>> {
        Box::pin(async move {
            self.repository
                .append_event(event)
                .await
                .map_err(|_| ConsumerError::Failed {
                    reason: "repository event persistence failed".into(),
                })
        })
    }
}
