use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::{
    bus::{BusFuture, ConsumerError, EventConsumer},
    models::TaskEvent,
};

#[derive(Debug, Default)]
pub struct ProjectionMetrics {
    failures: AtomicU64,
}

impl ProjectionMetrics {
    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }
}

/// Persists first, then attempts every external projection independently.
pub struct PersistenceFirstProjection {
    persistence: Arc<dyn EventConsumer>,
    siblings: Vec<Arc<dyn EventConsumer>>,
    metrics: Arc<ProjectionMetrics>,
}

impl PersistenceFirstProjection {
    pub fn new(
        persistence: Arc<dyn EventConsumer>,
        siblings: Vec<Arc<dyn EventConsumer>>,
        metrics: Arc<ProjectionMetrics>,
    ) -> Self {
        Self {
            persistence,
            siblings,
            metrics,
        }
    }
}

impl EventConsumer for PersistenceFirstProjection {
    fn consume<'a>(&'a self, event: &'a TaskEvent) -> BusFuture<'a, Result<(), ConsumerError>> {
        Box::pin(async move {
            self.persistence.consume(event).await?;
            let mut failed = false;
            for sibling in &self.siblings {
                if sibling.consume(event).await.is_err() {
                    failed = true;
                    self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                }
            }
            if failed {
                Err(ConsumerError::Failed {
                    reason: "one or more external projections failed".into(),
                })
            } else {
                Ok(())
            }
        })
    }
}
