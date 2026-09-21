use std::{collections::HashMap, sync::Arc};

use crate::matrix::{ReapAck, ReapControl};
use crate::{
    bus::{
        BusFuture, DispatchError, DispatchOutcome, DispatchRequest, FinalizationCapability,
        PreparedExecution, TaskDispatcher,
    },
    models::{EndpointId, TaskId},
    runtime::LeasedAcpDispatcher,
};

/// Routes the single ACP transport slot to endpoint-specific leased dispatchers.
pub struct AcpDispatcherRouter {
    endpoints: HashMap<EndpointId, Arc<LeasedAcpDispatcher>>,
}

impl crate::matrix::sealed::ReapIssuer for AcpDispatcherRouter {}

impl AcpDispatcherRouter {
    pub fn new(endpoints: HashMap<EndpointId, Arc<LeasedAcpDispatcher>>) -> Self {
        Self { endpoints }
    }

    fn endpoint(
        &self,
        request: &DispatchRequest<'_>,
    ) -> Result<&Arc<LeasedAcpDispatcher>, DispatchError> {
        self.endpoints
            .get(&request.target.id())
            .ok_or_else(|| DispatchError::NotAccepted {
                reason: "ACP endpoint dispatcher unavailable".into(),
            })
    }

    pub async fn shutdown(&self) {
        for dispatcher in self.endpoints.values() {
            dispatcher.shutdown().await;
        }
    }
}

impl ReapControl for AcpDispatcherRouter {
    fn reap<'a>(
        &'a self,
        target: EndpointId,
        task: TaskId,
    ) -> BusFuture<'a, Result<Option<ReapAck>, ()>> {
        Box::pin(async move {
            let Some(dispatcher) = self.endpoints.get(&target) else {
                return Ok(None);
            };
            Ok(dispatcher
                .cancel_and_reap_task(task, "operator pause")
                .await
                .map(|ack| ReapAck {
                    task_id: ack.task_id,
                    resource_key: ack.resource_key,
                    owner: ack.owner,
                    fence: ack.fence,
                }))
        })
    }
}

impl TaskDispatcher for AcpDispatcherRouter {
    fn deliver<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<(), DispatchError>> {
        Box::pin(async move { self.endpoint(&request)?.deliver(request).await })
    }

    fn execute<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<DispatchOutcome, DispatchError>> {
        Box::pin(async move { self.endpoint(&request)?.execute(request).await })
    }

    fn supports_prepared(&self) -> bool {
        true
    }

    fn execute_prepared<'a>(
        &'a self,
        request: DispatchRequest<'a>,
    ) -> BusFuture<'a, Result<PreparedExecution, DispatchError>> {
        Box::pin(async move { self.endpoint(&request)?.execute_prepared(request).await })
    }

    fn cancel_prepared<'a>(
        &'a self,
        request: DispatchRequest<'a>,
        reason: &'a str,
    ) -> BusFuture<'a, Result<FinalizationCapability, DispatchError>> {
        Box::pin(async move {
            self.endpoint(&request)?
                .cancel_prepared(request, reason)
                .await
        })
    }
}
