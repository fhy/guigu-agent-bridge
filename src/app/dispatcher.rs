use std::{collections::HashMap, sync::Arc};

use crate::{
    bus::{
        BusFuture, DispatchError, DispatchOutcome, DispatchRequest, FinalizationCapability,
        PreparedExecution, TaskDispatcher,
    },
    models::EndpointId,
    runtime::LeasedAcpDispatcher,
};

/// Routes the single ACP transport slot to endpoint-specific leased dispatchers.
pub struct AcpDispatcherRouter {
    endpoints: HashMap<EndpointId, Arc<LeasedAcpDispatcher>>,
}

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
