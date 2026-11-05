use super::*;

impl LeasedAcpDispatcher {
    pub(super) async fn deliver_with_renewal(
        &self,
        request: DispatchRequest<'_>,
        lease: &mut Lease,
    ) -> Result<(), DispatchError> {
        let delivery = self.dispatcher.deliver(request.clone());
        tokio::pin!(delivery);
        let heartbeat = (self.policy.lease_ttl / 2).max(Duration::from_millis(1));
        loop {
            let previous = lease.clone();
            let renewal = async {
                self.timer.sleep(heartbeat).await;
                self.store
                    .renew(&previous, self.clock.now(), self.policy.lease_ttl)
                    .await
            };
            tokio::select! {
                biased;
                result = renewal => {
                    match result {
                        Ok(renewed) => {
                            *lease = renewed;
                            tracing::debug!(task_id = %request.task.task_id, stage = "delivery-lease", fence = lease.fence, acknowledged = false, "execution lease renewed during bounded session setup");
                        }
                        Err(error) => {
                            tracing::error!(task_id = %request.task.task_id, stage = "delivery-lease", fence = lease.fence, category = renewal_category(&error), "lease renewal failed; retaining bounded setup before backend reap");
                            let _ = delivery.await;
                            return self.hold_setup_lease(lease).await;
                        }
                    }
                }
                result = &mut delivery => {
                    result?;
                    match self.store.renew(lease, self.clock.now(), self.policy.lease_ttl).await {
                        Ok(renewed) => {
                            *lease = renewed;
                            tracing::debug!(task_id = %request.task.task_id, stage = "delivery-lease", fence = lease.fence, acknowledged = true, "session setup completed with a renewed execution lease");
                            return Ok(());
                        }
                        Err(error) => {
                            tracing::error!(task_id = %request.task.task_id, stage = "delivery-lease", fence = lease.fence, category = renewal_category(&error), acknowledged = true, "post-setup lease refresh failed before any task prompt");
                            return self.hold_setup_lease(lease).await;
                        }
                    }
                }
            }
        }
    }

    async fn hold_setup_lease(&self, lease: &Lease) -> Result<(), DispatchError> {
        let reaped = self.dispatcher.shutdown_reaped().await;
        let held = self
            .store
            .release(lease, ReleaseDisposition::RecoveryNeeded, self.clock.now())
            .await
            .is_ok();
        tracing::error!(task_id = %lease.task_id, stage = "delivery-lease", fence = lease.fence, category = "lease-lost-before-prompt", reaped, held, "setup ownership was lost; no task prompt or automatic replay permitted");
        Err(DispatchError::RecoveryNeeded)
    }
}

fn renewal_category(error: &RuntimeError) -> &'static str {
    match error {
        RuntimeError::Expired => "lease-expired",
        RuntimeError::Fenced => "owner-fenced",
        RuntimeError::Storage(_) => "storage",
        _ => "runtime",
    }
}
