use super::{MatrixError, RecoveryBinding, timestamp};

pub(crate) struct MatrixSyncLease(RecoveryBinding);

#[cfg(test)]
#[path = "sync_shutdown_tests.rs"]
mod tests;

impl MatrixSyncLease {
    pub(super) fn new(binding: RecoveryBinding) -> Self {
        Self(binding)
    }

    pub(crate) async fn release(self) -> Result<(), MatrixError> {
        self.0
            .store
            .release_component_lease(
                "MatrixSync",
                &self.0.owner_token,
                self.0.owner_epoch,
                &timestamp(),
            )
            .await
            .map_err(|_| MatrixError::Storage)
    }
}
