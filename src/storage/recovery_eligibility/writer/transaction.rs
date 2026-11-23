use sqlx::{Sqlite, SqliteConnection, Transaction};
use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};
use tokio::sync::OwnedMutexGuard;

pub(crate) struct ScopedTransaction<'pool> {
    transaction: Transaction<'pool, Sqlite>,
    revision: Option<Arc<AtomicI64>>,
    guard: Option<OwnedMutexGuard<()>>,
    next_revision: Option<i64>,
}

impl<'pool> ScopedTransaction<'pool> {
    pub(super) fn new(
        transaction: Transaction<'pool, Sqlite>,
        revision: Option<Arc<AtomicI64>>,
        guard: Option<OwnedMutexGuard<()>>,
    ) -> Self {
        Self {
            transaction,
            revision,
            guard,
            next_revision: None,
        }
    }

    pub(super) fn stage_revision(
        &mut self,
        scope: &super::RecoveryScope,
        next: i64,
    ) -> Result<(), sqlx::Error> {
        if self
            .revision
            .as_ref()
            .is_none_or(|revision| !Arc::ptr_eq(revision, &scope.revision))
            || self.next_revision.is_some()
            || scope.revision.load(Ordering::Acquire).checked_add(1) != Some(next)
        {
            return Err(super::refused());
        }
        self.next_revision = Some(next);
        Ok(())
    }

    pub(crate) async fn commit(self) -> Result<(), sqlx::Error> {
        let Self {
            transaction,
            revision,
            guard,
            next_revision,
        } = self;
        transaction.commit().await?;
        if let (Some(revision), Some(next)) = (revision, next_revision) {
            revision.store(next, Ordering::Release);
        }
        drop(guard);
        Ok(())
    }

    pub(crate) async fn rollback(self) -> Result<(), sqlx::Error> {
        let Self {
            transaction, guard, ..
        } = self;
        let result = transaction.rollback().await;
        drop(guard);
        result
    }
}

impl Deref for ScopedTransaction<'_> {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl DerefMut for ScopedTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transaction
    }
}
