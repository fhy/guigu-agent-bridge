use super::SqliteRuntimeStore;
use crate::storage::recovery_eligibility::writer::{self, Mutation, ScopedTransaction};
use sqlx::SqliteConnection;

#[cfg(test)]
#[path = "recovery_audit_tests.rs"]
mod tests;

impl SqliteRuntimeStore {
    pub(super) async fn release_workspaces(
        &self,
        connection: &mut SqliteConnection,
        lease: &super::Lease,
        released: bool,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), sqlx::Error> {
        let claims: Vec<String> = sqlx::query_scalar("SELECT claim_id FROM workspace_claims WHERE runtime_owner=? AND task_id=? AND owner_fence=? AND state='active'")
            .bind(lease.owner.to_string()).bind(lease.task_id.to_string()).bind(super::to_i64(lease.fence).map_err(|_| sqlx::Error::Protocol("workspace fence overflow".into()))?).fetch_all(&mut *connection).await?;
        for claim in claims {
            let mutation = self.prepare(connection, "workspace_claims", &claim).await?;
            if released {
                writer::delete_workspace(
                    connection,
                    self.recovery.as_ref(),
                    &claim,
                    &lease.owner.to_string(),
                    &lease.task_id.to_string(),
                    super::to_i64(lease.fence)
                        .map_err(|_| sqlx::Error::Protocol("workspace fence overflow".into()))?,
                )
                .await?;
            } else {
                sqlx::query("UPDATE workspace_claims SET state='recovery_needed',revision=revision+1,updated_at=? WHERE claim_id=? AND state='active'")
                    .bind(super::ts(now)).bind(&claim).execute(&mut *connection).await?;
                self.complete(connection, mutation).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn transaction(&self) -> Result<ScopedTransaction<'_>, sqlx::Error> {
        writer::begin(&self.pool, self.recovery.as_ref()).await
    }

    pub(super) async fn prepare(
        &self,
        connection: &mut SqliteConnection,
        table: &str,
        key: &str,
    ) -> Result<Option<Mutation>, sqlx::Error> {
        let identity = writer::text_identity(table, key)?;
        writer::prepare_mutation(connection, self.recovery.as_ref(), &identity).await
    }

    pub(super) async fn prepare_existing_lease(
        &self,
        connection: &mut SqliteConnection,
        key: &str,
    ) -> Result<Option<Mutation>, sqlx::Error> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM execution_leases WHERE resource_key=?)",
        )
        .bind(key)
        .fetch_one(&mut *connection)
        .await?;
        if !exists {
            return Ok(None);
        }
        self.prepare(connection, "execution_leases", key).await
    }

    pub(super) async fn complete(
        &self,
        connection: &mut SqliteConnection,
        mutation: Option<Mutation>,
    ) -> Result<(), sqlx::Error> {
        writer::complete_mutation(connection, self.recovery.as_ref(), mutation).await
    }

    pub(super) async fn complete_reassigned_lease(
        &self,
        connection: &mut SqliteConnection,
        mutation: Option<Mutation>,
    ) -> Result<(), sqlx::Error> {
        writer::complete_reassigned_lease(connection, self.recovery.as_ref(), mutation).await
    }

    pub(super) async fn insert(
        &self,
        connection: &mut SqliteConnection,
        table: &str,
        key: &str,
    ) -> Result<(), sqlx::Error> {
        let identity = writer::text_identity(table, key)?;
        writer::inserted(connection, self.recovery.as_ref(), &identity).await
    }

    pub(super) async fn complete_or_insert(
        &self,
        connection: &mut SqliteConnection,
        table: &str,
        key: &str,
        mutation: Option<Mutation>,
    ) -> Result<(), sqlx::Error> {
        if mutation.is_some() {
            self.complete(connection, mutation).await
        } else {
            self.insert(connection, table, key).await
        }
    }
}
