#![allow(dead_code)]

use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum BindingError {
    #[error("invalid binding field")]
    Invalid,
    #[error("binding conflict")]
    Conflict,
    #[error("database failure")]
    Database,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionOwnerBinding {
    pub binding_id: String,
    pub task_id: String,
    pub delivery_id: String,
    pub attempt: i64,
    pub session_id_digest: String,
    pub endpoint_id: String,
    pub conversation_id: String,
    pub cwd_digest: String,
    pub backend_id: String,
    pub runtime_instance: String,
    pub runtime_generation: i64,
    pub resource_key: String,
    pub owner_fence: i64,
    pub child_pid: i64,
    pub child_starttime: i64,
    pub executable_fingerprint: String,
    pub boot_id: String,
    pub domain_device: i64,
    pub domain_inode: i64,
    pub binding_revision: i64,
    pub state: &'static str,
    pub created_at: String,
    pub updated_at: String,
}

impl SessionOwnerBinding {
    pub(crate) fn digest(value: &str) -> String {
        format!("{:x}", Sha256::digest(value.as_bytes()))
    }

    fn validate(&self) -> Result<(), BindingError> {
        if self.binding_id.is_empty()
            || self.task_id.is_empty()
            || self.delivery_id.is_empty()
            || self.attempt < 1
            || self.session_id_digest.len() != 64
            || self.cwd_digest.len() != 64
            || self.endpoint_id.is_empty()
            || self.conversation_id.is_empty()
            || self.backend_id.is_empty()
            || self.runtime_instance.is_empty()
            || self.runtime_generation < 1
            || self.resource_key.is_empty()
            || self.owner_fence < 1
            || self.child_pid < 1
            || self.executable_fingerprint.is_empty()
            || self.boot_id.is_empty()
            || self.domain_device < 0
            || self.domain_inode < 0
            || self.binding_revision < 1
            || !matches!(self.state, "bound" | "unknown" | "reaped")
        {
            return Err(BindingError::Invalid);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SessionOwnerBindingStore {
    pool: SqlitePool,
}

impl SessionOwnerBindingStore {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub(crate) async fn insert(&self, binding: &SessionOwnerBinding) -> Result<bool, BindingError> {
        binding.validate()?;
        let result = sqlx::query("INSERT INTO session_owner_bindings (binding_id,task_id,delivery_id,attempt,session_id_digest,endpoint_id,conversation_id,cwd_digest,backend_id,runtime_instance,runtime_generation,resource_key,owner_fence,child_pid,child_starttime,executable_fingerprint,boot_id,domain_device,domain_inode,binding_revision,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&binding.binding_id).bind(&binding.task_id).bind(&binding.delivery_id).bind(binding.attempt)
            .bind(&binding.session_id_digest).bind(&binding.endpoint_id).bind(&binding.conversation_id).bind(&binding.cwd_digest)
            .bind(&binding.backend_id).bind(&binding.runtime_instance).bind(binding.runtime_generation).bind(&binding.resource_key)
            .bind(binding.owner_fence).bind(binding.child_pid).bind(binding.child_starttime).bind(&binding.executable_fingerprint)
            .bind(&binding.boot_id).bind(binding.domain_device).bind(binding.domain_inode).bind(binding.binding_revision)
            .bind(binding.state).bind(&binding.created_at).bind(&binding.updated_at)
            .execute(&self.pool).await.map_err(|_| BindingError::Conflict)?;
        Ok(result.rows_affected() == 1)
    }

    pub(crate) async fn current(
        &self,
        task_id: &str,
        delivery_id: &str,
        attempt: i64,
    ) -> Result<Option<(String, i64, String)>, BindingError> {
        sqlx::query("SELECT state,binding_revision,session_id_digest FROM session_owner_bindings WHERE task_id=? AND delivery_id=? AND attempt=?")
            .bind(task_id).bind(delivery_id).bind(attempt).fetch_optional(&self.pool).await
            .map_err(|_| BindingError::Database)
            .map(|row| row.map(|row| (row.get(0), row.get(1), row.get(2))))
    }

    pub(crate) async fn mark_reaped(
        &self,
        binding_id: &str,
        revision: i64,
        now: &str,
    ) -> Result<bool, BindingError> {
        let result = sqlx::query("UPDATE session_owner_bindings SET state='reaped',binding_revision=binding_revision+1,updated_at=? WHERE binding_id=? AND binding_revision=? AND state='bound'")
            .bind(now).bind(binding_id).bind(revision).execute(&self.pool).await.map_err(|_| BindingError::Database)?;
        Ok(result.rows_affected() == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate};

    #[tokio::test]
    async fn digest_and_validation_are_deterministic_and_fail_closed() {
        assert_eq!(
            SessionOwnerBinding::digest("session"),
            SessionOwnerBinding::digest("session")
        );
        assert_ne!(
            SessionOwnerBinding::digest("session"),
            SessionOwnerBinding::digest("other")
        );
        let pool = connect(std::env::temp_dir().join(format!("r059-{}.db", uuid::Uuid::now_v7())))
            .await
            .unwrap();
        migrate(&pool).await.unwrap();
        let store = SessionOwnerBindingStore::new(pool.clone());
        let binding = SessionOwnerBinding {
            binding_id: "binding".into(),
            task_id: "task".into(),
            delivery_id: "delivery".into(),
            attempt: 1,
            session_id_digest: "x".repeat(64),
            endpoint_id: "endpoint".into(),
            conversation_id: "conversation".into(),
            cwd_digest: "y".repeat(64),
            backend_id: "agent@1".into(),
            runtime_instance: "runtime".into(),
            runtime_generation: 1,
            resource_key: "resource".into(),
            owner_fence: 1,
            child_pid: 1,
            child_starttime: 1,
            executable_fingerprint: "exe".into(),
            boot_id: "boot".into(),
            domain_device: 1,
            domain_inode: 1,
            binding_revision: 1,
            state: "bound",
            created_at: "now".into(),
            updated_at: "now".into(),
        };
        assert_eq!(store.insert(&binding).await, Err(BindingError::Conflict));
        pool.close().await;
    }
}
