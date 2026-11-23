use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};

use crate::matrix::MatrixError;

#[derive(Clone)]
pub struct CutoverStore {
    pub(super) pool: SqlitePool,
    pub(super) binding: String,
    pub(super) operation: String,
}

#[derive(Clone, PartialEq, Eq)]
pub struct CutoverState {
    pub baseline: Option<String>,
    pub watermark: Option<String>,
    pub revision: i64,
}

pub fn cutover_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

impl CutoverStore {
    pub async fn prepare(
        pool: SqlitePool,
        operation: &str,
        binding: &str,
        archive_digest: &str,
    ) -> Result<Self, MatrixError> {
        if operation.is_empty()
            || operation.len() > 128
            || !valid_digest(binding)
            || !valid_digest(archive_digest)
        {
            return Err(MatrixError::CutoverRefused);
        }
        let mut transaction = pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| MatrixError::Storage)?;
        let existing = sqlx::query(
            "SELECT operation_id,binding,archive_digest FROM matrix_cutover WHERE singleton=1",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| MatrixError::Storage)?;
        if let Some(row) = existing {
            if row.get::<String, _>(0) != operation
                || row.get::<String, _>(1) != binding
                || row.get::<String, _>(2) != archive_digest
            {
                return Err(MatrixError::CutoverRefused);
            }
        } else {
            let required: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_cutover_required")
                .fetch_one(&mut *transaction)
                .await
                .map_err(|_| MatrixError::Storage)?;
            if required != 0 {
                return Err(MatrixError::CutoverRefused);
            }
            let count: i64 = sqlx::query_scalar(
                "SELECT (SELECT count(*) FROM tasks) + (SELECT count(*) FROM transport_receipts)",
            )
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| MatrixError::Storage)?;
            if count != 0 {
                return Err(MatrixError::CutoverRefused);
            }
            sqlx::query("INSERT INTO matrix_cutover_required(singleton,operation_id) VALUES(1,?)")
                .bind(operation)
                .execute(&mut *transaction)
                .await
                .map_err(|_| MatrixError::Storage)?;
            sqlx::query("INSERT INTO matrix_cutover(singleton,operation_id,binding,archive_digest,phase) VALUES(1,?,?,?,'pending')")
                .bind(operation).bind(binding).bind(archive_digest)
                .execute(&mut *transaction).await.map_err(|_| MatrixError::Storage)?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| MatrixError::Storage)?;
        Self::open(pool, binding)
            .await?
            .ok_or(MatrixError::CutoverRefused)
    }

    pub async fn open(pool: SqlitePool, binding: &str) -> Result<Option<Self>, MatrixError> {
        let row = sqlx::query(
            "SELECT operation_id,binding,archive_digest FROM matrix_cutover WHERE singleton=1",
        )
        .fetch_optional(&pool)
        .await
        .map_err(|_| MatrixError::Storage)?;
        let required: Option<String> = sqlx::query_scalar(
            "SELECT operation_id FROM matrix_cutover_required WHERE singleton=1",
        )
        .fetch_optional(&pool)
        .await
        .map_err(|_| MatrixError::Storage)?;
        let Some(row) = row else {
            if required.is_some() {
                return Err(MatrixError::CutoverRefused);
            }
            return Ok(None);
        };
        if required.as_deref() != Some(row.get::<String, _>(0).as_str()) {
            return Err(MatrixError::CutoverRefused);
        }
        if !valid_digest(binding)
            || row.get::<String, _>(1) != binding
            || !valid_digest(&row.get::<String, _>(2))
        {
            return Err(MatrixError::CutoverRefused);
        }
        let store = Self {
            pool,
            binding: binding.to_owned(),
            operation: row.get(0),
        };
        store.state().await?;
        Ok(Some(store))
    }

    pub async fn state(&self) -> Result<CutoverState, MatrixError> {
        let row = sqlx::query("SELECT phase,baseline,baseline_digest,watermark,revision FROM matrix_cutover WHERE singleton=1 AND operation_id=? AND binding=?")
            .bind(&self.operation).bind(&self.binding).fetch_optional(&self.pool)
            .await.map_err(|_| MatrixError::Storage)?.ok_or(MatrixError::CutoverRefused)?;
        let phase: String = row.get(0);
        let baseline: Option<String> = row.get(1);
        let digest: Option<String> = row.get(2);
        let watermark: Option<String> = row.get(3);
        let revision: i64 = row.get(4);
        if revision < 1
            || match phase.as_str() {
                "pending" => baseline.is_some() || watermark.is_some() || digest.is_some(),
                "active" => {
                    baseline.as_ref().is_none_or(|token| {
                        token.is_empty()
                            || digest.as_deref() != Some(cutover_digest(token.as_bytes()).as_str())
                    }) || watermark.as_ref().is_none_or(|token| token.is_empty())
                }
                _ => true,
            }
        {
            return Err(MatrixError::CutoverRefused);
        }
        Ok(CutoverState {
            baseline,
            watermark,
            revision,
        })
    }

    pub async fn activate(&self, baseline: &str) -> Result<(), MatrixError> {
        if baseline.is_empty() {
            return Err(MatrixError::CutoverRefused);
        }
        let changed = sqlx::query("UPDATE matrix_cutover SET phase='active',baseline=?,baseline_digest=?,watermark=?,revision=revision+1 WHERE singleton=1 AND phase='pending' AND operation_id=? AND binding=?")
            .bind(baseline).bind(cutover_digest(baseline.as_bytes())).bind(baseline)
            .bind(&self.operation).bind(&self.binding).execute(&self.pool).await
            .map_err(|_| MatrixError::Storage)?.rows_affected();
        if changed != 1 {
            return Err(MatrixError::CutoverRefused);
        }
        let state = self.state().await?;
        if state.baseline.as_deref() != Some(baseline)
            || state.watermark.as_deref() != Some(baseline)
        {
            return Err(MatrixError::CutoverRefused);
        }
        Ok(())
    }

    pub async fn commit_batch(
        &self,
        expected: &CutoverState,
        token: &str,
    ) -> Result<(), MatrixError> {
        if token.is_empty() {
            return Err(MatrixError::CutoverRefused);
        }
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| MatrixError::Storage)?;
        let incomplete: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_cutover_dispositions WHERE operation_id=? AND state!='complete'")
            .bind(&self.operation).fetch_one(&mut *transaction).await.map_err(|_| MatrixError::Storage)?;
        if incomplete != 0 {
            return Err(MatrixError::CutoverRefused);
        }
        let changed = sqlx::query("UPDATE matrix_cutover SET watermark=?,revision=revision+1 WHERE singleton=1 AND phase='active' AND operation_id=? AND binding=? AND baseline=? AND watermark=? AND revision=?")
            .bind(token).bind(&self.operation).bind(&self.binding).bind(&expected.baseline)
            .bind(&expected.watermark).bind(expected.revision).execute(&mut *transaction)
            .await.map_err(|_| MatrixError::Storage)?.rows_affected();
        if changed != 1 {
            return Err(MatrixError::CutoverRefused);
        }
        transaction.commit().await.map_err(|_| MatrixError::Storage)
    }
}
