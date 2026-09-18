use chrono::{DateTime, Utc};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use super::{A2aError, wire::Message};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeRecord {
    pub exchange_id: String,
    pub peer_id: String,
    pub request_id: String,
    pub request_hash: String,
    pub external_task_id: Option<String>,
    pub internal_task_id: Option<String>,
    pub state: String,
    pub revision: i64,
    pub content_bytes: i64,
    pub content_cleaned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundReservation {
    New(ExchangeRecord),
    Replay(ExchangeRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupClaim {
    pub exchange_id: String,
    pub revision: i64,
}

#[derive(Debug, Clone)]
pub struct A2aStore {
    pool: SqlitePool,
}

impl A2aStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn reserve_inbound(
        &self,
        peer_id: &str,
        request_id: &str,
        request_hash: &str,
        context_id: &str,
        now: DateTime<Utc>,
    ) -> Result<InboundReservation, A2aError> {
        let mut tx = self.pool.begin().await?;
        if let Some(existing) = read_request(&mut tx, peer_id, "inbound", request_id).await? {
            tx.commit().await?;
            return if existing.request_hash == request_hash {
                Ok(InboundReservation::Replay(existing))
            } else {
                Err(A2aError::Conflict)
            };
        }
        let exchange_id = uuid::Uuid::now_v7().to_string();
        let timestamp = now.to_rfc3339();
        sqlx::query(
            "INSERT INTO a2a_exchanges \
             (exchange_id,peer_id,direction,request_id,request_hash,external_context_id,state,revision,content_bytes,cleanup_state,cleanup_revision,created_at,updated_at) \
             VALUES (?,?, 'inbound',?,?,?,'reserved',0,0,'pending',0,?,?)",
        )
        .bind(&exchange_id)
        .bind(peer_id)
        .bind(request_id)
        .bind(request_hash)
        .bind(context_id)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&mut *tx)
        .await?;
        let record = read_exchange(&mut tx, &exchange_id)
            .await?
            .ok_or(A2aError::Storage(sqlx::Error::RowNotFound))?;
        tx.commit().await?;
        Ok(InboundReservation::New(record))
    }

    pub async fn reserve_outbound(
        &self,
        peer_id: &str,
        request_id: &str,
        internal_task_id: &str,
        context_id: &str,
        now: DateTime<Utc>,
    ) -> Result<InboundReservation, A2aError> {
        let mut tx = self.pool.begin().await?;
        if let Some(existing) = read_request(&mut tx, peer_id, "outbound", request_id).await? {
            tx.commit().await?;
            return if existing.internal_task_id.as_deref() == Some(internal_task_id) {
                Ok(InboundReservation::Replay(existing))
            } else {
                Err(A2aError::Conflict)
            };
        }
        let exchange_id = uuid::Uuid::now_v7().to_string();
        let timestamp = now.to_rfc3339();
        sqlx::query(
            "INSERT INTO a2a_exchanges \
             (exchange_id,peer_id,direction,request_id,request_hash,external_context_id,internal_task_id,state,revision,content_bytes,cleanup_state,cleanup_revision,created_at,updated_at) \
             VALUES (?,?, 'outbound',?,?,?,?, 'reserved',0,0,'pending',0,?,?)",
        ).bind(&exchange_id).bind(peer_id).bind(request_id).bind(request_id).bind(context_id)
            .bind(internal_task_id).bind(&timestamp).bind(&timestamp).execute(&mut *tx).await?;
        let record = read_exchange(&mut tx, &exchange_id)
            .await?
            .ok_or(A2aError::Storage(sqlx::Error::RowNotFound))?;
        tx.commit().await?;
        Ok(InboundReservation::New(record))
    }

    pub async fn acknowledge_outbound(
        &self,
        exchange_id: &str,
        remote_task_id: &str,
        state: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, A2aError> {
        if !matches!(state, "submitted" | "working") {
            return Err(A2aError::Protocol("invalid acceptance state"));
        }
        Ok(sqlx::query(
            "UPDATE a2a_exchanges SET external_task_id=?,state=?,revision=revision+1,updated_at=? \
             WHERE exchange_id=? AND state='reserved' AND external_task_id IS NULL",
        )
        .bind(remote_task_id)
        .bind(state)
        .bind(now.to_rfc3339())
        .bind(exchange_id)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1)
    }

    pub async fn outbound_for_task(
        &self,
        peer_id: &str,
        internal_task_id: &str,
    ) -> Result<Option<ExchangeRecord>, A2aError> {
        let row = sqlx::query("SELECT * FROM a2a_exchanges WHERE peer_id=? AND direction='outbound' AND internal_task_id=?")
            .bind(peer_id).bind(internal_task_id).fetch_optional(&self.pool).await?;
        Ok(row.map(decode))
    }

    pub async fn by_external_task(
        &self,
        peer_id: &str,
        direction: &str,
        external_task_id: &str,
    ) -> Result<Option<ExchangeRecord>, A2aError> {
        let row = sqlx::query(
            "SELECT * FROM a2a_exchanges WHERE peer_id=? AND direction=? AND external_task_id=?",
        )
        .bind(peer_id)
        .bind(direction)
        .bind(external_task_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(decode))
    }

    pub async fn bind_task(
        &self,
        exchange_id: &str,
        internal_task_id: &str,
        external_task_id: &str,
        content_bytes: i64,
        now: DateTime<Utc>,
    ) -> Result<bool, A2aError> {
        let changed = sqlx::query(
            "UPDATE a2a_exchanges SET internal_task_id=?,external_task_id=?,state='submitted',revision=revision+1,content_bytes=?,updated_at=? \
             WHERE exchange_id=? AND state='reserved'",
        )
        .bind(internal_task_id)
        .bind(external_task_id)
        .bind(content_bytes)
        .bind(now.to_rfc3339())
        .bind(exchange_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(changed == 1)
    }

    pub async fn store_message(
        &self,
        exchange_id: &str,
        message: &Message,
        now: DateTime<Utc>,
    ) -> Result<i64, A2aError> {
        super::content::store_message(&self.pool, exchange_id, message, now).await
    }

    pub async fn store_artifacts(
        &self,
        exchange_id: &str,
        artifacts: &[super::wire::Artifact],
    ) -> Result<i64, A2aError> {
        super::content::store_artifacts(&self.pool, exchange_id, artifacts).await
    }

    pub async fn exchange(&self, exchange_id: &str) -> Result<Option<ExchangeRecord>, A2aError> {
        let mut connection = self.pool.acquire().await?;
        read_exchange_conn(&mut connection, exchange_id).await
    }

    pub async fn mark_terminal(
        &self,
        exchange_id: &str,
        expected_revision: i64,
        state: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, A2aError> {
        if !matches!(state, "completed" | "failed" | "canceled" | "rejected") {
            return Err(A2aError::Protocol("invalid terminal state"));
        }
        let now = now.to_rfc3339();
        Ok(sqlx::query(
            "UPDATE a2a_exchanges SET state=?,revision=revision+1,terminal_at=?,updated_at=? \
             WHERE exchange_id=? AND revision=? AND terminal_at IS NULL",
        )
        .bind(state)
        .bind(&now)
        .bind(&now)
        .bind(exchange_id)
        .bind(expected_revision)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1)
    }

    pub async fn retained_terminal_bytes(&self) -> Result<i64, A2aError> {
        let value: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(content_bytes),0) FROM a2a_exchanges WHERE terminal_at IS NOT NULL AND cleanup_state != 'cleaned'",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }

    pub async fn claim_cleanup(
        &self,
        runtime: &str,
        before: DateTime<Utc>,
        limit: u32,
        now: DateTime<Utc>,
    ) -> Result<Vec<CleanupClaim>, A2aError> {
        if limit == 0 || limit > 256 {
            return Err(A2aError::Config("cleanup batch must be 1..=256"));
        }
        let mut tx = self.pool.begin().await?;
        let ids: Vec<(String, i64)> = sqlx::query_as(
            "SELECT e.exchange_id,e.cleanup_revision FROM a2a_exchanges e \
             LEFT JOIN runtime_instances r ON r.instance_token=e.cleanup_owner \
             WHERE e.terminal_at IS NOT NULL AND e.terminal_at <= ? \
             AND (e.cleanup_state='pending' OR (e.cleanup_state='claimed' AND r.state='stopped')) \
             ORDER BY e.terminal_at,e.exchange_id LIMIT ?",
        )
        .bind(before.to_rfc3339())
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await?;
        let now = now.to_rfc3339();
        let mut claims = Vec::with_capacity(ids.len());
        for (id, revision) in ids {
            let changed = sqlx::query(
                "UPDATE a2a_exchanges SET cleanup_state='claimed',cleanup_owner=?,cleanup_revision=cleanup_revision+1,cleanup_claimed_at=?,updated_at=? \
                 WHERE exchange_id=? AND cleanup_revision=? \
                 AND (cleanup_state='pending' OR (cleanup_state='claimed' AND EXISTS \
                    (SELECT 1 FROM runtime_instances old WHERE old.instance_token=cleanup_owner AND old.state='stopped'))) \
                 AND EXISTS (SELECT 1 FROM runtime_instances WHERE instance_token=? AND state='active')",
            )
            .bind(runtime)
            .bind(&now)
            .bind(&now)
            .bind(&id)
            .bind(revision)
            .bind(runtime)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if changed == 1 {
                claims.push(CleanupClaim {
                    exchange_id: id,
                    revision: revision + 1,
                });
            }
        }
        tx.commit().await?;
        Ok(claims)
    }

    pub async fn clean_claim(
        &self,
        runtime: &str,
        claim: &CleanupClaim,
        now: DateTime<Utc>,
    ) -> Result<bool, A2aError> {
        let mut tx = self.pool.begin().await?;
        let valid: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM a2a_exchanges e JOIN runtime_instances r ON r.instance_token=e.cleanup_owner \
             WHERE e.exchange_id=? AND e.cleanup_state='claimed' AND e.cleanup_owner=? AND e.cleanup_revision=? \
             AND e.terminal_at IS NOT NULL AND r.state='active'",
        )
        .bind(&claim.exchange_id)
        .bind(runtime)
        .bind(claim.revision)
        .fetch_optional(&mut *tx)
        .await?;
        if valid.is_none() {
            tx.rollback().await?;
            return Ok(false);
        }
        sqlx::query("DELETE FROM a2a_messages WHERE exchange_id=?")
            .bind(&claim.exchange_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM a2a_artifacts WHERE exchange_id=?")
            .bind(&claim.exchange_id)
            .execute(&mut *tx)
            .await?;
        let now = now.to_rfc3339();
        let changed = sqlx::query(
            "UPDATE a2a_exchanges SET cleanup_state='cleaned',content_bytes=0,content_cleaned_at=?,updated_at=? \
             WHERE exchange_id=? AND cleanup_owner=? AND cleanup_revision=?",
        )
        .bind(&now)
        .bind(&now)
        .bind(&claim.exchange_id)
        .bind(runtime)
        .bind(claim.revision)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(changed == 1)
    }
}

async fn read_request(
    tx: &mut Transaction<'_, Sqlite>,
    peer: &str,
    direction: &str,
    request: &str,
) -> Result<Option<ExchangeRecord>, A2aError> {
    let row =
        sqlx::query("SELECT * FROM a2a_exchanges WHERE peer_id=? AND direction=? AND request_id=?")
            .bind(peer)
            .bind(direction)
            .bind(request)
            .fetch_optional(&mut **tx)
            .await?;
    Ok(row.map(decode))
}

async fn read_exchange(
    tx: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<Option<ExchangeRecord>, A2aError> {
    let row = sqlx::query("SELECT * FROM a2a_exchanges WHERE exchange_id=?")
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?;
    Ok(row.map(decode))
}

async fn read_exchange_conn(
    conn: &mut sqlx::pool::PoolConnection<Sqlite>,
    id: &str,
) -> Result<Option<ExchangeRecord>, A2aError> {
    let row = sqlx::query("SELECT * FROM a2a_exchanges WHERE exchange_id=?")
        .bind(id)
        .fetch_optional(&mut **conn)
        .await?;
    Ok(row.map(decode))
}

fn decode(row: sqlx::sqlite::SqliteRow) -> ExchangeRecord {
    ExchangeRecord {
        exchange_id: row.get("exchange_id"),
        peer_id: row.get("peer_id"),
        request_id: row.get("request_id"),
        request_hash: row.get("request_hash"),
        external_task_id: row.get("external_task_id"),
        internal_task_id: row.get("internal_task_id"),
        state: row.get("state"),
        revision: row.get("revision"),
        content_bytes: row.get("content_bytes"),
        content_cleaned: row.get::<String, _>("cleanup_state") == "cleaned",
    }
}
