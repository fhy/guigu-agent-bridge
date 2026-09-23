//! Storage error type: the failure surface of [`crate::storage`].
//!
//! `StorageError` is deliberately **independent of [`crate::Error`]**. The
//! top-level `Error` is the application entry-point boundary (`main`/`run`);
//! nothing on that path touches storage yet (wiring is T017), so adding a
//! `Storage` variant now would only create an unreachable branch. Keeping the
//! types separate also keeps `sqlx` out of the application error contract and
//! lets T009's event consumer map storage failures to
//! `ConsumerError::Failed { reason }` explicitly, where that mapping is visible.
//!
//! # Rendering contract
//!
//! No [`std::fmt::Display`] of a `StorageError` may contain SQL text, bound
//! values, prompt/message bodies, or credentials. sqlx's SQLite constraint
//! messages name tables and columns only (`UNIQUE constraint failed:
//! task_events.task_id, task_events.seq`) and never carry the statement or its
//! parameters, and [`crate::storage::codec`] never echoes the offending stored
//! value. This is verified by tests.

use std::path::PathBuf;

use sqlx::error::ErrorKind;
use thiserror::Error;

/// A failure raised by the storage layer.
///
/// `#[non_exhaustive]`: classification may be refined as T009 lands without
/// breaking downstream `match` arms (same precedent as `ConsumerError`).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StorageError {
    /// The database could not be opened or created.
    ///
    /// Includes a missing parent directory: `connect` never creates parent
    /// directories, so deployment must create them (T017).
    #[error("failed to open the database at {path}: {source}")]
    Open {
        /// The requested database path.
        path: PathBuf,
        /// The driver-level open failure.
        #[source]
        source: sqlx::Error,
    },
    /// Applying the embedded migrations failed, including a checksum mismatch on
    /// an already-applied migration (`MigrateError::VersionMismatch`) or a
    /// concurrent writer racing the migration.
    #[error("database migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    /// Any other driver or query failure that the constraint classification below
    /// does not claim.
    #[error("database query failed: {0}")]
    Query(#[source] sqlx::Error),
    /// A unique/primary-key constraint was violated.
    ///
    /// This is the idempotency signal: a replayed `event_id` or a replayed
    /// `(task_id, seq)` is a duplicate, not a corruption. The policy (ignore,
    /// tolerate, or propagate) belongs to T009.
    #[error("duplicate key: {detail}")]
    Duplicate {
        /// Constraint message naming the offending table/columns.
        detail: String,
    },
    /// A foreign key, `NOT NULL`, `CHECK`, or `STRICT` type constraint was
    /// violated.
    #[error("integrity constraint violated: {detail}")]
    IntegrityViolation {
        /// Constraint message naming the offending table/columns.
        detail: String,
    },
    /// The requested row does not exist (for example acknowledging an unknown
    /// delivery).
    #[error("{entity} not found: {id}")]
    NotFound {
        /// Logical entity name, e.g. `"delivery"`.
        entity: &'static str,
        /// The identifier that was looked up.
        id: String,
    },
    /// A stored integer cannot be represented by the model's type.
    ///
    /// SQLite `INTEGER` is signed 64-bit while `seq`/`version` are `u64`; values
    /// outside the representable range are reported instead of truncated or
    /// panicking.
    #[error("{field} out of range: {value}")]
    OutOfRange {
        /// Column the value came from.
        field: &'static str,
        /// The offending value, as stored.
        value: String,
    },
    /// A stored value cannot be decoded into its model type (unknown enum tag,
    /// invalid JSON, unparsable timestamp or identifier).
    ///
    /// Fail-fast: an unrecognised stored value is never mapped to a default.
    #[error("malformed stored value for {field}: {detail}")]
    Malformed {
        /// Column the value came from.
        field: &'static str,
        /// Reason the value could not be decoded, without the stored value.
        detail: String,
    },
}

impl From<sqlx::Error> for StorageError {
    /// Classify a driver error by its constraint kind.
    ///
    /// `UniqueViolation` becomes [`StorageError::Duplicate`] (the idempotency
    /// signal) and the foreign-key / `NOT NULL` / `CHECK` kinds become
    /// [`StorageError::IntegrityViolation`]; everything else stays
    /// [`StorageError::Query`]. Only the constraint message (table and column
    /// names) is carried over, never SQL text or bound values.
    fn from(error: sqlx::Error) -> Self {
        if let sqlx::Error::Database(database) = &error {
            let detail = database.message().to_string();
            match database.kind() {
                ErrorKind::UniqueViolation => return Self::Duplicate { detail },
                ErrorKind::ForeignKeyViolation
                | ErrorKind::NotNullViolation
                | ErrorKind::CheckViolation => {
                    return Self::IntegrityViolation { detail };
                }
                _ => {}
            }
        }
        Self::Query(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage;
    use std::sync::Arc;

    fn temp_db_path(tag: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "guigu-storage-error-{tag}-{}.db",
            uuid::Uuid::now_v7()
        ));
        path
    }

    async fn migrated_pool(tag: &str) -> (sqlx::SqlitePool, PathBuf) {
        let path = temp_db_path(tag);
        let pool = storage::connect(&path).await.expect("connect");
        storage::migrate(&pool).await.expect("migrate");
        (pool, path)
    }

    async fn seed_task(pool: &sqlx::SqlitePool) -> String {
        let conversation_id = crate::models::ConversationId::generate().to_string();
        sqlx::query(
            "INSERT INTO conversations (conversation_id, participants_json) VALUES (?, '[]')",
        )
        .bind(&conversation_id)
        .execute(pool)
        .await
        .expect("insert conversation");

        let task_id = crate::models::TaskId::generate().to_string();
        sqlx::query(
            "INSERT INTO tasks (task_id, root_task_id, from_agent, to_agent, conversation_id, \
             text, priority, depth, hops, version) \
             VALUES (?, ?, 'from', 'to', ?, 'body', 5, 0, 0, 0)",
        )
        .bind(&task_id)
        .bind(&task_id)
        .bind(&conversation_id)
        .execute(pool)
        .await
        .expect("insert task");
        task_id
    }

    #[test]
    fn storage_error_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StorageError>();
    }

    #[test]
    fn every_variant_renders_a_stable_shape() {
        let cases = [
            StorageError::Query(sqlx::Error::RowNotFound).to_string(),
            StorageError::Duplicate {
                detail: "UNIQUE constraint failed: task_events.seq".into(),
            }
            .to_string(),
            StorageError::IntegrityViolation {
                detail: "FOREIGN KEY constraint failed".into(),
            }
            .to_string(),
            StorageError::NotFound {
                entity: "delivery",
                id: "d1".into(),
            }
            .to_string(),
            StorageError::OutOfRange {
                field: "task_events.seq",
                value: "18446744073709551615".into(),
            }
            .to_string(),
            StorageError::Malformed {
                field: "task_events.status",
                detail: "unknown value".into(),
            }
            .to_string(),
        ];
        for rendered in cases {
            assert!(!rendered.is_empty());
        }

        let open = StorageError::Open {
            path: PathBuf::from("/tmp/x.db"),
            source: sqlx::Error::RowNotFound,
        };
        assert!(open.to_string().contains("/tmp/x.db"));
    }

    #[tokio::test]
    async fn a_unique_conflict_is_classified_as_duplicate() {
        let (pool, path) = migrated_pool("duplicate").await;
        let task_id = seed_task(&pool).await;
        let insert = "INSERT INTO task_events (event_id, task_id, seq, status, timestamp, payload) \
                      VALUES (?, ?, ?, 'queued', '2026-09-16T00:00:00.000000000Z', '\"queued\"')";

        sqlx::query(insert)
            .bind(crate::models::EventId::generate().to_string())
            .bind(&task_id)
            .bind(1_i64)
            .execute(&pool)
            .await
            .expect("first event");

        // Same (task_id, seq): the per-task ordering guard.
        let error = sqlx::query(insert)
            .bind(crate::models::EventId::generate().to_string())
            .bind(&task_id)
            .bind(1_i64)
            .execute(&pool)
            .await
            .map_err(StorageError::from)
            .expect_err("duplicate seq must fail");
        assert!(matches!(error, StorageError::Duplicate { .. }), "{error:?}");

        // Same event_id, different seq: the global dedup guard.
        let event_id = crate::models::EventId::generate().to_string();
        sqlx::query(insert)
            .bind(&event_id)
            .bind(&task_id)
            .bind(2_i64)
            .execute(&pool)
            .await
            .expect("second event");
        let error = sqlx::query(insert)
            .bind(&event_id)
            .bind(&task_id)
            .bind(3_i64)
            .execute(&pool)
            .await
            .map_err(StorageError::from)
            .expect_err("duplicate event id must fail");
        assert!(matches!(error, StorageError::Duplicate { .. }), "{error:?}");

        pool.close().await;
        remove_db_files(&path);
    }

    #[tokio::test]
    async fn foreign_key_and_check_conflicts_are_integrity_violations() {
        let (pool, path) = migrated_pool("integrity").await;

        // Unknown parent conversation.
        let error = sqlx::query(
            "INSERT INTO messages (message_id, conversation_id, sender, recipient, body, metadata_json) \
             VALUES (?, ?, 's', 'r', 'body', '{}')",
        )
        .bind(crate::models::MessageId::generate().to_string())
        .bind(crate::models::ConversationId::generate().to_string())
        .execute(&pool)
        .await
        .map_err(StorageError::from)
        .expect_err("unknown conversation must fail");
        assert!(
            matches!(error, StorageError::IntegrityViolation { .. }),
            "{error:?}"
        );

        // CHECK violation: priority outside 0..=10.
        let task_id = seed_task(&pool).await;
        let conversation_id = crate::models::ConversationId::generate().to_string();
        sqlx::query(
            "INSERT INTO conversations (conversation_id, participants_json) VALUES (?, '[]')",
        )
        .bind(&conversation_id)
        .execute(&pool)
        .await
        .expect("insert conversation");
        let error = sqlx::query(
            "INSERT INTO tasks (task_id, root_task_id, from_agent, to_agent, conversation_id, \
             text, priority, depth, hops, version) VALUES (?, ?, 'f', 't', ?, 'x', 99, 0, 0, 0)",
        )
        .bind(crate::models::TaskId::generate().to_string())
        .bind(&task_id)
        .bind(&conversation_id)
        .execute(&pool)
        .await
        .map_err(StorageError::from)
        .expect_err("priority 99 must fail");
        assert!(
            matches!(error, StorageError::IntegrityViolation { .. }),
            "{error:?}"
        );

        pool.close().await;
        remove_db_files(&path);
    }

    #[tokio::test]
    async fn rendered_errors_do_not_leak_planted_content() {
        let (pool, path) = migrated_pool("leak").await;
        let task_id = seed_task(&pool).await;
        const PLANTED: &str = "SECRET-BODY-DO-NOT-LEAK";

        let error = sqlx::query(
            "INSERT INTO task_events (event_id, task_id, seq, status, timestamp, payload) \
             VALUES (?, ?, 1, ?, '2026-09-16T00:00:00.000000000Z', ?)",
        )
        .bind(crate::models::EventId::generate().to_string())
        .bind(&task_id)
        .bind(PLANTED)
        .bind(format!("\"{PLANTED}\""))
        .execute(&pool)
        .await
        .map_err(StorageError::from)
        .expect_err("unknown status must fail");

        let rendered = error.to_string();
        assert!(!rendered.contains(PLANTED), "{rendered}");
        assert!(
            !rendered.to_lowercase().contains("insert into"),
            "{rendered}"
        );
        assert!(
            !rendered.contains("task_events (event_id"),
            "SQL text leaked: {rendered}"
        );

        pool.close().await;
        remove_db_files(&path);
    }

    #[test]
    fn a_shared_storage_error_survives_threads() {
        let error = Arc::new(StorageError::Query(sqlx::Error::RowNotFound));
        let clone = Arc::clone(&error);
        let handle = std::thread::spawn(move || clone.to_string());
        assert!(!handle.join().expect("thread").is_empty());
        assert!(!error.to_string().is_empty());
    }

    fn remove_db_files(path: &std::path::Path) {
        for suffix in ["", "-wal", "-shm"] {
            let mut candidate = path.as_os_str().to_owned();
            candidate.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(candidate));
        }
    }
}
