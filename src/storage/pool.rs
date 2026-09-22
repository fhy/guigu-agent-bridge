//! SQLite pool configuration and the embedded-migration entry points.
//!
//! [`connect`] and [`migrate`] are the two steps the assembly layer (T017) and
//! the tests drive explicitly. They are separate on purpose: the caller can
//! distinguish "the database could not be opened" from "the schema could not be
//! applied", which a combined helper would hide.
//!
//! Connection options are set per connection rather than inherited from driver
//! defaults, so a driver upgrade cannot silently change durability or locking
//! semantics (analysis §4.8, design gate D8):
//!
//! | option | value | why |
//! |--------|-------|-----|
//! | `journal_mode` | `WAL` | one writer with concurrent readers |
//! | `foreign_keys` | `ON` | referential integrity is enforced by the database |
//! | `busy_timeout` | 5 s | writer contention is queued, not failed |
//! | `synchronous` | `FULL` | the event store is the source of truth: fsync per commit |
//! | `create_if_missing` | `true` | a fresh deployment creates the file |
//! | pool `max_connections` | 5 | WAL serves read concurrency; SQLite has one writer |
//!
//! Parent directories are **not** created: a missing directory is reported as
//! [`StorageError::Open`] rather than silently materialised.
//!
//! `sqlx::migrate!("./migrations")` embeds the migration files at compile time.
//! Nothing is read from disk at runtime, and editing an already-applied file is
//! rejected by the recorded checksum.

use std::path::Path;
use std::time::Duration;

use sqlx::SqlitePool;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

use crate::storage::BusinessStoreOwner;
use crate::storage::StorageError;

pub fn connect_owner(path: impl AsRef<Path>) -> Result<BusinessStoreOwner, StorageError> {
    BusinessStoreOwner::open(path)
}

pub fn migrate_owner(owner: &BusinessStoreOwner) -> Result<(), StorageError> {
    owner.execute(|connection| {
        connection
            .execute_batch(include_str!("../../migrations/0001_init.sql"))
            .map_err(|error| StorageError::OwnerQuery(error.to_string()))?;
        connection
            .execute_batch(include_str!("../../migrations/0002_sessions.sql"))
            .map_err(|error| StorageError::OwnerQuery(error.to_string()))
    })
}

/// How long a writer waits for the SQLite write lock before failing.
///
/// Every write in this project is a single statement or a short transaction, so
/// a held lock is measured in microseconds to milliseconds and this value is an
/// abnormal-path backstop rather than a normal waiting path. It matches the sqlx
/// default, so no new semantics are introduced.
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of pooled connections.
///
/// WAL allows exactly one writer, so extra connections buy read concurrency
/// only. Five keeps the sqlx worker-thread count small; T016 can make it
/// configurable if it proves insufficient.
pub const MAX_CONNECTIONS: u32 = 5;

/// The migrations embedded from `migrations/` at compile time.
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Open (creating if needed) a SQLite pool for `path` with the frozen options.
///
/// Does **not** apply migrations; call [`migrate`] afterwards. The parent
/// directory must already exist — otherwise the result is
/// [`StorageError::Open`].
///
/// # Errors
///
/// [`StorageError::Open`] when the database file cannot be opened or created,
/// including when its parent directory does not exist.
pub async fn connect(path: impl AsRef<Path>) -> Result<SqlitePool, StorageError> {
    let path = path.as_ref();
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(DEFAULT_BUSY_TIMEOUT)
        .synchronous(SqliteSynchronous::Full);

    SqlitePoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .connect_with(options)
        .await
        .map_err(|source| StorageError::Open {
            path: path.to_path_buf(),
            source,
        })
}

/// Apply the embedded migrations to `pool`.
///
/// Idempotent: already-applied versions are skipped, so calling this on every
/// startup (or twice in a row) is a no-op. Each migration file runs inside a
/// single transaction, so a multi-statement migration either lands completely or
/// not at all.
///
/// # Errors
///
/// [`StorageError::Migrate`] when applying or verifying a migration fails —
/// notably a checksum mismatch on an already-applied file
/// (`MigrateError::VersionMismatch`), which is how migration immutability is
/// enforced rather than merely documented.
pub async fn migrate(pool: &SqlitePool) -> Result<(), StorageError> {
    MIGRATOR.run(pool).await.map_err(StorageError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_options_are_frozen() {
        // Pins the values declared in the design gate (D8): a change here is a
        // durability/locking change and must be re-confirmed by Coordinator.
        assert_eq!(DEFAULT_BUSY_TIMEOUT, Duration::from_secs(5));
        assert_eq!(MAX_CONNECTIONS, 5);
    }

    #[tokio::test]
    async fn connect_does_not_create_a_missing_parent_directory() {
        let mut path = std::env::temp_dir();
        path.push(format!("guigu-missing-parent-{}", uuid::Uuid::now_v7()));
        path.push("state.db");
        assert!(!path.parent().expect("parent").exists());

        let error = connect(&path).await.expect_err("open must fail");
        assert!(matches!(error, StorageError::Open { .. }), "{error:?}");
        assert!(!path.parent().expect("parent").exists());
    }

    #[test]
    fn owner_migration_bootstrap_applies_both_schema_files() {
        let path =
            std::env::temp_dir().join(format!("guigu-owner-migrate-{}.db", uuid::Uuid::now_v7()));
        let owner = connect_owner(&path).expect("owner");
        migrate_owner(&owner).expect("migrations");
        let table_count = owner
            .execute(|connection| {
                connection
                    .query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('tasks','sessions')", [], |row| row.get::<_, i64>(0))
                    .map_err(|error| StorageError::OwnerQuery(error.to_string()))
            })
            .expect("schema query");
        assert_eq!(table_count, 2);
    }
}
