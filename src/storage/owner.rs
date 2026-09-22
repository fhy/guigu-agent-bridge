//! Single-threaded business SQLite owner.
//!
//! Commands are bounded and FIFO. A command closure owns the connection (and
//! any transaction it opens) for its entire synchronous execution; no SQLite
//! handle crosses an async boundary or queue message.

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};

use rusqlite::Connection;

use super::StorageError;

const QUEUE_CAPACITY: usize = 64;

type Command = Box<dyn FnOnce(&mut Connection) + Send + 'static>;

pub struct BusinessStoreOwner {
    state: std::sync::Arc<OwnerState>,
}

pub type BusinessStore = BusinessStoreOwner;

struct OwnerState {
    path: PathBuf,
    sender: SyncSender<Command>,
    thread: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl BusinessStoreOwner {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref().to_path_buf();
        let (sender, receiver) = mpsc::sync_channel::<Command>(QUEUE_CAPACITY);
        let thread_path = path.clone();
        let thread = thread::Builder::new()
            .name("business-sqlite-owner".into())
            .spawn(move || {
                let Ok(mut connection) = Connection::open(thread_path) else {
                    return;
                };
                configure(&connection);
                while let Ok(command) = receiver.recv() {
                    command(&mut connection);
                }
            })
            .map_err(|error| StorageError::Owner(error.to_string()))?;
        Ok(Self {
            state: std::sync::Arc::new(OwnerState {
                path,
                sender,
                thread: std::sync::Mutex::new(Some(thread)),
            }),
        })
    }

    pub fn facade(&self) -> BusinessStore {
        BusinessStore {
            state: self.state.clone(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.state.path
    }

    pub fn transaction<R: Send + 'static>(
        &self,
        command: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<R, StorageError> + Send + 'static,
    ) -> Result<R, StorageError> {
        self.execute(move |connection| {
            let transaction = connection.transaction().map_err(map_sqlite_error)?;
            let result = command(&transaction)?;
            transaction.commit().map_err(map_sqlite_error)?;
            Ok(result)
        })
    }

    pub fn execute<R: Send + 'static>(
        &self,
        command: impl FnOnce(&mut Connection) -> Result<R, StorageError> + Send + 'static,
    ) -> Result<R, StorageError> {
        let sender = &self.state.sender;
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        sender
            .send(Box::new(move |connection| {
                let _ = result_tx.send(command(connection));
            }))
            .map_err(|_| StorageError::Owner("owner command queue closed".into()))?;
        result_rx
            .recv()
            .map_err(|_| StorageError::Owner("owner command result dropped".into()))?
    }
}

impl Drop for OwnerState {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.get_mut().expect("owner thread mutex").take() {
            let _ = thread.join();
        }
    }
}

fn configure(connection: &Connection) {
    let _ = connection.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000;",
    );
}

fn map_sqlite_error(error: rusqlite::Error) -> StorageError {
    StorageError::OwnerQuery(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_owner_keeps_transaction_atomic() {
        let path = std::env::temp_dir().join(format!("guigu-owner-{}.db", uuid::Uuid::now_v7()));
        let owner = BusinessStoreOwner::open(&path).expect("owner");
        owner
            .execute(|connection| {
                connection
                    .execute_batch(
                        "CREATE TABLE facts (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                    )
                    .map_err(map_sqlite_error)
            })
            .expect("schema");
        owner
            .transaction(|transaction| {
                transaction
                    .execute("INSERT INTO facts (id, value) VALUES (1, ?1)", ["ok"])
                    .map_err(map_sqlite_error)?;
                Ok(())
            })
            .expect("commit");
        let value = owner
            .execute(|connection| {
                connection
                    .query_row("SELECT value FROM facts WHERE id=1", [], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(map_sqlite_error)
            })
            .expect("read");
        assert_eq!(value, "ok");
    }

    #[test]
    fn owner_repository_query_uses_fifo_owner() {
        let path =
            std::env::temp_dir().join(format!("guigu-owner-agent-{}.db", uuid::Uuid::now_v7()));
        let owner = BusinessStoreOwner::open(&path).expect("owner");
        owner
            .execute(|connection| {
                connection
                    .execute_batch("CREATE TABLE agents (agent_id TEXT PRIMARY KEY)")
                    .map_err(map_sqlite_error)?;
                connection
                    .execute("INSERT INTO agents(agent_id) VALUES (?1)", ["agent-a"])
                    .map_err(map_sqlite_error)
            })
            .expect("seed");
        let repository = crate::storage::OwnerRepository::new(owner);
        assert!(repository.agent_exists("agent-a").expect("query"));
        assert!(!repository.agent_exists("missing").expect("query"));
    }

    #[test]
    fn owner_repository_write_rolls_back_on_constraint_error() {
        let path =
            std::env::temp_dir().join(format!("guigu-owner-write-{}.db", uuid::Uuid::now_v7()));
        let owner = BusinessStoreOwner::open(&path).expect("owner");
        owner
            .execute(|connection| {
                connection
                    .execute_batch("CREATE TABLE agents (endpoint_id TEXT PRIMARY KEY, agent_id TEXT UNIQUE NOT NULL, transport TEXT NOT NULL, enabled INTEGER NOT NULL, address_json TEXT, capabilities_json TEXT NOT NULL)")
                    .map_err(map_sqlite_error)
            })
            .expect("schema");
        let repository = crate::storage::OwnerRepository::new(owner);
        repository
            .insert_agent_raw("endpoint", "agent", "acp", true)
            .expect("insert");
        assert!(
            repository
                .insert_agent_raw("endpoint", "other", "acp", true)
                .is_err()
        );
        assert!(repository.agent_exists("agent").expect("original row"));
        assert!(!repository.agent_exists("other").expect("rollback row"));
    }

    #[test]
    fn event_owner_boundary_preserves_order_and_duplicate_facts() {
        let path =
            std::env::temp_dir().join(format!("guigu-owner-events-{}.db", uuid::Uuid::now_v7()));
        let owner = BusinessStoreOwner::open(&path).expect("owner");
        owner.execute(|connection| {
            connection.execute_batch("CREATE TABLE task_events(event_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, seq INTEGER NOT NULL, status TEXT NOT NULL, timestamp TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(task_id,seq))").map_err(map_sqlite_error)
        }).expect("schema");
        let insert = |id: &str, seq: i64, payload: &str| {
            owner.transaction(move |tx| {
            tx.execute("INSERT INTO task_events VALUES (?1,'task',?2,'queued','2026-01-01T00:00:00Z',?3)", rusqlite::params![id, seq, payload]).map_err(map_sqlite_error).map(|_| ())
        })
        };
        insert("e2", 2, "{\"ok\":true}").expect("insert 2");
        insert("e1", 1, "{\"ok\":true}").expect("insert 1");
        assert!(insert("e1", 1, "{\"ok\":true}").is_err());
        assert!(insert("e1", 1, "{\"different\":true}").is_err());
        let order: Vec<i64> = owner
            .execute(|connection| {
                let mut statement = connection
                    .prepare("SELECT seq FROM task_events WHERE task_id='task' ORDER BY seq ASC")
                    .map_err(map_sqlite_error)?;
                let rows = statement
                    .query_map([], |row| row.get(0))
                    .map_err(map_sqlite_error)?;
                rows.collect::<Result<Vec<i64>, _>>()
                    .map_err(map_sqlite_error)
            })
            .expect("order");
        assert_eq!(order, vec![1, 2]);
    }

    #[test]
    fn malformed_event_payload_is_fail_closed() {
        let error = crate::storage::codec::decode_json::<serde_json::Value>(
            "{not-json",
            "task_events.payload",
        )
        .expect_err("invalid payload must fail");
        assert!(matches!(
            error,
            StorageError::Malformed {
                field: "task_events.payload",
                ..
            }
        ));
    }

    #[test]
    fn delivery_owner_facts_are_ordered_and_acknowledgement_is_idempotent() {
        let path =
            std::env::temp_dir().join(format!("guigu-owner-delivery-{}.db", uuid::Uuid::now_v7()));
        let owner = BusinessStoreOwner::open(&path).expect("owner");
        owner.execute(|connection| {
            connection.execute_batch("CREATE TABLE deliveries(delivery_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, attempt INTEGER NOT NULL, target_endpoint_id TEXT NOT NULL, dispatched_at TEXT NOT NULL, acknowledged_at TEXT)").map_err(map_sqlite_error)
        }).expect("schema");
        owner.transaction(|tx| {
            tx.execute("INSERT INTO deliveries VALUES ('d2','t',2,'a','2026-01-01T00:00:02Z',NULL),('d1','t',1,'a','2026-01-01T00:00:01Z',NULL)", []).map_err(map_sqlite_error).map(|_| ())
        }).expect("seed");
        let ids: Vec<String> = owner.execute(|connection| {
            let mut statement = connection.prepare("SELECT delivery_id FROM deliveries WHERE acknowledged_at IS NULL ORDER BY dispatched_at ASC, delivery_id ASC").map_err(map_sqlite_error)?;
            let rows = statement.query_map([], |row| row.get(0)).map_err(map_sqlite_error)?;
            rows.collect::<Result<Vec<String>, _>>().map_err(map_sqlite_error)
        }).expect("list");
        assert_eq!(ids, vec!["d1", "d2"]);
        owner.transaction(|tx| tx.execute("UPDATE deliveries SET acknowledged_at='2026-01-01T00:00:03Z' WHERE delivery_id='d1'", []).map_err(map_sqlite_error).map(|_| ())).expect("ack");
        owner.transaction(|tx| tx.execute("UPDATE deliveries SET acknowledged_at='2026-01-01T00:00:04Z' WHERE delivery_id='d1' AND acknowledged_at IS NULL", []).map_err(map_sqlite_error).map(|_| ())).expect("idempotent ack");
    }
}
