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
    path: PathBuf,
    sender: Option<SyncSender<Command>>,
    thread: Option<JoinHandle<()>>,
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
            path,
            sender: Some(sender),
            thread: Some(thread),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn execute<R: Send + 'static>(
        &self,
        command: impl FnOnce(&mut Connection) -> Result<R, StorageError> + Send + 'static,
    ) -> Result<R, StorageError> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| StorageError::Owner("owner closed".into()))?;
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

impl Drop for BusinessStoreOwner {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn configure(connection: &Connection) {
    let _ = connection.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000;",
    );
}
