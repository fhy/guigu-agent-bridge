use std::{
    ffi::{CStr, CString},
    path::Path,
    time::{Duration, Instant},
};

use libsqlite3_sys as sqlite;

pub(super) struct ReadOnly {
    handle: *mut sqlite::sqlite3,
    deadline: Box<Instant>,
    _exclusive: Option<super::discovery_snapshot::Exclusive>,
}

impl ReadOnly {
    pub fn open(path: &Path) -> Result<Self, ()> {
        let exclusive = super::discovery_snapshot::Exclusive::acquire(path).ok();
        let mut uri = url::Url::from_file_path(path).map_err(|_| ())?;
        uri.query_pairs_mut()
            .append_pair("mode", "ro")
            .append_pair("readonly_shm", "1");
        if exclusive.is_some() {
            uri.query_pairs_mut().append_pair("immutable", "1");
        }
        let uri = CString::new(uri.as_str()).map_err(|_| ())?;
        let vfs = super::discovery_vfs::name()?;
        let mut handle = std::ptr::null_mut();
        let flags = sqlite::SQLITE_OPEN_READONLY
            | sqlite::SQLITE_OPEN_URI
            | sqlite::SQLITE_OPEN_NOFOLLOW
            | sqlite::SQLITE_OPEN_NOMUTEX;
        let status =
            unsafe { sqlite::sqlite3_open_v2(uri.as_ptr(), &mut handle, flags, vfs.as_ptr()) };
        let mut connection = Self {
            handle,
            deadline: Box::new(Instant::now() + Duration::from_secs(5)),
            _exclusive: exclusive,
        };
        if status != sqlite::SQLITE_OK || handle.is_null() {
            return Err(());
        }
        unsafe {
            sqlite::sqlite3_busy_timeout(handle, 5000);
            sqlite::sqlite3_progress_handler(
                handle,
                1000,
                Some(interrupt),
                (&mut *connection.deadline as *mut Instant).cast(),
            );
        }
        connection.rows(c"PRAGMA query_only=ON")?;
        connection.rows(c"BEGIN")?;
        Ok(connection)
    }

    pub fn rows(&self, sql: &CStr) -> Result<Vec<Vec<String>>, ()> {
        let mut statement = std::ptr::null_mut();
        let status = unsafe {
            sqlite::sqlite3_prepare_v2(
                self.handle,
                sql.as_ptr(),
                -1,
                &mut statement,
                std::ptr::null_mut(),
            )
        };
        if status != sqlite::SQLITE_OK {
            return Err(());
        }
        let result = self.read(statement);
        unsafe {
            sqlite::sqlite3_finalize(statement);
        }
        result
    }

    fn read(&self, statement: *mut sqlite::sqlite3_stmt) -> Result<Vec<Vec<String>>, ()> {
        let mut rows = Vec::new();
        loop {
            if Instant::now() >= *self.deadline || rows.len() >= 10000 {
                return Err(());
            }
            let status = unsafe { sqlite::sqlite3_step(statement) };
            if status == sqlite::SQLITE_DONE {
                return Ok(rows);
            }
            if status != sqlite::SQLITE_ROW {
                return Err(());
            }
            let count = unsafe { sqlite::sqlite3_column_count(statement) };
            let mut row = Vec::new();
            for index in 0..count {
                let pointer = unsafe { sqlite::sqlite3_column_text(statement, index) };
                let length = unsafe { sqlite::sqlite3_column_bytes(statement, index) };
                if pointer.is_null() || !(0..=65536).contains(&length) {
                    return Err(());
                }
                let bytes = unsafe { std::slice::from_raw_parts(pointer, length as usize) };
                row.push(std::str::from_utf8(bytes).map_err(|_| ())?.to_owned());
            }
            rows.push(row);
        }
    }
}

unsafe extern "C" fn interrupt(pointer: *mut std::ffi::c_void) -> i32 {
    let deadline = unsafe { &*pointer.cast::<Instant>() };
    i32::from(Instant::now() >= *deadline)
}

impl Drop for ReadOnly {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                sqlite::sqlite3_close(self.handle);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    fn snapshot(root: &Path) -> std::collections::BTreeMap<std::ffi::OsString, Vec<u8>> {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), std::fs::read(entry.path()).unwrap())
            })
            .collect()
    }

    #[tokio::test]
    async fn closed_wal_reads_without_creating_files_and_releases_exclusive_lock() {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("state.db");
        let pool = crate::storage::connect(&database).await.unwrap();
        sqlx::query("CREATE TABLE probe(value INTEGER)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO probe VALUES(7)")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let before = snapshot(root.path());
        let reader = ReadOnly::open(&database).unwrap();
        assert!(reader._exclusive.is_some());
        assert_eq!(
            reader
                .rows(c"SELECT CAST(value AS TEXT) FROM probe")
                .unwrap(),
            vec![vec!["7".to_owned()]]
        );
        assert!(super::super::discovery_snapshot::Exclusive::acquire(&database).is_err());
        assert_eq!(before, snapshot(root.path()));
        drop(reader);
        assert!(super::super::discovery_snapshot::Exclusive::acquire(&database).is_ok());
        assert_eq!(before, snapshot(root.path()));
    }

    #[tokio::test]
    async fn existing_wal_is_never_replaced_by_an_immutable_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("state.db");
        let mut writer = sqlx::SqliteConnection::connect_with(
            &sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .create_if_missing(true)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal),
        )
        .await
        .unwrap();
        sqlx::query("CREATE TABLE probe(value INTEGER)")
            .execute(&mut writer)
            .await
            .unwrap();
        sqlx::query("INSERT INTO probe VALUES(9)")
            .execute(&mut writer)
            .await
            .unwrap();
        let before = snapshot(root.path());
        let reader = ReadOnly::open(&database).unwrap();
        assert!(reader._exclusive.is_none());
        assert_eq!(
            reader
                .rows(c"SELECT CAST(value AS TEXT) FROM probe")
                .unwrap(),
            vec![vec!["9".to_owned()]]
        );
        drop(reader);
        assert_eq!(before, snapshot(root.path()));
        writer.close().await.unwrap();
    }
}
