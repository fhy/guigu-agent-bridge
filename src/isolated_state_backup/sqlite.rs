use std::{
    ffi::CString,
    os::fd::AsRawFd,
    path::Path,
    str::FromStr,
    time::{Duration, Instant},
};

use libsqlite3_sys as ffi;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

use super::MAX_FILE;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqliteFacts {
    pub version: u32,
    pub page_size: u32,
    pub page_count: u32,
    pub user_version: u32,
    pub schema_hash: [u8; 32],
}

pub(crate) async fn online_backup(
    source: &Path,
    destination: &std::fs::File,
) -> Result<SqliteFacts, ()> {
    let options = SqliteConnectOptions::from_str(source.to_str().ok_or(())?)
        .map_err(|_| ())?
        .read_only(true)
        .create_if_missing(false)
        .shared_cache(false);
    let mut source = SqliteConnection::connect_with(&options)
        .await
        .map_err(|_| ())?;
    sqlx::query("BEGIN")
        .execute(&mut source)
        .await
        .map_err(|_| ())?;
    let facts = facts(&mut source).await?;
    let mut handle = source.lock_handle().await.map_err(|_| ())?;
    let raw_source = handle.as_raw_handle().as_ptr();
    let destination_name =
        CString::new(format!("/proc/self/fd/{}", destination.as_raw_fd())).map_err(|_| ())?;
    let mut raw_destination = std::ptr::null_mut();
    let opened = unsafe {
        ffi::sqlite3_open_v2(
            destination_name.as_ptr(),
            &mut raw_destination,
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_URI | ffi::SQLITE_OPEN_NOMUTEX,
            std::ptr::null(),
        )
    };
    if opened != ffi::SQLITE_OK || raw_destination.is_null() {
        if !raw_destination.is_null() {
            unsafe {
                ffi::sqlite3_close(raw_destination);
            }
        }
        return Err(());
    }
    let copied =
        backup_raw(raw_source, raw_destination).and_then(|()| journal_delete(raw_destination));
    let closed = unsafe { ffi::sqlite3_close(raw_destination) };
    drop(handle);
    let rolled_back = sqlx::query("ROLLBACK").execute(&mut source).await;
    source.close().await.map_err(|_| ())?;
    if copied.is_err() || closed != ffi::SQLITE_OK || rolled_back.is_err() {
        return Err(());
    }
    Ok(facts)
}

fn journal_delete(database: *mut ffi::sqlite3) -> Result<(), ()> {
    let mut error = std::ptr::null_mut();
    let result = unsafe {
        ffi::sqlite3_exec(
            database,
            c"PRAGMA journal_mode=DELETE".as_ptr(),
            None,
            std::ptr::null_mut(),
            &mut error,
        )
    };
    if !error.is_null() {
        unsafe {
            ffi::sqlite3_free(error.cast());
        }
    }
    if result == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(())
    }
}

fn backup_raw(source: *mut ffi::sqlite3, destination: *mut ffi::sqlite3) -> Result<(), ()> {
    let main = c"main";
    let backup =
        unsafe { ffi::sqlite3_backup_init(destination, main.as_ptr(), source, main.as_ptr()) };
    if backup.is_null() {
        return Err(());
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut outcome = Err(());
    loop {
        let result = unsafe { ffi::sqlite3_backup_step(backup, 256) };
        if result == ffi::SQLITE_DONE {
            let pages = unsafe { ffi::sqlite3_backup_pagecount(backup) };
            let remaining = unsafe { ffi::sqlite3_backup_remaining(backup) };
            if pages >= 0 && remaining == 0 {
                outcome = Ok(());
            }
            break;
        }
        if result != ffi::SQLITE_OK || Instant::now() >= deadline {
            break;
        }
    }
    let finish = unsafe { ffi::sqlite3_backup_finish(backup) };
    if finish == ffi::SQLITE_OK {
        outcome
    } else {
        Err(())
    }
}

pub(crate) async fn verify(path: &Path) -> Result<SqliteFacts, ()> {
    let metadata = std::fs::metadata(path).map_err(|_| ())?;
    if !metadata.is_file() || metadata.len() > MAX_FILE {
        return Err(());
    }
    let options = SqliteConnectOptions::from_str(path.to_str().ok_or(())?)
        .map_err(|_| ())?
        .read_only(true)
        .create_if_missing(false)
        .shared_cache(false);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .map_err(|_| ())?;
    let check: String = sqlx::query_scalar("PRAGMA quick_check")
        .fetch_one(&mut connection)
        .await
        .map_err(|_| ())?;
    if check != "ok" {
        return Err(());
    }
    let facts = facts(&mut connection).await?;
    connection.close().await.map_err(|_| ())?;
    let bytes = u64::from(facts.page_size)
        .checked_mul(u64::from(facts.page_count))
        .ok_or(())?;
    if bytes > MAX_FILE || bytes > metadata.len() {
        return Err(());
    }
    Ok(facts)
}

async fn facts(connection: &mut SqliteConnection) -> Result<SqliteFacts, ()> {
    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| ())?;
    let page_count: i64 = sqlx::query_scalar("PRAGMA page_count")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| ())?;
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| ())?;
    if page_size <= 0 || page_count < 0 || user_version < 0 {
        return Err(());
    }
    let rows = sqlx::query("SELECT type,name,tbl_name,coalesce(sql,'') FROM sqlite_schema ORDER BY type,name,tbl_name,sql")
        .fetch_all(&mut *connection).await.map_err(|_| ())?;
    let mut schema = Sha256::new();
    for row in rows {
        for index in 0..4 {
            let value: String = row.try_get(index).map_err(|_| ())?;
            let length = u32::try_from(value.len()).map_err(|_| ())?;
            schema.update(length.to_be_bytes());
            schema.update(value.as_bytes());
        }
    }
    Ok(SqliteFacts {
        version: unsafe { ffi::sqlite3_libversion_number() as u32 },
        page_size: u32::try_from(page_size).map_err(|_| ())?,
        page_count: u32::try_from(page_count).map_err(|_| ())?,
        user_version: u32::try_from(user_version).map_err(|_| ())?,
        schema_hash: schema.finalize().into(),
    })
}
