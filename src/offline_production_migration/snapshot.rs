use super::{
    Failure, Request,
    evidence::hash_file,
    records::{self, Backup, Operation},
    verification::{self, Facts},
};
use libsqlite3_sys as ffi;
use sqlx::{Connection, SqliteConnection};
use std::{
    ffi::CString,
    fs::{File, OpenOptions},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::Path,
    time::{Duration, Instant},
};

pub(super) async fn inspect(database: &Path) -> Result<Facts, Failure> {
    let mut connection = verification::open(database, true).await?;
    let result = pinned_facts(&mut connection).await;
    connection.close().await.map_err(|_| Failure::Preflight)?;
    result
}

async fn pinned_facts(connection: &mut SqliteConnection) -> Result<Facts, Failure> {
    sqlx::query("BEGIN")
        .execute(&mut *connection)
        .await
        .map_err(|_| Failure::Preflight)?;
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| Failure::Preflight)?;
    if mode != "wal" {
        return Err(Failure::Preflight);
    }
    let result = verification::facts(connection).await?;
    if result.schema_digest != verification::expected_schema(result.version).await? {
        return Err(Failure::ManualRequired);
    }
    Ok(result)
}

pub(super) async fn create(req: &Request, operation: Operation) -> Result<Backup, Failure> {
    let mut source = verification::open(&req.database, true).await?;
    let result = copy_snapshot(req, operation, &mut source).await;
    source.close().await.map_err(|_| Failure::Preflight)?;
    result
}

async fn copy_snapshot(
    req: &Request,
    operation: Operation,
    source: &mut SqliteConnection,
) -> Result<Backup, Failure> {
    let before = pinned_facts(source).await?;
    if before.version != 18 {
        return Err(Failure::ManualRequired);
    }
    let destination = req.manifest_dir.join("snapshot.db");
    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&destination)
        .map_err(|_| Failure::Preflight)?;
    copy(source, &file).await?;
    let mut reopened = verification::open(&destination, true).await?;
    let result = verification::facts(&mut reopened).await;
    reopened.close().await.map_err(|_| Failure::Preflight)?;
    if result? != before {
        return Err(Failure::Preflight);
    }
    file.sync_all().map_err(|_| Failure::Preflight)?;
    records::sync_dir(&req.manifest_dir)?;
    super::flow::fault("after-backup-bytes");
    let backup = Backup {
        operation,
        before,
        sha256: hash_file(&destination)?,
    };
    records::publish(&req.manifest_dir.join("backup_published.json"), &backup)?;
    super::flow::fault("after-backup-record");
    Ok(backup)
}

async fn copy(source: &mut SqliteConnection, file: &File) -> Result<(), Failure> {
    let mut handle = source.lock_handle().await.map_err(|_| Failure::Preflight)?;
    let name = CString::new(format!("/proc/self/fd/{}", file.as_raw_fd()))
        .map_err(|_| Failure::Preflight)?;
    let mut destination = std::ptr::null_mut();
    let opened = unsafe {
        ffi::sqlite3_open_v2(
            name.as_ptr(),
            &mut destination,
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_NOMUTEX,
            std::ptr::null(),
        )
    };
    if opened != ffi::SQLITE_OK || destination.is_null() {
        if !destination.is_null() {
            unsafe {
                ffi::sqlite3_close(destination);
            }
        }
        return Err(Failure::Preflight);
    }
    let result = unsafe { copy_raw(handle.as_raw_handle().as_ptr(), destination) };
    let closed = unsafe { ffi::sqlite3_close(destination) };
    if closed != ffi::SQLITE_OK {
        return Err(Failure::Preflight);
    }
    result
}

unsafe fn copy_raw(
    source: *mut ffi::sqlite3,
    destination: *mut ffi::sqlite3,
) -> Result<(), Failure> {
    let backup = unsafe {
        ffi::sqlite3_backup_init(destination, c"main".as_ptr(), source, c"main".as_ptr())
    };
    if backup.is_null() {
        return Err(Failure::Preflight);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let result = loop {
        let status = unsafe { ffi::sqlite3_backup_step(backup, 256) };
        if status == ffi::SQLITE_DONE {
            break Ok(());
        }
        if Instant::now() >= deadline
            || ![ffi::SQLITE_OK, ffi::SQLITE_BUSY, ffi::SQLITE_LOCKED].contains(&status)
        {
            break Err(Failure::Preflight);
        }
        if status != ffi::SQLITE_OK {
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    let finished = unsafe { ffi::sqlite3_backup_finish(backup) };
    if finished != ffi::SQLITE_OK {
        return Err(Failure::Preflight);
    }
    result
}

pub(super) async fn validate(req: &Request, expected: &Operation) -> Result<Backup, Failure> {
    let backup: Backup = records::load(&req.manifest_dir.join("backup_published.json"))?;
    if &backup.operation != expected
        || hash_file(&req.manifest_dir.join("snapshot.db"))? != backup.sha256
        || backup.before.version != 18
    {
        return Err(Failure::ManualRequired);
    }
    let mut connection = verification::open(&req.manifest_dir.join("snapshot.db"), true).await?;
    let result = verification::facts(&mut connection).await;
    connection.close().await.map_err(|_| Failure::Preflight)?;
    if result? != backup.before {
        return Err(Failure::ManualRequired);
    }
    Ok(backup)
}
