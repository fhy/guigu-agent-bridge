use std::{
    ffi::CString,
    os::fd::AsRawFd,
    str::FromStr,
    time::{Duration, Instant},
};

use libsqlite3_sys as ffi;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

use super::{MAX_FILE, Paths, backup::SourceSet, codec::Identity, fs};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqliteFacts {
    pub version: u32,
    pub page_size: u32,
    pub page_count: u32,
    pub user_version: u32,
    pub schema_hash: [u8; 32],
}

pub(crate) async fn online_backup(
    paths: &Paths,
    s0: &SourceSet,
    source_main: &std::fs::File,
    destination: &std::fs::File,
) -> Result<(SqliteFacts, SourceSet), ()> {
    let retained = fs::identity(&source_main.metadata().map_err(|_| ())?)?;
    if !fs::same(retained, s0.main) {
        return Err(());
    }
    super::fault::hit("sqlite-s0-retained")?;
    let source_path = format!("/proc/self/fd/{}", source_main.as_raw_fd());
    let options = SqliteConnectOptions::from_str(&source_path)
        .map_err(|_| ())?
        .read_only(true)
        .create_if_missing(false)
        .shared_cache(false);
    let mut source = SqliteConnection::connect_with(&options)
        .await
        .map_err(|_| ())?;
    super::fault::hit("sqlite-s1-open")?;
    validate_main(paths, s0.main)?;
    if !fs::same(
        retained,
        fs::identity(&source_main.metadata().map_err(|_| ())?)?,
    ) {
        return Err(());
    }
    sqlx::query("BEGIN")
        .execute(&mut source)
        .await
        .map_err(|_| ())?;
    let facts = facts(&mut source).await?;
    super::fault::hit("sqlite-s3-transaction")?;
    let s3 = super::backup::source_set(paths)?;
    validate_observation(s0, &s3, None)?;
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
    super::fault::hit("sqlite-s5-copy")?;
    let s5 = super::backup::source_set(paths)?;
    validate_observation(s0, &s5, Some(&s3))?;
    let closed = unsafe { ffi::sqlite3_close(raw_destination) };
    drop(handle);
    let rolled_back = sqlx::query("ROLLBACK").execute(&mut source).await;
    source.close().await.map_err(|_| ())?;
    super::fault::hit("sqlite-s7-close")?;
    if copied.is_err() || closed != ffi::SQLITE_OK || rolled_back.is_err() {
        return Err(());
    }
    let s7 = super::backup::source_set(paths)?;
    validate_observation(s0, &s7, Some(&s3))?;
    Ok((facts, s7))
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

pub(crate) async fn verify_retained(file: &std::fs::File) -> Result<SqliteFacts, ()> {
    let before = fs::identity(&file.metadata().map_err(|_| ())?)?;
    let path = format!("/proc/self/fd/{}", file.as_raw_fd());
    let options = SqliteConnectOptions::from_str(&path)
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
    let after = fs::identity(&file.metadata().map_err(|_| ())?)?;
    if bytes > MAX_FILE || bytes > before.size || !fs::same(before, after) {
        return Err(());
    }
    Ok(facts)
}

fn validate_main(paths: &Paths, expected: Identity) -> Result<(), ()> {
    let root = fs::open_dir(&paths.root)?;
    let file = fs::open_leaf(&root, "state.db", false)?;
    let observed = fs::identity(&file.metadata().map_err(|_| ())?)?;
    if fs::same(observed, expected) {
        Ok(())
    } else {
        Err(())
    }
}

fn validate_observation(
    s0: &SourceSet,
    observed: &SourceSet,
    baseline: Option<&SourceSet>,
) -> Result<(), ()> {
    if s0.main != observed.main
        || s0.main_hash != observed.main_hash
        || s0.wal != observed.wal
        || s0.wal_hash != observed.wal_hash
    {
        return Err(());
    }
    match (s0.shm, baseline.and_then(|v| v.shm), observed.shm) {
        (None, None, None) => Ok(()),
        (None, None, Some(now))
            if now.uid == s0.main.uid && now.gid == s0.main.gid && now.mode == s0.main.mode =>
        {
            Ok(())
        }
        (None, Some(base), Some(now)) if fs::same(base, now) => Ok(()),
        (None, Some(_), None) if baseline.is_some() => Ok(()),
        (Some(initial), Some(base), Some(now))
            if fs::same(initial, base) && fs::same(base, now) =>
        {
            Ok(())
        }
        (Some(initial), Some(base), None) if fs::same(initial, base) && baseline.is_some() => {
            Ok(())
        }
        (Some(initial), None, Some(now)) if fs::same(initial, now) => Ok(()),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(inode: u64, size: u64) -> Identity {
        Identity {
            device: 1,
            inode,
            size,
            uid: 1000,
            gid: 1000,
            mode: 0o600,
        }
    }
    fn set(shm: Option<Identity>) -> SourceSet {
        SourceSet {
            main: id(1, 10),
            wal: Some(id(2, 20)),
            shm,
            hash: [0; 32],
            bytes: 30,
            main_hash: [1; 32],
            wal_hash: Some([2; 32]),
        }
    }
    #[test]
    fn s0_s7_shm_transition_table_is_exhaustive() {
        let absent = set(None);
        assert!(validate_observation(&absent, &set(None), None).is_ok());
        let created = set(Some(id(3, 30)));
        assert!(validate_observation(&absent, &created, None).is_ok());
        assert!(validate_observation(&absent, &created, Some(&created)).is_ok());
        assert!(validate_observation(&absent, &set(None), Some(&created)).is_ok());
        let existing = set(Some(id(4, 40)));
        assert!(validate_observation(&existing, &existing, None).is_ok());
        assert!(validate_observation(&existing, &existing, Some(&existing)).is_ok());
        assert!(validate_observation(&existing, &set(None), Some(&existing)).is_ok());
        assert!(validate_observation(&existing, &set(Some(id(5, 40))), Some(&existing)).is_err());
        assert!(validate_observation(&absent, &set(Some(id(3, 31))), Some(&created)).is_err());
        let mut main_drift = existing.clone();
        main_drift.main_hash = [9; 32];
        assert!(validate_observation(&existing, &main_drift, Some(&existing)).is_err());
        let mut wal_drift = existing.clone();
        wal_drift.wal_hash = Some([9; 32]);
        assert!(validate_observation(&existing, &wal_drift, Some(&existing)).is_err());
    }
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
