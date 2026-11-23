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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SnapshotStage {
    RetainedSource,
    SourceOptions,
    SourceOpen,
    SourceRevalidation,
    SourceBegin,
    SourceFacts,
    S3Observation,
    DestinationName,
    DestinationOpen,
    CopyOrJournal,
    S5Observation,
    DestinationClose,
    SourceRollback,
    SourceClose,
    S7Observation,
}

#[cfg(test)]
impl SnapshotStage {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::RetainedSource => "retained-source",
            Self::SourceOptions => "source-options",
            Self::SourceOpen => "source-open",
            Self::SourceRevalidation => "source-revalidation",
            Self::SourceBegin => "source-begin",
            Self::SourceFacts => "source-facts",
            Self::S3Observation => "s3-observation",
            Self::DestinationName => "destination-name",
            Self::DestinationOpen => "destination-open",
            Self::CopyOrJournal => "copy-or-journal",
            Self::S5Observation => "s5-observation",
            Self::DestinationClose => "destination-close",
            Self::SourceRollback => "source-rollback",
            Self::SourceClose => "source-close",
            Self::S7Observation => "s7-observation",
        }
    }
}

pub(crate) async fn online_backup(
    paths: &Paths,
    s0: &SourceSet,
    source_main: &std::fs::File,
    destination: &std::fs::File,
) -> Result<(SqliteFacts, SourceSet), SnapshotStage> {
    let retained = fs::identity(
        &source_main
            .metadata()
            .map_err(|_| SnapshotStage::RetainedSource)?,
    )
    .map_err(|_| SnapshotStage::RetainedSource)?;
    if !fs::same(retained, s0.main) {
        return Err(SnapshotStage::RetainedSource);
    }
    super::fault::hit("sqlite-s0-retained").map_err(|_| SnapshotStage::RetainedSource)?;
    let source_path = format!("/proc/self/fd/{}", source_main.as_raw_fd());
    let options = SqliteConnectOptions::from_str(&source_path)
        .map_err(|_| SnapshotStage::SourceOptions)?
        .read_only(true)
        .create_if_missing(false)
        .shared_cache(false);
    let mut source = SqliteConnection::connect_with(&options)
        .await
        .map_err(|_| SnapshotStage::SourceOpen)?;
    super::fault::hit("sqlite-s1-open").map_err(|_| SnapshotStage::SourceOpen)?;
    validate_main(paths, s0.main).map_err(|_| SnapshotStage::SourceRevalidation)?;
    if !fs::same(
        retained,
        fs::identity(
            &source_main
                .metadata()
                .map_err(|_| SnapshotStage::SourceRevalidation)?,
        )
        .map_err(|_| SnapshotStage::SourceRevalidation)?,
    ) {
        return Err(SnapshotStage::SourceRevalidation);
    }
    sqlx::query("BEGIN")
        .execute(&mut source)
        .await
        .map_err(|_| SnapshotStage::SourceBegin)?;
    let facts = facts(&mut source)
        .await
        .map_err(|_| SnapshotStage::SourceFacts)?;
    super::fault::hit("sqlite-s3-transaction").map_err(|_| SnapshotStage::S3Observation)?;
    let s3 = super::backup::source_set(paths).map_err(|_| SnapshotStage::S3Observation)?;
    validate_observation(s0, &s3, None).map_err(|_| SnapshotStage::S3Observation)?;
    let mut handle = source
        .lock_handle()
        .await
        .map_err(|_| SnapshotStage::SourceFacts)?;
    let raw_source = handle.as_raw_handle().as_ptr();
    let destination_name = CString::new(format!("/proc/self/fd/{}", destination.as_raw_fd()))
        .map_err(|_| SnapshotStage::DestinationName)?;
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
        return Err(SnapshotStage::DestinationOpen);
    }
    let copied =
        backup_raw(raw_source, raw_destination).and_then(|()| journal_delete(raw_destination));
    super::fault::hit("sqlite-s5-copy").map_err(|_| SnapshotStage::CopyOrJournal)?;
    let s5 = super::backup::source_set(paths).map_err(|_| SnapshotStage::S5Observation)?;
    validate_observation(s0, &s5, Some(&s3)).map_err(|_| SnapshotStage::S5Observation)?;
    let closed = unsafe { ffi::sqlite3_close(raw_destination) };
    drop(handle);
    let rolled_back = sqlx::query("ROLLBACK").execute(&mut source).await;
    source
        .close()
        .await
        .map_err(|_| SnapshotStage::SourceClose)?;
    super::fault::hit("sqlite-s7-close").map_err(|_| SnapshotStage::SourceClose)?;
    if copied.is_err() {
        return Err(SnapshotStage::CopyOrJournal);
    }
    if closed != ffi::SQLITE_OK {
        return Err(SnapshotStage::DestinationClose);
    }
    if rolled_back.is_err() {
        return Err(SnapshotStage::SourceRollback);
    }
    let s7 = super::backup::source_set(paths).map_err(|_| SnapshotStage::S7Observation)?;
    validate_observation(s0, &s7, Some(&s3)).map_err(|_| SnapshotStage::S7Observation)?;
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
    if s0.main != observed.main || s0.main_hash != observed.main_hash {
        return Err(());
    }
    if s0.wal.is_none() && s0.shm.is_none() {
        return validate_created_sidecars(s0, observed, baseline);
    }
    if s0.wal != observed.wal || s0.wal_hash != observed.wal_hash {
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

fn validate_created_sidecars(
    s0: &SourceSet,
    observed: &SourceSet,
    baseline: Option<&SourceSet>,
) -> Result<(), ()> {
    let valid_created = |set: &SourceSet| {
        let (Some(wal), Some(wal_hash), Some(shm)) = (set.wal, set.wal_hash, set.shm) else {
            return false;
        };
        wal.size == 0
            && wal_hash == <[u8; 32]>::from(Sha256::digest([]))
            && [wal, shm].iter().all(|sidecar| {
                sidecar.uid == s0.main.uid
                    && sidecar.gid == s0.main.gid
                    && sidecar.mode == s0.main.mode
            })
    };
    match baseline {
        None => match (observed.wal, observed.shm) {
            (None, None) => Ok(()),
            (Some(_), Some(_)) if valid_created(observed) => Ok(()),
            _ => Err(()),
        },
        Some(base) if base.wal.is_none() && base.shm.is_none() => {
            if observed.wal.is_none() && observed.shm.is_none() {
                Ok(())
            } else {
                Err(())
            }
        }
        Some(base) if valid_created(base) => match (observed.wal, observed.shm) {
            (None, None) => Ok(()),
            (Some(_), Some(_))
                if observed.wal == base.wal
                    && observed.wal_hash == base.wal_hash
                    && observed.shm == base.shm =>
            {
                Ok(())
            }
            _ => Err(()),
        },
        Some(_) => Err(()),
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

#[cfg(test)]
#[path = "sqlite_unit_tests.rs"]
mod tests;
