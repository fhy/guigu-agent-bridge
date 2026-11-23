use std::{ffi::CString, os::unix::ffi::OsStrExt, path::Path};

use libsqlite3_sys as sqlite;

pub(super) struct Exclusive(*mut sqlite::sqlite3);

impl Exclusive {
    pub(super) fn acquire(path: &Path) -> Result<Self, ()> {
        if sidecar(path, "-wal")? || sidecar(path, "-journal")? {
            return Err(());
        }
        let mut uri = url::Url::from_file_path(path).map_err(|_| ())?;
        uri.query_pairs_mut().append_pair("mode", "rw");
        let uri = CString::new(uri.as_str()).map_err(|_| ())?;
        let mut handle = std::ptr::null_mut();
        let status = unsafe {
            sqlite::sqlite3_open_v2(
                uri.as_ptr(),
                &mut handle,
                sqlite::SQLITE_OPEN_READWRITE
                    | sqlite::SQLITE_OPEN_URI
                    | sqlite::SQLITE_OPEN_NOFOLLOW,
                std::ptr::null(),
            )
        };
        if status != sqlite::SQLITE_OK || handle.is_null() {
            if !handle.is_null() {
                unsafe { sqlite::sqlite3_close(handle) };
            }
            return Err(());
        }
        let guard = Self(handle);
        let mut file: *mut sqlite::sqlite3_file = std::ptr::null_mut();
        if unsafe {
            sqlite::sqlite3_file_control(
                handle,
                std::ptr::null(),
                sqlite::SQLITE_FCNTL_FILE_POINTER,
                (&mut file as *mut *mut sqlite::sqlite3_file).cast(),
            )
        } != sqlite::SQLITE_OK
            || file.is_null()
        {
            return Err(());
        }
        let methods = unsafe { (*file).pMethods };
        let lock = unsafe { (*methods).xLock.ok_or(())? };
        if unsafe { lock(file, sqlite::SQLITE_LOCK_SHARED) } != sqlite::SQLITE_OK
            || unsafe { lock(file, sqlite::SQLITE_LOCK_EXCLUSIVE) } != sqlite::SQLITE_OK
            || sidecar(path, "-wal")?
            || sidecar(path, "-journal")?
        {
            return Err(());
        }
        Ok(guard)
    }
}

fn sidecar(database: &Path, suffix: &str) -> Result<bool, ()> {
    let mut name = database.as_os_str().as_bytes().to_vec();
    name.extend_from_slice(suffix.as_bytes());
    let name = std::ffi::OsStr::from_bytes(&name);
    match std::fs::symlink_metadata(name) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(()),
    }
}

impl Drop for Exclusive {
    fn drop(&mut self) {
        unsafe { sqlite::sqlite3_close(self.0) };
    }
}
