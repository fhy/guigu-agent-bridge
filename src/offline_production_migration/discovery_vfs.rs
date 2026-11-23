use std::sync::OnceLock;

use libsqlite3_sys as sqlite;

fn base() -> *mut sqlite::sqlite3_vfs {
    static BASE: OnceLock<usize> = OnceLock::new();
    *BASE.get_or_init(|| unsafe { sqlite::sqlite3_vfs_find(std::ptr::null()) as usize })
        as *mut sqlite::sqlite3_vfs
}

pub(super) fn name() -> Result<&'static std::ffi::CStr, ()> {
    static REGISTERED: OnceLock<bool> = OnceLock::new();
    let registered = REGISTERED.get_or_init(|| unsafe {
        let original = base();
        if original.is_null() {
            return false;
        }
        let mut vfs = Box::new(std::ptr::read(original));
        vfs.zName = c"r092-readonly-discovery".as_ptr();
        vfs.szOsFile += std::mem::size_of::<*const sqlite::sqlite3_io_methods>() as i32;
        vfs.xOpen = Some(open);
        vfs.xDelete = Some(delete);
        let pointer = Box::into_raw(vfs);
        if sqlite::sqlite3_vfs_register(pointer, 0) != sqlite::SQLITE_OK {
            drop(Box::from_raw(pointer));
            return false;
        }
        true
    });
    if *registered {
        Ok(c"r092-readonly-discovery")
    } else {
        Err(())
    }
}

unsafe fn original_methods(file: *mut sqlite::sqlite3_file) -> *const sqlite::sqlite3_io_methods {
    unsafe {
        let original = base();
        let tail = file
            .cast::<u8>()
            .add((*original).szOsFile as usize)
            .cast::<*const sqlite::sqlite3_io_methods>();
        tail.read_unaligned()
    }
}

unsafe extern "C" fn open(
    _vfs: *mut sqlite::sqlite3_vfs,
    name: *const std::ffi::c_char,
    file: *mut sqlite::sqlite3_file,
    flags: i32,
    output: *mut i32,
) -> i32 {
    unsafe {
        let original = base();
        if name.is_null() || original.is_null() {
            return sqlite::SQLITE_CANTOPEN;
        }
        let flags = (flags
            & !(sqlite::SQLITE_OPEN_READWRITE
                | sqlite::SQLITE_OPEN_CREATE
                | sqlite::SQLITE_OPEN_DELETEONCLOSE))
            | sqlite::SQLITE_OPEN_READONLY
            | sqlite::SQLITE_OPEN_NOFOLLOW;
        let Some(open) = (*original).xOpen else {
            return sqlite::SQLITE_CANTOPEN;
        };
        let result = open(original, name, file, flags, output);
        if result != sqlite::SQLITE_OK {
            return result;
        }
        let methods = (*file).pMethods;
        let tail = file
            .cast::<u8>()
            .add((*original).szOsFile as usize)
            .cast::<*const sqlite::sqlite3_io_methods>();
        tail.write_unaligned(methods);
        let mut readonly = Box::new(std::ptr::read(methods));
        readonly.xClose = Some(close);
        readonly.xWrite = Some(write);
        readonly.xTruncate = Some(truncate);
        if readonly.xShmMap.is_some() {
            readonly.xShmMap = Some(shm_map);
        }
        (*file).pMethods = Box::into_raw(readonly);
        result
    }
}

unsafe extern "C" fn close(file: *mut sqlite::sqlite3_file) -> i32 {
    unsafe {
        let allocated = (*file).pMethods;
        let methods = original_methods(file);
        let result = (*methods)
            .xClose
            .map_or(sqlite::SQLITE_IOERR_CLOSE, |close| close(file));
        drop(Box::from_raw(allocated.cast_mut()));
        result
    }
}

unsafe extern "C" fn shm_map(
    file: *mut sqlite::sqlite3_file,
    page: i32,
    size: i32,
    _extend: i32,
    result: *mut *mut std::ffi::c_void,
) -> i32 {
    unsafe {
        let methods = original_methods(file);
        let Some(map) = (*methods).xShmMap else {
            return sqlite::SQLITE_IOERR_SHMMAP;
        };
        let status = map(file, page, size, 0, result);
        if status == sqlite::SQLITE_OK {
            sqlite::SQLITE_READONLY
        } else {
            status
        }
    }
}

unsafe extern "C" fn write(
    _file: *mut sqlite::sqlite3_file,
    _buffer: *const std::ffi::c_void,
    _length: i32,
    _offset: i64,
) -> i32 {
    sqlite::SQLITE_READONLY
}

unsafe extern "C" fn truncate(_file: *mut sqlite::sqlite3_file, _length: i64) -> i32 {
    sqlite::SQLITE_READONLY
}

unsafe extern "C" fn delete(
    _vfs: *mut sqlite::sqlite3_vfs,
    _name: *const std::ffi::c_char,
    _sync: i32,
) -> i32 {
    sqlite::SQLITE_READONLY
}
