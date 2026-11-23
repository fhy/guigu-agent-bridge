//! Test-only coordination of a real WAL checkpoint during connection close.
use libsqlite3_sys as sqlite;
use sqlx::SqliteConnection;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
pub(super) struct Gate {
    armed: AtomicBool,
    entered: AtomicBool,
    released: Mutex<bool>,
    wake: Condvar,
}

impl Gate {
    pub(super) fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
    pub(super) async fn wait_entered(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !self.entered.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("closing connection reached main-file xSync");
    }
    pub(super) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

fn paths() -> &'static Mutex<HashMap<PathBuf, Arc<Gate>>> {
    static PATHS: OnceLock<Mutex<HashMap<PathBuf, Arc<Gate>>>> = OnceLock::new();
    PATHS.get_or_init(Mutex::default)
}

pub(super) struct Registration {
    path: PathBuf,
    pub(super) gate: Arc<Gate>,
}
impl Registration {
    pub(super) fn new(path: &Path) -> Self {
        let gate = Arc::new(Gate::default());
        assert!(
            paths()
                .lock()
                .unwrap()
                .insert(path.to_owned(), gate.clone())
                .is_none()
        );
        Self {
            path: path.to_owned(),
            gate,
        }
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.gate.release();
        paths().lock().unwrap().remove(&self.path);
    }
}

pub(super) async fn before_open(path: &Path) {
    let gate = paths().lock().unwrap().get(path).cloned();
    if let Some(gate) = gate
        && gate.armed.load(Ordering::SeqCst)
    {
        // Fix the scheduling order: the predecessor starts checkpointing before
        // its successor connects. With awaited close the successor comes later.
        gate.wait_entered().await;
    }
}

struct Hook {
    original: usize,
    allocated: usize,
    gate: Arc<Gate>,
}
fn hooks() -> &'static Mutex<HashMap<usize, Hook>> {
    static HOOKS: OnceLock<Mutex<HashMap<usize, Hook>>> = OnceLock::new();
    HOOKS.get_or_init(Mutex::default)
}

pub(super) async fn install(c: &mut SqliteConnection, path: &Path) {
    let gate = paths().lock().unwrap().get(path).cloned();
    let Some(gate) = gate else {
        return;
    };
    let mut handle = c.lock_handle().await.unwrap();
    let mut file: *mut sqlite::sqlite3_file = std::ptr::null_mut();
    // SQLx's exclusive handle guard prevents worker access while we replace the
    // main file's methods. The original VFS still performs every filesystem IO.
    unsafe {
        assert_eq!(
            sqlite::sqlite3_file_control(
                handle.as_raw_handle().as_ptr(),
                c"main".as_ptr(),
                sqlite::SQLITE_FCNTL_FILE_POINTER,
                (&mut file as *mut *mut sqlite::sqlite3_file).cast()
            ),
            sqlite::SQLITE_OK
        );
        assert!(!file.is_null());
        let original = (*file).pMethods;
        let mut methods = Box::new(std::ptr::read(original));
        methods.xSync = Some(sync);
        methods.xClose = Some(close);
        let allocated = Box::into_raw(methods);
        hooks().lock().unwrap().insert(
            file as usize,
            Hook {
                original: original as usize,
                allocated: allocated as usize,
                gate,
            },
        );
        (*file).pMethods = allocated;
    }
}

unsafe extern "C" fn sync(file: *mut sqlite::sqlite3_file, flags: i32) -> i32 {
    let (original, gate) = {
        let hooks = hooks().lock().unwrap();
        let hook = &hooks[&(file as usize)];
        (
            hook.original as *const sqlite::sqlite3_io_methods,
            hook.gate.clone(),
        )
    };
    if gate.armed.swap(false, Ordering::SeqCst) {
        gate.entered.store(true, Ordering::SeqCst);
        let released = gate.released.lock().unwrap();
        let (_released, timeout) = gate
            .wake
            .wait_timeout_while(released, Duration::from_secs(10), |released| !*released)
            .unwrap();
        if timeout.timed_out() {
            return sqlite::SQLITE_IOERR_FSYNC;
        }
    }
    // The original callback remains valid for the entire SQLite file lifetime.
    unsafe { (*original).xSync.unwrap()(file, flags) }
}

unsafe extern "C" fn close(file: *mut sqlite::sqlite3_file) -> i32 {
    let hook = hooks().lock().unwrap().remove(&(file as usize)).unwrap();
    unsafe {
        let original = hook.original as *const sqlite::sqlite3_io_methods;
        (*file).pMethods = original;
        let result = (*original).xClose.unwrap()(file);
        drop(Box::from_raw(
            hook.allocated as *mut sqlite::sqlite3_io_methods,
        ));
        result
    }
}
