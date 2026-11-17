//! Per-runtime executable validation. Durable fingerprints retain their original format.
use std::{
    fs::{File, Metadata},
    io::Read,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};

// Cold reads of the retained 662-MB object took 13 s; under measured I/O
// pressure a 60-s read reached only 706 MB while a concurrent read completed
// in 56 s. Give startup a finite 90-s
// window, including queueing, while keeping periodic validation at 10 s.
const CAPTURE_BUDGET: Duration = Duration::from_secs(90);
const BUDGET: Duration = Duration::from_secs(10);
const MAX_BYTES: u64 = 1024 * 1024 * 1024;
const DIGEST_CHUNK: usize = 1024 * 1024;
type Attributes = Vec<(Vec<u8>, Vec<u8>)>;

type CaptureResult<T> = Result<T, &'static str>;
const CAPTURE_PHASES: [&str; 13] = [
    "worker-queue",
    "domain-before",
    "open-pinned",
    "metadata-before",
    "attributes-before",
    "digest",
    "digest-stamp",
    "attributes-after",
    "metadata-after",
    "open-current",
    "final-stamp",
    "domain-after",
    "complete",
];

// Invocation-local progress, never an executable cache. Worker and outer events
// share an ID so a late worker result cannot be attributed to another startup.
struct CaptureProgress {
    id: u64,
    requested: Instant,
    started: OnceLock<Instant>,
    phase: AtomicUsize,
    bytes: AtomicU64,
    read_us: AtomicU64,
    hash_us: AtomicU64,
    cancelled: AtomicBool,
}
impl CaptureProgress {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            requested: Instant::now(),
            started: OnceLock::new(),
            phase: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
            read_us: AtomicU64::new(0),
            hash_us: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
        }
    }
    fn phase(progress: Option<&Self>, phase: usize) {
        if let Some(progress) = progress {
            progress.phase.store(phase, Ordering::Relaxed);
        }
    }
    fn emit(&self, event: &'static str, reason: &'static str) {
        use std::io::Write as _;
        let started = self.started.get();
        let queue_us = started.map_or_else(
            || self.requested.elapsed().as_micros(),
            |start| start.duration_since(self.requested).as_micros(),
        );
        let work_us = started.map_or(0, |start| start.elapsed().as_micros());
        let phase = CAPTURE_PHASES[self.phase.load(Ordering::Relaxed)];
        let _ = writeln!(
            std::io::stderr().lock(),
            "executable-capture pid={} invocation={} event={event} phase={phase} reason={reason} worker_started={} queue_us={queue_us} work_us={work_us} bytes={} read_us={} hash_us={}",
            std::process::id(),
            self.id,
            started.is_some(),
            self.bytes.load(Ordering::Relaxed),
            self.read_us.load(Ordering::Relaxed),
            self.hash_us.load(Ordering::Relaxed)
        );
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Stamp {
    object: (u64, u64, u64),
    authority: (u32, u32, u32),
    modified: (i64, i64),
    changed: (i64, i64),
}
impl Stamp {
    fn read(file: &File) -> Option<Self> {
        Self::read_checked(file).ok()
    }
    fn read_checked(file: &File) -> CaptureResult<Self> {
        let m = file.metadata().map_err(|_| "metadata-io")?;
        if !m.is_file() {
            return Err("not-regular-file");
        }
        if m.len() > MAX_BYTES {
            return Err("size-limit");
        }
        Ok(Self::from_metadata(&m))
    }
    fn from_metadata(m: &Metadata) -> Self {
        Self {
            object: (m.dev(), m.ino(), m.len()),
            authority: (m.uid(), m.gid(), m.mode()),
            modified: (m.mtime(), m.mtime_nsec()),
            changed: (m.ctime(), m.ctime_nsec()),
        }
    }
    fn same_identity(&self, other: &Self) -> bool {
        self.object == other.object
            && self.authority == other.authority
            && self.modified == other.modified
    }
}

#[derive(Debug)]
struct Executable {
    file: File,
    stamp: Stamp,
    attributes: Attributes,
    digest: [u8; 32],
}
impl Executable {
    #[cfg(test)]
    fn pin(file: File, deadline: Instant) -> Option<Self> {
        Self::pin_checked(file, deadline, None).ok()
    }
    fn pin_checked(
        file: File,
        deadline: Instant,
        progress: Option<&CaptureProgress>,
    ) -> CaptureResult<Self> {
        CaptureProgress::phase(progress, 3);
        let stamp = Stamp::read_checked(&file)?;
        CaptureProgress::phase(progress, 4);
        let attributes = attributes_checked(&file)?;
        let digest = digest_checked(&file, &stamp, deadline, progress)?;
        CaptureProgress::phase(progress, 7);
        if attributes != attributes_checked(&file)? {
            return Err("attributes-changed");
        }
        CaptureProgress::phase(progress, 8);
        if Stamp::read_checked(&file)? != stamp {
            return Err("stamp-changed");
        }
        Ok(Self {
            file,
            stamp,
            attributes,
            digest,
        })
    }
    fn validate(&mut self, current: File, deadline: Instant) -> Option<()> {
        let before = Stamp::read(&current)?;
        if !self.stamp.same_identity(&before)
            || Stamp::read(&self.file)? != before
            || attributes(&current)? != self.attributes
        {
            return None;
        }
        if before != self.stamp && digest(&self.file, &before, deadline)? != self.digest {
            return None;
        }
        // Even a cached hit authenticates both the pinned and current running object.
        if attributes(&current)? != self.attributes
            || Stamp::read(&self.file)? != before
            || Stamp::read(&current)? != before
        {
            return None;
        }
        self.stamp = before;
        Some(())
    }
}

fn digest(file: &File, before: &Stamp, deadline: Instant) -> Option<[u8; 32]> {
    digest_checked(file, before, deadline, None).ok()
}

fn digest_checked(
    file: &File,
    before: &Stamp,
    deadline: Instant,
    progress: Option<&CaptureProgress>,
) -> CaptureResult<[u8; 32]> {
    use std::os::unix::fs::FileExt;
    let mut hash = Sha256::new();
    let mut buffer = [0; DIGEST_CHUNK];
    let mut offset = 0;
    CaptureProgress::phase(progress, 5);
    loop {
        capture_checkpoint(deadline, progress)?;
        let read_at = Instant::now();
        let result = file.read_at(&mut buffer, offset);
        if let Some(p) = progress {
            p.read_us
                .fetch_add(read_at.elapsed().as_micros() as u64, Ordering::Relaxed);
        }
        let count = result.map_err(|_| "digest-read-io")?;
        capture_checkpoint(deadline, progress)?;
        if count == 0 {
            break;
        }
        offset += count as u64;
        if let Some(p) = progress {
            p.bytes.store(offset, Ordering::Relaxed);
        }
        if offset > before.object.2 || offset > MAX_BYTES {
            return Err("digest-length-bound");
        }
        let hash_at = Instant::now();
        hash.update(&buffer[..count]);
        if let Some(p) = progress {
            p.hash_us
                .fetch_add(hash_at.elapsed().as_micros() as u64, Ordering::Relaxed);
        }
    }
    if offset != before.object.2 {
        return Err("digest-length-mismatch");
    }
    capture_checkpoint(deadline, progress)?;
    CaptureProgress::phase(progress, 6);
    if Stamp::read_checked(file)? != *before {
        return Err("digest-stamp-changed");
    }
    Ok(hash.finalize().into())
}

fn capture_checkpoint(deadline: Instant, progress: Option<&CaptureProgress>) -> CaptureResult<()> {
    if progress.is_some_and(|p| p.cancelled.load(Ordering::Acquire)) {
        return Err("capture-cancelled");
    }
    if Instant::now() >= deadline {
        return Err("digest-deadline");
    }
    Ok(())
}

// Abort workers that have not started, and stop running read-only workers at the
// next checkpoint. A kernel read itself cannot be interrupted by dropping an await.
struct CaptureCancellation {
    progress: Arc<CaptureProgress>,
    abort: tokio::task::AbortHandle,
}
impl Drop for CaptureCancellation {
    fn drop(&mut self) {
        self.progress.cancelled.store(true, Ordering::Release);
        self.abort.abort();
    }
}

// ACLs, capabilities and other security labels are part of effective authority.
// Bound every syscall buffer; unreadable or changing attribute sets fail closed.
fn attributes(file: &File) -> Option<Attributes> {
    attributes_checked(file).ok()
}
fn attributes_checked(file: &File) -> CaptureResult<Attributes> {
    let mut names = vec![0u8; 65536];
    let count =
        unsafe { libc::flistxattr(file.as_raw_fd(), names.as_mut_ptr().cast(), names.len()) };
    if count < 0 {
        return if std::io::Error::last_os_error().raw_os_error() == Some(libc::EOPNOTSUPP) {
            Ok(Vec::new())
        } else {
            Err("xattr-list-io")
        };
    }
    names.truncate(count as usize);
    let mut result = Vec::new();
    let mut remaining = 65536usize;
    for name in names.split_inclusive(|b| *b == 0) {
        if name.last() != Some(&0) {
            return Err("xattr-name-shape");
        }
        let mut bytes = vec![0u8; remaining];
        let count = unsafe {
            libc::fgetxattr(
                file.as_raw_fd(),
                name.as_ptr().cast(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
            )
        };
        if count < 0 {
            return Err("xattr-read-io");
        }
        if count as usize > remaining {
            return Err("xattr-size-bound");
        }
        bytes.truncate(count as usize);
        remaining -= count as usize;
        result.push((name.to_vec(), bytes));
    }
    result.sort();
    Ok(result)
}

fn cgroup() -> Option<String> {
    cgroup_checked().ok()
}
fn cgroup_checked() -> CaptureResult<String> {
    let mut text = String::new();
    File::open("/proc/self/cgroup")
        .map_err(|_| "domain-open-io")?
        .take(65537)
        .read_to_string(&mut text)
        .map_err(|_| "domain-read-io")?;
    if text.trim().is_empty() || text.len() > 65536 {
        return Err("domain-bound");
    }
    Ok(text.trim().to_owned())
}

/// Cache belongs to one heartbeat task, never global and never copied across startups.
/// On timeout the read-only worker may finish, but cannot publish or replace this cache.
#[derive(Clone, Debug)]
pub(crate) struct RuntimeExecutable {
    inner: std::sync::Arc<std::sync::Mutex<Option<Executable>>>,
    fingerprint: String,
    cgroup: String,
}
impl RuntimeExecutable {
    pub(crate) async fn capture() -> Option<Self> {
        Self::capture_with(CAPTURE_BUDGET, Self::capture_checked).await
    }
    async fn capture_with(
        budget: Duration,
        work: impl FnOnce(&CaptureProgress, Instant) -> CaptureResult<Self> + Send + 'static,
    ) -> Option<Self> {
        let progress = Arc::new(CaptureProgress::new());
        let deadline = progress.requested + budget;
        let worker_progress = progress.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _ = worker_progress.started.set(Instant::now());
            let result = capture_checkpoint(deadline, Some(&worker_progress))
                .and_then(|()| work(&worker_progress, deadline));
            worker_progress.emit(
                "worker-finished",
                result.as_ref().err().copied().unwrap_or("ok"),
            );
            result
        });
        let _cancellation = CaptureCancellation {
            progress: progress.clone(),
            abort: worker.abort_handle(),
        };
        match tokio::time::timeout_at(deadline.into(), worker).await {
            Err(_) => {
                progress.emit("outer-timeout", "deadline");
                None
            }
            Ok(Err(_)) => {
                progress.emit("worker-join", "join-failed");
                None
            }
            Ok(Ok(result)) => result.ok(),
        }
    }
    fn capture_checked(progress: &CaptureProgress, deadline: Instant) -> CaptureResult<Self> {
        CaptureProgress::phase(Some(progress), 1);
        let domain = cgroup_checked()?;
        CaptureProgress::phase(Some(progress), 2);
        let file = File::open("/proc/self/exe").map_err(|_| "pinned-open-io")?;
        let executable = Executable::pin_checked(file, deadline, Some(progress))?;
        CaptureProgress::phase(Some(progress), 9);
        let current = File::open("/proc/self/exe").map_err(|_| "current-open-io")?;
        CaptureProgress::phase(Some(progress), 10);
        if Stamp::read_checked(&current)? != executable.stamp {
            return Err("final-stamp-changed");
        }
        CaptureProgress::phase(Some(progress), 11);
        if cgroup_checked()? != domain {
            return Err("final-domain-changed");
        }
        capture_checkpoint(deadline, Some(progress))?;
        let s = &executable.stamp;
        let fingerprint = format!(
            "pid:{};exe:{}:{}:{}:{}:{};cgroup:{}",
            std::process::id(),
            s.object.0,
            s.object.1,
            s.object.2,
            s.changed.0,
            s.changed.1,
            domain,
        );
        CaptureProgress::phase(Some(progress), 12);
        Ok(Self {
            inner: std::sync::Arc::new(std::sync::Mutex::new(Some(executable))),
            fingerprint,
            cgroup: domain,
        })
    }
    pub(crate) fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub(crate) fn same_cache(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) async fn validate(&self) -> Option<String> {
        let inner = self.inner.clone();
        let domain = self.cgroup.clone();
        tokio::time::timeout(
            BUDGET,
            tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().ok()?;
                let valid = (|| {
                    if cgroup()? != domain {
                        return None;
                    }
                    guard
                        .as_mut()?
                        .validate(File::open("/proc/self/exe").ok()?, Instant::now() + BUDGET)?;
                    if cgroup()? != domain {
                        return None;
                    }
                    Some(())
                })();
                if valid.is_none() {
                    *guard = None;
                }
                valid
            }),
        )
        .await
        .ok()?
        .ok()??;
        Some(self.fingerprint.clone())
    }
}

#[cfg(test)]
#[path = "owner_executable_tests.rs"]
mod tests;
