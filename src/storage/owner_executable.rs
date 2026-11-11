//! Per-runtime executable validation. Durable fingerprints retain their original format.
use std::{
    fs::{File, Metadata},
    io::Read,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};

const BUDGET: Duration = Duration::from_secs(10);
const MAX_BYTES: u64 = 1024 * 1024 * 1024;
type Attributes = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Stamp {
    object: (u64, u64, u64),
    authority: (u32, u32, u32),
    modified: (i64, i64),
    changed: (i64, i64),
}
impl Stamp {
    fn read(file: &File) -> Option<Self> {
        let m = file.metadata().ok()?;
        (m.is_file() && m.len() <= MAX_BYTES).then(|| Self::from_metadata(&m))
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
    fn pin(file: File, deadline: Instant) -> Option<Self> {
        let stamp = Stamp::read(&file)?;
        let attributes = attributes(&file)?;
        let digest = digest(&file, &stamp, deadline)?;
        if attributes != self::attributes(&file)? || Stamp::read(&file)? != stamp {
            return None;
        }
        Some(Self {
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
    use std::os::unix::fs::FileExt;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    let mut offset = 0;
    loop {
        if Instant::now() >= deadline {
            return None;
        }
        let count = file.read_at(&mut buffer, offset).ok()?;
        if count == 0 {
            break;
        }
        offset += count as u64;
        if offset > before.object.2 || offset > MAX_BYTES {
            return None;
        }
        hash.update(&buffer[..count]);
    }
    if offset != before.object.2 || Stamp::read(file)? != *before {
        return None;
    }
    Some(hash.finalize().into())
}

// ACLs, capabilities and other security labels are part of effective authority.
// Bound every syscall buffer; unreadable or changing attribute sets fail closed.
fn attributes(file: &File) -> Option<Attributes> {
    let mut names = vec![0u8; 65536];
    let count =
        unsafe { libc::flistxattr(file.as_raw_fd(), names.as_mut_ptr().cast(), names.len()) };
    if count < 0 {
        return (std::io::Error::last_os_error().raw_os_error() == Some(libc::EOPNOTSUPP))
            .then(Vec::new);
    }
    names.truncate(count as usize);
    let mut result = Vec::new();
    let mut remaining = 65536usize;
    for name in names.split_inclusive(|b| *b == 0) {
        if name.last() != Some(&0) {
            return None;
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
        if count < 0 || count as usize > remaining {
            return None;
        }
        bytes.truncate(count as usize);
        remaining -= count as usize;
        result.push((name.to_vec(), bytes));
    }
    result.sort();
    Some(result)
}

fn cgroup() -> Option<String> {
    let mut text = String::new();
    File::open("/proc/self/cgroup")
        .ok()?
        .take(65537)
        .read_to_string(&mut text)
        .ok()?;
    (!text.trim().is_empty() && text.len() <= 65536).then(|| text.trim().to_owned())
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
        tokio::time::timeout(
            BUDGET,
            tokio::task::spawn_blocking(|| {
                let domain = cgroup()?;
                let executable =
                    Executable::pin(File::open("/proc/self/exe").ok()?, Instant::now() + BUDGET)?;
                let current = File::open("/proc/self/exe").ok()?;
                if Stamp::read(&current)? != executable.stamp || cgroup()? != domain {
                    return None;
                }
                let s = &executable.stamp;
                let fingerprint = format!(
                    "pid:{};exe:{}:{}:{}:{}:{};cgroup:{}",
                    std::process::id(),
                    s.object.0,
                    s.object.1,
                    s.object.2,
                    s.changed.0,
                    s.changed.1,
                    domain
                );
                Some(Self {
                    inner: std::sync::Arc::new(std::sync::Mutex::new(Some(executable))),
                    fingerprint,
                    cgroup: domain,
                })
            }),
        )
        .await
        .ok()?
        .ok()?
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
