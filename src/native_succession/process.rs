use super::state::{Owner, Result, refuse};
#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;
use crate::config::Config;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Read,
    os::unix::fs::MetadataExt,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Physical {
    path: PathBuf,
    device: u64,
    inode: u64,
    mode: u32,
    digest: Option<String>,
    config_change_time: Option<(i64, i64)>,
    // Keeping the immutable config inode open prevents unlink/recreate from
    // reusing it even on filesystems whose change-time resolution is coarse.
    #[serde(skip)]
    _config_handle: Option<Arc<fs::File>>,
}
impl PartialEq for Physical {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.device == other.device
            && self.inode == other.inode
            && self.mode == other.mode
            && self.digest == other.digest
            && self.config_change_time == other.config_change_time
    }
}
impl Eq for Physical {}
impl Physical {
    fn capture(path: &Path, hash: bool) -> Result<Self> {
        let meta = fs::symlink_metadata(path).map_err(|_| refuse("physical-identity"))?;
        if path.canonicalize().map_err(|_| refuse("physical-path"))? != path
            || meta.file_type().is_symlink()
            || (meta.is_file() && meta.nlink() != 1)
        {
            return Err(refuse("physical-alias"));
        }
        let (digest, handle) = if hash {
            let mut file = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path)
                .map_err(|_| refuse("config-open"))?;
            let opened = file.metadata().map_err(|_| refuse("config-metadata"))?;
            if opened.dev() != meta.dev() || opened.ino() != meta.ino() || !opened.is_file() {
                return Err(refuse("config-open-drift"));
            }
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)
                .map_err(|_| refuse("config-read"))?;
            (
                Some(format!("{:x}", Sha256::digest(bytes))),
                Some(Arc::new(file)),
            )
        } else {
            (None, None)
        };
        Ok(Self {
            path: path.into(),
            device: meta.dev(),
            inode: meta.ino(),
            mode: meta.mode(),
            digest,
            config_change_time: hash.then_some((meta.ctime(), meta.ctime_nsec())),
            _config_handle: handle,
        })
    }
    fn verify(&self) -> Result<()> {
        if &Self::capture(&self.path, self.digest.is_some())? != self {
            return Err(refuse("physical-drift"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Exclusion {
    pub kind: String,
    pub owner: Owner,
    pub observed_at: String,
    pub observer_pid: u32,
    pub account: String,
    pub device: String,
    physical: Vec<Physical>,
    pub inventory_digest: String,
}

pub(crate) fn capture(owner: &Owner, config: &Config, path: &Path) -> Result<Exclusion> {
    let store = config
        .transports
        .matrix
        .crypto_store_path
        .as_ref()
        .ok_or_else(|| refuse("store"))?;
    let mut physical = vec![
        Physical::capture(&config.bridge.database, false)?,
        Physical::capture(path, true)?,
        Physical::capture(store, false)?,
        Physical::capture(&config.bridge.session_root, false)?,
    ];
    let mut roots = vec![
        config
            .bridge
            .database
            .parent()
            .ok_or_else(|| refuse("database-parent"))?
            .to_path_buf(),
        config.bridge.session_root.clone(),
        store.clone(),
        path.parent()
            .ok_or_else(|| refuse("config-parent"))?
            .to_path_buf(),
    ];
    let private_roots = roots.clone();
    for agent in config.agents.values().filter(|agent| agent.enabled) {
        for workspace in agent.workspace.iter().chain(&agent.additional_workspaces) {
            if !roots.contains(workspace) {
                physical.push(Physical::capture(workspace, false)?);
                roots.push(workspace.clone());
            }
        }
    }
    let digest = inspect(
        Path::new("/proc"),
        owner,
        &roots,
        &private_roots,
        &config.transports.matrix.device_id,
        std::process::id(),
    )?;
    Ok(Exclusion {
        kind: "current-scoped-exclusion-v1".into(),
        owner: owner.clone(),
        observed_at: chrono::Utc::now().to_rfc3339(),
        observer_pid: std::process::id(),
        account: config.transports.matrix.user_id.clone(),
        device: config.transports.matrix.device_id.clone(),
        physical,
        inventory_digest: digest,
    })
}
impl Exclusion {
    #[cfg(test)]
    pub(crate) fn modeled(owner: Owner) -> Self {
        Self {
            kind: "current-scoped-exclusion-v1".into(),
            owner,
            observed_at: chrono::Utc::now().to_rfc3339(),
            observer_pid: std::process::id(),
            account: "@bridge:example.test".into(),
            device: "BRIDGE".into(),
            physical: Vec::new(),
            inventory_digest: "modeled-resource-proof".into(),
        }
    }
    pub(crate) fn revalidate(&self, config: &Config, path: &Path) -> Result<()> {
        for binding in &self.physical {
            binding.verify()?;
        }
        let current = capture(&self.owner, config, path)?;
        if current.account != self.account
            || current.device != self.device
            || current.physical != self.physical
        {
            return Err(refuse("identity-drift"));
        }
        Ok(())
    }
}

fn owner_absent(proc_root: &Path, owner: &Owner) -> Result<()> {
    if owner.pid <= 0
        || owner.starttime <= 0
        || owner.boot.is_empty()
        || owner.fingerprint.is_empty()
    {
        return Err(refuse("owner-identity"));
    }
    let boot = fs::read_to_string(proc_root.join("sys/kernel/random/boot_id"))
        .map_err(|_| refuse("boot-unavailable"))?;
    if boot.trim() != owner.boot {
        return Err(refuse("boot-changed"));
    }
    match fs::symlink_metadata(proc_root.join(owner.pid.to_string())) {
        Ok(_) => return Err(refuse("live-or-reused-owner-pid")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(_) => return Err(refuse("owner-unreadable")),
    }
    Ok(())
}

fn inspect(
    proc_root: &Path,
    owner: &Owner,
    roots: &[PathBuf],
    private_roots: &[PathBuf],
    device: &str,
    self_pid: u32,
) -> Result<String> {
    owner_absent(proc_root, owner)?;
    let domain = owner
        .fingerprint
        .split_once(";cgroup:")
        .map(|(_, s)| s)
        .ok_or_else(|| refuse("owner-domain"))?;
    if !domain.starts_with("0::/") {
        return Err(refuse("owner-domain"));
    }
    let mut scanned = Vec::new();
    for entry in fs::read_dir(proc_root).map_err(|_| refuse("process-list"))? {
        let entry = entry.map_err(|_| refuse("process-entry"))?;
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        if pid == self_pid {
            continue;
        }
        let path = entry.path();
        let meta = match fs::metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(refuse("process-metadata")),
        };
        if meta.uid() != unsafe { libc::geteuid() } {
            continue;
        }
        match inspect_one(&path, owner, roots, private_roots, device, domain) {
            Ok(Some(start)) => scanned.push((pid, start)),
            Ok(None) => (),
            Err(e) if !path.exists() => {
                let _ = e;
            }
            Err(e) => return Err(e),
        }
        if scanned.len() > 16384 {
            return Err(refuse("process-bound"));
        }
    }
    scanned.sort();
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&scanned).map_err(|_| refuse("process-encoding"))?)
    ))
}

fn inspect_one(
    path: &Path,
    owner: &Owner,
    roots: &[PathBuf],
    private_roots: &[PathBuf],
    device: &str,
    domain: &str,
) -> Result<Option<i64>> {
    let stat = fs::read_to_string(path.join("stat")).map_err(|_| refuse("process-stat"))?;
    let fields = stat
        .rsplit_once(") ")
        .ok_or_else(|| refuse("process-stat"))?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let start: i64 = fields
        .get(19)
        .ok_or_else(|| refuse("process-start"))?
        .parse()
        .map_err(|_| refuse("process-start"))?;
    // It cannot be a spawned descendant, but accessible selectors can still
    // reveal a competitor. Unrelated older processes need no universal census.
    if start < owner.starttime {
        inspect_preexisting(path, private_roots, device, domain)?;
        return Ok(None);
    }
    let cgroup = fs::read_to_string(path.join("cgroup"))
        .map_err(|_| refuse("relevant-domain-unreadable"))?;
    if cgroup.trim() == domain {
        return Err(refuse("related-domain-member"));
    }
    let environment =
        fs::read(path.join("environ")).map_err(|_| refuse("relevant-environment-unreadable"))?;
    let command =
        fs::read(path.join("cmdline")).map_err(|_| refuse("relevant-command-unreadable"))?;
    let marker = format!("MATRIX_DEVICE_ID={device}");
    if environment
        .split(|b| *b == 0)
        .any(|v| v == marker.as_bytes())
        || roots.iter().any(|root| {
            let bytes = root.as_os_str().as_encoded_bytes();
            contains(&environment, bytes) || contains(&command, bytes)
        })
    {
        return Err(refuse("related-escaped-process"));
    }
    let cwd = fs::read_link(path.join("cwd")).map_err(|_| refuse("relevant-cwd-unreadable"))?;
    if roots.iter().any(|r| cwd.starts_with(r)) {
        return Err(refuse("related-workspace"));
    }
    for entry in
        fs::read_dir(path.join("fd")).map_err(|_| refuse("relevant-resources-unreadable"))?
    {
        let target = match fs::read_link(entry.map_err(|_| refuse("resource-entry"))?.path()) {
            Ok(target) => target,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(refuse("resource-unreadable")),
        };
        if roots.iter().any(|r| target.starts_with(r)) {
            return Err(refuse("related-open-resource"));
        }
    }
    let after = fs::read_to_string(path.join("stat")).map_err(|_| refuse("process-raced"))?;
    let next = after
        .rsplit_once(") ")
        .and_then(|(_, s)| s.split_whitespace().nth(19));
    if next != Some(start.to_string().as_str()) {
        return Err(refuse("process-raced"));
    }
    Ok(Some(start))
}
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|x| x == needle)
}

fn inspect_preexisting(path: &Path, roots: &[PathBuf], device: &str, domain: &str) -> Result<()> {
    if fs::read_to_string(path.join("cgroup")).is_ok_and(|s| s.trim() == domain) {
        return Err(refuse("related-domain-member"));
    }
    let environment = fs::read(path.join("environ")).unwrap_or_default();
    let command = fs::read(path.join("cmdline")).unwrap_or_default();
    let marker = format!("MATRIX_DEVICE_ID={device}");
    if environment
        .split(|b| *b == 0)
        .any(|v| v == marker.as_bytes())
        || roots.iter().any(|root| {
            let bytes = root.as_os_str().as_encoded_bytes();
            contains(&environment, bytes) || contains(&command, bytes)
        })
        || fs::read_link(path.join("cwd")).is_ok_and(|cwd| roots.iter().any(|r| cwd.starts_with(r)))
    {
        return Err(refuse("preexisting-resource-competitor"));
    }
    if let Ok(entries) = fs::read_dir(path.join("fd")) {
        for entry in entries.flatten() {
            if fs::read_link(entry.path())
                .is_ok_and(|target| roots.iter().any(|r| target.starts_with(r)))
            {
                return Err(refuse("preexisting-open-resource"));
            }
        }
    }
    Ok(())
}
