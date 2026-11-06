use std::{
    collections::BTreeMap,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::storage::RuntimeOwnerEvidence;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileBinding {
    path: PathBuf,
    device: u64,
    inode: u64,
    digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Capsule {
    version: u8,
    pub(crate) owner: String,
    pub(crate) generation: i64,
    pub(crate) initial_revision: i64,
    pub(crate) fingerprint: String,
    pub(crate) pid: i64,
    pub(crate) starttime: i64,
    pub(crate) boot: String,
    pub(crate) boottime: i64,
    pub(crate) captured_at: String,
    database: FileBinding,
    config: FileBinding,
    binary: FileBinding,
    manager_pid: u32,
    manager_start: u64,
    manager_uid: u32,
    unit: String,
    invocation: String,
    domain: String,
    domain_device: u64,
    domain_inode: u64,
    start_monotonic: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Terminal {
    invocation: String,
    active: String,
    sub: String,
    result: String,
    code: i64,
    status: i64,
    exit_monotonic: u64,
}

pub(crate) struct Proof {
    pub(crate) capsule: Capsule,
    pub(crate) binding: String,
    pub(crate) terminal: Terminal,
    pub(crate) observed_at: String,
    pub(crate) boottime: i64,
}

fn refused() -> std::io::Error {
    std::io::Error::other("systemd owner observation refused")
}

mod capsule_io;
use capsule_io::{file_binding, proc_start, read_private, write_private};

async fn properties(unit: &str) -> std::io::Result<BTreeMap<String, String>> {
    if !unit.ends_with(".service")
        || !unit
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.@".contains(&byte))
    {
        return Err(refused());
    }
    let output = tokio::time::timeout(Duration::from_secs(5), tokio::process::Command::new("/usr/bin/systemctl")
        .args(["--user", "show", unit, "--property=Id,MainPID,ExecMainPID,InvocationID,ControlGroup,KillMode,Restart,RemainAfterExit,ActiveState,SubState,Result,ExecMainCode,ExecMainStatus,ExecMainStartTimestampMonotonic,ExecMainExitTimestampMonotonic"])
        .kill_on_drop(true).output()).await.map_err(|_| refused())??;
    if !output.status.success() {
        return Err(refused());
    }
    let text = String::from_utf8(output.stdout).map_err(|_| refused())?;
    let mut fields = BTreeMap::new();
    for line in text.lines() {
        let (key, value) = line.split_once('=').ok_or_else(refused)?;
        if fields.insert(key.into(), value.into()).is_some() {
            return Err(refused());
        }
    }
    Ok(fields)
}

fn field<'value>(
    fields: &'value BTreeMap<String, String>,
    key: &str,
) -> std::io::Result<&'value str> {
    fields.get(key).map(String::as_str).ok_or_else(refused)
}

fn common(capsule: &Capsule, fields: &BTreeMap<String, String>) -> std::io::Result<()> {
    if field(fields, "Id")? != capsule.unit
        || field(fields, "InvocationID")? != capsule.invocation
        || field(fields, "KillMode")? != "control-group"
        || field(fields, "Restart")? != "no"
        || field(fields, "RemainAfterExit")? != "yes"
        || field(fields, "ExecMainPID")? != capsule.pid.to_string()
        || field(fields, "ExecMainStartTimestampMonotonic")? != capsule.start_monotonic
    {
        return Err(refused());
    }
    Ok(())
}

pub(crate) async fn capture(
    database: &Path,
    owner: &str,
    fingerprint: &str,
    evidence: &RuntimeOwnerEvidence,
) -> std::io::Result<Option<String>> {
    let Some(destination) = std::env::var_os("BRIDGE_OWNER_CAPSULE") else {
        return Ok(None);
    };
    let config = std::env::var_os("BRIDGE_OWNER_CONFIG").ok_or_else(refused)?;
    let cgroup = std::fs::read_to_string("/proc/self/cgroup")?;
    let domain = cgroup
        .trim()
        .strip_prefix("0::")
        .ok_or_else(refused)?
        .to_owned();
    if !domain.starts_with('/') || domain.split('/').any(|part| part == "..") {
        return Err(refused());
    }
    let unit = domain.rsplit('/').next().ok_or_else(refused)?.to_owned();
    let fields = properties(&unit).await?;
    let manager_pid = unsafe { libc::getppid() } as u32;
    if std::fs::read_to_string(format!("/proc/{manager_pid}/comm"))?.trim() != "systemd" {
        return Err(refused());
    }
    let manager_uid = std::fs::metadata(format!("/proc/{manager_pid}"))?.uid();
    let metadata = std::fs::metadata(format!("/sys/fs/cgroup{domain}"))?;
    let capsule = Capsule {
        version: 1,
        owner: owner.into(),
        generation: 1,
        initial_revision: 1,
        fingerprint: fingerprint.into(),
        pid: evidence.pid,
        starttime: evidence.proc_starttime,
        boot: evidence.boot_id.clone(),
        boottime: evidence.boottime_ns,
        captured_at: Utc::now().to_rfc3339(),
        database: file_binding(database)?,
        config: file_binding(Path::new(&config))?,
        binary: file_binding(&std::env::current_exe()?)?,
        manager_pid,
        manager_start: proc_start(manager_pid)?,
        manager_uid,
        unit,
        invocation: field(&fields, "InvocationID")?.into(),
        domain,
        domain_device: metadata.dev(),
        domain_inode: metadata.ino(),
        start_monotonic: field(&fields, "ExecMainStartTimestampMonotonic")?.into(),
    };
    common(&capsule, &fields)?;
    let members =
        std::fs::read_to_string(format!("/sys/fs/cgroup{}/cgroup.procs", capsule.domain))?;
    if capsule.invocation.len() != 32
        || field(&fields, "MainPID")? != evidence.pid.to_string()
        || field(&fields, "ControlGroup")? != capsule.domain
        || members.split_whitespace().collect::<Vec<_>>() != [evidence.pid.to_string()]
        || proc_start(evidence.pid as u32)? != evidence.proc_starttime as u64
        || manager_uid != unsafe { libc::geteuid() }
    {
        return Err(refused());
    }
    let bytes = serde_json::to_vec(&capsule).map_err(|_| refused())?;
    write_private(Path::new(&destination), &bytes)?;
    Ok(Some(
        serde_json::json!({"systemd_capsule_v1": format!("{:x}",Sha256::digest(&bytes))})
            .to_string(),
    ))
}

fn terminal(capsule: &Capsule, fields: &BTreeMap<String, String>) -> std::io::Result<Terminal> {
    common(capsule, fields)?;
    let active = field(fields, "ActiveState")?;
    let sub = field(fields, "SubState")?;
    if !matches!((active, sub), ("active", "exited") | ("failed", "failed"))
        || field(fields, "MainPID")? != "0"
        || !field(fields, "ControlGroup")?.is_empty()
    {
        return Err(refused());
    }
    let exit_monotonic = field(fields, "ExecMainExitTimestampMonotonic")?
        .parse::<u64>()
        .map_err(|_| refused())?;
    if exit_monotonic
        <= capsule
            .start_monotonic
            .parse::<u64>()
            .map_err(|_| refused())?
    {
        return Err(refused());
    }
    let code = field(fields, "ExecMainCode")?
        .parse::<i64>()
        .map_err(|_| refused())?;
    let status = field(fields, "ExecMainStatus")?
        .parse::<i64>()
        .map_err(|_| refused())?;
    if !(1..=3).contains(&code) || status < 0 {
        return Err(refused());
    }
    Ok(Terminal {
        invocation: capsule.invocation.clone(),
        active: active.into(),
        sub: sub.into(),
        result: field(fields, "Result")?.into(),
        code,
        status,
        exit_monotonic,
    })
}

async fn observe(path: &Path, database: &Path, config: &Path) -> std::io::Result<Proof> {
    let bytes = read_private(path)?;
    let capsule: Capsule = serde_json::from_slice(&bytes).map_err(|_| refused())?;
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let metadata = std::fs::metadata(database)?;
    if capsule.version != 1
        || capsule.generation != 1
        || capsule.initial_revision != 1
        || capsule.boot != boot.trim()
        || capsule.database.path != database.canonicalize()?
        || (capsule.database.device, capsule.database.inode) != (metadata.dev(), metadata.ino())
        || file_binding(config)? != capsule.config
        || file_binding(&capsule.binary.path)? != capsule.binary
        || proc_start(capsule.manager_pid)? != capsule.manager_start
        || std::fs::metadata(format!("/proc/{}", capsule.manager_pid))?.uid() != capsule.manager_uid
        || capsule.manager_uid != unsafe { libc::geteuid() }
        || capsule.domain_inode == 0
    {
        return Err(refused());
    }
    if Path::new(&format!("/proc/{}", capsule.pid)).exists() {
        return Err(refused());
    }
    let first = terminal(&capsule, &properties(&capsule.unit).await?)?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let second = terminal(&capsule, &properties(&capsule.unit).await?)?;
    if first != second {
        return Err(refused());
    }
    let clock = RuntimeOwnerEvidence::capture().ok_or_else(refused)?;
    let completed = serde_json::to_vec(&serde_json::json!({"start":capsule,"terminal":second}))
        .map_err(|_| refused())?;
    let terminal_path = path.with_extension("terminal.json");
    match write_private(&terminal_path, &completed) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if read_private(&terminal_path)? != completed {
                return Err(refused());
            }
        }
        Err(error) => return Err(error),
    }
    Ok(Proof {
        capsule,
        binding: serde_json::json!({"systemd_capsule_v1":format!("{:x}",Sha256::digest(&bytes))})
            .to_string(),
        terminal: second,
        observed_at: Utc::now().to_rfc3339(),
        boottime: clock.boottime_ns,
    })
}

fn parse(
    arguments: &[std::ffi::OsString],
) -> Result<(PathBuf, PathBuf, PathBuf, i64), &'static str> {
    if arguments.len() != 9 || arguments[0] != "recover-systemd-owner" {
        return Err("arguments");
    }
    let mut fields = BTreeMap::new();
    for pair in arguments[1..].as_chunks::<2>().0 {
        let key = pair[0].to_str().ok_or("arguments")?;
        if !["--database", "--config", "--capsule", "--revision"].contains(&key)
            || fields.insert(key, &pair[1]).is_some()
        {
            return Err("arguments");
        }
    }
    let database = PathBuf::from(fields["--database"]);
    let config_path = PathBuf::from(fields["--config"]);
    let revision = fields["--revision"]
        .to_str()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .ok_or("revision")?;
    Ok((
        database,
        config_path,
        PathBuf::from(fields["--capsule"]),
        revision,
    ))
}

pub(crate) async fn run(arguments: &[std::ffi::OsString]) -> Result<(), &'static str> {
    let (database, config_path, capsule_path, revision) = parse(arguments)?;
    let config = crate::config::load(&config_path).map_err(|_| "config")?;
    if config
        .bridge
        .database
        .canonicalize()
        .map_err(|_| "database")?
        != database.canonicalize().map_err(|_| "database")?
    {
        return Err("database");
    }
    let guard = crate::runtime_admission::Guard::acquire(&database, &config_path, "startup")
        .map_err(|_| "exclusion")?;
    let proof = observe(&capsule_path, &database, &config_path)
        .await
        .map_err(|_| "observation")?;
    let pool = crate::storage::connect(&database)
        .await
        .map_err(|_| "database")?;
    let administrative: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_binding")
        .fetch_one(&pool)
        .await
        .map_err(|_| "admin")?;
    if administrative != 0 {
        return Err("admin");
    }
    let context = crate::storage::recovery_eligibility::readback_nonadmin(
        &pool,
        config.transports.matrix.enabled,
    )
    .await
    .map_err(|_| "classification")?;
    let scope = crate::storage::recovery_eligibility::RecoveryScope::new(
        context,
        crate::storage::recovery_eligibility::EffectDomain::LocalHold,
        None,
    );
    let recovery =
        crate::storage::MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
    let collector = uuid::Uuid::now_v7().to_string();
    let epoch = crate::admission_hold::acquire_fence(
        &recovery,
        &scope,
        &collector,
        config.transports.matrix.enabled,
    )
    .await
    .map_err(|_| "fence")?;
    let store = crate::storage::ReliabilityStore::with_recovery(pool.clone(), scope);
    let result = store
        .finalize_systemd_owner(&proof, revision)
        .await
        .map_err(|_| "writer");
    let release = if let Some(epoch) = epoch {
        recovery
            .release_component_lease("MatrixSync", &collector, epoch, &Utc::now().to_rfc3339())
            .await
            .map_err(|_| "release")
    } else {
        Ok(())
    };
    pool.close().await;
    guard.revalidate().map_err(|_| "exclusion")?;
    if !result? {
        return Err("predicate");
    }
    release?;
    println!("recover-systemd-owner result=reaped reason=systemd-terminal-v1");
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod scoped_tests;
