use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

const O_NOFOLLOW: i32 = 0o400000;
const MAX_IDENTITY_CONTENT_SIZE: u64 = 1024 * 1024 * 1024;
const IDENTITY_HASH_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Sidecar {
    Absent,
    Present {
        device: u64,
        inode: u64,
        mode: u32,
        uid: u32,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct Sidecars {
    wal: Sidecar,
    shm: Sidecar,
    parent_device: u64,
    database_uid: u32,
}

impl Sidecars {
    pub(crate) fn capture(database: &Path) -> Result<Self, Error> {
        use std::os::unix::fs::MetadataExt;
        let database_meta = std::fs::symlink_metadata(database).map_err(|_| Error::Rejected)?;
        if !database_meta.is_file() || database_meta.nlink() != 1 {
            return Err(Error::Rejected);
        }
        let parent = database.parent().ok_or(Error::Rejected)?;
        let parent_meta = std::fs::symlink_metadata(parent).map_err(|_| Error::Rejected)?;
        let capture = |suffix: &str| {
            let path = PathBuf::from(format!("{}{suffix}", database.to_string_lossy()));
            match std::fs::symlink_metadata(path) {
                Ok(meta)
                    if meta.is_file()
                        && meta.nlink() == 1
                        && meta.mode() & 0o022 == 0
                        && meta.uid() == database_meta.uid()
                        && meta.dev() == parent_meta.dev() =>
                {
                    Ok(Sidecar::Present {
                        device: meta.dev(),
                        inode: meta.ino(),
                        mode: meta.mode(),
                        uid: meta.uid(),
                    })
                }
                Ok(_) => Err(Error::Rejected),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Sidecar::Absent),
                Err(_) => Err(Error::Rejected),
            }
        };
        Ok(Self {
            wal: capture("-wal")?,
            shm: capture("-shm")?,
            parent_device: parent_meta.dev(),
            database_uid: database_meta.uid(),
        })
    }

    pub(crate) fn revalidate(&self, database: &Path) -> Result<(), Error> {
        let after = Self::capture(database)?;
        if self.parent_device != after.parent_device || self.database_uid != after.database_uid {
            return Err(Error::Rejected);
        }
        match (&self.wal, &self.shm, &after.wal, &after.shm) {
            (before_wal, before_shm, after_wal, after_shm)
                if before_wal == after_wal && before_shm == after_shm =>
            {
                Ok(())
            }
            (
                Sidecar::Absent,
                Sidecar::Absent,
                Sidecar::Present { .. },
                Sidecar::Present { .. },
            )
            | (
                Sidecar::Present { .. },
                Sidecar::Present { .. },
                Sidecar::Absent,
                Sidecar::Absent,
            ) => Ok(()),
            (
                before_wal @ Sidecar::Present { .. },
                Sidecar::Absent,
                after_wal,
                Sidecar::Present { .. },
            ) if before_wal == after_wal => Ok(()),
            _ => Err(Error::Rejected),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("rejected")]
    Rejected,
    #[error("database")]
    Database,
}
impl Error {
    pub const fn status(&self) -> u8 {
        3
    }
    pub const fn slug(&self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::Database => "database",
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Recovered,
    AlreadyStopped,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    operation_id: String,
    isolated_root: String,
    marker: String,
    config: String,
    config_identity: String,
    database: String,
    database_identity: String,
    binary: String,
    binary_identity: String,
    unit: String,
    evidence: String,
    manifest_digest: String,
    pre_owner_state_digest: String,
    post_owner_state_digest: String,
    receipt_digest: String,
    receipt_sibling_digest: String,
    commit_heartbeat: String,
}

#[derive(Debug)]
pub struct PrepareArgs {
    root: PathBuf,
    config: PathBuf,
    database: PathBuf,
    unit: String,
    manifest: PathBuf,
    evidence: PathBuf,
    operation_id: String,
    commit_heartbeat: String,
}

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, Error> {
    if args.len() != 3 || args[0] != "reconcile-isolated-legacy-owner" || args[1] != "--manifest" {
        return Err(Error::Rejected);
    }
    Ok(PathBuf::from(&args[2]))
}

pub fn parse_prepare_args(args: &[std::ffi::OsString]) -> Result<PrepareArgs, Error> {
    const FLAGS: &[&str] = &[
        "--root",
        "--config",
        "--database",
        "--unit",
        "--manifest",
        "--evidence",
        "--operation-id",
        "--commit-heartbeat",
    ];
    if args.len() != 17 || args[0] != "prepare-isolated-legacy-owner-manifest" {
        return Err(Error::Rejected);
    }
    let mut values = Vec::with_capacity(FLAGS.len());
    for (index, flag) in FLAGS.iter().enumerate() {
        if args[index * 2 + 1] != *flag {
            return Err(Error::Rejected);
        }
        values.push(args[index * 2 + 2].clone());
    }
    let text = |index: usize| {
        values[index]
            .to_str()
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or(Error::Rejected)
    };
    let operation_id = text(6)?;
    uuid::Uuid::parse_str(&operation_id).map_err(|_| Error::Rejected)?;
    let commit_heartbeat = text(7)?;
    let parsed = chrono::DateTime::parse_from_rfc3339(&commit_heartbeat)
        .map_err(|_| Error::Rejected)?
        .with_timezone(&chrono::Utc)
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    if parsed != commit_heartbeat {
        return Err(Error::Rejected);
    }
    Ok(PrepareArgs {
        root: PathBuf::from(&values[0]),
        config: PathBuf::from(&values[1]),
        database: PathBuf::from(&values[2]),
        unit: text(3)?,
        manifest: PathBuf::from(&values[4]),
        evidence: PathBuf::from(&values[5]),
        operation_id,
        commit_heartbeat,
    })
}
pub(crate) fn digest(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update((p.len() as u64).to_be_bytes());
        h.update(p.as_bytes());
    }
    format!("{:x}", h.finalize())
}

pub(crate) fn path_identity(path: &Path, include_content: bool) -> Result<String, Error> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let canonical = path.canonicalize().map_err(|_| Error::Rejected)?;
    if canonical != path {
        return Err(Error::Rejected);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW)
        .open(path)
        .map_err(|_| Error::Rejected)?;
    let before = file.metadata().map_err(|_| Error::Rejected)?;
    if !before.is_file() {
        return Err(Error::Rejected);
    }
    let content = if include_content {
        format!(
            "{:x}",
            identity_content_hash_observed(&mut file, before.len(), |_, _| {})?
        )
    } else {
        "identity-only".to_owned()
    };
    let after = file.metadata().map_err(|_| Error::Rejected)?;
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.mode() != after.mode()
        || before.len() != after.len()
    {
        return Err(Error::Rejected);
    }
    Ok(digest(&[
        &canonical.to_string_lossy(),
        &before.dev().to_string(),
        &before.ino().to_string(),
        &before.mode().to_string(),
        &content,
    ]))
}

fn identity_content_hash_observed(
    file: &mut std::fs::File,
    initial_size: u64,
    mut observed: impl FnMut(u64, usize),
) -> Result<sha2::digest::Output<Sha256>, Error> {
    if initial_size > MAX_IDENTITY_CONTENT_SIZE {
        return Err(Error::Rejected);
    }
    let mut hash = Sha256::new();
    let mut buffer = [0u8; IDENTITY_HASH_BUFFER_SIZE];
    let mut total = 0u64;
    loop {
        let count = file.read(&mut buffer).map_err(|_| Error::Rejected)?;
        if count == 0 {
            observed(total, count);
            break;
        }
        total = total.checked_add(count as u64).ok_or(Error::Rejected)?;
        if total > initial_size || total > MAX_IDENTITY_CONTENT_SIZE {
            return Err(Error::Rejected);
        }
        hash.update(&buffer[..count]);
        observed(total, count);
    }
    if total != initial_size {
        return Err(Error::Rejected);
    }
    Ok(hash.finalize())
}
async fn logical_digest_projected(
    c: &mut SqliteConnection,
    stopped_owner: Option<(&str, &str)>,
) -> Result<String, Error> {
    const TABLES: &[(&str, &str, &[&str])] = &[
        (
            "runtime_instances",
            "instance_token",
            &[
                "instance_token",
                "started_at",
                "heartbeat_at",
                "state",
                "process_fingerprint",
            ],
        ),
        (
            "task_admissions",
            "task_id",
            &[
                "task_id",
                "state",
                "revision",
                "runtime_instance",
                "updated_at",
            ],
        ),
        (
            "agent_work_queue",
            "queue_id",
            &[
                "queue_id",
                "state",
                "revision",
                "runtime_owner",
                "owner_fence",
                "send_started",
                "updated_at",
            ],
        ),
        (
            "execution_leases",
            "resource_key",
            &[
                "resource_key",
                "task_id",
                "owner_token",
                "fence",
                "state",
                "heartbeat_at",
                "expires_at",
            ],
        ),
        (
            "task_continuations",
            "task_id",
            &[
                "task_id",
                "resource_key",
                "delivery_id",
                "lease_fence",
                "revision",
                "state",
                "heartbeat_at",
            ],
        ),
        (
            "delivery_dispositions",
            "delivery_id,task_id,attempt",
            &["delivery_id", "task_id", "attempt", "state", "reason_code"],
        ),
    ];
    let mut h = Sha256::new();
    let schema: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&mut *c)
        .await
        .map_err(|_| Error::Database)?;
    let user: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut *c)
        .await
        .map_err(|_| Error::Database)?;
    for value in [schema.to_string(), user.to_string()] {
        h.update((value.len() as u64).to_be_bytes());
        h.update(value);
    }
    for (table, order, columns) in TABLES {
        let query = format!(
            "SELECT {} FROM {table} ORDER BY {order}",
            columns
                .iter()
                .map(|v| format!("quote({v})"))
                .collect::<Vec<_>>()
                .join(",")
        );
        // Every identifier is selected from the static TABLES allow-list above.
        let rows = sqlx::query(sqlx::AssertSqlSafe(query))
            .fetch_all(&mut *c)
            .await
            .map_err(|_| Error::Database)?;
        h.update((table.len() as u64).to_be_bytes());
        h.update(table.as_bytes());
        for row in rows {
            for (index, column) in columns.iter().enumerate() {
                h.update((column.len() as u64).to_be_bytes());
                h.update(column.as_bytes());
                let mut value: String = row.try_get(index).map_err(|_| Error::Database)?;
                if *table == "runtime_instances"
                    && row.try_get::<String, _>(0).map_err(|_| Error::Database)?
                        == stopped_owner
                            .map(|projection| projection.0)
                            .unwrap_or_default()
                {
                    if *column == "heartbeat_at" {
                        value = format!(
                            "'{}'",
                            stopped_owner.ok_or(Error::Rejected)?.1.replace('\'', "''")
                        );
                    } else if *column == "state" {
                        value = "'stopped'".to_owned();
                    }
                }
                h.update((value.len() as u64).to_be_bytes());
                h.update(value);
            }
        }
    }
    Ok(format!("{:x}", h.finalize()))
}

async fn logical_digest(c: &mut SqliteConnection) -> Result<String, Error> {
    logical_digest_projected(c, None).await
}

async fn sibling_digest(c: &mut SqliteConnection, operation: &str) -> Result<String, Error> {
    let rows = sqlx::query("SELECT operation_id,receipt_digest FROM legacy_owner_reconciliation_receipts WHERE operation_id<>? ORDER BY operation_id")
        .bind(operation).fetch_all(&mut *c).await.map_err(|_| Error::Database)?;
    let mut values = Vec::new();
    for row in rows {
        values.push(row.try_get::<String, _>(0).map_err(|_| Error::Database)?);
        values.push(row.try_get::<String, _>(1).map_err(|_| Error::Database)?);
    }
    Ok(digest(
        &values.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

async fn eligible(c: &mut SqliteConnection) -> Result<(), Error> {
    let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_instances WHERE state IN ('active','stopping') AND process_fingerprint NOT LIKE 'pid:%'").fetch_one(&mut *c).await.map_err(|_|Error::Database)?;
    let other: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_instances WHERE state<>'stopped' AND NOT (state IN ('active','stopping') AND process_fingerprint NOT LIKE 'pid:%')").fetch_one(&mut *c).await.map_err(|_|Error::Database)?;
    let admissions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_admissions WHERE state<>'terminal'")
            .fetch_one(&mut *c)
            .await
            .map_err(|_| Error::Database)?;
    let queue: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_work_queue WHERE state NOT IN ('completed','expired','superseded')").fetch_one(&mut *c).await.map_err(|_|Error::Database)?;
    let leases: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_leases WHERE state<>'released'")
            .fetch_one(&mut *c)
            .await
            .map_err(|_| Error::Database)?;
    let continuations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_continuations WHERE state<>'terminal'")
            .fetch_one(&mut *c)
            .await
            .map_err(|_| Error::Database)?;
    let dispositions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM delivery_dispositions WHERE state NOT IN ('acknowledged','terminal')",
    )
    .fetch_one(&mut *c)
    .await
    .map_err(|_| Error::Database)?;
    if owners == 1
        && other == 0
        && admissions == 0
        && queue == 0
        && leases == 0
        && continuations == 0
        && dispositions == 0
    {
        Ok(())
    } else {
        Err(Error::Rejected)
    }
}

fn read_manifest(path: &Path) -> Result<(Manifest, Vec<u8>), Error> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW)
        .open(path)
        .map_err(|_| Error::Rejected)?;
    let before = file.metadata().map_err(|_| Error::Rejected)?;
    if !before.is_file() || before.mode() & 0o777 != 0o600 || before.nlink() != 1 {
        return Err(Error::Rejected);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|_| Error::Rejected)?;
    let after = file.metadata().map_err(|_| Error::Rejected)?;
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.mode() != after.mode()
        || after.nlink() != 1
    {
        return Err(Error::Rejected);
    }
    let manifest = serde_json::from_slice(&bytes).map_err(|_| Error::Rejected)?;
    Ok((manifest, bytes))
}

fn authenticate_root(m: &Manifest, manifest_path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let root = Path::new(&m.isolated_root);
    if root.canonicalize().map_err(|_| Error::Rejected)? != root || !root.is_dir() {
        return Err(Error::Rejected);
    }
    let root_meta = std::fs::symlink_metadata(root).map_err(|_| Error::Rejected)?;
    if root_meta.mode() & 0o022 != 0 {
        return Err(Error::Rejected);
    }
    let marker = Path::new(&m.marker);
    if marker != root.join(".t050-isolated-marker")
        || manifest_path.parent() != Some(root)
        || Path::new(&m.database).parent() != Some(root)
        || Path::new(&m.evidence).parent() != Some(root)
    {
        return Err(Error::Rejected);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW)
        .open(marker)
        .map_err(|_| Error::Rejected)?;
    let meta = file.metadata().map_err(|_| Error::Rejected)?;
    if !meta.is_file() || meta.mode() & 0o777 != 0o600 || meta.nlink() != 1 {
        return Err(Error::Rejected);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|_| Error::Rejected)?;
    let expected = format!(
        "t050-isolated-root-v1\noperation_id={}\nunit={}\n",
        m.operation_id, m.unit
    );
    if bytes != expected.as_bytes() {
        return Err(Error::Rejected);
    }
    Ok(())
}

pub(crate) fn preflight_unit(unit: &str) -> Result<(), Error> {
    let mut command = {
        #[cfg(debug_assertions)]
        {
            std::process::Command::new(
                std::env::var_os("T050_RECONCILE_TEST_SYSTEMCTL")
                    .unwrap_or_else(|| "systemctl".into()),
            )
        }
        #[cfg(not(debug_assertions))]
        {
            std::process::Command::new("systemctl")
        }
    };
    let output = command
        .args([
            "--user",
            "show",
            unit,
            "--property=ActiveState",
            "--property=SubState",
            "--property=MainPID",
        ])
        .output()
        .map_err(|_| Error::Rejected)?;
    if !output.status.success() || output.stderr.len() > 4096 {
        return Err(Error::Rejected);
    }
    let text = std::str::from_utf8(&output.stdout).map_err(|_| Error::Rejected)?;
    if parse_stopped_unit_state(text) {
        Ok(())
    } else {
        Err(Error::Rejected)
    }
}

fn parse_stopped_unit_state(text: &str) -> bool {
    let mut active = None;
    let mut sub = None;
    let mut pid = None;
    for line in text.lines() {
        let Some((key, value)) = line.split_once('=') else {
            #[cfg(debug_assertions)]
            return text.lines().collect::<Vec<_>>() == ["inactive", "dead", "0"];
            #[cfg(not(debug_assertions))]
            return false;
        };
        let slot = match key {
            "ActiveState" => &mut active,
            "SubState" => &mut sub,
            "MainPID" => &mut pid,
            _ => return false,
        };
        if slot.replace(value).is_some() {
            return false;
        }
    }
    active == Some("inactive") && sub == Some("dead") && pid == Some("0")
}

fn proc_root() -> PathBuf {
    #[cfg(debug_assertions)]
    if let Some(root) = std::env::var_os("T050_RECONCILE_TEST_PROC_ROOT") {
        return PathBuf::from(root);
    }
    PathBuf::from("/proc")
}

fn same_file(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

fn process_gone_after_child_not_found(
    process: &Path,
    authenticated: &std::fs::Metadata,
    area: RuntimeUnknownArea,
) -> Result<(), RuntimeAbsenceUnknown> {
    #[cfg(debug_assertions)]
    {
        let disappear = process.join(".t050-test-disappear-on-child-not-found");
        if disappear.exists() {
            std::fs::remove_file(disappear)
                .and_then(|()| std::fs::remove_dir(process))
                .map_err(|error| RuntimeAbsenceUnknown::io(area, &error))?;
        }
    }
    match std::fs::metadata(process) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(RuntimeAbsenceUnknown::io(area, &error)),
        Ok(current) if same_file(&current, authenticated) => Err(RuntimeAbsenceUnknown::io(
            area,
            &std::io::Error::from(std::io::ErrorKind::NotFound),
        )),
        Ok(_) => Err(RuntimeAbsenceUnknown::parse(area)),
    }
}

fn observed_process_uid(
    process: &Path,
    metadata: &std::fs::Metadata,
) -> Result<u32, RuntimeAbsenceUnknown> {
    use std::os::unix::fs::MetadataExt;
    #[cfg(debug_assertions)]
    {
        // Process fixtures preserve real exe/cmdline/fd facts while varying ownership.
        let override_path = process.join(".t050-test-uid");
        match std::fs::read_to_string(override_path) {
            Ok(value) => {
                return value
                    .trim()
                    .parse()
                    .map_err(|_| RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Identity));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(RuntimeAbsenceUnknown::io(
                    RuntimeUnknownArea::Identity,
                    &error,
                ));
            }
        }
    }
    Ok(metadata.uid())
}

fn is_deployment_presence(
    _process_uid: u32,
    _config_uid: u32,
    executable_matches: bool,
    config_matches: bool,
    listener_matches: bool,
) -> bool {
    executable_matches && (config_matches || listener_matches)
}

fn deployment_presence_kind(config_matches: bool, listener_matches: bool) -> RuntimeAbsence {
    if config_matches {
        RuntimeAbsence::Process
    } else if listener_matches {
        RuntimeAbsence::Listener
    } else {
        RuntimeAbsence::Clean
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RuntimeAbsence {
    Clean,
    Process,
    Listener,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeUnknownArea {
    Procfs,
    Identity,
    DeploymentMetadata,
    ProcessExecutableLinkDeploymentOwner,
    ProcessExecutableLinkPrivilegedKernelOwner,
    ProcessExecutableLinkPrivilegedUserspaceOwner,
    ProcessExecutableLinkPrivilegedOwnerClassification,
    ProcessExecutableLinkUnprivilegedOtherOwner,
    ProcessExecutableMetadata,
    Cmdline,
    Descriptor,
    Socket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeUnknownReason {
    Permission,
    Io,
    Parse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RuntimeAbsenceUnknown {
    area: RuntimeUnknownArea,
    reason: RuntimeUnknownReason,
}

impl RuntimeAbsenceUnknown {
    fn io(area: RuntimeUnknownArea, error: &std::io::Error) -> Self {
        Self {
            area,
            reason: if error.kind() == std::io::ErrorKind::PermissionDenied {
                RuntimeUnknownReason::Permission
            } else {
                RuntimeUnknownReason::Io
            },
        }
    }

    fn parse(area: RuntimeUnknownArea) -> Self {
        Self {
            area,
            reason: RuntimeUnknownReason::Parse,
        }
    }

    pub(crate) fn cause(self) -> &'static str {
        match (self.area, self.reason) {
            (RuntimeUnknownArea::Procfs, RuntimeUnknownReason::Permission) => {
                "runtime-unknown-procfs-permission"
            }
            (RuntimeUnknownArea::Procfs, RuntimeUnknownReason::Io) => "runtime-unknown-procfs-io",
            (RuntimeUnknownArea::Procfs, RuntimeUnknownReason::Parse) => {
                "runtime-unknown-procfs-parse"
            }
            (RuntimeUnknownArea::Identity, RuntimeUnknownReason::Permission) => {
                "runtime-unknown-identity-permission"
            }
            (RuntimeUnknownArea::Identity, RuntimeUnknownReason::Io) => {
                "runtime-unknown-identity-io"
            }
            (RuntimeUnknownArea::Identity, RuntimeUnknownReason::Parse) => {
                "runtime-unknown-identity-parse"
            }
            (RuntimeUnknownArea::DeploymentMetadata, RuntimeUnknownReason::Permission) => {
                "runtime-unknown-deployment-metadata-permission"
            }
            (RuntimeUnknownArea::DeploymentMetadata, RuntimeUnknownReason::Io) => {
                "runtime-unknown-deployment-metadata-io"
            }
            (RuntimeUnknownArea::DeploymentMetadata, RuntimeUnknownReason::Parse) => {
                "runtime-unknown-deployment-metadata-parse"
            }
            (
                RuntimeUnknownArea::ProcessExecutableLinkDeploymentOwner,
                RuntimeUnknownReason::Permission,
            ) => "runtime-unknown-process-executable-link-deployment-owner-permission",
            (
                RuntimeUnknownArea::ProcessExecutableLinkDeploymentOwner,
                RuntimeUnknownReason::Io,
            ) => "runtime-unknown-process-executable-link-deployment-owner-io",
            (
                RuntimeUnknownArea::ProcessExecutableLinkDeploymentOwner,
                RuntimeUnknownReason::Parse,
            ) => "runtime-unknown-process-executable-link-deployment-owner-parse",
            (
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedKernelOwner,
                RuntimeUnknownReason::Permission,
            ) => "runtime-unknown-process-executable-link-privileged-kernel-owner-permission",
            (
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedKernelOwner,
                RuntimeUnknownReason::Io,
            ) => "runtime-unknown-process-executable-link-privileged-kernel-owner-io",
            (
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedKernelOwner,
                RuntimeUnknownReason::Parse,
            ) => "runtime-unknown-process-executable-link-privileged-kernel-owner-parse",
            (
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedUserspaceOwner,
                RuntimeUnknownReason::Permission,
            ) => "runtime-unknown-process-executable-link-privileged-userspace-owner-permission",
            (
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedUserspaceOwner,
                RuntimeUnknownReason::Io,
            ) => "runtime-unknown-process-executable-link-privileged-userspace-owner-io",
            (
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedUserspaceOwner,
                RuntimeUnknownReason::Parse,
            ) => "runtime-unknown-process-executable-link-privileged-userspace-owner-parse",
            (
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedOwnerClassification,
                RuntimeUnknownReason::Permission,
            ) => {
                "runtime-unknown-process-executable-link-privileged-owner-classification-permission"
            }
            (
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedOwnerClassification,
                RuntimeUnknownReason::Io,
            ) => "runtime-unknown-process-executable-link-privileged-owner-classification-io",
            (
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedOwnerClassification,
                RuntimeUnknownReason::Parse,
            ) => "runtime-unknown-process-executable-link-privileged-owner-classification-parse",
            (
                RuntimeUnknownArea::ProcessExecutableLinkUnprivilegedOtherOwner,
                RuntimeUnknownReason::Permission,
            ) => "runtime-unknown-process-executable-link-unprivileged-other-owner-permission",
            (
                RuntimeUnknownArea::ProcessExecutableLinkUnprivilegedOtherOwner,
                RuntimeUnknownReason::Io,
            ) => "runtime-unknown-process-executable-link-unprivileged-other-owner-io",
            (
                RuntimeUnknownArea::ProcessExecutableLinkUnprivilegedOtherOwner,
                RuntimeUnknownReason::Parse,
            ) => "runtime-unknown-process-executable-link-unprivileged-other-owner-parse",
            (RuntimeUnknownArea::ProcessExecutableMetadata, RuntimeUnknownReason::Permission) => {
                "runtime-unknown-process-executable-metadata-permission"
            }
            (RuntimeUnknownArea::ProcessExecutableMetadata, RuntimeUnknownReason::Io) => {
                "runtime-unknown-process-executable-metadata-io"
            }
            (RuntimeUnknownArea::ProcessExecutableMetadata, RuntimeUnknownReason::Parse) => {
                "runtime-unknown-process-executable-metadata-parse"
            }
            (RuntimeUnknownArea::Cmdline, RuntimeUnknownReason::Permission) => {
                "runtime-unknown-cmdline-permission"
            }
            (RuntimeUnknownArea::Cmdline, RuntimeUnknownReason::Io) => "runtime-unknown-cmdline-io",
            (RuntimeUnknownArea::Cmdline, RuntimeUnknownReason::Parse) => {
                "runtime-unknown-cmdline-parse"
            }
            (RuntimeUnknownArea::Descriptor, RuntimeUnknownReason::Permission) => {
                "runtime-unknown-fd-permission"
            }
            (RuntimeUnknownArea::Descriptor, RuntimeUnknownReason::Io) => "runtime-unknown-fd-io",
            (RuntimeUnknownArea::Descriptor, RuntimeUnknownReason::Parse) => {
                "runtime-unknown-fd-parse"
            }
            (RuntimeUnknownArea::Socket, RuntimeUnknownReason::Permission) => {
                "runtime-unknown-socket-permission"
            }
            (RuntimeUnknownArea::Socket, RuntimeUnknownReason::Io) => "runtime-unknown-socket-io",
            (RuntimeUnknownArea::Socket, RuntimeUnknownReason::Parse) => {
                "runtime-unknown-socket-parse"
            }
        }
    }
}

fn privileged_link_area(process: &Path) -> Result<RuntimeUnknownArea, RuntimeAbsenceUnknown> {
    let status = std::fs::read_to_string(process.join("status")).map_err(|error| {
        RuntimeAbsenceUnknown::io(
            RuntimeUnknownArea::ProcessExecutableLinkPrivilegedOwnerClassification,
            &error,
        )
    })?;
    let mut kernel = None;
    for line in status.lines() {
        let Some(value) = line.strip_prefix("Kthread:") else {
            continue;
        };
        if kernel.is_some() {
            return Err(RuntimeAbsenceUnknown::parse(
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedOwnerClassification,
            ));
        }
        kernel = match value.trim() {
            "0" => Some(false),
            "1" => Some(true),
            _ => {
                return Err(RuntimeAbsenceUnknown::parse(
                    RuntimeUnknownArea::ProcessExecutableLinkPrivilegedOwnerClassification,
                ));
            }
        };
    }
    match kernel {
        Some(true) => Ok(RuntimeUnknownArea::ProcessExecutableLinkPrivilegedKernelOwner),
        Some(false) => Ok(RuntimeUnknownArea::ProcessExecutableLinkPrivilegedUserspaceOwner),
        None => Err(RuntimeAbsenceUnknown::parse(
            RuntimeUnknownArea::ProcessExecutableLinkPrivilegedOwnerClassification,
        )),
    }
}

fn process_executable_metadata(
    process: &Path,
    process_uid: u32,
    config_uid: u32,
) -> Result<Option<std::fs::Metadata>, RuntimeAbsenceUnknown> {
    let executable = process.join("exe");
    let link_area = || {
        if process_uid == config_uid {
            Ok(RuntimeUnknownArea::ProcessExecutableLinkDeploymentOwner)
        } else if process_uid == 0 {
            privileged_link_area(process)
        } else {
            Ok(RuntimeUnknownArea::ProcessExecutableLinkUnprivilegedOtherOwner)
        }
    };
    #[cfg(debug_assertions)]
    if process.join(".t050-test-exe-link-permission").exists() {
        let error = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        return Err(RuntimeAbsenceUnknown::io(link_area()?, &error));
    }
    match std::fs::read_link(&executable) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(RuntimeAbsenceUnknown::io(link_area()?, &error)),
    }
    match std::fs::metadata(executable) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(RuntimeAbsenceUnknown::io(
            RuntimeUnknownArea::ProcessExecutableMetadata,
            &error,
        )),
    }
}

fn classify_process_or_listener(
    binary: &Path,
    config: &Path,
    listener_inodes: &std::collections::BTreeSet<u64>,
    ignored_pid: Option<u32>,
) -> Result<RuntimeAbsence, RuntimeAbsenceUnknown> {
    use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};

    let root = proc_root();
    let binary_meta = std::fs::metadata(binary).map_err(|error| {
        RuntimeAbsenceUnknown::io(RuntimeUnknownArea::DeploymentMetadata, &error)
    })?;
    let config_meta = std::fs::metadata(config).map_err(|error| {
        RuntimeAbsenceUnknown::io(RuntimeUnknownArea::DeploymentMetadata, &error)
    })?;
    let config_uid = config_meta.uid();
    let entries = std::fs::read_dir(&root)
        .map_err(|error| RuntimeAbsenceUnknown::io(RuntimeUnknownArea::Procfs, &error))?;
    'processes: for entry in entries {
        let entry =
            entry.map_err(|error| RuntimeAbsenceUnknown::io(RuntimeUnknownArea::Procfs, &error))?;
        if entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
            .is_none()
        {
            continue;
        }
        let pid = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Procfs))?;
        if Some(pid) == ignored_pid {
            continue;
        }
        let process = entry.path();
        let process_meta = match std::fs::metadata(&process) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(RuntimeAbsenceUnknown::io(
                    RuntimeUnknownArea::Procfs,
                    &error,
                ));
            }
        };
        let process_uid = observed_process_uid(&process, &process_meta)?;
        let command_line = match std::fs::read(process.join("cmdline")) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                process_gone_after_child_not_found(
                    &process,
                    &process_meta,
                    RuntimeUnknownArea::Cmdline,
                )?;
                continue;
            }
            Err(error) => {
                return Err(RuntimeAbsenceUnknown::io(
                    RuntimeUnknownArea::Cmdline,
                    &error,
                ));
            }
        };
        if command_line.is_empty() || command_line.last() != Some(&0) {
            return Err(RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Cmdline));
        }
        let config_argument = command_line
            .split(|byte| *byte == 0)
            .any(|argument| argument == config.as_os_str().as_bytes());

        let descriptors = match std::fs::read_dir(process.join("fd")) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                process_gone_after_child_not_found(
                    &process,
                    &process_meta,
                    RuntimeUnknownArea::Descriptor,
                )?;
                continue;
            }
            Err(error) => {
                return Err(RuntimeAbsenceUnknown::io(
                    RuntimeUnknownArea::Descriptor,
                    &error,
                ));
            }
        };
        let mut config_descriptor = false;
        let mut listener_descriptor = false;
        for descriptor in descriptors {
            let descriptor = descriptor.map_err(|error| {
                RuntimeAbsenceUnknown::io(RuntimeUnknownArea::Descriptor, &error)
            })?;
            match std::fs::metadata(descriptor.path()) {
                Ok(metadata) => {
                    config_descriptor |= same_file(&metadata, &config_meta);
                    listener_descriptor |= listener_inodes.contains(&metadata.ino());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    process_gone_after_child_not_found(
                        &process,
                        &process_meta,
                        RuntimeUnknownArea::Descriptor,
                    )?;
                    continue 'processes;
                }
                Err(error) => {
                    return Err(RuntimeAbsenceUnknown::io(
                        RuntimeUnknownArea::Descriptor,
                        &error,
                    ));
                }
            }
        }
        let config_matches = config_argument || config_descriptor;
        if !config_matches && !listener_descriptor {
            continue;
        }

        let executable_meta = match process_executable_metadata(&process, process_uid, config_uid)?
        {
            Some(metadata) => metadata,
            None => {
                process_gone_after_child_not_found(
                    &process,
                    &process_meta,
                    RuntimeUnknownArea::ProcessExecutableMetadata,
                )?;
                continue;
            }
        };
        if !same_file(&executable_meta, &binary_meta) {
            continue;
        }
        if is_deployment_presence(
            process_uid,
            config_uid,
            true,
            config_matches,
            listener_descriptor,
        ) {
            return Ok(deployment_presence_kind(
                config_matches,
                listener_descriptor,
            ));
        }
    }
    Ok(RuntimeAbsence::Clean)
}

fn fixed_listener_inodes() -> Result<std::collections::BTreeSet<u64>, RuntimeAbsenceUnknown> {
    let root = proc_root();
    let mut inodes = std::collections::BTreeSet::new();
    for table in [root.join("net/tcp"), root.join("net/tcp6")] {
        let text = std::fs::read_to_string(table)
            .map_err(|error| RuntimeAbsenceUnknown::io(RuntimeUnknownArea::Socket, &error))?;
        let mut lines = text.lines();
        let header = lines
            .next()
            .ok_or_else(|| RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Socket))?;
        if !header
            .split_whitespace()
            .any(|field| field == "local_address")
            || !header.split_whitespace().any(|field| field == "st")
        {
            return Err(RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Socket));
        }
        for line in lines {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 10 {
                return Err(RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Socket));
            }
            let (_, port) = fields[1]
                .rsplit_once(':')
                .ok_or_else(|| RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Socket))?;
            let port = u16::from_str_radix(port, 16)
                .map_err(|_| RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Socket))?;
            if fields[3] == "0A" && matches!(port, 9291 | 9292) {
                inodes.insert(
                    fields[9]
                        .parse::<u64>()
                        .map_err(|_| RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Socket))?,
                );
            }
        }
    }
    Ok(inodes)
}

pub(crate) fn preflight_runtime_absence(
    binary: &Path,
    config: &Path,
    ignored_pid: Option<u32>,
) -> Result<(), Error> {
    match classify_runtime_absence(binary, config, ignored_pid).map_err(|_| Error::Rejected)? {
        RuntimeAbsence::Clean => Ok(()),
        RuntimeAbsence::Process | RuntimeAbsence::Listener => Err(Error::Rejected),
    }
}

pub(crate) fn preflight_runtime_absence_authenticated(
    binary: &Path,
    config: &Path,
    ignored_pid: Option<u32>,
    admission: &crate::runtime_admission::Guard,
) -> Result<(), Error> {
    match classify_runtime_absence_authenticated(binary, config, ignored_pid, admission)
        .map_err(|_| Error::Rejected)?
    {
        RuntimeAbsence::Clean => Ok(()),
        RuntimeAbsence::Process | RuntimeAbsence::Listener => Err(Error::Rejected),
    }
}

pub(crate) fn classify_runtime_absence_authenticated(
    binary: &Path,
    config: &Path,
    ignored_pid: Option<u32>,
    admission: &crate::runtime_admission::Guard,
) -> Result<RuntimeAbsence, RuntimeAbsenceUnknown> {
    #[cfg(debug_assertions)]
    if std::env::var_os("T050_RECONCILE_TEST_PROC_ROOT").is_some() {
        return classify_runtime_absence(binary, config, ignored_pid);
    }
    match crate::privileged_proc_proof::prove_runtime_absence(admission)
        .map_err(|_| RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Procfs))?
    {
        crate::privileged_proc_proof::ProofResult::Clean => Ok(RuntimeAbsence::Clean),
        crate::privileged_proc_proof::ProofResult::Process => Ok(RuntimeAbsence::Process),
        crate::privileged_proc_proof::ProofResult::Listener => Ok(RuntimeAbsence::Listener),
        crate::privileged_proc_proof::ProofResult::Unknown => {
            Err(RuntimeAbsenceUnknown::parse(RuntimeUnknownArea::Procfs))
        }
    }
}

pub(crate) fn classify_runtime_absence(
    binary: &Path,
    config: &Path,
    ignored_pid: Option<u32>,
) -> Result<RuntimeAbsence, RuntimeAbsenceUnknown> {
    let listener_inodes = fixed_listener_inodes()?;
    classify_process_or_listener(binary, config, &listener_inodes, ignored_pid)
}

pub(crate) fn publish(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let parent = path.parent().ok_or(Error::Rejected)?;
    if let Ok(meta) = std::fs::symlink_metadata(path)
        && (!meta.is_file() || meta.mode() & 0o777 != 0o600 || meta.nlink() != 1)
    {
        return Err(Error::Rejected);
    }
    let temp = parent.join(format!(".t050-evidence.{}", uuid::Uuid::now_v7()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(O_NOFOLLOW)
        .open(&temp)
        .map_err(|_| Error::Rejected)?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| Error::Rejected)?;
    std::fs::rename(&temp, path).map_err(|_| Error::Rejected)?;
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|_| Error::Rejected)
}

pub(crate) fn preflight_publication(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() && meta.mode() & 0o777 == 0o600 && meta.nlink() == 1 => Ok(()),
        Ok(_) => Err(Error::Rejected),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(Error::Rejected),
    }
}

pub(crate) fn publish_new(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    use std::os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt},
    };

    fn rename_noreplace(from: &Path, to: &Path) -> std::io::Result<()> {
        use std::ffi::{CString, c_char, c_int, c_uint};
        const AT_FDCWD: c_int = -100;
        const RENAME_NOREPLACE: c_uint = 1;
        unsafe extern "C" {
            fn renameat2(
                olddirfd: c_int,
                oldpath: *const c_char,
                newdirfd: c_int,
                newpath: *const c_char,
                flags: c_uint,
            ) -> c_int;
        }
        let from = CString::new(from.as_os_str().as_bytes())
            .map_err(|_| std::io::Error::other("invalid temporary path"))?;
        let to = CString::new(to.as_os_str().as_bytes())
            .map_err(|_| std::io::Error::other("invalid publication path"))?;
        // Linux renameat2 provides the atomic no-replace publication required here.
        if unsafe {
            renameat2(
                AT_FDCWD,
                from.as_ptr(),
                AT_FDCWD,
                to.as_ptr(),
                RENAME_NOREPLACE,
            )
        } == 0
        {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
    let parent = path.parent().ok_or(Error::Rejected)?;
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(Error::Rejected),
    }
    let temp = parent.join(format!(".t050-manifest.{}", uuid::Uuid::now_v7()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(O_NOFOLLOW)
        .open(&temp)
        .map_err(|_| Error::Rejected)?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| Error::Rejected)?;
    let metadata = file.metadata().map_err(|_| Error::Rejected)?;
    if !metadata.is_file() || metadata.mode() & 0o777 != 0o600 || metadata.nlink() != 1 {
        let _ = std::fs::remove_file(&temp);
        return Err(Error::Rejected);
    }
    if rename_noreplace(&temp, path).is_err() {
        let _ = std::fs::remove_file(&temp);
        return Err(Error::Rejected);
    }
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| Error::Rejected)?;
    Ok(())
}

pub(crate) fn ensure_marker(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    match std::fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.is_file() && metadata.mode() & 0o777 == 0o600 && metadata.nlink() == 1 =>
        {
            let mut file = OpenOptions::new()
                .read(true)
                .custom_flags(O_NOFOLLOW)
                .open(path)
                .map_err(|_| Error::Rejected)?;
            let mut observed = Vec::new();
            file.read_to_end(&mut observed)
                .map_err(|_| Error::Rejected)?;
            if observed == bytes {
                Ok(())
            } else {
                Err(Error::Rejected)
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => publish_new(path, bytes),
        _ => Err(Error::Rejected),
    }
}

#[cfg(debug_assertions)]
fn precommit_test_pause() -> Result<(), Error> {
    use std::os::unix::fs::OpenOptionsExt;
    let Some(path) = std::env::var_os("T050_RECONCILE_TEST_PRECOMMIT") else {
        return Ok(());
    };
    let path = PathBuf::from(path);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(O_NOFOLLOW)
        .open(&path)
        .map_err(|_| Error::Rejected)?;
    file.write_all(b"ready\n")
        .and_then(|_| file.sync_all())
        .map_err(|_| Error::Rejected)?;
    while !path.with_extension("continue").exists() {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn precommit_test_pause() -> Result<(), Error> {
    Ok(())
}
pub async fn reconcile(path: impl AsRef<Path>) -> Result<Outcome, Error> {
    let (m, manifest_bytes) = read_manifest(path.as_ref())?;
    authenticate_root(&m, path.as_ref())?;
    let binary = std::env::current_exe()
        .map_err(|_| Error::Rejected)?
        .canonicalize()
        .map_err(|_| Error::Rejected)?;
    if Path::new(&m.binary)
        .canonicalize()
        .map_err(|_| Error::Rejected)?
        != binary
    {
        return Err(Error::Rejected);
    }
    if path_identity(Path::new(&m.config), true)? != m.config_identity
        || path_identity(Path::new(&m.database), false)? != m.database_identity
        || path_identity(&binary, true)? != m.binary_identity
    {
        return Err(Error::Rejected);
    }
    let admission = crate::runtime_admission::Guard::acquire(
        Path::new(&m.database),
        Path::new(&m.config),
        "reconcile",
    )
    .map_err(|_| Error::Rejected)?;
    if digest(&[
        &m.operation_id,
        &m.isolated_root,
        &m.marker,
        &m.config,
        &m.config_identity,
        &m.database,
        &m.database_identity,
        &m.binary,
        &m.binary_identity,
        &m.unit,
        &m.evidence,
        &m.pre_owner_state_digest,
        &m.post_owner_state_digest,
        &m.commit_heartbeat,
    ]) != m.manifest_digest
    {
        return Err(Error::Rejected);
    }
    preflight_unit(&m.unit)?;
    preflight_runtime_absence(&binary, Path::new(&m.config), None)?;
    preflight_publication(Path::new(&m.evidence))?;
    let sidecars = Sidecars::capture(Path::new(&m.database))?;
    let opts = SqliteConnectOptions::new()
        .filename(&m.database)
        .create_if_missing(false)
        .foreign_keys(true);
    let mut c = SqliteConnection::connect_with(&opts)
        .await
        .map_err(|_| Error::Database)?;
    sidecars.revalidate(Path::new(&m.database))?;
    preflight_unit(&m.unit)?;
    preflight_runtime_absence(&binary, Path::new(&m.config), None)?;
    let (_, revalidated_manifest) = read_manifest(path.as_ref())?;
    if revalidated_manifest != manifest_bytes {
        return Err(Error::Rejected);
    }
    admission.revalidate().map_err(|_| Error::Rejected)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut c)
        .await
        .map_err(|_| Error::Database)?;
    let result=async {
        admission.revalidate().map_err(|_|Error::Rejected)?;
        if sibling_digest(&mut c,&m.operation_id).await? != m.receipt_sibling_digest { return Err(Error::Rejected); }
        if let Some(r)=sqlx::query("SELECT manifest_digest,pre_owner_state_digest,post_owner_state_digest,receipt_digest,receipt_sibling_digest,commit_heartbeat FROM legacy_owner_reconciliation_receipts WHERE operation_id=?").bind(&m.operation_id).fetch_optional(&mut c).await.map_err(|_|Error::Database)? {
            let values:(String,String,String,String,String,String)=(r.get(0),r.get(1),r.get(2),r.get(3),r.get(4),r.get(5));
            let expected_receipt=digest(&[&m.operation_id,&m.manifest_digest,&m.pre_owner_state_digest,&m.post_owner_state_digest,&m.commit_heartbeat,"recovered"]);
            if expected_receipt!=m.receipt_digest || values!=(m.manifest_digest.clone(),m.pre_owner_state_digest.clone(),m.post_owner_state_digest.clone(),m.receipt_digest.clone(),m.receipt_sibling_digest.clone(),m.commit_heartbeat.clone()) || logical_digest(&mut c).await?!=m.post_owner_state_digest { return Err(Error::Rejected); }
            return Ok(Outcome::AlreadyStopped)
        }
        if logical_digest(&mut c).await?!=m.pre_owner_state_digest { return Err(Error::Rejected); }
        eligible(&mut c).await?;
        let rows=sqlx::query("SELECT instance_token,process_fingerprint,state,started_at,heartbeat_at FROM runtime_instances WHERE state IN ('active','stopping')").fetch_all(&mut c).await.map_err(|_|Error::Database)?;
        if rows.len()!=1 { return Err(Error::Rejected); } let r=&rows[0]; let fp:String=r.get(1); if fp.is_empty() || fp.starts_with("pid:") || !fp.is_ascii() || fp.chars().any(char::is_whitespace){return Err(Error::Rejected)}
        let changed=sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token=? AND process_fingerprint=? AND state=? AND started_at=? AND heartbeat_at=?").bind(&m.commit_heartbeat).bind(r.get::<String,_>(0)).bind(fp).bind(r.get::<String,_>(2)).bind(r.get::<String,_>(3)).bind(r.get::<String,_>(4)).execute(&mut c).await.map_err(|_|Error::Database)?;
        if changed.rows_affected()!=1 || logical_digest(&mut c).await?!=m.post_owner_state_digest {return Err(Error::Rejected)}
        let rd=digest(&[&m.operation_id,&m.manifest_digest,&m.pre_owner_state_digest,&m.post_owner_state_digest,&m.commit_heartbeat,"recovered"]); if rd!=m.receipt_digest{return Err(Error::Rejected)}
        sqlx::query("INSERT INTO legacy_owner_reconciliation_receipts VALUES(?,?,?,?,?,?,?,'recovered')").bind(&m.operation_id).bind(&m.manifest_digest).bind(&m.pre_owner_state_digest).bind(&m.post_owner_state_digest).bind(&m.receipt_digest).bind(&m.receipt_sibling_digest).bind(&m.commit_heartbeat).execute(&mut c).await.map_err(|_|Error::Database)?;
        precommit_test_pause()?;
        Ok(Outcome::Recovered)
    }.await;
    match result {
        Ok(v) => {
            admission.revalidate().map_err(|_| Error::Rejected)?;
            preflight_unit(&m.unit)?;
            preflight_runtime_absence(&binary, Path::new(&m.config), None)?;
            sqlx::query("COMMIT")
                .execute(&mut c)
                .await
                .map_err(|_| Error::Database)?;
            sidecars.revalidate(Path::new(&m.database))?;
            #[cfg(debug_assertions)]
            if std::env::var_os("T050_RECONCILE_TEST_LOSE_EVIDENCE").is_some() {
                return Err(Error::Rejected);
            }
            let evidence = format!(
                "t050-legacy-reconciliation status=0 result={} manifest={} receipt={}\n",
                if v == Outcome::Recovered {
                    "recovered"
                } else {
                    "already-stopped"
                },
                Sha256::digest(&manifest_bytes)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
                m.receipt_digest
            );
            publish(Path::new(&m.evidence), evidence.as_bytes())?;
            Ok(v)
        }
        Err(e) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut c).await;
            sidecars.revalidate(Path::new(&m.database))?;
            Err(e)
        }
    }
}

pub async fn prepare_manifest(args: PrepareArgs) -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;

    let root = args.root.canonicalize().map_err(|_| Error::Rejected)?;
    let root_metadata = std::fs::symlink_metadata(&root).map_err(|_| Error::Rejected)?;
    if root != args.root
        || !root_metadata.is_dir()
        || root_metadata.mode() & 0o022 != 0
        || args.unit != "guigu-codex-test.service"
    {
        return Err(Error::Rejected);
    }
    let config = args.config.canonicalize().map_err(|_| Error::Rejected)?;
    let database = args.database.canonicalize().map_err(|_| Error::Rejected)?;
    let binary = std::env::current_exe()
        .map_err(|_| Error::Rejected)?
        .canonicalize()
        .map_err(|_| Error::Rejected)?;
    if config != args.config || database != args.database {
        return Err(Error::Rejected);
    }
    let initial_config_identity = path_identity(&config, true)?;
    let initial_database_identity = path_identity(&database, false)?;
    let initial_binary_identity = path_identity(&binary, true)?;
    let marker = root.join(".t050-isolated-marker");
    if args.database.parent() != Some(root.as_path())
        || args.manifest.parent() != Some(root.as_path())
        || args.evidence.parent() != Some(root.as_path())
    {
        return Err(Error::Rejected);
    }
    match std::fs::symlink_metadata(&args.manifest) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(Error::Rejected),
    }
    let admission = crate::runtime_admission::Guard::acquire(&database, &config, "reconcile")
        .map_err(|_| Error::Rejected)?;
    preflight_unit(&args.unit)?;
    preflight_runtime_absence(&binary, &config, Some(std::process::id()))?;
    preflight_publication(&args.evidence)?;
    let sidecars = Sidecars::capture(&database)?;
    let mut connection = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(false)
            .foreign_keys(true),
    )
    .await
    .map_err(|_| Error::Database)?;
    sidecars.revalidate(&database)?;
    preflight_unit(&args.unit)?;
    preflight_runtime_absence(&binary, &config, Some(std::process::id()))?;
    admission.revalidate().map_err(|_| Error::Rejected)?;
    sqlx::query("BEGIN")
        .execute(&mut connection)
        .await
        .map_err(|_| Error::Database)?;
    let generated = async {
        eligible(&mut connection).await?;
        let owners = sqlx::query(
            "SELECT quote(instance_token) FROM runtime_instances WHERE state IN ('active','stopping')",
        )
        .fetch_all(&mut connection)
        .await
        .map_err(|_| Error::Database)?;
        if owners.len() != 1 {
            return Err(Error::Rejected);
        }
        let owner: String = owners[0].try_get(0).map_err(|_| Error::Database)?;
        let pre = logical_digest(&mut connection).await?;
        let post =
            logical_digest_projected(&mut connection, Some((&owner, &args.commit_heartbeat)))
                .await?;
        let siblings = sibling_digest(&mut connection, &args.operation_id).await?;
        Ok((pre, post, siblings))
    }
    .await;
    let _ = sqlx::query("ROLLBACK").execute(&mut connection).await;
    connection.close().await.map_err(|_| Error::Database)?;
    let (pre, post, receipt_sibling_digest) = generated?;
    sidecars.revalidate(&database)?;
    admission.revalidate().map_err(|_| Error::Rejected)?;
    preflight_unit(&args.unit)?;
    preflight_runtime_absence(&binary, &config, Some(std::process::id()))?;
    let config_identity = path_identity(&config, true)?;
    let database_identity = path_identity(&database, false)?;
    let binary_identity = path_identity(&binary, true)?;
    if config_identity != initial_config_identity
        || database_identity != initial_database_identity
        || binary_identity != initial_binary_identity
    {
        return Err(Error::Rejected);
    }
    let strings = (
        root.to_string_lossy().into_owned(),
        marker.to_string_lossy().into_owned(),
        config.to_string_lossy().into_owned(),
        database.to_string_lossy().into_owned(),
        binary.to_string_lossy().into_owned(),
        args.evidence.to_string_lossy().into_owned(),
    );
    let manifest_digest = digest(&[
        &args.operation_id,
        &strings.0,
        &strings.1,
        &strings.2,
        &config_identity,
        &strings.3,
        &database_identity,
        &strings.4,
        &binary_identity,
        &args.unit,
        &strings.5,
        &pre,
        &post,
        &args.commit_heartbeat,
    ]);
    let receipt_digest = digest(&[
        &args.operation_id,
        &manifest_digest,
        &pre,
        &post,
        &args.commit_heartbeat,
        "recovered",
    ]);
    let manifest = Manifest {
        operation_id: args.operation_id.clone(),
        isolated_root: strings.0,
        marker: strings.1,
        config: strings.2,
        config_identity,
        database: strings.3,
        database_identity,
        binary: strings.4,
        binary_identity,
        unit: args.unit.clone(),
        evidence: strings.5,
        manifest_digest,
        pre_owner_state_digest: pre,
        post_owner_state_digest: post,
        receipt_digest,
        receipt_sibling_digest,
        commit_heartbeat: args.commit_heartbeat,
    };
    let marker_bytes = format!(
        "t050-isolated-root-v1\noperation_id={}\nunit={}\n",
        args.operation_id, args.unit
    );
    ensure_marker(&marker, marker_bytes.as_bytes())?;
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|_| Error::Rejected)?;
    publish_new(&args.manifest, &manifest_bytes)?;
    admission.revalidate().map_err(|_| Error::Rejected)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate};
    use serde_json::json;

    fn sidecar_fixture() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf, Sidecars) {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("state.db");
        let wal = root.path().join("state.db-wal");
        let shm = root.path().join("state.db-shm");
        for path in [&database, &wal, &shm] {
            std::fs::write(path, b"fixture").unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let sidecars = Sidecars::capture(&database).unwrap();
        (root, database, wal, shm, sidecars)
    }

    #[test]
    fn retained_sidecar_lifecycle_accepts_only_paired_sqlite_close() {
        let (_root, database, wal, shm, sidecars) = sidecar_fixture();
        std::fs::remove_file(&wal).unwrap();
        std::fs::remove_file(&shm).unwrap();
        assert!(sidecars.revalidate(&database).is_ok());

        let (_root, database, wal, _shm, sidecars) = sidecar_fixture();
        std::fs::remove_file(wal).unwrap();
        assert!(sidecars.revalidate(&database).is_err());

        let (_root, database, _wal, shm, sidecars) = sidecar_fixture();
        std::fs::remove_file(&shm).unwrap();
        std::fs::write(&shm, b"replacement").unwrap();
        assert!(sidecars.revalidate(&database).is_err());

        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("state.db");
        std::fs::write(&database, b"fixture").unwrap();
        let sidecars = Sidecars::capture(&database).unwrap();
        std::fs::write(root.path().join("state.db-wal"), b"partial").unwrap();
        assert!(sidecars.revalidate(&database).is_err());
    }

    #[test]
    fn content_identity_streams_compatibly_and_rejects_oversized_sparse_files() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("artifact");
        let bytes = vec![0x5a; IDENTITY_HASH_BUFFER_SIZE + 17];
        std::fs::write(&path, &bytes).unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        use std::os::unix::fs::MetadataExt;
        let expected = digest(&[
            &path.to_string_lossy(),
            &metadata.dev().to_string(),
            &metadata.ino().to_string(),
            &metadata.mode().to_string(),
            &format!("{:x}", Sha256::digest(&bytes)),
        ]);
        assert_eq!(path_identity(&path, true).unwrap(), expected);

        let oversized = root.path().join("oversized");
        std::fs::File::create(&oversized)
            .unwrap()
            .set_len(MAX_IDENTITY_CONTENT_SIZE + 1)
            .unwrap();
        assert!(path_identity(&oversized, true).is_err());
        let mut oversized_reader = std::fs::File::open(&oversized).unwrap();
        let mut observed = false;
        assert!(
            identity_content_hash_observed(
                &mut oversized_reader,
                MAX_IDENTITY_CONTENT_SIZE + 1,
                |_, _| observed = true,
            )
            .is_err()
        );
        assert!(!observed);
    }

    #[test]
    fn content_identity_rejects_growth_and_short_eof_with_restored_metadata() {
        use std::io::{Seek, Write};

        let growing = tempfile::NamedTempFile::new().unwrap();
        let initial_size = (IDENTITY_HASH_BUFFER_SIZE * 2) as u64;
        growing.as_file().set_len(initial_size).unwrap();
        let mut growing_reader = growing.reopen().unwrap();
        let mut grew = false;
        assert!(
            identity_content_hash_observed(&mut growing_reader, initial_size, |_, count| {
                if count != 0 && !grew {
                    let mut writer = std::fs::OpenOptions::new()
                        .append(true)
                        .open(growing.path())
                        .unwrap();
                    writer
                        .write_all(&vec![0xa5; IDENTITY_HASH_BUFFER_SIZE])
                        .unwrap();
                    grew = true;
                }
            })
            .is_err()
        );
        assert!(grew);
        assert_eq!(
            growing_reader.stream_position().unwrap(),
            initial_size + IDENTITY_HASH_BUFFER_SIZE as u64
        );
        assert_eq!(
            growing.as_file().metadata().unwrap().len(),
            initial_size + IDENTITY_HASH_BUFFER_SIZE as u64
        );

        let short = tempfile::tempfile().unwrap();
        short.set_len(initial_size).unwrap();
        let mut short_reader = short.try_clone().unwrap();
        let mut shortened = false;
        let mut restored = false;
        assert!(
            identity_content_hash_observed(&mut short_reader, initial_size, |_, count| {
                if count != 0 && !shortened {
                    short.set_len(IDENTITY_HASH_BUFFER_SIZE as u64).unwrap();
                    shortened = true;
                } else if count == 0 {
                    short.set_len(initial_size).unwrap();
                    restored = true;
                }
            })
            .is_err()
        );
        assert!(restored);
        assert_eq!(short.metadata().unwrap().len(), initial_size);
    }

    #[test]
    fn deployment_identity_is_not_filtered_by_uid() {
        assert!(is_deployment_presence(65_534, 1_000, true, true, false));
        assert!(is_deployment_presence(0, 1_000, true, false, true));
        assert!(!is_deployment_presence(65_534, 1_000, true, false, false));
        assert!(!is_deployment_presence(65_534, 1_000, false, true, true));
        assert_eq!(
            deployment_presence_kind(true, true),
            RuntimeAbsence::Process
        );
        assert_eq!(
            deployment_presence_kind(false, true),
            RuntimeAbsence::Listener
        );
        assert_eq!(
            deployment_presence_kind(false, false),
            RuntimeAbsence::Clean
        );
        let permission = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            RuntimeAbsenceUnknown::io(RuntimeUnknownArea::DeploymentMetadata, &permission).cause(),
            "runtime-unknown-deployment-metadata-permission"
        );
        assert_eq!(
            RuntimeAbsenceUnknown::io(
                RuntimeUnknownArea::ProcessExecutableLinkDeploymentOwner,
                &permission,
            )
            .cause(),
            "runtime-unknown-process-executable-link-deployment-owner-permission"
        );
        assert_eq!(
            RuntimeAbsenceUnknown::io(
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedKernelOwner,
                &permission,
            )
            .cause(),
            "runtime-unknown-process-executable-link-privileged-kernel-owner-permission"
        );
        assert_eq!(
            RuntimeAbsenceUnknown::io(
                RuntimeUnknownArea::ProcessExecutableLinkPrivilegedUserspaceOwner,
                &permission,
            )
            .cause(),
            "runtime-unknown-process-executable-link-privileged-userspace-owner-permission"
        );
        assert_eq!(
            RuntimeAbsenceUnknown::io(
                RuntimeUnknownArea::ProcessExecutableLinkUnprivilegedOtherOwner,
                &permission,
            )
            .cause(),
            "runtime-unknown-process-executable-link-unprivileged-other-owner-permission"
        );
        assert_eq!(
            RuntimeAbsenceUnknown::io(RuntimeUnknownArea::ProcessExecutableMetadata, &permission)
                .cause(),
            "runtime-unknown-process-executable-metadata-permission"
        );
    }

    #[test]
    fn unit_state_parser_is_named_order_independent_and_fail_closed() {
        let stopped = "MainPID=0\nActiveState=inactive\nSubState=dead\n";
        assert!(parse_stopped_unit_state(stopped));
        assert!(parse_stopped_unit_state(stopped));
        for rejected in [
            "MainPID=1\nActiveState=active\nSubState=running\n",
            "MainPID=0\nActiveState=failed\nSubState=failed\n",
            "ActiveState=inactive\nSubState=dead\n",
            "MainPID=0\nActiveState=inactive\nActiveState=inactive\nSubState=dead\n",
            "MainPID=0\nActiveState=inactive\nSubState=dead\nUnknown=value\n",
            "",
        ] {
            assert!(!parse_stopped_unit_state(rejected));
        }
    }

    #[tokio::test]
    async fn real_sqlite_legacy_owner_cas_and_repeat_are_atomic() {
        let root = std::env::temp_dir().join(format!("t050-legacy-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let db = root.join("state.db");
        let pool = connect(&db).await.unwrap();
        migrate(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO runtime_instances VALUES('legacy','start','heart','active','legacy-fp')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        let mut c = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
        let pre = logical_digest(&mut c).await.unwrap();
        sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at='commit'")
            .execute(&mut c)
            .await
            .unwrap();
        let post = logical_digest(&mut c).await.unwrap();
        sqlx::query("UPDATE runtime_instances SET state='active',heartbeat_at='heart'")
            .execute(&mut c)
            .await
            .unwrap();
        c.close().await.unwrap();
        let op = "op";
        let dbs = db.to_string_lossy();
        let config = root.join("config.toml");
        std::fs::write(&config, "fixture=true\n").unwrap();
        let config = config
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let binary = std::env::current_exe()
            .unwrap()
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let evidence = root.join("evidence.txt").to_string_lossy().into_owned();
        let marker = root.join(".t050-isolated-marker");
        std::fs::write(
            &marker,
            "t050-isolated-root-v1\noperation_id=op\nunit=fixture.service\n",
        )
        .unwrap();
        std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o600)).unwrap();
        let systemctl = root.join("systemctl-fixture");
        std::fs::write(&systemctl, "#!/bin/sh\nprintf 'inactive\\ndead\\n0\\n'\n").unwrap();
        std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o700)).unwrap();
        let proc_root = root.join("proc");
        std::fs::create_dir_all(proc_root.join("net")).unwrap();
        for table in ["tcp", "tcp6"] {
            std::fs::write(
                proc_root.join("net").join(table),
                b"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n",
            )
            .unwrap();
        }
        unsafe {
            std::env::set_var("T050_RECONCILE_TEST_SYSTEMCTL", &systemctl);
            std::env::set_var("T050_RECONCILE_TEST_PROC_ROOT", &proc_root);
        }
        let root_s = root.to_string_lossy().into_owned();
        let marker_s = marker.to_string_lossy().into_owned();
        let sibling = digest(&[]);
        let config_identity = path_identity(Path::new(&config), true).unwrap();
        let database_identity = path_identity(Path::new(&*dbs), false).unwrap();
        let binary_identity = path_identity(Path::new(&binary), true).unwrap();
        let md = digest(&[
            op,
            &root_s,
            &marker_s,
            &config,
            &config_identity,
            &dbs,
            &database_identity,
            &binary,
            &binary_identity,
            "fixture.service",
            &evidence,
            &pre,
            &post,
            "commit",
        ]);
        let rd = digest(&[op, &md, &pre, &post, "commit", "recovered"]);
        let manifest = root.join("manifest.json");
        std::fs::write(&manifest,serde_json::to_vec(&json!({"operation_id":op,"isolated_root":root_s,"marker":marker_s,"config":config,"config_identity":config_identity,"database":dbs,"database_identity":database_identity,"binary":binary,"binary_identity":binary_identity,"unit":"fixture.service","evidence":evidence,"manifest_digest":md,"pre_owner_state_digest":pre,"post_owner_state_digest":post,"receipt_digest":rd,"receipt_sibling_digest":sibling,"commit_heartbeat":"commit"})).unwrap()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&manifest, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(reconcile(&manifest).await.unwrap(), Outcome::Recovered);
        assert_eq!(reconcile(&manifest).await.unwrap(), Outcome::AlreadyStopped);
        unsafe {
            std::env::remove_var("T050_RECONCILE_TEST_SYSTEMCTL");
            std::env::remove_var("T050_RECONCILE_TEST_PROC_ROOT");
        }
    }
}
