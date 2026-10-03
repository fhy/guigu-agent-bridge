use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

const O_NOFOLLOW: i32 = 0o400000;

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
struct Sidecars {
    wal: Sidecar,
    shm: Sidecar,
    parent_device: u64,
    database_uid: u32,
}

impl Sidecars {
    fn capture(database: &Path) -> Result<Self, Error> {
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

    fn revalidate(&self, database: &Path) -> Result<(), Error> {
        let after = Self::capture(database)?;
        if self.parent_device != after.parent_device || self.database_uid != after.database_uid {
            return Err(Error::Rejected);
        }
        for (before, after) in [(&self.wal, &after.wal), (&self.shm, &after.shm)] {
            if !matches!(
                (before, after),
                (Sidecar::Absent, Sidecar::Absent | Sidecar::Present { .. })
            ) && before != after
            {
                return Err(Error::Rejected);
            }
        }
        Ok(())
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

#[derive(Deserialize)]
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

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, Error> {
    if args.len() != 3 || args[0] != "reconcile-isolated-legacy-owner" || args[1] != "--manifest" {
        return Err(Error::Rejected);
    }
    Ok(PathBuf::from(&args[2]))
}
fn digest(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update((p.len() as u64).to_be_bytes());
        h.update(p.as_bytes());
    }
    format!("{:x}", h.finalize())
}

fn path_identity(path: &Path, include_content: bool) -> Result<String, Error> {
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
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|_| Error::Rejected)?;
        format!("{:x}", Sha256::digest(bytes))
    } else {
        "identity-only".to_owned()
    };
    let after = file.metadata().map_err(|_| Error::Rejected)?;
    if before.dev() != after.dev() || before.ino() != after.ino() || before.mode() != after.mode() {
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
async fn logical_digest(c: &mut SqliteConnection) -> Result<String, Error> {
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
                let value: String = row.try_get(index).map_err(|_| Error::Database)?;
                h.update((value.len() as u64).to_be_bytes());
                h.update(value);
            }
        }
    }
    Ok(format!("{:x}", h.finalize()))
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

fn preflight_unit(unit: &str) -> Result<(), Error> {
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
            "--value",
        ])
        .output()
        .map_err(|_| Error::Rejected)?;
    if !output.status.success() || output.stderr.len() > 4096 {
        return Err(Error::Rejected);
    }
    let text = std::str::from_utf8(&output.stdout).map_err(|_| Error::Rejected)?;
    let values = text.lines().collect::<Vec<_>>();
    if values == ["inactive", "dead", "0"] {
        Ok(())
    } else {
        Err(Error::Rejected)
    }
}

fn publish(path: &Path, bytes: &[u8]) -> Result<(), Error> {
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

fn preflight_publication(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() && meta.mode() & 0o777 == 0o600 && meta.nlink() == 1 => Ok(()),
        Ok(_) => Err(Error::Rejected),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(Error::Rejected),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate};
    use serde_json::json;

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
        unsafe {
            std::env::set_var("T050_RECONCILE_TEST_SYSTEMCTL", &systemctl);
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
        }
    }
}
