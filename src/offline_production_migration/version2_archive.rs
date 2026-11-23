use std::{fs, os::unix::fs::OpenOptionsExt, path::Path};

use sqlx::{Connection, SqliteConnection};

use super::{
    Failure, Request, evidence, records,
    verification::{self, Facts},
    version2_records::{self as graph, Archive, Root},
};

pub(super) async fn inspect(path: &Path) -> Result<Facts, Failure> {
    let mut connection = verification::open(path, true).await?;
    let result = pinned(&mut connection).await;
    connection.close().await.map_err(|_| Failure::Preflight)?;
    result
}

async fn pinned(connection: &mut SqliteConnection) -> Result<Facts, Failure> {
    sqlx::query("BEGIN")
        .execute(&mut *connection)
        .await
        .map_err(|_| Failure::Preflight)?;
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| Failure::Preflight)?;
    if mode != "wal" {
        return Err(Failure::Preflight);
    }
    let facts = verification::facts_through(connection, 25).await?;
    if facts.schema_digest != verification::expected_schema(facts.version).await? {
        return Err(Failure::ManualRequired);
    }
    Ok(facts)
}

pub(super) async fn create(req: &Request, root: &Root) -> Result<Archive, Failure> {
    let mut connection = verification::open(&req.database, true).await?;
    let result = create_pinned(req, root, &mut connection).await;
    connection.close().await.map_err(|_| Failure::Preflight)?;
    result
}

async fn create_pinned(
    req: &Request,
    root: &Root,
    source: &mut SqliteConnection,
) -> Result<Archive, Failure> {
    let before = pinned(source).await?;
    if before.version != root.initial_ledger {
        return Err(Failure::ManualRequired);
    }
    let path = req.manifest_dir.join("archive-v2.db");
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
        .map_err(|_| Failure::ManualRequired)?;
    super::snapshot::copy(source, &file).await?;
    let mut copied = verification::open(&path, true).await?;
    let copied_facts = verification::facts_through(&mut copied, 25).await;
    copied.close().await.map_err(|_| Failure::Preflight)?;
    if copied_facts? != before {
        return Err(Failure::Preflight);
    }
    file.sync_all().map_err(|_| Failure::Preflight)?;
    records::sync_dir(&req.manifest_dir)?;
    super::flow::fault("v2-after-archive-bytes");
    let archive = Archive {
        root_digest: records::encoded_digest(root)?,
        snapshot_digest: evidence::hash_file(&path)?,
        before,
    };
    records::publish(&req.manifest_dir.join("archive-v2.json"), &archive)?;
    Ok(archive)
}

pub(super) async fn validate(req: &Request, root: &Root) -> Result<Archive, Failure> {
    use std::os::unix::fs::MetadataExt;
    let snapshot = req.manifest_dir.join("archive-v2.db");
    evidence::canonical(&snapshot)?;
    let metadata = fs::symlink_metadata(&snapshot).map_err(|_| Failure::ManualRequired)?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(Failure::ManualRequired);
    }
    let archive: Archive = graph::load(&req.manifest_dir.join("archive-v2.json"))?;
    if archive.root_digest != records::encoded_digest(root)?
        || archive.before.version != root.initial_ledger
        || evidence::hash_file(&req.manifest_dir.join("archive-v2.db"))? != archive.snapshot_digest
    {
        return Err(Failure::ManualRequired);
    }
    let mut connection = verification::open(&req.manifest_dir.join("archive-v2.db"), true).await?;
    let facts = verification::facts_through(&mut connection, 25).await;
    connection.close().await.map_err(|_| Failure::Preflight)?;
    if facts? != archive.before {
        return Err(Failure::ManualRequired);
    }
    Ok(archive)
}

pub(super) async fn current(req: &Request, archive: &Archive) -> Result<Facts, Failure> {
    let mut connection = verification::open(&req.database, true).await?;
    let facts = pinned(&mut connection).await?;
    let preserved =
        verification::preserved_exact(&mut connection, &archive.before, facts.version).await;
    connection.close().await.map_err(|_| Failure::Preflight)?;
    preserved?;
    if facts.version < archive.before.version {
        return Err(Failure::ManualRequired);
    }
    Ok(facts)
}

pub(super) async fn validate_predecessor(
    req: &Request,
    root: &Root,
    version: i64,
) -> Result<(), Failure> {
    if root.predecessor.is_none() {
        return Ok(());
    }
    let anchor = records::anchor(&req.database);
    let path = if graph::load::<Root>(&anchor).is_ok() {
        req.manifest_dir.join("predecessor-root-v1.json")
    } else {
        anchor
    };
    let prior: records::Operation = records::load(&path)?;
    let original = Request {
        manifest_dir: prior.manifest_dir.clone(),
        ..req.clone()
    };
    let backup = super::snapshot::validate(&original, &prior).await?;
    let complete: records::Complete = records::load(&prior.manifest_dir.join("complete.json"))?;
    if complete.intent.backup_digest != records::encoded_digest(&backup)?
        || complete.intent.operation != prior
        || complete.after.version != 20
    {
        return Err(Failure::ManualRequired);
    }
    let mut retained = verification::open(&prior.manifest_dir.join("snapshot.db"), true).await?;
    let exact = verification::facts_through(&mut retained, 25).await;
    retained.close().await.map_err(|_| Failure::Preflight)?;
    let mut current = verification::open(&req.database, true).await?;
    let result = verification::preserved_exact(&mut current, &exact?, version).await;
    current.close().await.map_err(|_| Failure::Preflight)?;
    result
}
