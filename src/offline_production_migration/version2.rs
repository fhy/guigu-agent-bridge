use sqlx::{Connection, Row};

use super::{
    Failure, Mode, Request, RuntimeLock,
    evidence::{self, Evidence},
    launcher, records, verification, version2_archive as archive,
    version2_records::{self as graph, Archive, Complete, Intent, Root},
};

pub(super) async fn execute(req: Request, live_launcher: bool) -> Result<String, Failure> {
    use std::os::unix::fs::MetadataExt;
    let mut evidence = Evidence::read(&req)?;
    if std::fs::metadata(&req.manifest_dir)
        .map_err(|_| Failure::Preflight)?
        .uid()
        != unsafe { libc::geteuid() }
    {
        return Err(Failure::Preflight);
    }
    launcher::check(&evidence.listeners, live_launcher, "v2-pre-lock").await?;
    let _lock = RuntimeLock::acquire(&req.database).map_err(|_| Failure::Busy)?;
    launcher::check(&evidence.listeners, live_launcher, "v2-post-lock").await?;
    evidence.recheck(&req)?;
    records::no_pending(&req)?;
    let before = archive::inspect(&req.database).await?;
    let root = graph::root(&req, &evidence, before.version)?;
    archive::validate_predecessor(&req, &root, before.version).await?;
    if req.mode == Mode::Preflight {
        validate_preflight(&req, &root, &before).await?;
        return Ok(format!(
            "offline-migrate-production result=preflight ledger={} target=25",
            before.version
        ));
    }
    prepare_root(&req, &root)?;
    super::flow::fault("v2-after-root");
    let backup = prepare_archive(&req, &root).await?;
    let current = archive::current(&req, &backup).await?;
    let intent = prepare_intent(&req, &root, &backup, &current)?;
    super::flow::fault("v2-after-intent");
    if req.mode != Mode::Verify {
        migrate(&req, &root, &backup, &intent, &mut evidence, live_launcher).await?;
    }
    verify_complete(&req, &root, &backup, &intent).await?;
    Ok("offline-migrate-production result=complete ledger=25".into())
}

async fn validate_preflight(
    req: &Request,
    root: &Root,
    facts: &verification::Facts,
) -> Result<(), Failure> {
    let path = records::anchor(&req.database);
    if graph::load::<Root>(&path).is_ok() {
        let backup = archive::validate(req, root).await?;
        let current = archive::current(req, &backup).await?;
        let intent: Intent = graph::load(&req.manifest_dir.join("migration-intent-v2.json"))?;
        if intent != expected_intent(root, &backup)? {
            return Err(Failure::ManualRequired);
        }
        if req.manifest_dir.join("schema-complete-v2.json").exists() {
            verify_complete(req, root, &backup, &intent).await?;
        } else if current.version < root.initial_ledger {
            return Err(Failure::ManualRequired);
        }
    } else {
        if std::fs::read_dir(&req.manifest_dir)
            .map_err(|_| Failure::Preflight)?
            .next()
            .is_some()
        {
            return Err(Failure::ManualRequired);
        }
        validate_legacy_complete(root, facts).await?;
    }
    Ok(())
}

async fn validate_legacy_complete(root: &Root, facts: &verification::Facts) -> Result<(), Failure> {
    if root.predecessor.is_some() {
        let previous: records::Operation =
            records::load(&records::anchor(&root.operation.database))?;
        let complete: records::Complete =
            records::load(&previous.manifest_dir.join("complete.json"))?;
        if facts.version != 20
            || complete.after != super::snapshot::inspect(&root.operation.database).await?
        {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

fn prepare_root(req: &Request, root: &Root) -> Result<(), Failure> {
    let anchored = graph::load::<Root>(&records::anchor(&req.database)).is_ok();
    if req.mode == Mode::Verify {
        return if anchored {
            Ok(())
        } else {
            Err(Failure::ManualRequired)
        };
    }
    if req.mode == Mode::Resume && !anchored && !req.manifest_dir.join("root-v2.json").exists() {
        return Err(Failure::ManualRequired);
    }
    if req.mode == Mode::Apply && anchored {
        return Err(Failure::ManualRequired);
    }
    if !anchored
        && !req.manifest_dir.join("root-v2.json").exists()
        && std::fs::read_dir(&req.manifest_dir)
            .map_err(|_| Failure::Preflight)?
            .next()
            .is_some()
    {
        return Err(Failure::ManualRequired);
    }
    graph::publish_root(req, root)
}

async fn prepare_archive(req: &Request, root: &Root) -> Result<Archive, Failure> {
    if req.manifest_dir.join("archive-v2.json").exists() {
        return archive::validate(req, root).await;
    }
    if req.mode == Mode::Verify {
        return Err(Failure::ManualRequired);
    }
    let current = archive::inspect(&req.database).await?;
    validate_legacy_snapshot(root, &current).await?;
    archive::create(req, root).await
}

async fn validate_legacy_snapshot(
    root: &Root,
    current: &verification::Facts,
) -> Result<(), Failure> {
    if root.predecessor.is_some() {
        let prior: records::Operation =
            records::load(&root.operation.manifest_dir.join("predecessor-root-v1.json"))?;
        let complete: records::Complete = records::load(&prior.manifest_dir.join("complete.json"))?;
        if current.version != 20
            || complete.after != super::snapshot::inspect(&root.operation.database).await?
        {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

fn expected_intent(root: &Root, backup: &Archive) -> Result<Intent, Failure> {
    Ok(Intent {
        root_digest: records::encoded_digest(root)?,
        archive_digest: records::encoded_digest(backup)?,
        target_schema: 25,
        migration_digest: evidence::migration_digest_through(25),
    })
}

fn prepare_intent(
    req: &Request,
    root: &Root,
    backup: &Archive,
    current: &verification::Facts,
) -> Result<Intent, Failure> {
    let expected = expected_intent(root, backup)?;
    let path = req.manifest_dir.join("migration-intent-v2.json");
    if path.exists() {
        if graph::load::<Intent>(&path)? != expected {
            return Err(Failure::ManualRequired);
        }
    } else {
        if req.mode == Mode::Verify || *current != backup.before {
            return Err(Failure::ManualRequired);
        }
        records::publish(&path, &expected)?;
    }
    Ok(expected)
}

async fn migrate(
    req: &Request,
    root: &Root,
    backup: &Archive,
    intent: &Intent,
    evidence: &mut Evidence,
    live: bool,
) -> Result<(), Failure> {
    let mut current = archive::current(req, backup).await?;
    while current.version < 25 {
        evidence.recheck(req)?;
        launcher::check(&evidence.listeners, live, "v2-pre-prefix").await?;
        let next = current.version + 1;
        let mut connection = verification::open(&req.database, false).await?;
        let result = verification::embedded_through(next)
            .run(&mut connection)
            .await;
        connection.close().await.map_err(|_| Failure::Migration)?;
        result.map_err(|_| Failure::Migration)?;
        super::flow::fault(&format!("v2-after-{next}"));
        evidence.recheck(req)?;
        current = archive::current(req, backup).await?;
    }
    evidence.recheck(req)?;
    launcher::check(&evidence.listeners, live, "v2-pre-binding").await?;
    bind(req, root, intent).await?;
    super::flow::fault("v2-after-binding");
    let complete = Complete {
        intent_digest: records::encoded_digest(intent)?,
        actual_schema: 25,
        schema_digest: current.schema_digest,
        preserved_digest: records::encoded_digest(&backup.before.tables)?,
    };
    let path = req.manifest_dir.join("schema-complete-v2.json");
    if path.exists() {
        if graph::load::<Complete>(&path)? != complete {
            return Err(Failure::ManualRequired);
        }
    } else {
        records::publish(&path, &complete)?;
    }
    super::flow::fault("v2-after-complete");
    complete_binding(req, &complete).await
}

async fn bind(req: &Request, root: &Root, intent: &Intent) -> Result<(), Failure> {
    let mut connection = verification::open(&req.database, false).await?;
    let mut tx = connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|_| Failure::Migration)?;
    sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest,phase,revision) VALUES(1,2,?,?,?,?,?,'schema_pending',0) ON CONFLICT(singleton) DO NOTHING")
        .bind(&root.db_generation).bind(database_digest(root)?)
        .bind(records::encoded_digest(root)?).bind(&req.operation_id).bind(records::encoded_digest(intent)?)
        .execute(&mut *tx).await.map_err(|_| Failure::ManualRequired)?;
    let row = sqlx::query("SELECT db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest FROM offline_recovery_binding WHERE singleton=1")
        .fetch_one(&mut *tx).await.map_err(|_| Failure::ManualRequired)?;
    let expected = [
        &root.db_generation,
        &database_digest(root)?,
        &records::encoded_digest(root)?,
        &req.operation_id,
        &records::encoded_digest(intent)?,
    ];
    for (index, value) in expected.iter().enumerate() {
        if row.get::<String, _>(index) != **value {
            return Err(Failure::ManualRequired);
        }
    }
    sqlx::query("UPDATE runtime_eligibility_bootstrap SET state='admin_reserved' WHERE singleton=1 AND state='pending'")
        .execute(&mut *tx).await.map_err(|_| Failure::ManualRequired)?;
    let reserved: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_eligibility_bootstrap WHERE singleton=1 AND state='admin_reserved' AND generation IS NULL AND inventory_digest IS NULL")
        .fetch_one(&mut *tx).await.map_err(|_| Failure::ManualRequired)?;
    if reserved != 1 {
        return Err(Failure::ManualRequired);
    }
    tx.commit().await.map_err(|_| Failure::Migration)?;
    connection.close().await.map_err(|_| Failure::Migration)
}

async fn complete_binding(req: &Request, complete: &Complete) -> Result<(), Failure> {
    let mut connection = verification::open(&req.database, false).await?;
    let digest = records::encoded_digest(complete)?;
    let result = sqlx::query("UPDATE offline_recovery_binding SET schema_complete_digest=?,phase='schema_ready',revision=revision+1 WHERE singleton=1 AND phase='schema_pending' AND schema_complete_digest IS NULL AND schema_intent_digest=?")
        .bind(&digest).bind(&complete.intent_digest).execute(&mut connection).await.map_err(|_| Failure::Migration)?;
    if result.rows_affected() == 0 {
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT schema_complete_digest FROM offline_recovery_binding WHERE singleton=1",
        )
        .fetch_one(&mut connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        if existing.as_deref() != Some(&digest) {
            return Err(Failure::ManualRequired);
        }
    }
    connection.close().await.map_err(|_| Failure::Migration)
}

async fn verify_complete(
    req: &Request,
    root: &Root,
    backup: &Archive,
    intent: &Intent,
) -> Result<(), Failure> {
    let complete: Complete = graph::load(&req.manifest_dir.join("schema-complete-v2.json"))?;
    let current = archive::current(req, backup).await?;
    if complete.intent_digest != records::encoded_digest(intent)?
        || complete.actual_schema != 25
        || current.version != 25
        || complete.schema_digest != current.schema_digest
        || complete.preserved_digest != records::encoded_digest(&backup.before.tables)?
    {
        return Err(Failure::ManualRequired);
    }
    let mut connection = verification::open(&req.database, true).await?;
    let row = sqlx::query("SELECT root_digest,schema_intent_digest,schema_complete_digest,db_generation,admin_operation_id FROM offline_recovery_binding WHERE singleton=1")
        .fetch_one(&mut connection).await.map_err(|_| Failure::ManualRequired)?;
    let expected = [
        records::encoded_digest(root)?,
        records::encoded_digest(intent)?,
        records::encoded_digest(&complete)?,
        root.db_generation.clone(),
        req.operation_id.clone(),
    ];
    for (index, value) in expected.iter().enumerate() {
        if row.get::<String, _>(index) != *value {
            return Err(Failure::ManualRequired);
        }
    }
    connection.close().await.map_err(|_| Failure::Migration)
}

fn database_digest(root: &Root) -> Result<String, Failure> {
    records::encoded_digest(&(&root.operation.database, &root.operation.database_identity))
}
