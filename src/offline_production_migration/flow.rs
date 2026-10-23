use super::{
    Failure, Mode, Request, RuntimeLock,
    evidence::{self, Evidence},
    launcher,
    records::{self, Backup, Complete, Intent, Operation},
    snapshot,
    verification::{self, Facts},
};
use sqlx::Connection;

pub async fn run(req: Request) -> Result<String, Failure> {
    execute(req, true).await
}

pub(super) async fn execute(req: Request, live_launcher: bool) -> Result<String, Failure> {
    let mut evidence = Evidence::read(&req)?;
    launcher::check(&evidence.listeners, live_launcher, "pre-lock").await?;
    let _lock = RuntimeLock::acquire(&req.database).map_err(|_| Failure::Busy)?;
    launcher::check(&evidence.listeners, live_launcher, "post-lock").await?;
    evidence.recheck(&req)?;
    records::no_pending(&req)?;
    if req.mode == Mode::Preflight {
        let facts = snapshot::inspect(&req.database).await?;
        preflight_operation(&req, &evidence, &facts).await?;
        evidence.recheck(&req)?;
        return Ok(format!(
            "offline-migrate-production result=preflight ledger={}",
            facts.version
        ));
    }
    let expected = records::operation(&req, &evidence);
    let before = snapshot::inspect(&req.database).await?;
    let backup = prepare(&req, &expected, &before).await?;
    evidence.recheck(&req)?;
    launcher::check(&evidence.listeners, live_launcher, "pre-intent").await?;
    let current = validate_current(&req, &backup).await?;
    let intent = intent(&req, &expected, &backup, &current)?;
    fault("after-intent");
    if req.mode == Mode::Verify {
        verify_complete(&req, &intent, &backup).await?;
    } else {
        let complete_path = req.manifest_dir.join("complete.json");
        if complete_path.exists() {
            verify_complete(&req, &intent, &backup).await?;
        } else {
            evidence.recheck(&req)?;
            launcher::check(&evidence.listeners, live_launcher, "pre-migration").await?;
            migrate(&req, &backup).await?;
            evidence.recheck(&req)?;
            let after = validate_current(&req, &backup).await?;
            if after.version != 20 {
                return Err(Failure::Migration);
            }
            records::publish(&complete_path, &Complete { intent, after })?;
        }
    }
    Ok("offline-migrate-production result=complete ledger=20".into())
}

async fn preflight_operation(
    req: &Request,
    evidence: &Evidence,
    facts: &Facts,
) -> Result<(), Failure> {
    let path = records::anchor(&req.database);
    if !path.exists() {
        if facts.version != 18 {
            return Err(Failure::ManualRequired);
        }
        return records::no_residue(req);
    }
    let expected = records::operation(req, evidence);
    let operation: Operation = records::load(&path)?;
    if operation != expected {
        return Err(Failure::ManualRequired);
    }
    let backup = snapshot::validate(req, &expected).await?;
    let intent_path = req.manifest_dir.join("migration_intent.json");
    if intent_path.exists() {
        let existing: Intent = records::load(&intent_path)?;
        if existing.operation != expected
            || existing.backup_digest != records::encoded_digest(&backup)?
            || existing.migration_digest != evidence::migration_digest()
        {
            return Err(Failure::ManualRequired);
        }
    } else if facts != &backup.before {
        return Err(Failure::ManualRequired);
    }
    validate_current(req, &backup).await?;
    if req.manifest_dir.join("complete.json").exists() {
        let existing: Intent = records::load(&intent_path)?;
        verify_complete(req, &existing, &backup).await?;
    }
    Ok(())
}

async fn prepare(req: &Request, expected: &Operation, before: &Facts) -> Result<Backup, Failure> {
    if req.mode == Mode::Apply {
        records::no_residue(req)?;
        if before.version != 18 {
            return Err(Failure::ManualRequired);
        }
        records::publish(&records::anchor(&req.database), expected)?;
        fault("after-operation");
        snapshot::create(req, expected.clone()).await
    } else {
        let operation: Operation = records::load(&records::anchor(&req.database))?;
        if &operation != expected {
            return Err(Failure::ManualRequired);
        }
        snapshot::validate(req, expected).await
    }
}

fn intent(
    req: &Request,
    operation: &Operation,
    backup: &Backup,
    current: &Facts,
) -> Result<Intent, Failure> {
    let expected = Intent {
        operation: operation.clone(),
        backup_digest: records::encoded_digest(backup)?,
        migration_digest: evidence::migration_digest(),
    };
    let path = req.manifest_dir.join("migration_intent.json");
    if path.exists() {
        let existing: Intent = records::load(&path)?;
        if existing != expected {
            return Err(Failure::ManualRequired);
        }
    } else {
        if current != &backup.before || req.mode == Mode::Verify {
            return Err(Failure::ManualRequired);
        }
        records::publish(&path, &expected)?;
    }
    Ok(expected)
}

async fn validate_current(req: &Request, backup: &Backup) -> Result<Facts, Failure> {
    let mut connection = verification::open(&req.database, true).await?;
    let result = async {
        sqlx::query("BEGIN")
            .execute(&mut connection)
            .await
            .map_err(|_| Failure::Migration)?;
        let facts = verification::facts(&mut connection).await?;
        if facts.schema_digest != verification::expected_schema(facts.version).await? {
            return Err(Failure::ManualRequired);
        }
        verification::preserved(&mut connection, &backup.before, facts.version).await?;
        Ok(facts)
    }
    .await;
    connection.close().await.map_err(|_| Failure::Migration)?;
    result
}

async fn migrate(req: &Request, backup: &Backup) -> Result<(), Failure> {
    let before = validate_current(req, backup).await?;
    if before.version == 20 {
        return Ok(());
    }
    let mut connection = verification::open(&req.database, false).await?;
    if before.version == 18 {
        let result = verification::embedded_through(19)
            .run(&mut connection)
            .await;
        if result.is_err() {
            connection.close().await.map_err(|_| Failure::Migration)?;
            return Err(Failure::Migration);
        }
        fault("after-19");
    }
    let result = verification::embedded_through(20)
        .run(&mut connection)
        .await;
    connection.close().await.map_err(|_| Failure::Migration)?;
    fault("after-20");
    result.map_err(|_| Failure::Migration)
}

pub(super) fn fault(_phase: &str) {
    #[cfg(test)]
    if std::env::var("R071_TEST_CRASH").as_deref() == Ok(_phase) {
        std::process::exit(86);
    }
}

async fn verify_complete(req: &Request, intent: &Intent, backup: &Backup) -> Result<(), Failure> {
    let complete: Complete = records::load(&req.manifest_dir.join("complete.json"))?;
    if &complete.intent != intent
        || complete.after.version != 20
        || validate_current(req, backup).await? != complete.after
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}
