use sqlx::Connection;

use super::{
    Failure, Request, RuntimeLock, TransitionRequest,
    evidence::Evidence,
    identity_records::{IdentityEvidence, IdentityIntent},
    launcher, records, transition, version2, version2_archive, version2_records as graph,
};

pub(super) async fn execute(request: TransitionRequest) -> Result<String, Failure> {
    let migration = Request {
        mode: request.mode,
        target_schema: 25,
        config: request.config.clone(),
        database: request.database.clone(),
        operation_id: request.operation_id.clone(),
        manifest_dir: request.manifest_dir.clone(),
        release_provenance: request.release_provenance.clone(),
        executable: request.executable.clone(),
    };
    let mut provenance = Evidence::read(&migration)?;
    launcher::check(&provenance.listeners, true, "transition-pre-lock").await?;
    let _lock = RuntimeLock::acquire(&request.database).map_err(|_| Failure::Busy)?;
    launcher::check(&provenance.listeners, true, "transition-post-lock").await?;
    provenance.recheck(&migration)?;
    records::no_pending(&migration)?;
    let root = graph::root(&migration, &provenance, 25)?;
    let backup = version2_archive::validate(&migration, &root).await?;
    let intent: graph::Intent =
        graph::load(&migration.manifest_dir.join("migration-intent-v2.json"))?;
    if intent.archive_digest != records::encoded_digest(&backup)? {
        return Err(Failure::ManualRequired);
    }
    let active = active_binding(&request.database).await?;
    if active {
        if !matches!(
            request.mode,
            super::arguments::Mode::Resume | super::arguments::Mode::Verify
        ) {
            return Err(Failure::ManualRequired);
        }
        super::administrative_startup::verify_active(&request.database).await?;
    } else {
        version2::verify_complete(&migration, &root, &backup, &intent).await?;
    }
    verify_identity(&migration, &root, &request.transition_plan, active)?;
    provenance.recheck(&migration)?;
    launcher::check(&provenance.listeners, true, "transition-pre-write").await?;
    transition::run(request).await
}

async fn active_binding(database: &std::path::Path) -> Result<bool, Failure> {
    let mut connection = super::verification::open(database, true).await?;
    let phase: String =
        sqlx::query_scalar("SELECT phase FROM offline_recovery_binding WHERE singleton=1")
            .fetch_one(&mut connection)
            .await
            .map_err(|_| Failure::ManualRequired)?;
    connection.close().await.map_err(|_| Failure::Migration)?;
    Ok(phase == "active")
}

fn verify_identity(
    request: &Request,
    root: &graph::Root,
    plan_path: &std::path::Path,
    active: bool,
) -> Result<(), Failure> {
    let schema: graph::Complete =
        graph::load(&request.manifest_dir.join("schema-complete-v2.json"))?;
    let intent: IdentityIntent =
        graph::load(&request.manifest_dir.join("identity-intent-v1.json"))?;
    let identity: IdentityEvidence =
        graph::load(&request.manifest_dir.join("identity-evidence-v1.json"))?;
    intent.validate(root, &records::encoded_digest(&schema)?)?;
    identity.validate(
        &intent,
        if active {
            identity.captured_at
        } else {
            chrono::Utc::now()
        },
    )?;
    let plan = transition::Plan::load(plan_path)?;
    if plan.root_digest != records::encoded_digest(root)?
        || plan.schema_complete_digest != records::encoded_digest(&schema)?
        || plan.identity_digest != records::encoded_digest(&identity)?
    {
        return Err(Failure::ManualRequired);
    }
    let config = crate::config::load(&request.config).map_err(|_| Failure::Preflight)?;
    let matrix = config.transports.matrix;
    let store = matrix.crypto_store_path.ok_or(Failure::Preflight)?;
    if !matrix.device_trusted
        || matrix.access_token.expose().is_empty()
        || (!active
            && crate::offline_identity::store_generation(&store).map_err(|_| Failure::Preflight)?
                != identity.store_after)
    {
        return Err(Failure::ManualRequired);
    }
    let failure = request.manifest_dir.join("identity-failure-v1.json");
    if !matches!(std::fs::symlink_metadata(failure), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}
