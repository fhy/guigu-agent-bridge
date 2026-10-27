use super::runtime::AppError;
use crate::{
    config::Config,
    matrix::{MatrixClient, MatrixSync, SqliteSyncTokenStore},
    storage::MatrixRecoveryStore,
};
use chrono::Utc;
use sqlx::SqlitePool;
use std::sync::Arc;

pub(super) async fn prepare_sync(
    client: MatrixClient,
    config: &Config,
    pool: &SqlitePool,
    runtime: &str,
) -> Result<(MatrixClient, MatrixSync), AppError> {
    let recovery = MatrixRecoveryStore::new(pool.clone());
    recovery
        .reconcile_restart()
        .await
        .map_err(|_| AppError::Runtime)?;
    let control = recovery
        .control()
        .await
        .map_err(|_| AppError::Runtime)?
        .ok_or(AppError::Runtime)?;
    if control.operation_id.trim().is_empty() || control.restart_hold_state.is_some() {
        return Err(AppError::Runtime);
    }
    match control.phase.as_str() {
        "baseline" if control.watermark.is_none() => {
            let baseline = client
                .baseline_sync_token()
                .await
                .map_err(|_| AppError::Matrix)?;
            if baseline.is_empty() {
                return Err(AppError::Matrix);
            }
            recovery
                .activate_cutover(
                    &control.operation_id,
                    &baseline,
                    &Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                )
                .await
                .map_err(|_| AppError::Runtime)?;
        }
        "active"
            if control
                .watermark
                .as_ref()
                .is_some_and(|token| !token.is_empty()) => {}
        _ => return Err(AppError::Runtime),
    }
    let now = Utc::now();
    let epoch = recovery
        .acquire_component_lease(
            "MatrixSync",
            runtime,
            &now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            &(now + chrono::Duration::seconds(60))
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
        .await
        .map_err(|_| AppError::Runtime)?;
    let active = client
        .isolated_active()
        .await
        .map_err(|_| AppError::Matrix)?;
    let sync = MatrixSync::new(
        active.clone(),
        Arc::new(SqliteSyncTokenStore::new(pool.clone())),
        config.transports.matrix.sync_capacity,
    )
    .map(|sync| {
        sync.with_recovery(
            recovery,
            &config.transports.matrix.user_id,
            control.operation_id,
            epoch,
        )
        .with_component_owner(runtime.to_owned())
    })
    .map_err(|_| AppError::Matrix)?;
    sync.rebuild_state().await.map_err(|_| AppError::Matrix)?;
    Ok((active, sync))
}
