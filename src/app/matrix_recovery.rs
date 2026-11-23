use super::runtime::AppError;
use crate::{
    config::Config,
    matrix::{MatrixClient, MatrixSync, SqliteSyncTokenStore},
    storage::MatrixRecoveryStore,
};
use chrono::Utc;
use sqlx::SqlitePool;
use std::sync::Arc;

pub(super) async fn outbox_store(
    pool: &SqlitePool,
    owner: &str,
    scope: Option<crate::storage::recovery_eligibility::RecoveryScope>,
) -> Result<crate::storage::ReliabilityStore, AppError> {
    let Some(scope) = scope else {
        return Ok(crate::storage::ReliabilityStore::new(pool.clone()));
    };
    let recovery = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
    let now = Utc::now();
    let epoch = recovery
        .acquire_component_lease(
            "OutboxDrain",
            owner,
            &now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            &(now + chrono::Duration::seconds(60))
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
        .await
        .map_err(|_| AppError::Runtime)?;
    let scope = scope.for_component(crate::storage::recovery_eligibility::LiveFence {
        component: "OutboxDrain".into(),
        owner: owner.into(),
        epoch,
    });
    Ok(crate::storage::ReliabilityStore::with_recovery(
        pool.clone(),
        scope,
    ))
}

pub(super) async fn prepare_sync(
    client: MatrixClient,
    config: &Config,
    pool: &SqlitePool,
    runtime: &str,
    recovery_scope: Option<crate::storage::recovery_eligibility::RecoveryScope>,
) -> Result<(MatrixClient, MatrixSync), AppError> {
    let recovery = MatrixRecoveryStore::new_scoped(pool.clone(), recovery_scope.clone());
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
    let mut activation_epoch = None;
    match control.phase.as_str() {
        "baseline" if control.watermark.is_none() => {
            tracing::debug!(
                stage = "baseline-publication",
                checkpoint_present = false,
                phase = "baseline",
                "establishing durable cutover before routing input"
            );
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
            activation_epoch = Some(epoch);
            let request_digest = crate::storage::cutover_digest(
                format!("{}:{}", control.operation_id, runtime).as_bytes(),
            );
            let governed = recovery
                .begin_activation_intent(
                    runtime,
                    epoch,
                    &format!("{runtime}-{epoch}"),
                    &request_digest,
                    &now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                )
                .await
                .map_err(|_| AppError::Runtime)?;
            let baseline = client
                .baseline_sync_token()
                .await
                .map_err(|_| AppError::Matrix)?;
            if baseline.is_empty() {
                return Err(AppError::Matrix);
            }
            let now_text = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
            if governed {
                let digest = crate::storage::cutover_digest(baseline.as_bytes());
                recovery
                    .prepare_activation_intent(runtime, epoch, &baseline, &digest, &now_text)
                    .await
                    .map_err(|_| AppError::Runtime)?;
                recovery
                    .commit_activation_intent(
                        &control.operation_id,
                        runtime,
                        epoch,
                        &baseline,
                        &now_text,
                    )
                    .await
                    .map_err(|_| AppError::Runtime)?;
            } else {
                recovery
                    .activate_cutover(&control.operation_id, &baseline, &now_text)
                    .await
                    .map_err(|_| AppError::Runtime)?;
            }
            tracing::info!(
                stage = "baseline-publication",
                committed = true,
                historical_events_executed = 0,
                "durable baseline published before active consumer"
            );
        }
        "active"
            if control
                .watermark
                .as_ref()
                .is_some_and(|token| !token.is_empty()) =>
        {
            tracing::debug!(
                stage = "baseline-publication",
                checkpoint_present = true,
                phase = "active",
                "resuming existing checkpoint without rebaseline"
            );
        }
        _ => {
            tracing::error!(
                stage = "baseline-publication",
                category = "phase-or-checkpoint",
                checkpoint_present = control.watermark.is_some(),
                "startup refused without discarding durable state"
            );
            return Err(AppError::Runtime);
        }
    }
    let now = Utc::now();
    let epoch = match activation_epoch {
        Some(epoch) => epoch,
        None => recovery
            .acquire_component_lease(
                "MatrixSync",
                runtime,
                &now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                &(now + chrono::Duration::seconds(60))
                    .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            )
            .await
            .map_err(|_| AppError::Runtime)?,
    };
    if let Some(scope) = recovery_scope.as_ref() {
        scope
            .install_matrix_fence(crate::storage::recovery_eligibility::LiveFence {
                component: "MatrixSync".into(),
                owner: runtime.into(),
                epoch,
            })
            .map_err(|_| AppError::Runtime)?;
    }
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

#[cfg(test)]
#[path = "cutover_lifecycle_tests.rs"]
mod cutover_lifecycle_tests;
