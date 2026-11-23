use super::*;

pub(super) async fn restore(
    config: &Config,
    health: &Arc<HealthState>,
    generation: u64,
) -> Result<MatrixClient, ()> {
    let identity_health = Arc::clone(health);
    let mut identity_matched = false;
    let mut store_bound = false;
    let client = match MatrixClient::restore_with_identity(
        &config.transports.matrix,
        || {
            identity_matched = true;
            identity_health.matrix().mark_authentication_succeeded();
            identity_health.matrix().transition(
                generation,
                &[MatrixPhase::Preflight],
                MatrixPhase::IdentityMatched,
            );
        },
        || {
            store_bound = true;
            identity_health.matrix().mark_store_binding_succeeded();
        },
        || {
            identity_health.matrix().mark_session_restore_succeeded();
        },
    )
    .await
    {
        Ok(client) => client,
        Err(error) => {
            identity_health.matrix().mark_restore_failure(
                identity_matched,
                store_bound,
                matrix_failure(&error),
            );
            health.matrix().transition(
                generation,
                &[MatrixPhase::Preflight, MatrixPhase::IdentityMatched],
                MatrixPhase::Failed(matrix_failure(&error)),
            );
            tracing::warn!(
                category = matrix_failure(&error).as_log_str(),
                "Matrix startup is health-only"
            );
            return Err(());
        }
    };
    health.matrix().transition(
        generation,
        &[MatrixPhase::IdentityMatched],
        MatrixPhase::StoreBound,
    );
    let deadline = tokio::time::Instant::now() + MATRIX_INITIALIZATION_BUDGET;
    initialize(&client, health, generation, deadline).await?;
    prove(&client, health, generation, deadline).await?;
    health.matrix().transition(
        generation,
        &[MatrixPhase::StoreBound],
        MatrixPhase::KeyProved,
    );
    Ok(client)
}

async fn initialize(
    client: &MatrixClient,
    health: &Arc<HealthState>,
    generation: u64,
    deadline: tokio::time::Instant,
) -> Result<(), ()> {
    let e2ee = within_matrix_deadline(deadline, client.initialize_e2ee()).await;
    match e2ee {
        DeadlineOutcome::Completed(Ok(())) => {
            health.matrix().mark_e2ee_succeeded();
        }
        DeadlineOutcome::Completed(Err(error)) => {
            let reason = matrix_failure(&error);
            health.matrix().mark_e2ee_failure(reason);
            health.matrix().transition(
                generation,
                &[MatrixPhase::StoreBound],
                MatrixPhase::Failed(reason),
            );
            tracing::warn!(
                category = reason.as_log_str(),
                "Matrix startup is health-only"
            );
            return Err(());
        }
        DeadlineOutcome::TimedOut => {
            let reason = MatrixFailure::Timeout;
            health.matrix().mark_e2ee_timeout();
            health.matrix().transition(
                generation,
                &[MatrixPhase::StoreBound],
                MatrixPhase::Failed(reason),
            );
            tracing::warn!(
                category = reason.as_log_str(),
                "Matrix startup is health-only"
            );
            return Err(());
        }
        DeadlineOutcome::NotAttempted => {
            let reason = MatrixFailure::Timeout;
            health.matrix().transition(
                generation,
                &[MatrixPhase::StoreBound],
                MatrixPhase::Failed(reason),
            );
            tracing::warn!(
                category = reason.as_log_str(),
                "Matrix startup is health-only"
            );
            return Err(());
        }
    }
    Ok(())
}

async fn prove(
    client: &MatrixClient,
    health: &Arc<HealthState>,
    generation: u64,
    deadline: tokio::time::Instant,
) -> Result<(), ()> {
    let proof = within_matrix_deadline(deadline, client.prove_server_device_key()).await;
    match proof {
        DeadlineOutcome::Completed(Ok(())) => health.matrix().mark_key_proof_succeeded(),
        DeadlineOutcome::Completed(Err(error)) => {
            let reason = matrix_failure(&error);
            health.matrix().mark_key_proof_failure(reason);
            health.matrix().transition(
                generation,
                &[MatrixPhase::StoreBound],
                MatrixPhase::Failed(reason),
            );
            tracing::warn!(
                category = reason.as_log_str(),
                "Matrix startup is health-only"
            );
            return Err(());
        }
        DeadlineOutcome::TimedOut => {
            let reason = MatrixFailure::Timeout;
            health.matrix().mark_key_proof_timeout();
            health.matrix().transition(
                generation,
                &[MatrixPhase::StoreBound],
                MatrixPhase::Failed(reason),
            );
            tracing::warn!(
                category = reason.as_log_str(),
                "Matrix startup is health-only"
            );
            return Err(());
        }
        DeadlineOutcome::NotAttempted => {
            let reason = MatrixFailure::Timeout;
            health.matrix().transition(
                generation,
                &[MatrixPhase::StoreBound],
                MatrixPhase::Failed(reason),
            );
            tracing::warn!(
                category = reason.as_log_str(),
                "Matrix startup is health-only"
            );
            return Err(());
        }
    }
    Ok(())
}
