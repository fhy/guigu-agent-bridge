//! Read-only state at C4 observation boundaries; only synthetic loopback metadata.
use super::*;

pub(super) async fn wait_projection(
    pool: &sqlx::SqlitePool,
    server: &MockServer,
    runtime: &AppRuntime,
    expected: usize,
    stop_at_send: bool,
) -> Result<(), &'static str> {
    // ACP output is independent. Allow pre-send scheduling/busy (30s), HTTP (30s),
    // completion-only retry (30s), and 5s margin. Never mask a failed owner.
    let start = tokio::time::Instant::now();
    let mut last = None;
    let mut old_boundary = false;
    let wait = async {
        loop {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT stable_txn_id,state FROM projection_outbox ORDER BY created_at",
            )
            .fetch_all(pool)
            .await
            .unwrap();
            let requests = replies(server).await;
            let sent = rows.iter().filter(|(_, state)| state == "sent").count();
            let sending = rows.iter().any(|(txn, state)| {
                state == "sending" && requests.iter().any(|(path, _)| path.ends_with(txn))
            });
            let state = (rows.clone(), requests.len());
            if last.as_ref() != Some(&state) {
                snapshot(pool, server, Some(runtime), "projection-transition").await;
                last = Some(state);
            }
            if !old_boundary && start.elapsed() >= Duration::from_secs(15) {
                snapshot(pool, server, Some(runtime), "old-15s-boundary").await;
                old_boundary = true;
            }
            let health = runtime.health_state().snapshot().await;
            if health.readiness != crate::runtime::Readiness::Ready {
                return Err("required owner/readiness failed");
            }
            if rows.iter().any(|(_, state)| state == "abandoned") {
                return Err("projection send failed");
            }
            if rows.len() == expected
                && ((!stop_at_send && sent == expected)
                    || (stop_at_send && sent == expected - 1 && sending))
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let result = tokio::time::timeout(Duration::from_secs(95), wait)
        .await
        .unwrap_or(Err("projection lifecycle observation expired"));
    if result.is_err() {
        snapshot(pool, server, Some(runtime), "projection-wait-failed").await;
    }
    result
}

pub(super) async fn snapshot(
    pool: &sqlx::SqlitePool,
    server: &MockServer,
    runtime: Option<&AppRuntime>,
    boundary: &str,
) {
    let projections: Vec<(String, i64, bool, Option<i64>)> = sqlx::query_as(
        "SELECT state,attempt_count,claim_owner IS NOT NULL,claim_revision FROM projection_outbox",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let batches: Vec<(String, i64)> =
        sqlx::query_as("SELECT state,count(*) FROM matrix_recovery_batches GROUP BY state")
            .fetch_all(pool)
            .await
            .unwrap();
    let leases: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT component,state,epoch FROM matrix_component_leases ORDER BY component",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let sends = replies(server).await;
    let projection_calls = sends.iter().filter(|(p, _)| !p.contains("/t051-")).count();
    let health = match runtime {
        Some(runtime) => Some(runtime.health_state().snapshot().await),
        None => None,
    };
    eprintln!(
        "C4 boundary={boundary} projections(state,attempt,claimed,revision)={projections:?} \
         sync={batches:?} leases={leases:?} projection_http_calls={projection_calls} \
         health={health:?} shutdown_complete={}",
        runtime.is_none()
    );
}
