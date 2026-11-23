//! C4 actual loopback runtime; not live E2EE.
use super::*;
#[path = "history_completion_support.rs"]
mod support;
use support::*;
#[path = "history_completion_diagnostics.rs"]
mod diagnostics;

#[tokio::test]
#[ignore = "explicit C4 loopback SDK/ACP completion and same-store reopen"]
async fn completion_then_agent_reply_and_same_pair_restart() {
    let capture = crate::test_diagnostics::Capture::default();
    let _ = tracing::subscriber::set_global_default(capture.subscriber());
    let (f, server, stream) = prepare().await;
    let runtime = start(&f, &server).await;
    let pool = runtime.test_storage_pool();
    watermark(&pool, "C").await;
    *stream.0.lock().unwrap() = ("E".into(), vec![event("$task-one", "complete a test task")]);
    watermark(&pool, "E").await;
    completed(&pool, 1).await;
    successful_reply(&server, "$task-one").await;
    let result = diagnostics::wait_projection(&pool, &server, &runtime, 1, false).await;
    if let Err(error) = result {
        eprintln!("{}", capture.output());
        let cleanup = runtime.shutdown().await;
        panic!("first projection: {error}; cleanup={cleanup:?}");
    }
    assert_terminal_projections(&pool, 1).await;
    println!("C4 first durable_sent=1 attempt_count=1");
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM matrix_recovery_receipts WHERE state='dispositioned'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(receipt_count, 1);
    drop(pool);
    runtime.shutdown().await.unwrap();
    println!("C4 first normal_shutdown=ok");
    second_cycle(&f, &server, &stream, receipt_count).await;
    let pool = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM matrix_recovery_receipts WHERE state='dispositioned'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_events WHERE status='completed'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    diagnostics::snapshot(&pool, &server, None, "second-reopened").await;
    assert_terminal_projections(&pool, 2).await;
    assert_unique(&server).await;
    println!("C4 second durable_sent=2 attempt_count=1 unique_projection_sends=2");
    pool.close().await;
    println!("C4 ACP_completed=2 successful_replies=2 same_pair=true normal_shutdowns=2");
}

async fn assert_terminal_projections(pool: &sqlx::SqlitePool, expected: usize) {
    let rows: Vec<(String, i64, i64)> =
        sqlx::query_as("SELECT state,attempt_count,claim_revision FROM projection_outbox")
            .fetch_all(pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), expected);
    assert!(rows.iter().all(|r| r == &("sent".into(), 1, 1)), "{rows:?}");
}

async fn second_cycle(f: &Fixture, server: &MockServer, stream: &Stream, receipt_count: i64) {
    // Same files, config, device, crypto and checkpoint. No restore or mutation.
    stream.0.lock().unwrap().1 = vec![];
    let runtime = start(f, server).await;
    let pool = runtime.test_storage_pool();
    watermark(&pool, "E").await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM matrix_recovery_receipts WHERE state='dispositioned'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        receipt_count
    );
    *stream.0.lock().unwrap() = (
        "F".into(),
        vec![event("$task-two", "complete another test task")],
    );
    watermark(&pool, "F").await;
    completed(&pool, 2).await;
    successful_reply(server, "$task-two").await;
    // A durable claim precedes send authorization and can still be sync-busy.
    // Wait for the actual HTTP request, not just state='sending', before stopping.
    let result = diagnostics::wait_projection(&pool, server, &runtime, 2, true).await;
    if let Err(error) = result {
        let cleanup = runtime.shutdown().await;
        panic!("second projection send: {error}; cleanup={cleanup:?}");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        runtime.health_state().snapshot().await.readiness,
        crate::runtime::Readiness::Ready
    );
    drop(pool);
    runtime.shutdown().await.unwrap();
    println!("C4 second normal_shutdown=ok send_overlap=true");
}

async fn assert_unique(server: &MockServer) {
    let outputs = replies(server).await;
    assert_eq!(
        outputs
            .iter()
            .filter(|(p, _)| !p.contains("/t051-"))
            .count(),
        2
    );
    let transactions: std::collections::BTreeSet<_> = outputs.iter().map(|(p, _)| p).collect();
    assert_eq!(transactions.len(), outputs.len(), "no transaction resend");
    for id in ["$task-one", "$task-two"] {
        assert_eq!(
            outputs
                .iter()
                .filter(
                    |(_, v)| v["m.relates_to"]["m.in_reply_to"]["event_id"] == id
                        && v["body"]
                            .as_str()
                            .is_some_and(|b| b.contains("Hello, world"))
                )
                .count(),
            1
        );
    }
}
