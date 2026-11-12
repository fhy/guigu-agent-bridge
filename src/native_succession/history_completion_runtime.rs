//! C4 actual loopback runtime; not live E2EE.
use super::*;
#[path = "history_completion_support.rs"]
mod support;
use support::*;

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
    let completion = tokio::spawn({
        let pool = pool.clone();
        async move { terminal_projection(&pool, 1).await }
    })
    .await;
    if completion.is_err() {
        eprintln!("{}", capture.output());
    }
    completion.unwrap();
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM matrix_recovery_receipts WHERE state='dispositioned'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(receipt_count, 1);
    drop(pool);
    runtime.shutdown().await.unwrap();
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
    terminal_projection(&pool, 2).await;
    pool.close().await;
    println!("C4 ACP_completed=2 successful_replies=2 same_pair=true normal_shutdowns=2");
}

async fn terminal_projection(pool: &sqlx::SqlitePool, expected: i64) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let n: i64 =
                sqlx::query_scalar("SELECT count(*) FROM projection_outbox WHERE state='sent'")
                    .fetch_one(pool)
                    .await
                    .unwrap();
            if n == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("durable completion after successful send");
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
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let sending: i64 =
                sqlx::query_scalar("SELECT count(*) FROM projection_outbox WHERE state='sending'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            if sending == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("shutdown overlaps network completion");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    assert_unique(server).await;
    assert_eq!(
        runtime.health_state().snapshot().await.readiness,
        crate::runtime::Readiness::Ready
    );
    drop(pool);
    runtime.shutdown().await.unwrap();
}

async fn assert_unique(server: &MockServer) {
    let outputs = replies(server).await;
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
