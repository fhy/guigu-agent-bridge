//! Actual SDK -> classified progress -> native publication -> AppRuntime restart.
use super::*;
use crate::matrix::coverage::Coverage;
use tracing::instrument::WithSubscriber;

async fn held(f: &Fixture, server: &MockServer) {
    hold::initialize_response(f, server, two_rooms("C", false)).await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("full_state", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(hold::response(
            "refused-next",
            true,
            vec![],
        )))
        .with_priority(1)
        .up_to_n_times(1)
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"errcode":"M_UNKNOWN","error":"synthetic refusal"})),
        )
        .with_priority(1)
        .up_to_n_times(1)
        .expect(1)
        .mount(server)
        .await;
    hold::produce_refusal(f, server).await;
}
async fn pages(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("full_state", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(two_rooms("D", true)))
        .with_priority(1)
        .up_to_n_times(1)
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/sync"))
        .and(query_param("since", "D"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"next_batch":"D"}))
                .set_delay(Duration::from_secs(1)),
        )
        .with_priority(2)
        .mount(server)
        .await;
    // One interrupted response after twelve committed pages; its reservation remains charged.
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
        .and(query_param("from", "p12"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"errcode":"M_UNKNOWN","error":"synthetic interruption"})),
        )
        .with_priority(1)
        .up_to_n_times(1)
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
        .and(query_param("from", "second-page"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"start":"second-page","end":"second-done","chunk":[second_anchor()]}),
        ))
        .with_priority(2)
        .expect(1)
        .mount(server)
        .await;

    for n in 0..41 {
        let start = if n == 0 {
            "recover-page".into()
        } else {
            format!("p{n}")
        };
        let events = if n == 40 {
            vec![hold::anchor()]
        } else {
            (0..100).map(|i|json!({
            "type":"m.room.message","event_id":format!("$segment-{n}-{i}"),"sender":"@other:example.test",
            "origin_server_ts":3,"content":{"msgtype":"m.notice","body":"silent history"}
        })).collect()
        };
        Mock::given(method("GET"))
            .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
            .and(query_param("from", start.clone()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"start":start,"end":format!("p{}",n+1),"chunk":events})),
            )
            .with_priority(2)
            .expect(1)
            .mount(server)
            .await;
    }
}
fn second_anchor() -> serde_json::Value {
    let mut a = hold::anchor();
    a["event_id"] = "$second-anchor".into();
    a
}
fn two_rooms(next: &str, limited: bool) -> serde_json::Value {
    let mut response = hold::response(
        next,
        limited,
        if limited {
            vec![]
        } else {
            vec![hold::anchor()]
        },
    );
    let mut second = response["rooms"]["join"]["!room:example.test"].clone();
    second["timeline"]["prev_batch"] = "second-page".into();
    second["timeline"]["events"] = json!(if limited {
        vec![]
    } else {
        vec![second_anchor()]
    });
    response["rooms"]["join"]["!second:example.test"] = second;
    response
}

async fn journal(pool: &sqlx::SqlitePool) -> Coverage {
    let mut c = pool.acquire().await.unwrap();
    crate::storage::recovery_eligibility::writer::succession::coverage::read(&mut c)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
#[ignore = "explicit isolated producer and 4000-event SDK/storage lifecycle"]
async fn coverage_sdk_4000_interruption_resume_ready_and_restart() {
    let f = Fixture::new();
    let server = MockServer::start().await;
    mount_server(&server).await;
    held(&f, &server).await;
    pages(&server).await;
    let pool = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    let mut c = pool.acquire().await.unwrap();
    let before = super::super::state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let captured = crate::test_diagnostics::Capture::default();
    let failed = AppRuntime::start_with_config_and_a2a_faults(
        f.root.path().join("bridge.toml"),
        f.config(true, &server.uri()),
        Arc::new(crate::a2a::NoFault),
    )
    .with_subscriber(captured.subscriber())
    .await
    .unwrap();
    println!("{}", captured.output());
    assert_eq!(
        failed.health_state().snapshot().await.readiness,
        crate::runtime::Readiness::RecoveryBlocked
    );
    failed.shutdown().await.unwrap();
    let progress = journal(&pool).await;
    assert_eq!(progress.pages.len(), 12);
    assert_eq!(progress.attempts, 13);
    assert_eq!(progress.sync_attempts, 1);
    assert_eq!(progress.status, "collecting");
    assert_eq!(progress.refusal.as_deref(), Some("page-transport"));
    let mut c = pool.acquire().await.unwrap();
    assert_eq!(
        super::super::state::read(&mut c).await.unwrap().as_ref(),
        Some(&before)
    );
    drop(c);
    let began = std::time::Instant::now();
    for _ in 0..2 {
        lifecycle::start(f.root.path(), &server.uri())
            .await
            .shutdown()
            .await
            .unwrap();
    }
    let result = journal(&pool).await;
    assert_eq!(result.status, "published");
    assert_eq!(result.attempts, 43);
    assert_eq!(result.sync_attempts, 1);
    assert!(result.segments >= 4);
    let receipts: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_receipts WHERE event_id LIKE '$segment-%' AND state='dispositioned'").fetch_one(&pool).await.unwrap();
    assert_eq!(receipts, 4000);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert!(history_fixture::notices(&pool).await.is_empty());
    assert!(
        !server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.url.path().contains("/send/") || r.url.path().contains("/login"))
    );
    pool.close().await;
    println!(
        "sdk_4000_resume=true limited_rooms=2 no_refetch=true no_new_sync=true ready_restart=true no_replay=true elapsed_ms={}",
        began.elapsed().as_millis()
    );
}
