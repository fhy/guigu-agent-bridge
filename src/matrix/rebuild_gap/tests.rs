use super::*;
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path_regex, query_param},
};

fn value(id: &str) -> Value {
    json!({"type":"m.room.message","event_id":id,"sender":"@alice:example.test","origin_server_ts":1,"content":{"msgtype":"m.notice","body":"notice"}})
}
fn event(id: &str) -> TimelineEvent {
    TimelineEvent::from_plaintext(serde_json::from_value(value(id)).unwrap())
}
fn anchor() -> Anchor {
    Anchor {
        room: "!room:example.test".into(),
        event: "$anchor".into(),
        batch: "old".into(),
        digest: format!(
            "{:x}",
            Sha256::digest(value("$anchor").to_string().as_bytes())
        ),
    }
}

#[test]
fn succession_gap_anchor_drift_duplicate_and_unrouted_guards() {
    let mut events = Vec::new();
    assert!(
        append_page(
            &anchor(),
            &mut events,
            vec![event("$new"), event("$anchor"), event("$old")]
        )
        .unwrap()
    );
    assert_eq!(events.len(), 1);
    let mut changed = anchor();
    changed.digest = "changed".into();
    assert!(append_page(&changed, &mut Vec::new(), vec![event("$anchor")]).is_err());
    assert!(validate_events(&[event("$same"), event("$same")]).is_err());
    let mut config = crate::config::load_from_str("").unwrap();
    let mut raw = value("$text");
    raw["content"]["msgtype"] = "m.text".into();
    assert!(unrouted_no_effect(&config, &raw, "!other:example.test"));
    for body in [" /cancel task", "@agent: prompt"] {
        raw["content"]["body"] = body.into();
        assert!(!unrouted_no_effect(&config, &raw, "!other:example.test"));
    }
    raw["content"]["body"] = "ordinary".into();
    config
        .transports
        .matrix
        .routes
        .rooms
        .insert("!other:example.test".into(), "agent".into());
    assert!(!unrouted_no_effect(&config, &raw, "!other:example.test"));
    raw["type"] = "m.room.encrypted".into();
    assert!(!unrouted_no_effect(&config, &raw, "!else:example.test"));
}

async fn client(server: &MockServer) -> MatrixClient {
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"versions":["v1.1"]})))
        .mount(server)
        .await;
    Mock::given(method("GET")).and(path_regex("/_matrix/client/.*/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"next_batch":"C","rooms":{"join":{"!room:example.test":{"timeline":{"limited":true,"prev_batch":"page-1","events":[value("$last")]}}}}}))).mount(server).await;
    let mut client = MatrixClient::for_test().await;
    client.inner = matrix_sdk::Client::builder()
        .homeserver_url(server.uri())
        .build()
        .await
        .unwrap();
    client
        .inner
        .restore_session(matrix_sdk::authentication::matrix::MatrixSession {
            meta: matrix_sdk::SessionMeta {
                user_id: "@bridge:example.test".parse().unwrap(),
                device_id: "BRIDGE".into(),
            },
            tokens: matrix_sdk::SessionTokens {
                access_token: "fixture".into(),
                refresh_token: None,
            },
        })
        .await
        .unwrap();
    client
}

#[tokio::test]
async fn succession_gap_sdk_pagination_proves_overlap_and_refuses_missing_or_changed_anchor() {
    for mode in ["complete", "missing", "changed"] {
        let server = MockServer::start().await;
        let client = client(&server).await;
        let mut response = client
            .inner
            .sync_once(SyncSettings::default())
            .await
            .unwrap();
        let f =
            crate::storage::recovery_eligibility::writer::succession::tests::fixture(false).await;
        // This fixture injects only the already committed overlap records. The
        // actual SDK performs both paginated requests and decryption conversion.
        sqlx::query("INSERT INTO matrix_recovery_receipts(account_id,room_id,event_id,cutover_operation_id,payload_digest,state,batch_id,created_at,updated_at) VALUES('@bridge:example.test','!room:example.test','$anchor',?,?,'dispositioned','prior','t','t')")
            .bind(&f.operation).bind(&anchor().digest).execute(&f.pool).await.unwrap();
        Mock::given(method("GET"))
            .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
            .and(query_param("from", "page-1"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    json!({"start":"page-1","end":"page-2","chunk":[value("$middle")]}),
                ),
            )
            .expect(1)
            .mount(&server)
            .await;
        let mut last = value("$anchor");
        if mode == "changed" {
            last["content"]["body"] = "changed".into();
        }
        let chunk = if mode == "missing" {
            vec![]
        } else {
            vec![last]
        };
        Mock::given(method("GET"))
            .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
            .and(query_param("from", "page-2"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"start":"page-2","chunk":chunk})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let result = fill(&client, &f.pool, &f.operation, &mut response).await;
        assert_eq!(result.is_ok(), mode == "complete", "{mode}: {result:?}");
        assert_eq!(
            response
                .rooms
                .joined
                .values()
                .next()
                .unwrap()
                .timeline
                .limited,
            mode != "complete"
        );
        let watermark: String = sqlx::query_scalar("SELECT watermark FROM matrix_recovery_control")
            .fetch_one(&f.pool)
            .await
            .unwrap();
        assert_eq!(watermark, "B");
        f.pool.close().await;
    }
}

#[tokio::test]
async fn gap_predicates_and_exact_readonly_retry_do_not_disclose_inputs() {
    for (mode, expected) in [
        ("anchor", "anchor-absent"),
        ("cursor", "cursor-absent"),
        ("shape", "page-start-mismatch"),
        ("end", "page-end-absent"),
        ("repeat", "cursor-repeat"),
        ("transport", "page-transport"),
        ("digest", "anchor-digest-mismatch"),
        ("duplicate", "event-duplicate"),
    ] {
        let server = MockServer::start().await;
        let client = client(&server).await;
        let mut response = client
            .inner
            .sync_once(SyncSettings::default())
            .await
            .unwrap();
        let f =
            crate::storage::recovery_eligibility::writer::succession::tests::fixture(false).await;
        if mode != "anchor" {
            sqlx::query("INSERT INTO matrix_recovery_receipts(account_id,room_id,event_id,cutover_operation_id,payload_digest,state,batch_id,created_at,updated_at) VALUES('@bridge:example.test','!room:example.test','$anchor',?,?,'dispositioned','prior','t','t')")
                .bind(&f.operation).bind(&anchor().digest).execute(&f.pool).await.unwrap();
        }
        if mode == "cursor" {
            response
                .rooms
                .joined
                .values_mut()
                .next()
                .unwrap()
                .timeline
                .prev_batch = None;
        }
        predicate_page(&server, mode).await;
        for _ in 0..2 {
            let failure = fill(&client, &f.pool, &f.operation, &mut response)
                .await
                .unwrap_err();
            assert_eq!(failure.reason, expected, "{mode}");
            let capture = crate::test_diagnostics::Capture::default();
            tracing::subscriber::with_default(capture.subscriber(), || failure.report());
            let output = capture.output();
            assert!(output.contains(expected));
            for private in [
                "$anchor", "$last", "page-1", "secret-", "@bridge", "!room", "SELECT",
            ] {
                assert!(!output.contains(private), "{private}");
            }
        }
        let watermark: String = sqlx::query_scalar("SELECT watermark FROM matrix_recovery_control")
            .fetch_one(&f.pool)
            .await
            .unwrap();
        assert_eq!(watermark, "B");
        f.pool.close().await;
    }
}

#[test]
fn gap_event_size_identity_and_limit_fail_closed_with_distinct_predicates() {
    let missing = TimelineEvent::from_plaintext(
        serde_json::from_value(json!({"type":"m.room.message","content":{}})).unwrap(),
    );
    assert_eq!(
        validate_events(&[missing]).unwrap_err().reason,
        "event-id-absent"
    );
    let mut huge = value("$large");
    huge["content"]["body"] = "x".repeat(16 * 1024 * 1024).into();
    let huge = TimelineEvent::from_plaintext(serde_json::from_value(huge).unwrap());
    assert_eq!(validate_events(&[huge]).unwrap_err().reason, "byte-limit");
    let mut events = vec![event("$existing"); MAX_RECOVERY_EVENTS];
    assert_eq!(
        append_page(&anchor(), &mut events, vec![event("$extra")])
            .unwrap_err()
            .reason,
        "event-limit"
    );
}

async fn mount_pages(server: &MockServer, count: usize, anchor_page: Option<usize>) {
    for page_number in 1..=count {
        let cursor = format!("page-{page_number}");
        let end = format!("page-{}", page_number + 1);
        let chunk = if anchor_page == Some(page_number) {
            vec![value("$anchor")]
        } else {
            vec![value(&format!("$page-{page_number}"))]
        };
        Mock::given(method("GET"))
            .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
            .and(query_param("from", cursor.clone()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"start":cursor,"end":end,"chunk":chunk})),
            )
            .expect(1)
            .mount(server)
            .await;
    }
}

#[tokio::test]
async fn succession_gap_covers_history_past_original_page_window() {
    let server = MockServer::start().await;
    let client = client(&server).await;
    let mut response = client
        .inner
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    let f = crate::storage::recovery_eligibility::writer::succession::tests::fixture(false).await;
    sqlx::query("INSERT INTO matrix_recovery_receipts(account_id,room_id,event_id,cutover_operation_id,payload_digest,state,batch_id,created_at,updated_at) VALUES('@bridge:example.test','!room:example.test','$anchor',?,?,'dispositioned','prior','t','t')")
        .bind(&f.operation)
        .bind(&anchor().digest)
        .execute(&f.pool)
        .await
        .unwrap();
    mount_pages(&server, 17, Some(17)).await;

    let anchors = fill(&client, &f.pool, &f.operation, &mut response)
        .await
        .unwrap();
    assert_eq!(anchors.len(), 1);
    let timeline = &response.rooms.joined.values().next().unwrap().timeline;
    assert!(!timeline.limited);
    assert_eq!(timeline.events.len(), 17);
    f.pool.close().await;
}

#[tokio::test]
async fn succession_gap_retains_a_finite_page_limit_after_window_extension() {
    let server = MockServer::start().await;
    let client = client(&server).await;
    let mut response = client
        .inner
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    let f = crate::storage::recovery_eligibility::writer::succession::tests::fixture(false).await;
    sqlx::query("INSERT INTO matrix_recovery_receipts(account_id,room_id,event_id,cutover_operation_id,payload_digest,state,batch_id,created_at,updated_at) VALUES('@bridge:example.test','!room:example.test','$anchor',?,?,'dispositioned','prior','t','t')")
        .bind(&f.operation)
        .bind(&anchor().digest)
        .execute(&f.pool)
        .await
        .unwrap();
    mount_pages(&server, MAX_RECOVERY_PAGES, None).await;

    let capture = crate::test_diagnostics::Capture::default();
    let failure = fill(&client, &f.pool, &f.operation, &mut response)
        .await
        .unwrap_err();
    assert_eq!(failure.reason, "page-limit");
    tracing::subscriber::with_default(capture.subscriber(), || failure.report());
    let output = capture.output();
    assert!(output.contains("page-limit"));
    assert!(output.contains("page=32"));
    assert!(output.contains("events=32"));
    f.pool.close().await;
}

async fn predicate_page(server: &MockServer, mode: &str) {
    let mut raw = value("$anchor");
    if mode == "digest" {
        raw["content"]["body"] = "secret-changed".into();
    }
    let chunk = match mode {
        "end" | "repeat" => vec![],
        "duplicate" => vec![value("$last"), raw],
        _ => vec![raw],
    };
    let mut body = json!({"start":"page-1","chunk":chunk});
    if mode == "shape" {
        body["start"] = "secret-other-cursor".into();
    }
    if mode == "repeat" {
        body["end"] = "page-1".into();
    }
    let reply = if mode == "transport" {
        ResponseTemplate::new(403)
            .set_body_json(json!({"errcode":"M_FORBIDDEN","error":"secret-server-error"}))
    } else {
        ResponseTemplate::new(200).set_body_json(body)
    };
    Mock::given(method("GET"))
        .and(path_regex("/_matrix/client/.*/rooms/.*/messages"))
        .respond_with(reply)
        .mount(server)
        .await;
}
