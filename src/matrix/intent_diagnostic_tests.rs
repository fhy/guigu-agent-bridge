use super::intent_diagnostic::{self as diagnostic, Position};
use serde_json::{Value, json};
use tracing::instrument::WithSubscriber;
const ROOM: &str = "!SECRET-room:example.test";
fn config() -> crate::config::Config {
    crate::config::load_from_str("[agents.worker]\ntransport='acp'\ncommand='must-not-start'\nworkspace='/tmp'\nenabled=true\n[transports.matrix]\nuser_id='@bridge:example.test'\nallowed_users=['@SECRET-sender:example.test']\n").unwrap()
}
fn event() -> Value {
    json!({"event_id":"$SECRET-event","type":"m.room.message","sender":"@SECRET-sender:example.test","content":{"msgtype":"m.text","body":"@missing: SECRET-body"}})
}
#[tokio::test]
async fn precise_route_predicates_preserve_acceptance_and_redact_all_stages() {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let mut c = pool.acquire().await.unwrap();
    for (case, expected) in [
        (0, "route-admission-intent"),
        (1, "permission-reply-intent"),
        (2, "unavailable-reply-intent"),
        (3, "route-config"),
        (4, "route-ambiguous"),
        (5, "route-identity-invalid"),
        (6, "admin-intent"),
        (7, "event-malformed"),
        (8, "event-unknown"),
        (9, "event-id"),
    ] {
        let (cfg, value) = branch(case);
        let capture = crate::test_diagnostics::Capture::default();
        for position in [
            Position::target(3, 5),
            Position::page(3, 5),
            Position::publication(3, 5),
        ] {
            let error = diagnostic::check(&mut c, &cfg, ROOM, &value, position)
                .with_subscriber(capture.subscriber())
                .await
                .unwrap_err();
            assert_eq!(diagnostic::predicate(&error), expected, "case {case}");
            assert_eq!(
                error.to_string(),
                format!("encountered unexpected or invalid data: native-succession/{expected}")
            );
        }
        let output = capture.output();
        assert!(!output.contains("SECRET"));
        assert!(output.contains(expected));
        for key in [
            "target",
            "page",
            "final",
            "event_digest",
            "room_digest",
            "payload_digest",
        ] {
            assert!(output.contains(key), "{key}");
        }
    }
    super::ingress_decision::no_effect(&mut c, &config(), ROOM, &event())
        .await
        .unwrap();
    drop(c);
    pool.close().await;
}

#[test]
fn diagnostic_never_formats_unknown_errors_and_bounds_positions() {
    let capture = crate::test_diagnostics::Capture::default();
    tracing::subscriber::with_default(capture.subscriber(), || {
        diagnostic::report_for_test(
            &sqlx::Error::Protocol("SECRET-error".into()),
            ROOM,
            &event(),
            Position::target(usize::MAX, usize::MAX),
        );
    });
    let output = capture.output();
    assert!(!output.contains("SECRET"));
    assert!(output.contains("route-unexpected"));
    assert!(output.contains("12800"));
    assert_eq!(
        diagnostic::predicate(&sqlx::Error::RowNotFound),
        "storage-error"
    );
}

fn branch(case: usize) -> (crate::config::Config, Value) {
    let mut cfg = config();
    let mut value = event();
    cfg.transports
        .matrix
        .routes
        .aliases
        .insert("missing".into(), "worker".into());
    match case {
        1 => cfg.transports.matrix.allowed_users.clear(),
        2 => {
            cfg.transports.matrix.routes.aliases.clear();
            cfg.transports.matrix.monitor_room = ROOM.into();
        }
        3 => {
            cfg.agents.get_mut("worker").unwrap().enabled = false;
        }
        4 => {
            value["content"]["body"] = "SECRET-body".into();
            cfg.transports
                .matrix
                .routes
                .direct
                .insert(ROOM.into(), vec!["worker".into(), "worker".into()]);
        }
        5 => value["sender"] = "SECRET-invalid".into(),
        6 => value["content"]["body"] = "/status SECRET-body".into(),
        7 => {
            value.as_object_mut().unwrap().remove("sender");
        }
        8 => value["type"] = "m.room.encrypted".into(),
        9 => {
            value.as_object_mut().unwrap().remove("event_id");
        }
        _ => (),
    }
    (cfg, value)
}
