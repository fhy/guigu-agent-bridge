use guigu_agent_bridge::a2a::wire::{WireProfile, normalize_send, normalize_task_query};
use serde_json::{Value, json};

fn message() -> Value {
    json!({"messageId":"message-a","role":"user","parts":[{"kind":"text","text":"x"}]})
}

#[test]
fn standard_and_legacy_send_shapes_are_explicit() {
    let standard = normalize_send(json!({"message":message()}), &json!(7)).unwrap();
    assert_eq!(standard.profile, WireProfile::A2aV030);
    assert_eq!(standard.request_id, "7");
    assert!(!standard.context_id.is_empty());

    let legacy = normalize_send(
        json!({"protocolVersion":"0.3.0","requestId":"request-a",
            "contextId":"context-a","message":message()}),
        &json!("rpc-only"),
    )
    .unwrap();
    assert_eq!(legacy.profile, WireProfile::LegacyOwnedV030);
    assert_eq!(legacy.request_id, "request-a");
    assert_eq!(legacy.context_id, "context-a");
    assert_ne!(standard.canonical, legacy.canonical);
}

#[test]
fn mixed_and_incomplete_profiles_are_rejected() {
    let cases = [
        json!({"protocolVersion":"0.3.0","requestId":"r","contextId":"c",
            "message":message(),"metadata":{}}),
        json!({"protocolVersion":"0.3.0","message":message()}),
        json!({"requestId":"r","message":message()}),
        json!({"contextId":"c","message":message()}),
        json!({"message":message(),"unknown":true}),
        json!({}),
        Value::Null,
    ];
    for value in cases {
        assert!(normalize_send(value, &json!("rpc")).is_err());
    }
}

#[test]
fn rpc_id_types_and_bounds_are_enforced() {
    for valid in [json!("x"), json!(0), json!(u64::MAX)] {
        assert!(normalize_send(json!({"message":message()}), &valid).is_ok());
    }
    for invalid in [
        json!(""),
        json!(-1),
        json!(1.5),
        Value::Null,
        json!([]),
        json!({}),
    ] {
        assert!(normalize_send(json!({"message":message()}), &invalid).is_err());
    }
    assert!(normalize_send(json!({"message":message()}), &json!("x".repeat(256))).is_ok());
    assert!(normalize_send(json!({"message":message()}), &json!("x".repeat(257))).is_err());
}

#[test]
fn extensions_message_and_context_are_bounded() {
    assert!(
        normalize_send(
            json!({"message":message(),"configuration":{"mode":"x"},"metadata":[1]}),
            &json!("r")
        )
        .is_ok()
    );
    assert!(
        normalize_send(
            json!({"message":message(),"metadata":"x".repeat(65_537)}),
            &json!("r")
        )
        .is_err()
    );
    let mut too_many = Vec::new();
    for _ in 0..65 {
        too_many.push(json!({"kind":"text","text":"x"}));
    }
    assert!(
        normalize_send(
            json!({"message":{"messageId":"m","role":"user","parts":too_many}}),
            &json!("r")
        )
        .is_err()
    );
    assert!(
        normalize_send(
            json!({"message":{"messageId":"m","contextId":"x".repeat(257),
            "role":"user","parts":[]}}),
            &json!("r")
        )
        .is_err()
    );
}

#[test]
fn task_query_profiles_and_mixed_shapes_are_enforced() {
    let standard = normalize_task_query(json!({"id":"task-a"})).unwrap();
    assert_eq!(standard.profile, WireProfile::A2aV030);
    let legacy =
        normalize_task_query(json!({"protocolVersion":"0.3.0","taskId":"task-a"})).unwrap();
    assert_eq!(legacy.profile, WireProfile::LegacyOwnedV030);
    for invalid in [
        json!({"id":"task-a","taskId":"task-a","protocolVersion":"0.3.0"}),
        json!({"taskId":"task-a"}),
        json!({"protocolVersion":"9.9.9","taskId":"task-a"}),
        json!({"id":""}),
        json!({"id":7}),
        json!({"id":"task-a","metadata":{}}),
        Value::Null,
    ] {
        assert!(normalize_task_query(invalid).is_err());
    }
}
