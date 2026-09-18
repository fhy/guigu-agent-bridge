use std::collections::BTreeMap;

use guigu_agent_bridge::bus::EndpointRegistry;
use guigu_agent_bridge::config::{ConfigError, load_from_str_with_env};
use guigu_agent_bridge::models::EndpointAddress;

fn env() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("HOME".into(), "/tmp".into()),
        ("A2A_TOKEN".into(), "secret-value".into()),
    ])
}

#[test]
fn a2a_is_disabled_and_bounded_by_default() {
    let config = load_from_str_with_env("", &env()).unwrap();
    assert!(!config.transports.a2a.enabled);
    assert!(config.transports.a2a.peers.is_empty());
    assert_eq!(config.transports.a2a.cleanup_batch, 32);
    assert!(
        config.transports.a2a.retained_bytes_low_watermark
            < config.transports.a2a.retained_bytes_ceiling
    );
}

#[test]
fn a2a_peer_and_endpoint_are_addressable_without_exposing_secret() {
    let config = load_from_str_with_env(
        r#"
[transports.a2a]
enabled = true
listen = "tcp:127.0.0.1:0"
exposed_endpoints = ["remote"]

[transports.a2a.peers.lan]
url = "http://127.0.0.1:9000/a2a/worker/rpc"
expected_peer_id = "lan-peer"
token = "{env:A2A_TOKEN}"
allowed_targets = ["worker"]
danger_accept_invalid_certs = true

[agents.remote]
transport = "a2a"
peer = "lan"
enabled = true
"#,
        &env(),
    )
    .unwrap();
    assert_eq!(
        config.transports.a2a.peers["lan"].token.to_string(),
        "<redacted>"
    );
    assert!(config.transports.a2a.peers["lan"].danger_accept_invalid_certs);
    let endpoint = EndpointRegistry::from_config(&config)
        .get_by_agent_id("remote")
        .unwrap()
        .clone();
    assert!(matches!(endpoint.address(), Some(EndpointAddress::A2a { peer }) if peer == "lan"));
}

#[test]
fn a2a_peer_tls_bypass_defaults_off_and_url_credentials_are_rejected() {
    let verified = r#"[transports.a2a.peers.lan]
url = "https://peer.local/rpc"
expected_peer_id = "peer"
token = "{env:A2A_TOKEN}"
"#;
    let config = load_from_str_with_env(verified, &env()).unwrap();
    assert!(!config.transports.a2a.peers["lan"].danger_accept_invalid_certs);

    let credentialed = r#"[transports.a2a.peers.lan]
url = "https://user:secret@peer.local/rpc"
expected_peer_id = "peer"
token = "{env:A2A_TOKEN}"
danger_accept_invalid_certs = true
"#;
    let error = load_from_str_with_env(credentialed, &env()).unwrap_err();
    assert!(matches!(error, ConfigError::Validation { ref field, .. } if field.ends_with(".url")));
    assert!(!error.to_string().contains("secret"));

    let unsafe_key = r#"[transports.a2a.peers."unsafe\nkey"]
url = "https://peer.local/rpc"
expected_peer_id = "peer"
token = "{env:A2A_TOKEN}"
danger_accept_invalid_certs = true
"#;
    assert!(load_from_str_with_env(unsafe_key, &env()).is_err());
}

#[test]
fn a2a_literal_token_and_bad_cleanup_bounds_fail_closed() {
    let literal = r#"[transports.a2a.peers.lan]
url = "https://peer.local/rpc"
expected_peer_id = "peer"
token = "literal-secret"
"#;
    let error = load_from_str_with_env(literal, &env()).unwrap_err();
    assert!(matches!(error, ConfigError::Validation { field, .. } if field.ends_with(".token")));
    let bounds = "[transports.a2a]\nretained_bytes_ceiling=10\nretained_bytes_low_watermark=10\n";
    assert!(load_from_str_with_env(bounds, &env()).is_err());
}
