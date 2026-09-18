use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use guigu_agent_bridge::a2a::wire::{AgentCard, Capabilities, PROTOCOL_VERSION, Part, TaskState};
use guigu_agent_bridge::a2a::{ListenPolicy, validate_listener, validate_peer_url};
use url::Url;

#[test]
fn v030_fixture_and_unknown_state_are_strict() {
    let card = AgentCard {
        name: "bridge".into(),
        description: "trusted LAN".into(),
        url: "http://127.0.0.1:9000".into(),
        protocol_version: PROTOCOL_VERSION.into(),
        capabilities: Capabilities {
            streaming: false,
            push_notifications: false,
        },
        skills: vec![],
    };
    let value = serde_json::to_value(card).unwrap();
    assert_eq!(value["protocolVersion"], "0.3.0");
    assert_eq!(value["capabilities"]["streaming"], false);
    assert!(serde_json::from_str::<TaskState>("\"future-state\"").is_err());
    assert!(matches!(
        serde_json::from_str::<Part>(r#"{"kind":"file_uri","uri":"file:///etc/passwd"}"#).unwrap(),
        Part::FileUri { .. }
    ));
}

#[test]
fn listener_rejects_wildcard_public_and_unforced_private() {
    let default = ListenPolicy {
        allow_private_plaintext: false,
    };
    assert!(validate_listener("127.0.0.1:0".parse().unwrap(), default).is_ok());
    assert!(validate_listener("0.0.0.0:9000".parse().unwrap(), default).is_err());
    assert!(validate_listener("8.8.8.8:9000".parse().unwrap(), default).is_err());
    assert!(validate_listener("192.168.1.8:9000".parse().unwrap(), default).is_err());
    assert!(
        validate_listener(
            "192.168.1.8:9000".parse().unwrap(),
            ListenPolicy {
                allow_private_plaintext: true
            }
        )
        .is_ok()
    );
}

#[test]
fn peer_policy_requires_https_except_loopback_and_rejects_mixed_dns() {
    let loopback = [SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 80)];
    assert!(
        validate_peer_url(
            &Url::parse("http://localhost/a2a/x/rpc").unwrap(),
            &loopback
        )
        .is_ok()
    );
    let private = ["192.168.1.2:443".parse().unwrap()];
    assert!(validate_peer_url(&Url::parse("http://peer.local/rpc").unwrap(), &private).is_err());
    assert!(validate_peer_url(&Url::parse("https://peer.local/rpc").unwrap(), &private).is_ok());
    let mixed = [
        "192.168.1.2:443".parse().unwrap(),
        "8.8.8.8:443".parse().unwrap(),
    ];
    assert!(validate_peer_url(&Url::parse("https://peer.local/rpc").unwrap(), &mixed).is_err());
    assert!(
        validate_peer_url(
            &Url::parse("https://user@peer.local/rpc").unwrap(),
            &private
        )
        .is_err()
    );
}
