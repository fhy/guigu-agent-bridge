//! Integration test: the repository's `config.example.toml` must parse into a
//! fully validated [`Config`] with dummy environment values injected.
//!
//! This exercises the real loading pipeline (parse → env substitution →
//! deserialize → build/validate) against the shipped example, per the T003
//! acceptance criterion "config.example.toml 可完整解析为 Config".

use std::collections::BTreeMap;
use std::path::PathBuf;

use guigu_agent_bridge::config::load_from_str_with_env;
use guigu_agent_bridge::models::TransportType;

#[test]
fn config_example_parses_with_dummy_env() {
    let text = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.toml"),
    )
    .expect("config.example.toml must exist in the repository");

    let mut env = BTreeMap::new();
    env.insert("HOME".to_string(), "/home/tester".to_string());
    env.insert(
        "MATRIX_USER_ID".to_string(),
        "@user:example.org".to_string(),
    );
    env.insert(
        "MATRIX_ACCESS_TOKEN".to_string(),
        "s3cr3t-token".to_string(),
    );

    let config = load_from_str_with_env(&text, &env).expect("config.example.toml should parse");

    // [bridge]
    assert_eq!(
        config.bridge.database,
        PathBuf::from("/home/tester/.local/share/guigu-agent-bridge/state.db")
    );
    assert_eq!(
        config.bridge.session_root,
        PathBuf::from("/home/tester/.cache/guigu-agent-bridge/sessions")
    );
    assert_eq!(config.bridge.max_task_depth, 8);
    assert_eq!(config.bridge.max_task_hops, 16);
    assert_eq!(config.bridge.default_timeout_seconds, 300);

    // [transports.matrix]
    assert!(!config.transports.matrix.enabled);
    assert_eq!(
        config.transports.matrix.homeserver,
        "https://matrix.example"
    );
    assert_eq!(config.transports.matrix.user_id, "@user:example.org");
    assert_eq!(
        config.transports.matrix.access_token.expose(),
        "s3cr3t-token"
    );
    assert_eq!(config.transports.matrix.monitor_room, "");

    // [agents.example]
    assert_eq!(config.agents.len(), 1);
    let agent = &config.agents["example"];
    assert_eq!(agent.transport, TransportType::Acp);
    assert_eq!(agent.command.as_deref(), Some("/usr/local/bin/opencode"));
    assert_eq!(agent.args, vec!["acp".to_string()]);
    assert!(!agent.enabled);

    // The token must never appear in the Debug rendering of the config.
    let debug = format!("{config:?}");
    assert!(debug.contains("<redacted>"), "expected redaction: {debug}");
    assert!(!debug.contains("s3cr3t-token"), "token leaked: {debug}");
}
