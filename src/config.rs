//! Configuration loading and validation for the bridge.
//!
//! This module owns the [`Config`] tree consumed by `main`, the transports, and
//! the adapters. Loading is a synchronous pipeline
//! (read → parse → secret-reference check → env substitution → deserialize →
//! build/validate); the resulting [`Config`] is immutable. Credentials are read
//! only from the environment: `transports.matrix.access_token` must be a
//! whole-value `{env:VAR}` reference (literal or embedded credentials are
//! rejected), and the resolved value is wrapped in [`SecretString`] so it can
//! never leak through `Debug`/`Display` or error messages.
//!
//! Configuration parsing is intentionally separate from semantic validation so callers
//! can report source errors before applying runtime policy.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

use crate::models::TransportType;

/// Root configuration for the bridge.
///
/// Built by the loading pipeline and immutable thereafter. Field names and types
/// are a public contract consumed by `main`, the transports (T010), and the ACP
/// adapter (T014); changes require re-evaluating all consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Bridge-wide settings (paths, limits, timeout).
    pub bridge: BridgeConfig,
    /// Transport settings.
    pub transports: TransportsConfig,
    /// Bounded ACP continuation and lease policy.
    pub runtime: RuntimeConfig,
    /// Agent endpoint declarations, keyed by agent ID (deterministic order).
    pub agents: BTreeMap<String, AgentEndpointConfig>,
}

/// Bridge-wide settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeConfig {
    /// SQLite database path (already `~`-expanded).
    pub database: PathBuf,
    /// Session root directory (already `~`-expanded).
    pub session_root: PathBuf,
    /// Maximum task-tree nesting depth. Valid range `1..=64`.
    pub max_task_depth: u32,
    /// Maximum agent-to-agent hops. Valid range `1..=256`.
    pub max_task_hops: u32,
    /// Default task timeout in seconds. Must be `>= 1`.
    pub default_timeout_seconds: u64,
    /// Capacity of the in-memory task queue.
    pub queue_capacity: usize,
    /// Capacity of the task-event channel.
    pub event_capacity: usize,
    /// Graceful shutdown deadline.
    pub shutdown_timeout_seconds: u64,
    /// Optional loopback-only health listener.
    pub health_bind: Option<SocketAddr>,
}

/// Bounded continuation and execution-lease policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub max_turns: u32,
    pub max_wall_seconds: u64,
    pub max_inactivity_seconds: u64,
    pub max_no_progress: u32,
    pub max_output_bytes: u64,
    pub lease_ttl_seconds: u64,
}

/// Transport settings. New transports are added here (a public-contract change
/// requiring Coordinator confirmation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportsConfig {
    /// Matrix transport settings.
    pub matrix: MatrixTransportConfig,
    /// Trusted-LAN A2A adapter settings.
    pub a2a: A2aTransportConfig,
    pub gateway: GatewayTransportConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayTransportConfig {
    pub enabled: bool,
    pub room_id: String,
    pub peer_id: String,
    pub local_endpoint_id: String,
    pub remote_endpoint_id: String,
    pub allowed_senders: Vec<String>,
    pub generation: u64,
    pub max_payload_bytes: usize,
    pub deadline_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct A2aTransportConfig {
    pub enabled: bool,
    pub listen: String,
    pub allow_private_plaintext: bool,
    pub max_body_bytes: usize,
    pub terminal_content_ttl_seconds: u64,
    pub retained_bytes_ceiling: u64,
    pub retained_bytes_low_watermark: u64,
    pub cleanup_batch: u32,
    pub exposed_endpoints: Vec<String>,
    pub peers: BTreeMap<String, A2aPeerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct A2aPeerConfig {
    pub url: String,
    pub expected_peer_id: String,
    pub token: SecretString,
    pub allowed_targets: Vec<String>,
    pub private_ca: Option<PathBuf>,
    /// Disables HTTPS chain, hostname, and validity verification for this peer.
    /// HTTPS then encrypts traffic without authenticating the peer, allowing an
    /// active MITM to read or modify bearer credentials and task data.
    pub danger_accept_invalid_certs: bool,
}

/// Matrix transport settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatrixTransportConfig {
    /// Whether the Matrix transport is enabled.
    pub enabled: bool,
    /// Homeserver URL (validated as an absolute http(s) URL when non-empty).
    pub homeserver: String,
    /// Matrix user ID (opaque; format validation is T010's responsibility).
    pub user_id: String,
    /// Access token (redacted in `Debug`/`Display`; read via [`SecretString::expose`]).
    pub access_token: SecretString,
    /// Monitoring room ID (empty = no monitoring room; opaque otherwise).
    pub monitor_room: String,
    /// Capacity of the decoded sync-event channel.
    pub sync_capacity: usize,
    /// Ordinary-message allowlist. Empty is deny-all.
    pub allowed_users: Vec<String>,
    /// Hot-reloadable routing declarations.
    pub routes: MatrixRoutesConfig,
    /// Admin user allowlist. Empty is deny-all.
    pub admin_users: Vec<String>,
    /// Admin room scope. Empty is deny-all.
    pub admin_rooms: Vec<String>,
}

/// Matrix routing declarations, kept free of matrix-sdk types.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MatrixRoutesConfig {
    pub aliases: BTreeMap<String, String>,
    pub rooms: BTreeMap<String, String>,
    pub direct: BTreeMap<String, Vec<String>>,
}

/// A declared agent endpoint (a static declaration, not a runtime entity).
///
/// The agent ID is the key in [`Config::agents`]; the runtime
/// [`crate::models::AgentEndpoint`] (with a generated `EndpointId`) is built from
/// this declaration by T004/T014.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentEndpointConfig {
    /// Transport protocol (reuses the T002 model enum).
    pub transport: TransportType,
    /// Executable command. `Some(non-empty)` for `acp`; `None` otherwise.
    pub command: Option<String>,
    /// Command-line arguments. Only meaningful for `acp`.
    pub args: Vec<String>,
    /// Whether the agent is enabled to receive tasks.
    pub enabled: bool,
    /// Canonical ACP working directory. Required for enabled ACP endpoints.
    pub workspace: Option<PathBuf>,
    /// Explicit additional canonical workspace roots granted to ACP.
    pub additional_workspaces: Vec<PathBuf>,
    /// Opaque configured peer key. Required only for A2A endpoints.
    pub peer: Option<String>,
}

/// A secret string whose value is never rendered by `Debug` or `Display`.
///
/// The only way to read the value is [`SecretString::expose`]. This guarantees
/// that logging, panic messages, and error context can never leak a credential.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    /// Wrap a secret value.
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// The only way to read the secret value (used by the transport layer to
    /// authenticate).
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Whether the secret is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"<redacted>\"")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Errors produced while loading or validating configuration.
///
/// Every variant carries enough location information (file path, line/column, or
/// field path) to point the operator at the offending setting.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The config file could not be read.
    #[error("failed to read config file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The config file is not valid TOML (syntax error or duplicate key).
    #[error("failed to parse config file {path} at line {line}, column {col}: {message}")]
    Parse {
        path: PathBuf,
        line: u64,
        col: u64,
        message: String,
    },
    /// A referenced environment variable is not set.
    #[error("missing environment variable {var} (referenced at {field})")]
    EnvVarMissing { var: String, field: String },
    /// A `{env:...}` placeholder is malformed.
    #[error("malformed environment placeholder at {field}: {snippet}")]
    EnvVarMalformed { field: String, snippet: String },
    /// A path could not be expanded (`~user` or unset `HOME`).
    #[error("failed to expand path at {field}: {reason}")]
    PathExpansion { field: String, reason: String },
    /// The config failed to deserialize (unknown field, type mismatch, unknown
    /// transport value).
    #[error("failed to deserialize config: {message}")]
    Deserialize { message: String },
    /// A semantic validation rule was violated (secret-field messages never
    /// include the value).
    #[error("config validation failed at {field}: {message}")]
    Validation { field: String, message: String },
}

/// Load and validate the config at `path`, resolving `{env:VAR}` placeholders
/// against the current process environment.
pub fn load(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    // Pass the real path so parse errors identify the offending file; the
    // string-only entry points keep the `<string>` sentinel.
    parse_and_build(&text, Some(path), &process_env())
}

/// Load and validate config from a TOML string, resolving `{env:VAR}`
/// placeholders against the current process environment.
pub fn load_from_str(text: &str) -> Result<Config, ConfigError> {
    load_from_str_with_env(text, &process_env())
}

/// Load and validate config from a TOML string, resolving `{env:VAR}`
/// placeholders and `~` path expansion against the given environment mapping.
///
/// This is the injectable entry point used by tests (to avoid mutating the real
/// process environment) and reserved for T017 hot-reload.
pub fn load_from_str_with_env(
    text: &str,
    env: &BTreeMap<String, String>,
) -> Result<Config, ConfigError> {
    parse_and_build(text, None, env)
}

// ---------------------------------------------------------------------------
// Private raw layer (deserialization targets; never exported)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    bridge: RawBridge,
    #[serde(default)]
    transports: RawTransports,
    #[serde(default)]
    runtime: RawRuntime,
    #[serde(default)]
    agents: BTreeMap<String, RawAgent>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBridge {
    #[serde(default = "default_database")]
    database: String,
    #[serde(default = "default_session_root")]
    session_root: String,
    #[serde(default = "default_max_task_depth")]
    max_task_depth: u32,
    #[serde(default = "default_max_task_hops")]
    max_task_hops: u32,
    #[serde(default = "default_timeout")]
    default_timeout_seconds: u64,
    #[serde(default = "default_queue_capacity")]
    queue_capacity: usize,
    #[serde(default = "default_event_capacity")]
    event_capacity: usize,
    #[serde(default = "default_shutdown_timeout")]
    shutdown_timeout_seconds: u64,
    #[serde(default)]
    health_bind: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRuntime {
    #[serde(default = "default_max_turns")]
    max_turns: u32,
    #[serde(default = "default_max_wall")]
    max_wall_seconds: u64,
    #[serde(default = "default_max_inactivity")]
    max_inactivity_seconds: u64,
    #[serde(default = "default_max_no_progress")]
    max_no_progress: u32,
    #[serde(default = "default_max_output")]
    max_output_bytes: u64,
    #[serde(default = "default_lease_ttl")]
    lease_ttl_seconds: u64,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawTransports {
    #[serde(default)]
    matrix: RawMatrix,
    #[serde(default)]
    a2a: RawA2a,
    #[serde(default)]
    gateway: RawGateway,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGateway {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    room_id: String,
    #[serde(default)]
    peer_id: String,
    #[serde(default)]
    local_endpoint_id: String,
    #[serde(default)]
    remote_endpoint_id: String,
    #[serde(default)]
    allowed_senders: Vec<String>,
    #[serde(default)]
    generation: u64,
    #[serde(default = "default_gateway_payload")]
    max_payload_bytes: usize,
    #[serde(default = "default_gateway_deadline")]
    deadline_seconds: u64,
}

impl Default for RawGateway {
    fn default() -> Self {
        Self {
            enabled: false,
            room_id: String::new(),
            peer_id: String::new(),
            local_endpoint_id: String::new(),
            remote_endpoint_id: String::new(),
            allowed_senders: Vec::new(),
            generation: 0,
            max_payload_bytes: default_gateway_payload(),
            deadline_seconds: default_gateway_deadline(),
        }
    }
}

fn default_gateway_payload() -> usize {
    256 * 1024
}
fn default_gateway_deadline() -> u64 {
    300
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawA2a {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    listen: String,
    #[serde(default)]
    allow_private_plaintext: bool,
    #[serde(default = "default_a2a_body")]
    max_body_bytes: usize,
    #[serde(default = "default_a2a_ttl")]
    terminal_content_ttl_seconds: u64,
    #[serde(default = "default_a2a_ceiling")]
    retained_bytes_ceiling: u64,
    #[serde(default = "default_a2a_low")]
    retained_bytes_low_watermark: u64,
    #[serde(default = "default_a2a_batch")]
    cleanup_batch: u32,
    #[serde(default)]
    exposed_endpoints: Vec<String>,
    #[serde(default)]
    peers: BTreeMap<String, RawA2aPeer>,
}

impl Default for RawA2a {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: String::new(),
            allow_private_plaintext: false,
            max_body_bytes: default_a2a_body(),
            terminal_content_ttl_seconds: default_a2a_ttl(),
            retained_bytes_ceiling: default_a2a_ceiling(),
            retained_bytes_low_watermark: default_a2a_low(),
            cleanup_batch: default_a2a_batch(),
            exposed_endpoints: Vec::new(),
            peers: BTreeMap::new(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawA2aPeer {
    url: String,
    expected_peer_id: String,
    token: String,
    #[serde(default)]
    allowed_targets: Vec<String>,
    #[serde(default)]
    private_ca: String,
    #[serde(default)]
    danger_accept_invalid_certs: bool,
}

const fn default_a2a_body() -> usize {
    1_048_576
}
const fn default_a2a_ttl() -> u64 {
    86_400
}
const fn default_a2a_ceiling() -> u64 {
    67_108_864
}
const fn default_a2a_low() -> u64 {
    50_331_648
}
const fn default_a2a_batch() -> u32 {
    32
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMatrix {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    homeserver: String,
    #[serde(default)]
    user_id: String,
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    monitor_room: String,
    #[serde(default = "default_matrix_sync_capacity")]
    sync_capacity: usize,
    #[serde(default)]
    allowed_users: Vec<String>,
    #[serde(default)]
    routes: RawMatrixRoutes,
    #[serde(default)]
    admin_users: Vec<String>,
    #[serde(default)]
    admin_rooms: Vec<String>,
}

impl Default for RawMatrix {
    fn default() -> Self {
        Self {
            enabled: false,
            homeserver: String::new(),
            user_id: String::new(),
            access_token: String::new(),
            monitor_room: String::new(),
            sync_capacity: default_matrix_sync_capacity(),
            allowed_users: Vec::new(),
            routes: RawMatrixRoutes::default(),
            admin_users: Vec::new(),
            admin_rooms: Vec::new(),
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawMatrixRoutes {
    #[serde(default)]
    aliases: BTreeMap<String, String>,
    #[serde(default)]
    rooms: BTreeMap<String, String>,
    #[serde(default)]
    direct: BTreeMap<String, Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAgent {
    transport: TransportType,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    workspace: String,
    #[serde(default)]
    additional_workspaces: Vec<String>,
    #[serde(default)]
    peer: Option<String>,
}

impl Default for RawBridge {
    fn default() -> Self {
        Self {
            database: default_database(),
            session_root: default_session_root(),
            max_task_depth: default_max_task_depth(),
            max_task_hops: default_max_task_hops(),
            default_timeout_seconds: default_timeout(),
            queue_capacity: default_queue_capacity(),
            event_capacity: default_event_capacity(),
            shutdown_timeout_seconds: default_shutdown_timeout(),
            health_bind: String::new(),
        }
    }
}

impl Default for RawRuntime {
    fn default() -> Self {
        Self {
            max_turns: default_max_turns(),
            max_wall_seconds: default_max_wall(),
            max_inactivity_seconds: default_max_inactivity(),
            max_no_progress: default_max_no_progress(),
            max_output_bytes: default_max_output(),
            lease_ttl_seconds: default_lease_ttl(),
        }
    }
}

fn default_database() -> String {
    "~/.local/share/guigu-agent-bridge/state.db".to_string()
}
fn default_session_root() -> String {
    "~/.cache/guigu-agent-bridge/sessions".to_string()
}
fn default_max_task_depth() -> u32 {
    8
}
fn default_max_task_hops() -> u32 {
    16
}
fn default_timeout() -> u64 {
    300
}
fn default_queue_capacity() -> usize {
    1024
}
fn default_event_capacity() -> usize {
    1024
}
fn default_shutdown_timeout() -> u64 {
    30
}
fn default_max_turns() -> u32 {
    8
}
fn default_max_wall() -> u64 {
    900
}
fn default_max_inactivity() -> u64 {
    120
}
fn default_max_no_progress() -> u32 {
    2
}
fn default_max_output() -> u64 {
    1_048_576
}
fn default_lease_ttl() -> u64 {
    30
}
fn default_matrix_sync_capacity() -> usize {
    256
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

fn parse_and_build(
    text: &str,
    path: Option<&Path>,
    env: &BTreeMap<String, String>,
) -> Result<Config, ConfigError> {
    let path_buf = path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("<string>"));

    // Stage 2: parse TOML into a generic Value.
    let mut value = toml::from_str::<toml::Value>(text).map_err(|e| {
        let (line, col) = e
            .span()
            .map(|s| line_col_at(text, s.start))
            .unwrap_or((0, 0));
        ConfigError::Parse {
            path: path_buf.clone(),
            line,
            col,
            message: e.message().to_string(),
        }
    })?;

    // Stage 2.5: enforce the secret-reference contract on raw (pre-substitution)
    // values. `transports.matrix.access_token` must be a whole-value {env:VAR}
    // reference (or empty); literal or embedded credentials are rejected before
    // substitution erases their provenance.
    enforce_secret_references(&value)?;

    // Stage 3: substitute {env:VAR} placeholders in string scalars.
    let lookup = |name: &str| env.get(name).map(String::as_str);
    substitute_in_value(&mut value, "", &lookup)?;

    // Stage 4: deserialize into the raw config (deny_unknown_fields, types,
    // transport enum).
    let raw: RawConfig = value
        .try_into::<RawConfig>()
        .map_err(|e: toml::de::Error| ConfigError::Deserialize {
            message: e.to_string(),
        })?;

    // Stage 5: build the validated Config (path expansion + semantic checks).
    build_config(raw, env)
}

/// Enforce the secret-reference contract on raw (pre-substitution) values.
///
/// `transports.matrix.access_token` must be either empty or a whole-value
/// `{env:VAR}` reference. Literal credentials and embedded-token forms are
/// rejected so a live credential can never be persisted in a tracked config
/// file. The check runs before substitution, when the value's provenance is
/// still visible; the error message never includes the value.
fn enforce_secret_references(value: &toml::Value) -> Result<(), ConfigError> {
    const FIELD: &str = "transports.matrix.access_token";
    let token = value
        .get("transports")
        .and_then(|t| t.get("matrix"))
        .and_then(|m| m.get("access_token"))
        .and_then(|t| t.as_str());
    match token {
        Some(t) if !t.is_empty() => {
            // Placeholder syntax is the scanner's authority: a malformed
            // placeholder reports `EnvVarMalformed`, consistent with every other
            // field. Only well-formed values are then held to the whole-value
            // shape contract. The snippet is redacted so the configured value
            // (a potential credential) never reaches the rendered error.
            check_placeholders_well_formed(t, FIELD, true)?;
            if !is_whole_env_reference(t) {
                return Err(ConfigError::Validation {
                    field: FIELD.to_string(),
                    message: "must be a whole-value {env:VAR} reference; literal or \
                             embedded credentials are not allowed"
                        .to_string(),
                });
            }
        }
        _ => {}
    }
    if let Some(peers) = value
        .get("transports")
        .and_then(|v| v.get("a2a"))
        .and_then(|v| v.get("peers"))
        .and_then(toml::Value::as_table)
    {
        for (peer, fields) in peers {
            let Some(token) = fields.get("token").and_then(toml::Value::as_str) else {
                continue;
            };
            let field = format!("transports.a2a.peers.{peer}.token");
            check_placeholders_well_formed(token, &field, true)?;
            if !is_whole_env_reference(token) {
                return Err(ConfigError::Validation { field,
                    message: "must be a whole-value {env:VAR} reference; literal or embedded credentials are not allowed".into() });
            }
        }
    }
    Ok(())
}

/// Verify that every `{env:...}` placeholder in `s` is well-formed (a valid
/// variable name, properly closed). A string with no placeholders is
/// well-formed. Returns the first malformed placeholder as
/// [`ConfigError::EnvVarMalformed`].
///
/// When `redact_snippet` is true (secret fields), the `snippet` is replaced by
/// the fixed `<redacted>` marker so the configured value never reaches the
/// rendered error; non-secret fields keep the diagnostic snippet.
///
/// This mirrors the well-formedness logic of [`substitute_string`] but stops at
/// the first syntax problem without resolving variables, so it can run before
/// substitution. It is intentionally a separate pass: the general substitution
/// path keeps its single-pass, first-error-in-scan-order behavior.
fn check_placeholders_well_formed(
    s: &str,
    field: &str,
    redact_snippet: bool,
) -> Result<(), ConfigError> {
    const PREFIX: &str = "{env:";
    const REDACTED: &str = "<redacted>";
    let mut rest = s;
    while let Some(start) = rest.find(PREFIX) {
        let after_prefix = &rest[start + PREFIX.len()..];
        match after_prefix.find('}') {
            Some(end) => {
                let var_name = &after_prefix[..end];
                if !is_valid_env_var_name(var_name) {
                    let snippet = if redact_snippet {
                        REDACTED.to_string()
                    } else {
                        format!("{{env:{var_name}}}")
                    };
                    return Err(ConfigError::EnvVarMalformed {
                        field: field.to_string(),
                        snippet,
                    });
                }
                rest = &after_prefix[end + 1..];
            }
            None => {
                let snippet = if redact_snippet {
                    REDACTED.to_string()
                } else {
                    format!("{{env:{after_prefix}")
                };
                return Err(ConfigError::EnvVarMalformed {
                    field: field.to_string(),
                    snippet,
                });
            }
        }
    }
    Ok(())
}

/// Whether `s` is exactly one well-formed `{env:VAR}` reference and nothing
/// else (no surrounding or trailing text, no additional placeholders).
fn is_whole_env_reference(s: &str) -> bool {
    const PREFIX: &str = "{env:";
    let inner = s
        .strip_prefix(PREFIX)
        .and_then(|rest| rest.strip_suffix('}'))
        .unwrap_or("");
    is_valid_env_var_name(inner)
}

/// Recursively substitute `{env:VAR}` placeholders in every string scalar of a
/// TOML `Value`. Only strings (and string-array elements) are scanned; the
/// replacement value is never re-scanned.
fn substitute_in_value<'env>(
    value: &mut toml::Value,
    path: &str,
    lookup: &dyn for<'a> Fn(&'a str) -> Option<&'env str>,
) -> Result<(), ConfigError> {
    match value {
        toml::Value::String(s) => {
            *s = substitute_string(s, path, lookup)?;
        }
        toml::Value::Array(items) => {
            for (i, item) in items.iter_mut().enumerate() {
                let item_path = format!("{path}[{i}]");
                substitute_in_value(item, &item_path, lookup)?;
            }
        }
        toml::Value::Table(table) => {
            for (key, item) in table.iter_mut() {
                let item_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                substitute_in_value(item, &item_path, lookup)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Substitute `{env:VAR}` placeholders in a single string (single pass; the
/// replacement value is not re-scanned).
fn substitute_string<'env>(
    s: &str,
    field: &str,
    lookup: &dyn for<'a> Fn(&'a str) -> Option<&'env str>,
) -> Result<String, ConfigError> {
    const PREFIX: &str = "{env:";
    let mut result = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find(PREFIX) {
        result.push_str(&rest[..start]);
        let after_prefix = &rest[start + PREFIX.len()..];
        match after_prefix.find('}') {
            Some(end) => {
                let var_name = &after_prefix[..end];
                if !is_valid_env_var_name(var_name) {
                    return Err(ConfigError::EnvVarMalformed {
                        field: field.to_string(),
                        snippet: format!("{{env:{var_name}}}"),
                    });
                }
                match lookup(var_name) {
                    Some(value) => result.push_str(value),
                    None => {
                        return Err(ConfigError::EnvVarMissing {
                            var: var_name.to_string(),
                            field: field.to_string(),
                        });
                    }
                }
                rest = &after_prefix[end + 1..];
            }
            None => {
                return Err(ConfigError::EnvVarMalformed {
                    field: field.to_string(),
                    snippet: format!("{{env:{after_prefix}"),
                });
            }
        }
    }
    result.push_str(rest);
    Ok(result)
}

/// A valid POSIX environment variable name: `[A-Za-z_][A-Za-z0-9_]*`.
fn is_valid_env_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Expand a leading `~` or `~/…` to `$HOME`. `~user` and an unset `HOME` are
/// errors; a mid-path `~` and relative/absolute paths are left as-is.
fn expand_path(
    input: &str,
    field: &str,
    env: &BTreeMap<String, String>,
) -> Result<PathBuf, ConfigError> {
    if input.is_empty() {
        return Ok(PathBuf::new());
    }
    if input == "~" || input.starts_with("~/") {
        let home = match env.get("HOME") {
            Some(h) if !h.is_empty() => h.clone(),
            _ => {
                return Err(ConfigError::PathExpansion {
                    field: field.to_string(),
                    reason: "HOME is not set".to_string(),
                });
            }
        };
        if input == "~" {
            return Ok(PathBuf::from(home));
        }
        return Ok(PathBuf::from(home).join(&input[2..]));
    }
    if input.starts_with('~') {
        return Err(ConfigError::PathExpansion {
            field: field.to_string(),
            reason: "~user expansion is not supported; use ~ or an absolute path".to_string(),
        });
    }
    Ok(PathBuf::from(input))
}

fn build_config(raw: RawConfig, env: &BTreeMap<String, String>) -> Result<Config, ConfigError> {
    let database = expand_path(&raw.bridge.database, "bridge.database", env)?;
    if database.as_os_str().is_empty() {
        return Err(validation("bridge.database", "must not be empty"));
    }
    let session_root = expand_path(&raw.bridge.session_root, "bridge.session_root", env)?;
    if session_root.as_os_str().is_empty() {
        return Err(validation("bridge.session_root", "must not be empty"));
    }
    if !(1..=64).contains(&raw.bridge.max_task_depth) {
        return Err(validation(
            "bridge.max_task_depth",
            "must be in range 1..=64",
        ));
    }
    if !(1..=256).contains(&raw.bridge.max_task_hops) {
        return Err(validation(
            "bridge.max_task_hops",
            "must be in range 1..=256",
        ));
    }
    if raw.bridge.default_timeout_seconds < 1 {
        return Err(validation("bridge.default_timeout_seconds", "must be >= 1"));
    }

    validate_capacity("bridge.queue_capacity", raw.bridge.queue_capacity)?;
    validate_capacity("bridge.event_capacity", raw.bridge.event_capacity)?;
    if !(1..=300).contains(&raw.bridge.shutdown_timeout_seconds) {
        return Err(validation(
            "bridge.shutdown_timeout_seconds",
            "must be in range 1..=300",
        ));
    }
    let health_bind = if raw.bridge.health_bind.is_empty() {
        None
    } else {
        let address = raw
            .bridge
            .health_bind
            .parse::<SocketAddr>()
            .map_err(|_| validation("bridge.health_bind", "must be a valid socket address"))?;
        if !address.ip().is_loopback() {
            return Err(validation(
                "bridge.health_bind",
                "must bind to a loopback address",
            ));
        }
        Some(address)
    };

    let runtime = build_runtime(raw.runtime)?;

    let matrix = build_matrix(raw.transports.matrix)?;
    let a2a = build_a2a(raw.transports.a2a, env)?;
    let gateway = build_gateway(raw.transports.gateway)?;

    let mut agents = BTreeMap::new();
    for (id, raw_agent) in raw.agents {
        if !is_valid_agent_id(&id) {
            return Err(validation(
                &format!("agents.{id}"),
                "id must match ^[A-Za-z0-9_-]+$",
            ));
        }
        agents.insert(id.clone(), build_agent(&id, raw_agent, env)?);
    }

    Ok(Config {
        bridge: BridgeConfig {
            database,
            session_root,
            max_task_depth: raw.bridge.max_task_depth,
            max_task_hops: raw.bridge.max_task_hops,
            default_timeout_seconds: raw.bridge.default_timeout_seconds,
            queue_capacity: raw.bridge.queue_capacity,
            event_capacity: raw.bridge.event_capacity,
            shutdown_timeout_seconds: raw.bridge.shutdown_timeout_seconds,
            health_bind,
        },
        transports: TransportsConfig {
            matrix,
            a2a,
            gateway,
        },
        runtime,
        agents,
    })
}

fn build_a2a(
    raw: RawA2a,
    env: &BTreeMap<String, String>,
) -> Result<A2aTransportConfig, ConfigError> {
    if raw.enabled && raw.listen.is_empty() {
        return Err(validation(
            "transports.a2a.listen",
            "is required when A2A is enabled",
        ));
    }
    if !(1..=1_048_576).contains(&raw.max_body_bytes) {
        return Err(validation(
            "transports.a2a.max_body_bytes",
            "must be in range 1..=1048576",
        ));
    }
    if raw.cleanup_batch == 0 || raw.cleanup_batch > 256 {
        return Err(validation(
            "transports.a2a.cleanup_batch",
            "must be in range 1..=256",
        ));
    }
    if raw.retained_bytes_low_watermark >= raw.retained_bytes_ceiling {
        return Err(validation(
            "transports.a2a.retained_bytes_low_watermark",
            "must be below retained_bytes_ceiling",
        ));
    }
    let mut peers = BTreeMap::new();
    for (id, peer) in raw.peers {
        if !is_safe_a2a_peer_key(&id) || peer.expected_peer_id.is_empty() || peer.token.is_empty() {
            return Err(validation(
                "transports.a2a.peers",
                "peer key must be 1..=64 safe ASCII characters; expected_peer_id and token must not be empty",
            ));
        }
        let url = url::Url::parse(&peer.url)
            .map_err(|_| validation("transports.a2a.peers.url", "must be an absolute URL"))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(validation(
                "transports.a2a.peers.url",
                "must use http or https and include a host",
            ));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(validation(
                "transports.a2a.peers.url",
                "must not contain user-info credentials",
            ));
        }
        let private_ca = if peer.private_ca.is_empty() {
            None
        } else {
            Some(expand_path(
                &peer.private_ca,
                "transports.a2a.peers.private_ca",
                env,
            )?)
        };
        if url.scheme() == "http" && private_ca.is_some() {
            return Err(validation(
                "transports.a2a.peers.private_ca",
                "is valid only for HTTPS",
            ));
        }
        peers.insert(
            id,
            A2aPeerConfig {
                url: peer.url,
                expected_peer_id: peer.expected_peer_id,
                token: SecretString::new(peer.token),
                allowed_targets: peer.allowed_targets,
                private_ca,
                danger_accept_invalid_certs: peer.danger_accept_invalid_certs,
            },
        );
    }
    Ok(A2aTransportConfig {
        enabled: raw.enabled,
        listen: raw.listen,
        allow_private_plaintext: raw.allow_private_plaintext,
        max_body_bytes: raw.max_body_bytes,
        terminal_content_ttl_seconds: raw.terminal_content_ttl_seconds,
        retained_bytes_ceiling: raw.retained_bytes_ceiling,
        retained_bytes_low_watermark: raw.retained_bytes_low_watermark,
        cleanup_batch: raw.cleanup_batch,
        exposed_endpoints: raw.exposed_endpoints,
        peers,
    })
}

fn is_safe_a2a_peer_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn validate_capacity(field: &str, value: usize) -> Result<(), ConfigError> {
    if !(1..=65_536).contains(&value) {
        return Err(validation(field, "must be in range 1..=65536"));
    }
    Ok(())
}

fn build_runtime(raw: RawRuntime) -> Result<RuntimeConfig, ConfigError> {
    if !(1..=64).contains(&raw.max_turns) {
        return Err(validation("runtime.max_turns", "must be in range 1..=64"));
    }
    if !(1..=86_400).contains(&raw.max_wall_seconds) {
        return Err(validation(
            "runtime.max_wall_seconds",
            "must be in range 1..=86400",
        ));
    }
    if !(1..=3_600).contains(&raw.max_inactivity_seconds)
        || raw.max_inactivity_seconds > raw.max_wall_seconds
    {
        return Err(validation(
            "runtime.max_inactivity_seconds",
            "must be in range 1..=3600 and not exceed max_wall_seconds",
        ));
    }
    if !(1..=16).contains(&raw.max_no_progress) {
        return Err(validation(
            "runtime.max_no_progress",
            "must be in range 1..=16",
        ));
    }
    if !(1..=16_777_216).contains(&raw.max_output_bytes) {
        return Err(validation(
            "runtime.max_output_bytes",
            "must be in range 1..=16777216",
        ));
    }
    if !(2..=300).contains(&raw.lease_ttl_seconds)
        || raw.lease_ttl_seconds >= raw.max_inactivity_seconds
    {
        return Err(validation(
            "runtime.lease_ttl_seconds",
            "must be in range 2..=300 and below max_inactivity_seconds",
        ));
    }
    Ok(RuntimeConfig {
        max_turns: raw.max_turns,
        max_wall_seconds: raw.max_wall_seconds,
        max_inactivity_seconds: raw.max_inactivity_seconds,
        max_no_progress: raw.max_no_progress,
        max_output_bytes: raw.max_output_bytes,
        lease_ttl_seconds: raw.lease_ttl_seconds,
    })
}

fn build_gateway(raw: RawGateway) -> Result<GatewayTransportConfig, ConfigError> {
    if raw.enabled
        && (raw.room_id.is_empty()
            || raw.peer_id.is_empty()
            || raw.local_endpoint_id.is_empty()
            || raw.remote_endpoint_id.is_empty()
            || raw.allowed_senders.is_empty())
    {
        return Err(validation(
            "transports.gateway",
            "room, peer, endpoints and sender allowlist are required when enabled",
        ));
    }
    if !(1..=256 * 1024).contains(&raw.max_payload_bytes) {
        return Err(validation(
            "transports.gateway.max_payload_bytes",
            "must be in range 1..=262144",
        ));
    }
    if raw.deadline_seconds == 0 || raw.deadline_seconds > 86_400 {
        return Err(validation(
            "transports.gateway.deadline_seconds",
            "must be in range 1..=86400",
        ));
    }
    if raw.enabled {
        if uuid::Uuid::parse_str(&raw.local_endpoint_id).is_err()
            || uuid::Uuid::parse_str(&raw.remote_endpoint_id).is_err()
        {
            return Err(validation(
                "transports.gateway",
                "endpoint IDs must be UUIDs",
            ));
        }
        if raw
            .allowed_senders
            .iter()
            .any(|user| matrix_sdk::ruma::OwnedUserId::try_from(user.as_str()).is_err())
        {
            return Err(validation(
                "transports.gateway.allowed_senders",
                "entries must be Matrix user IDs",
            ));
        }
    }
    Ok(GatewayTransportConfig {
        enabled: raw.enabled,
        room_id: raw.room_id,
        peer_id: raw.peer_id,
        local_endpoint_id: raw.local_endpoint_id,
        remote_endpoint_id: raw.remote_endpoint_id,
        allowed_senders: raw.allowed_senders,
        generation: raw.generation,
        max_payload_bytes: raw.max_payload_bytes,
        deadline_seconds: raw.deadline_seconds,
    })
}

fn build_matrix(raw: RawMatrix) -> Result<MatrixTransportConfig, ConfigError> {
    // Format validation always runs: a non-empty homeserver must be a valid
    // http(s) URL even when the transport is disabled.
    if !raw.homeserver.is_empty() {
        validate_homeserver(&raw.homeserver)?;
    }
    // Non-empty validation only when enabled.
    if raw.enabled {
        if raw.homeserver.is_empty() {
            return Err(validation(
                "transports.matrix.homeserver",
                "must not be empty when matrix transport is enabled",
            ));
        }
        if raw.user_id.is_empty() {
            return Err(validation(
                "transports.matrix.user_id",
                "must not be empty when matrix transport is enabled",
            ));
        }
        if raw.access_token.is_empty() {
            return Err(validation(
                "transports.matrix.access_token",
                "must not be empty when matrix transport is enabled",
            ));
        }
    }
    validate_capacity("transports.matrix.sync_capacity", raw.sync_capacity)?;
    for (field, values) in [
        ("transports.matrix.allowed_users", &raw.allowed_users),
        ("transports.matrix.admin_users", &raw.admin_users),
        ("transports.matrix.admin_rooms", &raw.admin_rooms),
    ] {
        if values.iter().any(String::is_empty) {
            return Err(validation(field, "entries must not be empty"));
        }
    }
    for (alias, target) in &raw.routes.aliases {
        if alias.is_empty() || target.is_empty() {
            return Err(validation(
                "transports.matrix.routes.aliases",
                "aliases and targets must not be empty",
            ));
        }
    }
    for (room, target) in &raw.routes.rooms {
        if room.is_empty() || target.is_empty() {
            return Err(validation(
                "transports.matrix.routes.rooms",
                "rooms and targets must not be empty",
            ));
        }
    }
    for (room, targets) in &raw.routes.direct {
        if room.is_empty() || targets.is_empty() || targets.iter().any(String::is_empty) {
            return Err(validation(
                "transports.matrix.routes.direct",
                "rooms and target lists must not be empty",
            ));
        }
    }
    Ok(MatrixTransportConfig {
        enabled: raw.enabled,
        homeserver: raw.homeserver,
        user_id: raw.user_id,
        access_token: SecretString::new(raw.access_token),
        monitor_room: raw.monitor_room,
        sync_capacity: raw.sync_capacity,
        allowed_users: raw.allowed_users,
        routes: MatrixRoutesConfig {
            aliases: raw.routes.aliases,
            rooms: raw.routes.rooms,
            direct: raw.routes.direct,
        },
        admin_users: raw.admin_users,
        admin_rooms: raw.admin_rooms,
    })
}

fn validate_homeserver(homeserver: &str) -> Result<(), ConfigError> {
    const FIELD: &str = "transports.matrix.homeserver";
    let url = url::Url::parse(homeserver).map_err(|_| ConfigError::Validation {
        field: FIELD.to_string(),
        message: format!("must be an absolute http(s) URL, got {homeserver:?}"),
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ConfigError::Validation {
            field: FIELD.to_string(),
            message: format!("must use the http or https scheme, got {}", url.scheme()),
        });
    }
    if url.host_str().map(str::is_empty).unwrap_or(true) {
        return Err(ConfigError::Validation {
            field: FIELD.to_string(),
            message: format!("must have a non-empty host, got {homeserver:?}"),
        });
    }
    Ok(())
}

fn build_agent(
    id: &str,
    raw: RawAgent,
    env: &BTreeMap<String, String>,
) -> Result<AgentEndpointConfig, ConfigError> {
    let field = format!("agents.{id}");
    match raw.transport {
        TransportType::Acp => {
            let command = match &raw.command {
                Some(c) if !c.is_empty() => Some(c.clone()),
                Some(_) => {
                    return Err(validation(
                        &format!("{field}.command"),
                        "must not be empty for transport \"acp\"",
                    ));
                }
                None => {
                    return Err(validation(
                        &format!("{field}.command"),
                        "is required for transport \"acp\"",
                    ));
                }
            };
            let workspace = if raw.workspace.is_empty() {
                if raw.enabled {
                    return Err(validation(
                        &format!("{field}.workspace"),
                        "is required for an enabled ACP endpoint",
                    ));
                }
                None
            } else {
                let expanded = expand_path(&raw.workspace, &format!("{field}.workspace"), env)?;
                let canonical = std::fs::canonicalize(&expanded).map_err(|_| {
                    validation(
                        &format!("{field}.workspace"),
                        "must name an existing directory",
                    )
                })?;
                if !canonical.is_dir() {
                    return Err(validation(
                        &format!("{field}.workspace"),
                        "must name an existing directory",
                    ));
                }
                if canonical.to_str().is_none() {
                    return Err(validation(
                        &format!("{field}.workspace"),
                        "must be valid UTF-8",
                    ));
                }
                Some(canonical)
            };
            const MAX_ADDITIONAL_WORKSPACES: usize = 16;
            if raw.additional_workspaces.len() > MAX_ADDITIONAL_WORKSPACES {
                return Err(validation(
                    &format!("{field}.additional_workspaces"),
                    "contains too many entries",
                ));
            }
            let mut additional_workspaces = Vec::with_capacity(raw.additional_workspaces.len());
            for (index, value) in raw.additional_workspaces.iter().enumerate() {
                let item_field = format!("{field}.additional_workspaces[{index}]");
                if Path::new(value).is_relative() {
                    return Err(validation(&item_field, "must be absolute"));
                }
                let canonical = std::fs::canonicalize(value)
                    .map_err(|_| validation(&item_field, "must name an existing directory"))?;
                if !canonical.is_dir() || canonical.to_str().is_none() {
                    return Err(validation(
                        &item_field,
                        "must name an existing UTF-8 directory",
                    ));
                }
                if Some(&canonical) == workspace.as_ref()
                    || additional_workspaces.contains(&canonical)
                {
                    return Err(validation(&item_field, "duplicates another workspace"));
                }
                additional_workspaces.push(canonical);
            }
            additional_workspaces.sort();
            Ok(AgentEndpointConfig {
                transport: TransportType::Acp,
                command,
                args: raw.args,
                enabled: raw.enabled,
                workspace,
                additional_workspaces,
                peer: None,
            })
        }
        TransportType::Matrix | TransportType::Http => {
            if raw.command.is_some() || !raw.args.is_empty() || !raw.workspace.is_empty() {
                return Err(validation(
                    &field,
                    "command/args/workspace are only valid for transport \"acp\"",
                ));
            }
            Ok(AgentEndpointConfig {
                transport: raw.transport,
                command: None,
                args: Vec::new(),
                enabled: raw.enabled,
                workspace: None,
                additional_workspaces: Vec::new(),
                peer: None,
            })
        }
        TransportType::A2a => {
            if raw.command.is_some() || !raw.args.is_empty() || !raw.workspace.is_empty() {
                return Err(validation(
                    &field,
                    "command/args/workspace are only valid for transport \"acp\"",
                ));
            }
            let peer = raw.peer.filter(|value| !value.is_empty()).ok_or_else(|| {
                validation(
                    &format!("{field}.peer"),
                    "is required for transport \"a2a\"",
                )
            })?;
            Ok(AgentEndpointConfig {
                transport: TransportType::A2a,
                command: None,
                args: Vec::new(),
                enabled: raw.enabled,
                workspace: None,
                additional_workspaces: Vec::new(),
                peer: Some(peer),
            })
        }
    }
}

/// A valid agent ID: non-empty, `[A-Za-z0-9_-]+` (no whitespace, no `.`).
fn is_valid_agent_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Compute a 1-based (line, column) for a byte offset into `input`.
fn line_col_at(input: &str, byte_index: usize) -> (u64, u64) {
    if input.is_empty() {
        return (0, 0);
    }
    let bytes = input.as_bytes();
    let idx = byte_index.min(bytes.len() - 1);
    let before = &bytes[..idx];
    let line = before.iter().filter(|b| **b == b'\n').count() + 1;
    let line_start = before
        .iter()
        .rposition(|b| *b == b'\n')
        .map(|i| i + 1)
        .unwrap_or(0);
    let col = input[line_start..=idx].chars().count();
    (line as u64, col as u64)
}

fn process_env() -> BTreeMap<String, String> {
    std::env::vars().collect()
}

fn validation(field: &str, message: &str) -> ConfigError {
    ConfigError::Validation {
        field: field.to_string(),
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_with_home() -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert("HOME".to_string(), "/home/tester".to_string());
        m
    }

    fn env_with_home_and(vars: &[(&str, &str)]) -> BTreeMap<String, String> {
        let mut m = env_with_home();
        for (k, v) in vars {
            m.insert(k.to_string(), v.to_string());
        }
        m
    }

    #[test]
    fn production_defaults_are_bounded_and_health_is_disabled() {
        let config = load_from_str_with_env("", &env_with_home()).unwrap();
        assert_eq!(config.bridge.queue_capacity, 1024);
        assert_eq!(config.bridge.event_capacity, 1024);
        assert_eq!(config.bridge.shutdown_timeout_seconds, 30);
        assert_eq!(config.bridge.health_bind, None);
        assert_eq!(config.transports.matrix.sync_capacity, 256);
        assert_eq!(config.runtime.max_turns, 8);
        assert_eq!(config.runtime.lease_ttl_seconds, 30);
    }

    #[test]
    fn enabled_acp_requires_an_existing_workspace() {
        let missing = "[agents.a]\ntransport = \"acp\"\ncommand = \"cmd\"\nenabled = true\n";
        assert!(matches!(
            load_from_str_with_env(missing, &env_with_home()),
            Err(ConfigError::Validation { field, .. }) if field == "agents.a.workspace"
        ));
        let valid = "[agents.a]\ntransport = \"acp\"\ncommand = \"cmd\"\nworkspace = \"/tmp\"\nenabled = true\n";
        assert!(load_from_str_with_env(valid, &env_with_home()).is_ok());
    }

    #[test]
    fn health_bind_must_be_loopback() {
        let text = "[bridge]\nhealth_bind = \"0.0.0.0:9090\"\n";
        assert!(matches!(
            load_from_str_with_env(text, &env_with_home()),
            Err(ConfigError::Validation { field, .. }) if field == "bridge.health_bind"
        ));
    }

    /// A minimal matrix section whose `monitor_room` carries the value under
    /// test (opaque string, no validation interference; transport disabled).
    fn matrix_monitor_room(value: &str) -> String {
        format!("[transports.matrix]\nenabled = false\nmonitor_room = {value:?}\n")
    }

    // -- env substitution ---------------------------------------------------

    #[test]
    fn env_substitution_whole_value() {
        let env = env_with_home_and(&[("FOO", "bar")]);
        let config = load_from_str_with_env(&matrix_monitor_room("{env:FOO}"), &env).unwrap();
        assert_eq!(config.transports.matrix.monitor_room, "bar");
    }

    #[test]
    fn env_substitution_embedded_and_multiple() {
        let env = env_with_home_and(&[("A", "x"), ("B", "y")]);
        let config =
            load_from_str_with_env(&matrix_monitor_room("pre-{env:A}-mid-{env:B}-post"), &env)
                .unwrap();
        assert_eq!(config.transports.matrix.monitor_room, "pre-x-mid-y-post");
    }

    #[test]
    fn env_substitution_array_element() {
        let env = env_with_home_and(&[("FOO", "bar")]);
        let text = "[agents.a]\ntransport = \"acp\"\ncommand = \"cmd\"\nargs = [\"{env:FOO}\"]\n";
        let config = load_from_str_with_env(text, &env).unwrap();
        assert_eq!(config.agents["a"].args, vec!["bar".to_string()]);
    }

    #[test]
    fn env_substitution_replacement_not_rescanned() {
        // The value of FOO itself contains a placeholder; it must be kept
        // literally, not substituted again.
        let env = env_with_home_and(&[("FOO", "{env:BAR}")]);
        let config = load_from_str_with_env(&matrix_monitor_room("{env:FOO}"), &env).unwrap();
        assert_eq!(config.transports.matrix.monitor_room, "{env:BAR}");
    }

    #[test]
    fn env_substitution_no_placeholder_unchanged() {
        let env = env_with_home();
        let config = load_from_str_with_env(&matrix_monitor_room("plain"), &env).unwrap();
        assert_eq!(config.transports.matrix.monitor_room, "plain");
    }

    // -- env missing / empty / malformed -----------------------------------

    #[test]
    fn env_missing_reports_var_and_field() {
        let env = env_with_home();
        let err =
            load_from_str_with_env(&matrix_monitor_room("{env:UNSET_VAR}"), &env).unwrap_err();
        match err {
            ConfigError::EnvVarMissing { var, field } => {
                assert_eq!(var, "UNSET_VAR");
                assert_eq!(field, "transports.matrix.monitor_room");
            }
            other => panic!("expected EnvVarMissing, got {other:?}"),
        }
    }

    #[test]
    fn env_set_to_empty_replaces_with_empty() {
        let env = env_with_home_and(&[("FOO", "")]);
        let config = load_from_str_with_env(&matrix_monitor_room("{env:FOO}"), &env).unwrap();
        assert_eq!(config.transports.matrix.monitor_room, "");
    }

    #[test]
    fn env_malformed_empty_name() {
        let env = env_with_home();
        let err = load_from_str_with_env(&matrix_monitor_room("{env:}"), &env).unwrap_err();
        assert!(matches!(err, ConfigError::EnvVarMalformed { .. }));
    }

    #[test]
    fn env_malformed_bad_name() {
        let env = env_with_home();
        let err = load_from_str_with_env(&matrix_monitor_room("{env:BAD-NAME}"), &env).unwrap_err();
        match err {
            ConfigError::EnvVarMalformed { field, .. } => {
                assert_eq!(field, "transports.matrix.monitor_room");
            }
            other => panic!("expected EnvVarMalformed, got {other:?}"),
        }
    }

    #[test]
    fn env_malformed_unclosed() {
        let env = env_with_home();
        let err = load_from_str_with_env(&matrix_monitor_room("{env:FOO"), &env).unwrap_err();
        assert!(matches!(err, ConfigError::EnvVarMalformed { .. }));
    }

    #[test]
    fn disabled_section_placeholder_still_resolved() {
        // D3: env substitution is unconditional; a disabled matrix section that
        // references an unset variable still fails at load time.
        let env = env_with_home();
        let text = "[transports.matrix]\nenabled = false\naccess_token = \"{env:UNSET_TOKEN}\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        match err {
            ConfigError::EnvVarMissing { var, field } => {
                assert_eq!(var, "UNSET_TOKEN");
                assert_eq!(field, "transports.matrix.access_token");
            }
            other => panic!("expected EnvVarMissing, got {other:?}"),
        }
    }

    // -- path expansion -----------------------------------------------------

    #[test]
    fn path_expansion_tilde_and_tilde_slash() {
        let env = env_with_home();
        let text = "[bridge]\ndatabase = \"~/state.db\"\nsession_root = \"~\"\n";
        let config = load_from_str_with_env(text, &env).unwrap();
        assert_eq!(
            config.bridge.database,
            PathBuf::from("/home/tester/state.db")
        );
        assert_eq!(config.bridge.session_root, PathBuf::from("/home/tester"));
    }

    #[test]
    fn path_expansion_tilde_user_rejected() {
        let env = env_with_home();
        let text = "[bridge]\ndatabase = \"~alice/state.db\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        match err {
            ConfigError::PathExpansion { field, reason } => {
                assert_eq!(field, "bridge.database");
                assert!(reason.contains("~user"));
            }
            other => panic!("expected PathExpansion, got {other:?}"),
        }
    }

    #[test]
    fn path_expansion_mid_path_tilde_literal() {
        let env = env_with_home();
        let text = "[bridge]\ndatabase = \"data~/state.db\"\n";
        let config = load_from_str_with_env(text, &env).unwrap();
        assert_eq!(config.bridge.database, PathBuf::from("data~/state.db"));
    }

    #[test]
    fn path_expansion_relative_kept() {
        let env = env_with_home();
        let text = "[bridge]\ndatabase = \"data/state.db\"\n";
        let config = load_from_str_with_env(text, &env).unwrap();
        assert_eq!(config.bridge.database, PathBuf::from("data/state.db"));
    }

    #[test]
    fn path_expansion_home_unset_rejected() {
        let env = BTreeMap::new(); // no HOME
        let text = "[bridge]\ndatabase = \"~/state.db\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        match err {
            ConfigError::PathExpansion { field, reason } => {
                assert_eq!(field, "bridge.database");
                assert!(reason.contains("HOME"));
            }
            other => panic!("expected PathExpansion, got {other:?}"),
        }
    }

    // -- unknown field / duplicate agent / id charset -----------------------

    #[test]
    fn unknown_field_rejected() {
        let env = env_with_home();
        let text = "[bridge]\nmax_task_dapth = 8\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        match err {
            ConfigError::Deserialize { message } => {
                assert!(message.contains("max_task_dapth"), "got: {message}");
            }
            other => panic!("expected Deserialize, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_agent_table_rejected() {
        let env = env_with_home();
        let text = "[agents.foo]\ntransport = \"acp\"\ncommand = \"a\"\n\n[agents.foo]\ntransport = \"acp\"\ncommand = \"b\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }), "got: {err:?}");
    }

    #[test]
    fn agent_id_with_space_rejected() {
        let env = env_with_home();
        let text = "[agents.\"my agent\"]\ntransport = \"acp\"\ncommand = \"a\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        match err {
            ConfigError::Validation { field, .. } => {
                assert!(field.contains("my agent"), "got: {field}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn agent_id_with_dot_rejected() {
        let env = env_with_home();
        // `[agents.a.b]` builds a nested table, which cannot deserialize as a
        // flat agent entry.
        let text = "[agents.a.b]\ntransport = \"acp\"\ncommand = \"a\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        assert!(
            matches!(err, ConfigError::Deserialize { .. }),
            "got: {err:?}"
        );
    }

    // -- command / args -----------------------------------------------------

    #[test]
    fn acp_empty_command_rejected() {
        let env = env_with_home();
        let text = "[agents.a]\ntransport = \"acp\"\ncommand = \"\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        assert!(
            matches!(err, ConfigError::Validation { .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn acp_missing_command_rejected() {
        let env = env_with_home();
        let text = "[agents.a]\ntransport = \"acp\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        assert!(
            matches!(err, ConfigError::Validation { .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn acp_empty_args_allowed() {
        let env = env_with_home();
        let text = "[agents.a]\ntransport = \"acp\"\ncommand = \"cmd\"\nargs = []\n";
        let config = load_from_str_with_env(text, &env).unwrap();
        assert!(config.agents["a"].args.is_empty());
    }

    #[test]
    fn non_acp_command_rejected() {
        let env = env_with_home();
        let text = "[agents.a]\ntransport = \"matrix\"\ncommand = \"cmd\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        match err {
            ConfigError::Validation { field, message } => {
                assert_eq!(field, "agents.a");
                assert!(
                    message.contains("only valid for transport"),
                    "got: {message}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    // -- transport enum -----------------------------------------------------

    #[test]
    fn unknown_transport_rejected() {
        let env = env_with_home();
        let text = "[agents.a]\ntransport = \"grpc\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        assert!(
            matches!(err, ConfigError::Deserialize { .. }),
            "got: {err:?}"
        );
    }

    // -- URL validation -----------------------------------------------------

    #[test]
    fn homeserver_invalid_url_rejected() {
        let env = env_with_home();
        let text = "[transports.matrix]\nenabled = false\nhomeserver = \"not-a-url\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        assert!(
            matches!(err, ConfigError::Validation { .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn homeserver_wrong_scheme_rejected() {
        let env = env_with_home();
        let text = "[transports.matrix]\nenabled = false\nhomeserver = \"ftp://example.com\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        assert!(
            matches!(err, ConfigError::Validation { .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn homeserver_empty_disabled_allowed() {
        let env = env_with_home();
        let text = "[transports.matrix]\nenabled = false\n";
        let config = load_from_str_with_env(text, &env).unwrap();
        assert_eq!(config.transports.matrix.homeserver, "");
    }

    #[test]
    fn homeserver_empty_enabled_rejected() {
        // access_token must be a whole-value {env:VAR} reference (a literal is
        // rejected earlier), so inject a dummy token via the environment.
        let env = env_with_home_and(&[("TOKEN", "t")]);
        let text = "[transports.matrix]\nenabled = true\nuser_id = \"@u:x\"\naccess_token = \"{env:TOKEN}\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        match err {
            ConfigError::Validation { field, .. } => {
                assert_eq!(field, "transports.matrix.homeserver");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    // -- numeric ranges -----------------------------------------------------

    #[test]
    fn timeout_zero_rejected() {
        let env = env_with_home();
        let text = "[bridge]\ndefault_timeout_seconds = 0\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        assert!(
            matches!(err, ConfigError::Validation { .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn depth_out_of_range_rejected() {
        let env = env_with_home();
        for bad in ["0", "65"] {
            let text = format!("[bridge]\nmax_task_depth = {bad}\n");
            let err = load_from_str_with_env(&text, &env).unwrap_err();
            assert!(
                matches!(err, ConfigError::Validation { .. }),
                "depth {bad}: {err:?}"
            );
        }
    }

    #[test]
    fn hops_out_of_range_rejected() {
        let env = env_with_home();
        for bad in ["0", "257"] {
            let text = format!("[bridge]\nmax_task_hops = {bad}\n");
            let err = load_from_str_with_env(&text, &env).unwrap_err();
            assert!(
                matches!(err, ConfigError::Validation { .. }),
                "hops {bad}: {err:?}"
            );
        }
    }

    #[test]
    fn range_boundaries_accepted() {
        let env = env_with_home();
        let text =
            "[bridge]\nmax_task_depth = 1\nmax_task_hops = 256\ndefault_timeout_seconds = 1\n";
        let config = load_from_str_with_env(text, &env).unwrap();
        assert_eq!(config.bridge.max_task_depth, 1);
        assert_eq!(config.bridge.max_task_hops, 256);
        assert_eq!(config.bridge.default_timeout_seconds, 1);
    }

    // -- defaults -----------------------------------------------------------

    #[test]
    fn empty_file_uses_all_defaults() {
        let env = env_with_home();
        let config = load_from_str_with_env("", &env).unwrap();
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
        assert!(!config.transports.matrix.enabled);
        assert_eq!(config.transports.matrix.homeserver, "");
        assert!(config.agents.is_empty());
    }

    #[test]
    fn partial_bridge_uses_defaults_for_rest() {
        let env = env_with_home();
        let text = "[bridge]\nmax_task_depth = 3\n";
        let config = load_from_str_with_env(text, &env).unwrap();
        assert_eq!(config.bridge.max_task_depth, 3);
        assert_eq!(config.bridge.max_task_hops, 16);
        assert_eq!(config.bridge.default_timeout_seconds, 300);
    }

    // -- secret redaction ---------------------------------------------------

    #[test]
    fn secret_redacted_in_debug_and_display() {
        let env = env_with_home_and(&[("TOKEN", "s3cr3t-value")]);
        let text = "[transports.matrix]\nenabled = true\nhomeserver = \"https://matrix.example\"\nuser_id = \"@u:x\"\naccess_token = \"{env:TOKEN}\"\n";
        let config = load_from_str_with_env(text, &env).unwrap();

        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"), "debug should redact: {debug}");
        assert!(
            !debug.contains("s3cr3t-value"),
            "debug leaked the token: {debug}"
        );

        let display = config.transports.matrix.access_token.to_string();
        assert_eq!(display, "<redacted>");

        // The value is still readable through the explicit accessor.
        assert_eq!(
            config.transports.matrix.access_token.expose(),
            "s3cr3t-value"
        );
    }

    #[test]
    fn secret_validation_message_omits_value() {
        let env = env_with_home_and(&[("TOKEN", "")]);
        let text = "[transports.matrix]\nenabled = true\nhomeserver = \"https://matrix.example\"\nuser_id = \"@u:x\"\naccess_token = \"{env:TOKEN}\"\n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        match err {
            ConfigError::Validation { field, message } => {
                assert_eq!(field, "transports.matrix.access_token");
                assert!(!message.is_empty());
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    // -- secret reference contract (access_token) ---------------------------

    /// A matrix section whose `access_token` carries the value under test
    /// (transport disabled so no other validation interferes).
    fn matrix_access_token(value: &str) -> String {
        format!("[transports.matrix]\nenabled = false\naccess_token = {value:?}\n")
    }

    #[test]
    fn access_token_whole_env_reference_accepted() {
        let env = env_with_home_and(&[("TOKEN", "s3cr3t")]);
        let config = load_from_str_with_env(&matrix_access_token("{env:TOKEN}"), &env).unwrap();
        assert_eq!(config.transports.matrix.access_token.expose(), "s3cr3t");
    }

    #[test]
    fn access_token_literal_rejected() {
        let env = env_with_home();
        let err = load_from_str_with_env(&matrix_access_token("literal-secret"), &env).unwrap_err();
        match err {
            ConfigError::Validation { field, message } => {
                assert_eq!(field, "transports.matrix.access_token");
                assert!(message.contains("whole-value"), "got: {message}");
                // The error must never include the credential value.
                assert!(!message.contains("literal-secret"), "leaked: {message}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn access_token_embedded_rejected() {
        let env = env_with_home_and(&[("TOKEN", "s3cr3t")]);
        let err =
            load_from_str_with_env(&matrix_access_token("prefix-{env:TOKEN}"), &env).unwrap_err();
        match err {
            ConfigError::Validation { field, .. } => {
                assert_eq!(field, "transports.matrix.access_token");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn access_token_multiple_placeholders_rejected() {
        let env = env_with_home_and(&[("A", "x"), ("B", "y")]);
        let err = load_from_str_with_env(&matrix_access_token("{env:A}{env:B}"), &env).unwrap_err();
        assert!(
            matches!(err, ConfigError::Validation { .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn access_token_malformed_placeholder_reports_malformed() {
        // A malformed placeholder (invalid variable name) is a syntax problem,
        // reported as EnvVarMalformed (consistent with every other field), not
        // as the whole-value shape error. The snippet is redacted so the
        // configured value never appears in the rendered error.
        let env = env_with_home();
        let secret = "super-secret-credential-value";
        let err = load_from_str_with_env(&matrix_access_token(&format!("{{env:{secret}}}")), &env)
            .unwrap_err();
        match &err {
            ConfigError::EnvVarMalformed { field, snippet } => {
                assert_eq!(field, "transports.matrix.access_token");
                assert_eq!(snippet, "<redacted>", "snippet must be redacted");
            }
            other => panic!("expected EnvVarMalformed, got {other:?}"),
        }
        let rendered = format!("{err}");
        assert!(
            !rendered.contains(secret),
            "error message leaked the credential: {rendered}"
        );
    }

    #[test]
    fn access_token_malformed_unclosed_redacts_value() {
        // An unclosed placeholder is reported as EnvVarMalformed, but the
        // snippet is redacted so the configured value never appears in the
        // rendered error.
        let env = env_with_home();
        let secret = "super-secret-credential-value";
        let err = load_from_str_with_env(&matrix_access_token(&format!("{{env:{secret}")), &env)
            .unwrap_err();
        match &err {
            ConfigError::EnvVarMalformed { field, snippet } => {
                assert_eq!(field, "transports.matrix.access_token");
                assert_eq!(snippet, "<redacted>", "snippet must be redacted");
            }
            other => panic!("expected EnvVarMalformed, got {other:?}"),
        }
        let rendered = format!("{err}");
        assert!(
            !rendered.contains(secret),
            "error message leaked the credential: {rendered}"
        );
    }

    #[test]
    fn access_token_empty_allowed_when_disabled() {
        let env = env_with_home();
        let config = load_from_str_with_env(&matrix_access_token(""), &env).unwrap();
        assert!(config.transports.matrix.access_token.is_empty());
    }

    #[test]
    fn access_token_error_never_leaks_value() {
        let env = env_with_home();
        let secret = "super-secret-credential-value";
        let err = load_from_str_with_env(&matrix_access_token(secret), &env).unwrap_err();
        let rendered = format!("{err}");
        assert!(
            !rendered.contains(secret),
            "error message leaked the credential: {rendered}"
        );
    }

    // -- file errors --------------------------------------------------------

    #[test]
    fn missing_file_is_io_error() {
        let err = load("/nonexistent/path/to/config.toml").unwrap_err();
        assert!(matches!(err, ConfigError::Io { .. }), "got: {err:?}");
    }

    #[test]
    fn invalid_toml_reports_line_and_column() {
        let env = env_with_home();
        let text = "[bridge]\ndatabase = \"x\"\nmax_task_depth = \n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        match err {
            ConfigError::Parse { line, col, .. } => {
                assert!(line >= 3, "expected error on line 3, got {line}");
                assert!(col > 0, "expected non-zero column, got {col}");
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    #[test]
    fn load_reports_real_file_path_on_parse_error() {
        // Regression: a file-backed load must report the real path in the
        // Parse error, not the `<string>` sentinel.
        let path = std::env::temp_dir().join(format!(
            "guigu-t003-load-path-{}-invalid.toml",
            std::process::id()
        ));
        std::fs::write(&path, "[bridge]\nmax_task_depth = \n").expect("write temp config");
        let result = load(&path);
        let _ = std::fs::remove_file(&path); // best-effort cleanup
        match result {
            Err(ConfigError::Parse {
                path: reported,
                line,
                col,
                ..
            }) => {
                assert_eq!(reported, path, "Parse error must carry the real file path");
                assert!(line >= 2, "expected error on line >= 2, got {line}");
                assert!(col > 0, "expected non-zero column, got {col}");
            }
            other => panic!("expected Parse error, got {other:?}"),
        }
    }

    #[test]
    fn load_from_str_reports_string_path_on_parse_error() {
        // The string-only entry points keep the `<string>` sentinel.
        let env = env_with_home();
        let text = "[bridge]\nmax_task_depth = \n";
        let err = load_from_str_with_env(text, &env).unwrap_err();
        match err {
            ConfigError::Parse { path, .. } => {
                assert_eq!(
                    path,
                    PathBuf::from("<string>"),
                    "string entry must report <string>"
                );
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }
}
