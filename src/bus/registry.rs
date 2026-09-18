//! Deterministic endpoint identity and the runtime endpoint registry.
//!
//! An [`AgentTask`](crate::models::AgentTask) carries a resolved [`EndpointId`],
//! never a natural-language name. This module is the single place where a config
//! agent id (the key in [`Config::agents`]) becomes that [`EndpointId`]:
//!
//! ```text
//! config agent id --derive_endpoint_id--> EndpointId   (UUIDv5, stable forever)
//! ```
//!
//! The derivation is deterministic (ADR-003) so the registry is a pure function
//! of configuration: it rebuilds identically after a restart, without reading the
//! database, and T008/T009/T010/T011/T014 can reuse the same frozen namespace
//! instead of minting a different id for the same agent.
//!
//! # Registry contents (partial view)
//!
//! Every declared agent is registered with its identity, transport and enabled
//! flag, but the transport-specific [`EndpointAddress`] is only derivable for
//! `acp` today: unlike ACP, the config carries no `user_id`/`url` for
//! `matrix`/`http` agents (T003 left those fields to T010/T014). Such endpoints
//! are therefore *declared but not addressable*: identity resolution still works,
//! but a task submitted to them is rejected with
//! [`BusError::AddressUnavailable`] rather than silently queued or dropped.

use std::collections::BTreeMap;

use uuid::Uuid;

use crate::bus::BusError;
use crate::config::{AgentEndpointConfig, Config};
use crate::models::{AgentEndpoint, EndpointAddress, EndpointId, TransportType};

/// Frozen namespace label for endpoint identity derivation (ADR-003).
///
/// Together with [`Uuid::NAMESPACE_URL`] this label defines every [`EndpointId`].
/// It is frozen once released: changing it renames every endpoint and orphans
/// persisted tasks, so it must never change. It has exactly one source of truth —
/// consumers (T008/T009/T010/T011/T014) must call [`derive_endpoint_id`] rather
/// than repeat the string or the UUID.
pub const ENDPOINT_NAMESPACE_LABEL: &str = "guigu-agent-bridge/agent/v1";

/// The project endpoint namespace (UUIDv5 over the frozen label).
fn endpoint_namespace() -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, ENDPOINT_NAMESPACE_LABEL.as_bytes())
}

/// Derive the stable [`EndpointId`] of a config agent id.
///
/// Deterministic, allocation-free, and stable across processes, restarts and
/// machines. Renaming an agent id yields a different identity (equivalent to a
/// new agent) — intentional, and documented for operators in ADR-003.
pub fn derive_endpoint_id(agent_id: &str) -> EndpointId {
    EndpointId::from_uuid(Uuid::new_v5(&endpoint_namespace(), agent_id.as_bytes()))
}

/// One declared endpoint as known to the runtime registry.
///
/// Fields are private and set once by [`EndpointRegistry::from_config`]; records
/// are immutable afterwards, so the registry cannot hold a half-updated endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredEndpoint {
    id: EndpointId,
    agent_id: String,
    transport: TransportType,
    enabled: bool,
    address: Option<EndpointAddress>,
}

impl RegisteredEndpoint {
    /// The stable, deterministically derived identity (ADR-003).
    pub fn id(&self) -> EndpointId {
        self.id
    }

    /// The `Config::agents` key this endpoint was declared under.
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// The declared transport.
    pub fn transport(&self) -> TransportType {
        self.transport
    }

    /// The declared `enabled` flag (independent of addressability).
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// The transport address, when it can be derived from the current config.
    ///
    /// `Some` for `acp`; `None` for declared-but-not-addressable endpoints
    /// (`matrix`/`http` until T010/T014 extend [`AgentEndpointConfig`]).
    pub fn address(&self) -> Option<&EndpointAddress> {
        self.address.as_ref()
    }

    /// Whether this endpoint can be addressed at all (see [`Self::address`]).
    pub fn is_addressable(&self) -> bool {
        self.address.is_some()
    }

    /// Build the full [`AgentEndpoint`] model, if the address is derivable.
    ///
    /// `capabilities` is always empty in v1: `AgentEndpointConfig` declares no
    /// capabilities, so the registry invents none. Adapters that need capability
    /// negotiation own that data (T014).
    pub fn agent_endpoint(&self) -> Option<AgentEndpoint> {
        self.address.as_ref().map(|address| AgentEndpoint {
            id: self.id,
            transport: self.transport,
            address: address.clone(),
            enabled: self.enabled,
            capabilities: Vec::new(),
        })
    }
}

/// The runtime view of `Config::agents`, immutable after construction.
///
/// Consumers: T004 (submission validation), T005 (re-validation before dispatch,
/// address lookup), T011/T014 (routing and adapter construction). The registry is
/// shared as `Arc<EndpointRegistry>`, so it has no lock and no interior
/// mutability: there is no lock ordering to reason about and nothing can be held
/// across an `await`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointRegistry {
    /// Primary store, keyed by config agent id (deterministic order).
    by_agent_id: BTreeMap<String, RegisteredEndpoint>,
    /// Reverse index `EndpointId` -> config agent id.
    ///
    /// Keyed by the raw [`Uuid`] because the T002 id newtypes deliberately do not
    /// implement `Ord` and T004 does not extend them; this index is only ever
    /// probed, never iterated, so its ordering is not part of any contract.
    by_id: BTreeMap<Uuid, String>,
}

impl EndpointRegistry {
    /// Build the registry from a validated [`Config`].
    ///
    /// Infallible by design: T003 already validated the agent ids, the
    /// derivation cannot fail, and a `matrix`/`http` declaration without an
    /// address is registered (not rejected) so its identity stays resolvable —
    /// see the module docs. Nothing here can fail fast on config shape.
    ///
    /// A UUIDv5 collision between two agent ids is not handled: producing one
    /// requires breaking SHA-1, and endpoint ids are identifiers, not security
    /// primitives (ADR-003).
    pub fn from_config(config: &Config) -> Self {
        let mut by_agent_id = BTreeMap::new();
        let mut by_id = BTreeMap::new();
        for (agent_id, declared) in &config.agents {
            let id = derive_endpoint_id(agent_id);
            by_agent_id.insert(
                agent_id.clone(),
                RegisteredEndpoint {
                    id,
                    agent_id: agent_id.clone(),
                    transport: declared.transport,
                    enabled: declared.enabled,
                    address: derive_address(declared),
                },
            );
            by_id.insert(id.as_uuid(), agent_id.clone());
        }
        Self { by_agent_id, by_id }
    }

    /// Look up an endpoint by its derived identity.
    ///
    /// Returns every **declared** endpoint including not-yet-addressable ones;
    /// `None` means "not declared" and is what makes the target unknown.
    pub fn get(&self, id: EndpointId) -> Option<&RegisteredEndpoint> {
        self.by_id
            .get(&id.as_uuid())
            .and_then(|agent_id| self.by_agent_id.get(agent_id))
    }

    /// Look up an endpoint by its config agent id.
    pub fn get_by_agent_id(&self, agent_id: &str) -> Option<&RegisteredEndpoint> {
        self.by_agent_id.get(agent_id)
    }

    /// Resolve a config agent id to its stable [`EndpointId`].
    ///
    /// This is the entry point for external identity normalization (T010/T011):
    /// several external names can map to one agent id, and therefore to one
    /// endpoint identity.
    pub fn resolve_agent_id(&self, agent_id: &str) -> Option<EndpointId> {
        self.by_agent_id.get(agent_id).map(RegisteredEndpoint::id)
    }

    /// Whether `id` is declared in this registry.
    pub fn contains(&self, id: EndpointId) -> bool {
        self.by_id.contains_key(&id.as_uuid())
    }

    /// Iterate **all declared** endpoints, including not-yet-addressable ones, in
    /// config agent-id order (deterministic).
    ///
    /// Addressability is per record ([`RegisteredEndpoint::is_addressable`]), so
    /// callers that need only usable targets filter explicitly; nothing is hidden.
    pub fn iter(&self) -> impl Iterator<Item = &RegisteredEndpoint> + '_ {
        self.by_agent_id.values()
    }

    /// Number of declared endpoints.
    pub fn len(&self) -> usize {
        self.by_agent_id.len()
    }

    /// Whether no endpoint is declared.
    pub fn is_empty(&self) -> bool {
        self.by_agent_id.is_empty()
    }

    /// Classify a submit target, in the fixed order
    /// unknown → disabled → unaddressable.
    ///
    /// Single source of truth for target validation: `Bus::submit` uses it, and
    /// T005 can reuse it to re-validate before dispatch. Each rejection maps to
    /// exactly one [`BusError`] variant and names the endpoint.
    pub fn validate_target(&self, id: EndpointId) -> Result<&RegisteredEndpoint, BusError> {
        match self.get(id) {
            None => Err(BusError::UnknownTarget { endpoint_id: id }),
            Some(endpoint) if !endpoint.is_enabled() => {
                Err(BusError::TargetDisabled { endpoint_id: id })
            }
            Some(endpoint) if !endpoint.is_addressable() => {
                Err(BusError::AddressUnavailable { endpoint_id: id })
            }
            Some(endpoint) => Ok(endpoint),
        }
    }
}

/// Derive the transport address of a declaration, when the config carries enough
/// information to do so.
///
/// `matrix`/`http` declarations have no `user_id`/`url` yet (T003 left those
/// fields to T010/T014), so they are registered without an address.
fn derive_address(declared: &AgentEndpointConfig) -> Option<EndpointAddress> {
    match declared.transport {
        // T003 guarantees `Some(non-empty)` for `acp`; a missing command cannot
        // reach here from a loaded `Config`, and mapping it to "no address" keeps
        // the registry free of invented addresses.
        TransportType::Acp => declared
            .command
            .as_ref()
            .map(|command| EndpointAddress::Acp {
                command: command.clone(),
                args: declared.args.clone(),
            }),
        TransportType::Matrix | TransportType::Http => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// TOML → validated `Config` through the real T003 loading pipeline.
    fn load(toml: &str) -> Config {
        let mut env = BTreeMap::new();
        env.insert("HOME".to_string(), "/home/tester".to_string());
        crate::config::load_from_str_with_env(toml, &env).expect("test config must be valid")
    }

    const CONFIG: &str = r#"
[agents.alpha]
transport = "acp"
command = "alpha-acp"
args = ["--stdio"]
workspace = "/tmp"
enabled = true

[agents.beta]
transport = "acp"
command = "beta-acp"
enabled = false

[agents.http-svc]
transport = "http"
enabled = true

[agents.matrix-bot]
transport = "matrix"
enabled = true
"#;

    #[test]
    fn derive_endpoint_id_is_stable_and_distinct() {
        assert_eq!(
            derive_endpoint_id("alpha"),
            derive_endpoint_id("alpha"),
            "the same agent id must always map to the same identity"
        );
        assert_ne!(derive_endpoint_id("alpha"), derive_endpoint_id("beta"));
        // Renaming an agent id changes its identity (ADR-003: a rename is a new
        // agent). This is the documented operator-visible consequence.
        assert_ne!(derive_endpoint_id("alpha"), derive_endpoint_id("alpha2"));
    }

    #[test]
    fn derive_endpoint_id_matches_the_frozen_namespace() {
        // Pins the frozen contract: UUIDv5(UUIDv5(NAMESPACE_URL,
        // "guigu-agent-bridge/agent/v1"), agent_id). The expected values were
        // computed with an independent RFC 4122 implementation; a change here
        // means every persisted endpoint identity would be orphaned.
        assert_eq!(
            endpoint_namespace().to_string(),
            "9e911d81-ff83-5639-bf2f-5451dbedb07b"
        );
        assert_eq!(
            derive_endpoint_id("alpha").to_string(),
            "c8521746-e5fc-551a-b96b-86f1cd310117"
        );
        assert_eq!(
            derive_endpoint_id("worker").to_string(),
            "2b2cde10-11a2-5889-8966-50d0c826e5e6"
        );
        assert_eq!(
            derive_endpoint_id("matrix-bot").to_string(),
            "bd2428ac-46c7-5775-96d2-05159351e03f"
        );
    }

    #[test]
    fn registry_rebuilds_identically_from_the_same_config() {
        let config = load(CONFIG);
        let first = EndpointRegistry::from_config(&config);
        let second = EndpointRegistry::from_config(&config);

        assert_eq!(first, second);
        assert_eq!(first.len(), 4);
        assert!(!first.is_empty());
        let first_ids: Vec<EndpointId> = first.iter().map(RegisteredEndpoint::id).collect();
        let second_ids: Vec<EndpointId> = second.iter().map(RegisteredEndpoint::id).collect();
        assert_eq!(first_ids, second_ids, "identity must survive a rebuild");
    }

    #[test]
    fn registry_maps_agent_ids_and_endpoint_ids_both_ways() {
        let registry = EndpointRegistry::from_config(&load(CONFIG));

        for record in registry.iter() {
            assert_eq!(
                registry.resolve_agent_id(record.agent_id()),
                Some(record.id()),
                "agent id must resolve to the record's identity"
            );
            assert_eq!(registry.get_by_agent_id(record.agent_id()), Some(record));
            assert_eq!(registry.get(record.id()), Some(record));
            assert!(registry.contains(record.id()));
        }

        let alpha = registry
            .get_by_agent_id("alpha")
            .expect("alpha is declared");
        assert_eq!(alpha.id(), derive_endpoint_id("alpha"));
        assert_eq!(alpha.agent_id(), "alpha");
        assert_eq!(alpha.transport(), TransportType::Acp);
        assert!(alpha.is_enabled());

        assert_eq!(registry.get_by_agent_id("missing"), None);
        assert_eq!(registry.resolve_agent_id("missing"), None);
        assert_eq!(registry.get(EndpointId::generate()), None);
        assert!(!registry.contains(EndpointId::generate()));
    }

    #[test]
    fn iter_is_deterministic_config_agent_id_order_and_covers_every_transport() {
        let registry = EndpointRegistry::from_config(&load(CONFIG));
        let agent_ids: Vec<&str> = registry.iter().map(RegisteredEndpoint::agent_id).collect();
        assert_eq!(agent_ids, ["alpha", "beta", "http-svc", "matrix-bot"]);
    }

    #[test]
    fn acp_endpoints_carry_the_derived_address() {
        let registry = EndpointRegistry::from_config(&load(CONFIG));
        let alpha = registry.get_by_agent_id("alpha").expect("alpha");
        assert!(alpha.is_addressable());
        assert_eq!(
            alpha.address(),
            Some(&EndpointAddress::Acp {
                command: "alpha-acp".into(),
                args: vec!["--stdio".into()],
            })
        );

        let endpoint = alpha.agent_endpoint().expect("acp is addressable");
        assert_eq!(endpoint.id, derive_endpoint_id("alpha"));
        assert_eq!(endpoint.transport, TransportType::Acp);
        assert!(endpoint.enabled);
        assert!(
            endpoint.capabilities.is_empty(),
            "v1 declares no capabilities; the registry must not invent any"
        );
    }

    #[test]
    fn matrix_and_http_declarations_are_registered_but_not_addressable() {
        let registry = EndpointRegistry::from_config(&load(CONFIG));
        for agent_id in ["matrix-bot", "http-svc"] {
            let record = registry
                .get_by_agent_id(agent_id)
                .unwrap_or_else(|| panic!("{agent_id} must be registered"));
            assert!(record.is_enabled());
            assert!(!record.is_addressable());
            assert_eq!(record.address(), None);
            assert_eq!(record.agent_endpoint(), None);
            assert_eq!(registry.resolve_agent_id(agent_id), Some(record.id()));
        }

        assert_eq!(
            registry
                .get_by_agent_id("matrix-bot")
                .map(|r| r.transport()),
            Some(TransportType::Matrix)
        );
        assert_eq!(
            registry.get_by_agent_id("http-svc").map(|r| r.transport()),
            Some(TransportType::Http)
        );
    }

    #[test]
    fn validate_target_classifies_unknown_disabled_and_unaddressable_in_order() {
        let registry = EndpointRegistry::from_config(&load(CONFIG));

        // Unknown: nothing is declared under this id.
        let unknown = EndpointId::generate();
        assert_eq!(
            registry.validate_target(unknown).unwrap_err(),
            BusError::UnknownTarget {
                endpoint_id: unknown
            }
        );

        // Disabled wins over unaddressable would only matter for a disabled
        // matrix/http endpoint; assert the acp case here and the matrix case in
        // `validate_target_reports_disabled_before_unaddressable`.
        let beta = derive_endpoint_id("beta");
        assert_eq!(
            registry.validate_target(beta).unwrap_err(),
            BusError::TargetDisabled { endpoint_id: beta }
        );

        // Declared, enabled, but no address.
        let matrix = derive_endpoint_id("matrix-bot");
        assert_eq!(
            registry.validate_target(matrix).unwrap_err(),
            BusError::AddressUnavailable {
                endpoint_id: matrix
            }
        );

        // Addressable: accepted and returned.
        let alpha = derive_endpoint_id("alpha");
        let record = registry.validate_target(alpha).expect("alpha is usable");
        assert_eq!(record.id(), alpha);
    }

    #[test]
    fn validate_target_reports_disabled_before_unaddressable() {
        let registry = EndpointRegistry::from_config(&load(
            r#"
[agents.disabled-matrix]
transport = "matrix"
enabled = false

[agents.enabled-matrix]
transport = "matrix"
enabled = true
"#,
        ));

        let disabled = derive_endpoint_id("disabled-matrix");
        assert_eq!(
            registry.validate_target(disabled).unwrap_err(),
            BusError::TargetDisabled {
                endpoint_id: disabled
            },
            "disabled must be reported before unaddressable"
        );

        let enabled = derive_endpoint_id("enabled-matrix");
        assert_eq!(
            registry.validate_target(enabled).unwrap_err(),
            BusError::AddressUnavailable {
                endpoint_id: enabled
            }
        );
    }

    #[test]
    fn empty_configuration_yields_an_empty_registry() {
        let registry = EndpointRegistry::from_config(&load(""));
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert_eq!(registry.iter().count(), 0);
    }
}
