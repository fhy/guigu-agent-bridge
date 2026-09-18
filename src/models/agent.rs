//! Agent endpoint model: a stable, transport-agnostic description of an external
//! agent that can receive tasks.
//!
//! The model carries no fixed business role (planner / worker / reviewer). Roles
//! are not part of the bridge's domain; they belong to the agents themselves.

use serde::{Deserialize, Serialize};

use crate::models::ids::EndpointId;

/// The transport protocol used to reach an [`AgentEndpoint`].
///
/// Serialized as `snake_case` (`"acp"`, `"matrix"`, `"http"`). Unknown values are
/// rejected on deserialization (fail-fast): an unknown transport means no adapter
/// can handle it. Existing variants are never renamed or removed; new variants may
/// be added (a public-contract change requiring Coordinator confirmation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportType {
    /// Agent Client Protocol over a spawned subprocess (stdio JSON-RPC / JSONL).
    Acp,
    /// A Matrix user account.
    Matrix,
    /// An HTTP endpoint.
    Http,
}

/// Transport-specific address of an [`AgentEndpoint`].
///
/// The string fields hold **opaque external identifiers** (e.g. a Matrix `user_id`
/// or an ACP command line). They are never parsed into internal ID types here; the
/// adapter layer owns their interpretation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointAddress {
    /// Spawn an ACP backend subprocess.
    Acp {
        /// Executable command.
        command: String,
        /// Command-line arguments.
        args: Vec<String>,
    },
    /// Address a Matrix user.
    Matrix {
        /// Opaque Matrix `user_id` (e.g. `@agent:matrix.org`).
        user_id: String,
    },
    /// Call an HTTP endpoint.
    Http {
        /// Endpoint URL.
        url: String,
    },
}

/// An open-ended capability tag advertised by an [`AgentEndpoint`].
///
/// A newtype over `String` for type safety; the set of capabilities is open and
/// not fixed by the bridge.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Capability(String);

impl Capability {
    /// Wrap a capability tag.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The capability tag as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A stable, transport-agnostic description of an external agent that can receive
/// tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEndpoint {
    /// Stable endpoint identifier.
    pub id: EndpointId,
    /// Transport protocol used to reach this endpoint.
    pub transport: TransportType,
    /// Transport-specific address.
    pub address: EndpointAddress,
    /// Whether the endpoint is currently enabled to receive tasks.
    pub enabled: bool,
    /// Advertised capabilities (open set; no fixed business role).
    pub capabilities: Vec<Capability>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_endpoint() -> AgentEndpoint {
        AgentEndpoint {
            id: EndpointId::generate(),
            transport: TransportType::Acp,
            address: EndpointAddress::Acp {
                command: "codex-acp".into(),
                args: vec!["--stdio".into()],
            },
            enabled: true,
            capabilities: vec![Capability::new("code")],
        }
    }

    #[test]
    fn agent_endpoint_serde_round_trip() {
        let endpoint = sample_endpoint();
        let json = serde_json::to_string(&endpoint).expect("serialize");
        let back: AgentEndpoint = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, endpoint);
    }

    #[test]
    fn transport_type_uses_snake_case() {
        assert_eq!(
            serde_json::to_string(&TransportType::Acp).unwrap(),
            "\"acp\""
        );
        assert_eq!(
            serde_json::to_string(&TransportType::Matrix).unwrap(),
            "\"matrix\""
        );
        assert_eq!(
            serde_json::to_string(&TransportType::Http).unwrap(),
            "\"http\""
        );

        assert_eq!(
            serde_json::from_str::<TransportType>("\"acp\"").unwrap(),
            TransportType::Acp
        );
    }

    #[test]
    fn transport_type_rejects_unknown_value() {
        // Fail-fast: an unknown transport means no adapter can handle it.
        assert!(serde_json::from_str::<TransportType>("\"grpc\"").is_err());
        assert!(serde_json::from_str::<TransportType>("\"smtp\"").is_err());
    }

    #[test]
    fn endpoint_address_carries_opaque_external_ids() {
        // A Matrix user_id is an opaque external string; the model stores it as-is
        // and never coerces it into an internal ID type.
        let address = EndpointAddress::Matrix {
            user_id: "@agent:matrix.org".into(),
        };
        let json = serde_json::to_string(&address).expect("serialize");
        let back: EndpointAddress = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, address);
        match back {
            EndpointAddress::Matrix { user_id } => {
                assert_eq!(user_id, "@agent:matrix.org")
            }
            other => panic!("expected Matrix address, got {other:?}"),
        }
    }

    #[test]
    fn capability_round_trips_transparently() {
        let cap = Capability::new("code-review");
        assert_eq!(serde_json::to_string(&cap).unwrap(), "\"code-review\"");
        assert_eq!(cap.as_str(), "code-review");
        assert_eq!(cap.to_string(), "code-review");
    }
}
