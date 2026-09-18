use uuid::Uuid;

use crate::models::EndpointId;

const PEER_NAMESPACE_LABEL: &str = "guigu-agent-bridge/a2a-peer/v1";

/// Stable identity for a configured A2A peer, distinct from local agent identity.
pub fn derive_peer_endpoint_id(peer_id: &str) -> EndpointId {
    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, PEER_NAMESPACE_LABEL.as_bytes());
    EndpointId::from_uuid(Uuid::new_v5(&namespace, peer_id.as_bytes()))
}
