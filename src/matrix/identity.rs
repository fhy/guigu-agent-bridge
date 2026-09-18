use crate::models::EndpointId;
use matrix_sdk::ruma::OwnedUserId;
use uuid::Uuid;

const MATRIX_USER_NAMESPACE: &str = "guigu-agent-bridge/matrix-user/v1";

/// Derive a stable bridge endpoint identity for a canonical Matrix user ID.
pub fn derive_matrix_user_id(user_id: &str) -> Result<EndpointId, super::MatrixError> {
    let canonical: OwnedUserId = user_id.parse().map_err(|_| super::MatrixError::Protocol {
        detail: "invalid Matrix user ID",
    })?;
    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, MATRIX_USER_NAMESPACE.as_bytes());
    Ok(EndpointId::from_uuid(Uuid::new_v5(
        &namespace,
        canonical.as_str().as_bytes(),
    )))
}

/// Return an adapter working copy with the Matrix sender represented once.
pub fn with_matrix_participant(
    conversation: &crate::models::Conversation,
    user_id: &str,
) -> Result<crate::models::Conversation, super::MatrixError> {
    let participant = derive_matrix_user_id(user_id)?;
    let mut result = conversation.clone();
    if !result.participants.contains(&participant) {
        result.participants.push(participant);
    }
    Ok(result)
}
