//! Idempotent Matrix room/thread conversation resolution.

use crate::{
    models::{Conversation, ConversationId, ExternalRef, TransportType},
    storage::{Repository, StorageError},
};

use super::MatrixError;

const MAX_EXTERNAL_ID_BYTES: usize = 1024;

/// Resolve or create the bridge conversation for a Matrix room or thread.
pub async fn resolve_conversation(
    repository: &dyn Repository,
    room_id: &str,
    thread_root: Option<&str>,
) -> Result<Conversation, MatrixError> {
    if room_id.is_empty()
        || room_id.len() > MAX_EXTERNAL_ID_BYTES
        || thread_root.is_some_and(|root| root.is_empty() || root.len() > MAX_EXTERNAL_ID_BYTES)
    {
        return Err(MatrixError::Protocol {
            detail: "invalid Matrix conversation reference",
        });
    }
    let reference = ExternalRef {
        transport: TransportType::Matrix,
        external_id: room_id.to_owned(),
        thread_ref: thread_root.map(str::to_owned),
    };
    if let Some(found) = repository
        .conversation_by_external_ref(&reference)
        .await
        .map_err(|_| MatrixError::Storage)?
    {
        return Ok(found);
    }
    let candidate = Conversation {
        id: ConversationId::generate(),
        participants: Vec::new(),
        external_ref: Some(reference.clone()),
    };
    match repository.insert_conversation(&candidate).await {
        Ok(()) => Ok(candidate),
        Err(StorageError::Duplicate { .. }) => repository
            .conversation_by_external_ref(&reference)
            .await
            .map_err(|_| MatrixError::Storage)?
            .ok_or(MatrixError::Storage),
        Err(_) => Err(MatrixError::Storage),
    }
}
