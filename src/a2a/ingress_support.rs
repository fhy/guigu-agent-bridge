use serde_json::Value;
use uuid::Uuid;

use crate::{
    bus::BusError,
    models::{TaskId, TaskStatus},
};

use super::{
    A2aError, ExchangeRecord, IngressCause, IngressStage, Retryability,
    wire::{Part, TaskState},
};

pub(super) fn parse_task(row: &ExchangeRecord) -> Result<TaskId, A2aError> {
    row.internal_task_id
        .as_deref()
        .ok_or(A2aError::RecoveryNeeded)?
        .parse()
        .map_err(|_| A2aError::RecoveryNeeded)
}

pub(super) fn text_parts(parts: &[Part]) -> Result<String, A2aError> {
    let mut values = Vec::new();
    for part in parts {
        match part {
            Part::Text { text } => values.push(text.as_str()),
            Part::Data { .. } | Part::FileInline { .. } => {}
            Part::FileUri { .. } => return Err(A2aError::Unsupported),
        }
    }
    Ok(values.join("\n"))
}

pub(super) fn project(status: TaskStatus) -> TaskState {
    match status {
        TaskStatus::Queued => TaskState::Submitted,
        TaskStatus::Dispatched | TaskStatus::Running => TaskState::Working,
        TaskStatus::Completed => TaskState::Completed,
        TaskStatus::Failed | TaskStatus::TimedOut => TaskState::Failed,
        TaskStatus::Cancelled => TaskState::Canceled,
    }
}

pub(super) fn to_value<T: serde::Serialize>(value: T) -> Result<Value, A2aError> {
    serde_json::to_value(value).map_err(|_| {
        A2aError::ingress(
            IngressStage::ResponseEncode,
            IngressCause::Internal,
            Retryability::Never,
        )
    })
}

pub(super) fn invalid(stage: IngressStage) -> A2aError {
    A2aError::ingress(stage, IngressCause::InvalidParams, Retryability::Never)
}

pub(super) fn reject_bad_version(value: &Value) -> Result<(), A2aError> {
    if let Some(version) = value
        .as_object()
        .and_then(|object| object.get("protocolVersion"))
        && version.as_str() != Some(crate::a2a::wire::PROTOCOL_VERSION)
    {
        return Err(A2aError::ingress(
            IngressStage::VersionValidate,
            IngressCause::Unsupported,
            Retryability::Never,
        ));
    }
    Ok(())
}

pub(super) fn storage(stage: IngressStage) -> A2aError {
    A2aError::ingress(stage, IngressCause::Storage, Retryability::SameRequest)
}

pub(super) fn recovery(stage: IngressStage) -> A2aError {
    A2aError::ingress(stage, IngressCause::RecoveryRequired, Retryability::Never)
}

pub(super) fn target_unavailable(stage: IngressStage) -> A2aError {
    A2aError::ingress(
        stage,
        IngressCause::TargetUnavailable,
        Retryability::PolicyChange,
    )
}

pub(super) fn admission_error(error: BusError) -> A2aError {
    match error {
        BusError::QueueFull | BusError::EventBufferFull => A2aError::ingress(
            IngressStage::Admission,
            IngressCause::AdmissionBusy,
            Retryability::Backoff,
        ),
        BusError::TaskChannelClosed | BusError::EventSinkClosed => A2aError::ingress(
            IngressStage::Admission,
            IngressCause::AdmissionClosed,
            Retryability::ServiceRecovery,
        ),
        BusError::UnknownTarget { .. }
        | BusError::TargetDisabled { .. }
        | BusError::AddressUnavailable { .. } => target_unavailable(IngressStage::Admission),
    }
}

pub(super) fn external_task_id(exchange_id: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("guigu-agent-bridge:a2a:external-task:{exchange_id}").as_bytes(),
    )
    .to_string()
}

pub(super) fn admission_key(peer: &str, request_id: &str) -> String {
    let mut name = b"guigu-agent-bridge:a2a:admission-key\0".to_vec();
    name.extend_from_slice(&(peer.len() as u64).to_be_bytes());
    name.extend_from_slice(peer.as_bytes());
    name.extend_from_slice(&(request_id.len() as u64).to_be_bytes());
    name.extend_from_slice(request_id.as_bytes());
    Uuid::new_v5(&Uuid::NAMESPACE_OID, &name).to_string()
}
