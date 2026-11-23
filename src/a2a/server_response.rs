use axum::{Json, http::StatusCode, response::IntoResponse};
use serde_json::Value;

use super::{
    A2aError, IngressCause as Cause, IngressFailure, IngressStage as Stage, Retryability as Retry,
    wire::{JsonRpcError, JsonRpcResponse},
};

pub(super) fn classify(error: A2aError) -> IngressFailure {
    match error {
        A2aError::Ingress(value) => value,
        A2aError::Unsupported => {
            IngressFailure::new(Stage::Method, Cause::Unsupported, Retry::Never)
        }
        A2aError::TooLarge => {
            IngressFailure::new(Stage::ContentPersist, Cause::BodyTooLarge, Retry::Never)
        }
        A2aError::Conflict => IngressFailure::new(
            Stage::IdempotencyReserve,
            Cause::IdempotencyConflict,
            Retry::Never,
        ),
        A2aError::Storage(_) => IngressFailure::new(
            Stage::IdempotencyReserve,
            Cause::Storage,
            Retry::SameRequest,
        ),
        A2aError::RecoveryNeeded => {
            IngressFailure::new(Stage::Binding, Cause::RecoveryRequired, Retry::Never)
        }
        A2aError::Unauthorized => IngressFailure::new(
            Stage::Authentication,
            Cause::AuthenticationFailed,
            Retry::CredentialsChange,
        ),
        _ => IngressFailure::new(Stage::ResponseEncode, Cause::Internal, Retry::Never),
    }
}

pub(super) fn failure_response(
    correlation: uuid::Uuid,
    id: Value,
    method: &'static str,
    failure: IngressFailure,
) -> axum::response::Response {
    let (status, code, message) = match failure.cause {
        Cause::MalformedJson | Cause::InvalidParams => {
            (StatusCode::BAD_REQUEST, -32602, "invalid params")
        }
        Cause::Unsupported => (StatusCode::BAD_REQUEST, -32601, "unsupported message"),
        Cause::AuthenticationFailed => (StatusCode::UNAUTHORIZED, -32001, "unauthorized"),
        Cause::TargetUnavailable => (StatusCode::NOT_FOUND, -32001, "unauthorized"),
        Cause::BodyTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, -32002, "request too large"),
        Cause::IdempotencyConflict => (StatusCode::CONFLICT, -32009, "idempotency conflict"),
        Cause::Storage => (
            StatusCode::INTERNAL_SERVER_ERROR,
            -32010,
            "storage unavailable",
        ),
        Cause::AdmissionBusy => (StatusCode::SERVICE_UNAVAILABLE, -32011, "admission busy"),
        Cause::AdmissionClosed => (
            StatusCode::SERVICE_UNAVAILABLE,
            -32011,
            "admission unavailable",
        ),
        Cause::BindingIncomplete | Cause::RecoveryRequired => {
            (StatusCode::SERVICE_UNAVAILABLE, -32012, "recovery required")
        }
        Cause::Internal => (StatusCode::INTERNAL_SERVER_ERROR, -32603, "internal error"),
    };
    terminal_event(correlation, method, Some(failure), status, code);
    (status, Json(error(id, code, message))).into_response()
}

pub(super) fn method_class(method: &str) -> &'static str {
    match method {
        "message/send" => "send",
        "tasks/get" => "get",
        "tasks/cancel" => "cancel",
        _ => "other",
    }
}

pub(super) fn terminal_event(
    correlation: uuid::Uuid,
    method: &'static str,
    failure: Option<IngressFailure>,
    status: StatusCode,
    rpc_code: i32,
) {
    match failure {
        Some(value) => {
            tracing::info!(correlation=%correlation, stage=?value.stage, cause=?value.cause, retryability=?value.retryability, method_class=method, http_status=status.as_u16(), rpc_code, "A2A ingress terminal")
        }
        None => {
            tracing::info!(correlation=%correlation, stage="response_encode", cause="ok", retryability="never", method_class=method, http_status=status.as_u16(), rpc_code, "A2A ingress terminal")
        }
    }
}

fn error(id: Value, code: i32, message: &'static str) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.into(),
        }),
    }
}
