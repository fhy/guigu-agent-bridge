use crate::bus::DispatchOutcome;
use crate::models::TaskStatus;

use super::A2aError;
use super::wire::TaskState;

pub fn internal_status(state: TaskState) -> Result<TaskStatus, A2aError> {
    match state {
        TaskState::Submitted => Ok(TaskStatus::Queued),
        TaskState::Working => Ok(TaskStatus::Running),
        TaskState::Completed => Ok(TaskStatus::Completed),
        TaskState::Failed | TaskState::AuthRequired => Ok(TaskStatus::Failed),
        TaskState::Canceled => Ok(TaskStatus::Cancelled),
        TaskState::Rejected => Err(A2aError::Protocol("rejected is pre-acceptance only")),
        TaskState::InputRequired => Err(A2aError::Unsupported),
    }
}

pub fn outbound_outcome(state: TaskState) -> Result<Option<DispatchOutcome>, A2aError> {
    match state {
        TaskState::Submitted | TaskState::Working => Ok(None),
        TaskState::Completed => Ok(Some(DispatchOutcome::Completed {
            output: String::new(),
        })),
        TaskState::Failed => Ok(Some(DispatchOutcome::Failed {
            error: "remote_failed".into(),
        })),
        TaskState::Canceled => Ok(Some(DispatchOutcome::Failed {
            error: "remote_cancelled".into(),
        })),
        TaskState::Rejected => Err(A2aError::Protocol("remote rejected task")),
        TaskState::InputRequired | TaskState::AuthRequired => Err(A2aError::Unsupported),
    }
}
