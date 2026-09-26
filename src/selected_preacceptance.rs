//! In-process, redacted composition of T038 selection and T037 recovery.
use crate::{offline_preacceptance, readonly_tuple};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SelectedRecoveryError {
    #[error("selected pre-acceptance rejected: invalid arguments")]
    InvalidArguments,
    #[error("selected pre-acceptance rejected: empty")]
    Empty,
    #[error("selected pre-acceptance rejected: selection")]
    Selection,
    #[error("selected pre-acceptance rejected: recovery")]
    Recovery,
}

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, SelectedRecoveryError> {
    if args.len() != 3 || args[0] != "recover-selected-preacceptance" || args[1] != "--database" {
        return Err(SelectedRecoveryError::InvalidArguments);
    }
    let path = args[2]
        .to_str()
        .ok_or(SelectedRecoveryError::InvalidArguments)?;
    if path.is_empty() {
        return Err(SelectedRecoveryError::InvalidArguments);
    }
    Ok(PathBuf::from(path))
}

pub async fn recover(database: impl AsRef<Path>) -> Result<usize, SelectedRecoveryError> {
    let selected =
        readonly_tuple::select(database.as_ref())
            .await
            .map_err(|error| match error {
                readonly_tuple::SelectionError::Empty => SelectedRecoveryError::Empty,
                _ => SelectedRecoveryError::Selection,
            })?;
    let tuple = offline_preacceptance::PreAcceptanceTuple {
        delivery_id: selected.delivery_id,
        task_id: selected.task_id,
        attempt: selected.attempt,
    };
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    match offline_preacceptance::recover(database, &[tuple], &now).await {
        Ok(offline_preacceptance::PreAcceptanceOutcome::Recovered { count }) => Ok(count),
        Ok(offline_preacceptance::PreAcceptanceOutcome::AlreadyReconciled) => {
            Err(SelectedRecoveryError::Empty)
        }
        Err(_) => Err(SelectedRecoveryError::Recovery),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parser_rejects_shape_without_echoing_values() {
        let args = vec!["recover-selected-preacceptance".into(), "--database".into()];
        assert_eq!(
            parse_args(&args),
            Err(SelectedRecoveryError::InvalidArguments)
        );
    }
}
