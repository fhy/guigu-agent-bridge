//! Trusted-local Git workflow policy.
//!
//! This module records collaboration controls only. Git itself runs in the
//! trusted ACP/full-access deployment; these checks are not an OS sandbox.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

pub const MAX_PATHS: usize = 256;
pub const MAX_PATH_BYTES: usize = 16 * 1024;
pub const MAX_COMMIT_MESSAGE_BYTES: usize = 8 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Developer,
    Coordinator,
    Reviewer,
    Observer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repository {
    Code,
    Governance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Status,
    Diff,
    Add,
    Commit,
    Push,
    ResetHard,
    CleanFd,
    ForcePush,
    DeleteRemoteBranch,
    Tag,
    Release,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Readback {
    Confirmed,
    Uncertain,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allowlist {
    pub role: Role,
    pub repository: Repository,
    pub ref_name: String,
    pub path_prefixes: BTreeSet<String>,
    pub remote: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    WrongRepository,
    WrongRef,
    ObserverMutation,
    OperationDenied,
    InvalidPath,
    PathNotAllowed,
    TooManyPaths,
    PathsTooLarge,
    MessageTooLarge,
    EmptyAdd,
    WrongRemote,
    MissingCommitAuthorization,
    ResultMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAuthorization {
    pub task_id: String,
    pub base_commit: String,
    pub allowed_paths: BTreeSet<String>,
    pub expected_remote: String,
    pub expected_ref: String,
}

pub struct CommitRequest<'a> {
    pub repository: Repository,
    pub ref_name: &'a str,
    pub remote: &'a str,
    pub staged: &'a [String],
    pub result_paths: &'a [String],
    pub result_commit: &'a str,
    pub commit_message: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    pub role: Role,
    pub repository: Repository,
    pub operation: Operation,
    pub ref_name: String,
    pub result: Readback,
    pub detail: String,
}

#[derive(Clone, Default)]
pub struct WorkflowCapability {
    audit: Arc<Mutex<Vec<AuditRecord>>>,
}

impl WorkflowCapability {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn authorize_commit(
        &self,
        allowlist: &Allowlist,
        auth: &CommitAuthorization,
        request: CommitRequest<'_>,
    ) -> Result<(), PolicyError> {
        authorize(
            allowlist,
            request.repository,
            request.ref_name,
            Operation::Commit,
            request.staged,
            Some(request.commit_message),
        )?;
        if request.remote != auth.expected_remote || request.ref_name != auth.expected_ref {
            return Err(PolicyError::WrongRemote);
        }
        check_staged_paths(
            request.staged,
            &auth.allowed_paths.iter().cloned().collect::<Vec<_>>(),
        )?;
        check_staged_paths(
            request.result_paths,
            &auth.allowed_paths.iter().cloned().collect::<Vec<_>>(),
        )?;
        if request.result_commit.is_empty() || auth.base_commit.is_empty() {
            return Err(PolicyError::ResultMismatch);
        }
        Ok(())
    }

    pub fn record_readback(&self, record: AuditRecord) {
        let mut audit = self.audit.lock().expect("workflow audit mutex");
        if audit.len() >= 10_000 {
            audit.remove(0);
        }
        audit.push(record);
    }

    pub fn audit_snapshot(&self) -> Vec<AuditRecord> {
        self.audit.lock().expect("workflow audit mutex").clone()
    }
}

pub fn normalize_path(path: &str) -> Result<String, PolicyError> {
    if path.is_empty() || path.starts_with('/') || path.contains('\0') {
        return Err(PolicyError::InvalidPath);
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." || part == ".." || part == ".git" {
            return Err(PolicyError::InvalidPath);
        }
        parts.push(part);
    }
    Ok(parts.join("/"))
}

pub fn authorize(
    allowlist: &Allowlist,
    repository: Repository,
    ref_name: &str,
    operation: Operation,
    paths: &[String],
    commit_message: Option<&str>,
) -> Result<(), PolicyError> {
    let role_repository_ok = matches!(
        (allowlist.role, repository),
        (Role::Developer, Repository::Code)
            | (Role::Coordinator, Repository::Governance)
            | (Role::Reviewer, Repository::Governance)
            | (Role::Observer, Repository::Code | Repository::Governance)
    );
    if !role_repository_ok {
        return Err(PolicyError::WrongRepository);
    }
    if allowlist.repository != repository {
        return Err(PolicyError::WrongRepository);
    }
    if allowlist.ref_name != ref_name {
        return Err(PolicyError::WrongRef);
    }
    let mutation = matches!(
        operation,
        Operation::Add | Operation::Commit | Operation::Push
    );
    if allowlist.role == Role::Observer && mutation {
        return Err(PolicyError::ObserverMutation);
    }
    if matches!(
        operation,
        Operation::ResetHard
            | Operation::CleanFd
            | Operation::ForcePush
            | Operation::DeleteRemoteBranch
            | Operation::Tag
            | Operation::Release
    ) {
        return Err(PolicyError::OperationDenied);
    }
    if paths.len() > MAX_PATHS {
        return Err(PolicyError::TooManyPaths);
    }
    if operation == Operation::Add && paths.is_empty() {
        return Err(PolicyError::EmptyAdd);
    }
    let total = paths.iter().map(|path| path.len()).sum::<usize>();
    if total > MAX_PATH_BYTES {
        return Err(PolicyError::PathsTooLarge);
    }
    for path in paths {
        let normalized = normalize_path(path)?;
        if !allowlist
            .path_prefixes
            .iter()
            .any(|prefix| normalized == *prefix || normalized.starts_with(&format!("{prefix}/")))
        {
            return Err(PolicyError::PathNotAllowed);
        }
    }
    if let Some(message) = commit_message
        && message.len() > MAX_COMMIT_MESSAGE_BYTES
    {
        return Err(PolicyError::MessageTooLarge);
    }
    Ok(())
}

pub fn check_staged_paths(staged: &[String], allowlisted: &[String]) -> Result<(), PolicyError> {
    let allowed: BTreeSet<_> = allowlisted
        .iter()
        .map(|path| normalize_path(path))
        .collect::<Result<_, _>>()?;
    for path in staged {
        if !allowed.contains(&normalize_path(path)?) {
            return Err(PolicyError::PathNotAllowed);
        }
    }
    Ok(())
}

pub fn classify_readback(
    expected_old: &str,
    observed: Option<&str>,
    expected_new: &str,
) -> Readback {
    match observed {
        Some(value) if value == expected_new => Readback::Confirmed,
        Some(value) if value != expected_old => Readback::Conflict,
        _ => Readback::Uncertain,
    }
}

pub fn redact_output(output: &[u8]) -> String {
    let bounded = &output[..output.len().min(MAX_OUTPUT_BYTES)];
    let mut rendered = String::new();
    for line in String::from_utf8_lossy(bounded).lines() {
        let mut auth_words = 0_u8;
        let mut words = Vec::new();
        for word in line.split_whitespace() {
            let lower = word.to_ascii_lowercase();
            let sensitive = auth_words > 0
                || lower.contains("token=")
                || lower.contains("password=")
                || lower.contains("secret=");
            if lower == "authorization:" || lower.starts_with("authorization:") {
                auth_words = 2;
                words.push(word.to_string());
            } else if sensitive {
                auth_words = auth_words.saturating_sub(1);
                words.push("<redacted>".to_string());
            } else {
                auth_words = auth_words.saturating_sub(1);
                words.push(word.to_string());
            }
        }
        rendered.push_str(&words.join(" "));
        rendered.push('\n');
    }
    rendered.truncate(rendered.len().min(MAX_OUTPUT_BYTES));
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(role: Role) -> Allowlist {
        Allowlist {
            role,
            repository: Repository::Code,
            ref_name: "refs/heads/task/T024".into(),
            path_prefixes: ["src".into()].into_iter().collect(),
            remote: "origin".into(),
        }
    }

    #[test]
    fn explicit_allowlist_rejects_ambient_paths() {
        let result = authorize(
            &policy(Role::Developer),
            Repository::Code,
            "refs/heads/task/T024",
            Operation::Add,
            &["src/lib.rs".into(), "Cargo.toml".into()],
            None,
        );
        assert_eq!(result, Err(PolicyError::PathNotAllowed));
    }

    #[test]
    fn dangerous_operations_are_workflow_denied() {
        assert_eq!(
            authorize(
                &policy(Role::Developer),
                Repository::Code,
                "refs/heads/task/T024",
                Operation::ForcePush,
                &[],
                None
            ),
            Err(PolicyError::OperationDenied)
        );
    }

    #[test]
    fn readback_keeps_expected_old_uncertain() {
        assert_eq!(
            classify_readback("old", Some("old"), "new"),
            Readback::Uncertain
        );
        assert_eq!(
            classify_readback("old", Some("other"), "new"),
            Readback::Conflict
        );
        assert_eq!(
            classify_readback("old", Some("new"), "new"),
            Readback::Confirmed
        );
    }

    #[test]
    fn staged_paths_are_checked_independently() {
        assert_eq!(
            check_staged_paths(
                &["src/lib.rs".into(), "README.md".into()],
                &["src/lib.rs".into()]
            ),
            Err(PolicyError::PathNotAllowed)
        );
    }

    #[test]
    fn redaction_does_not_loop_or_leak_bearer_values() {
        let value = redact_output(b"token=secret Authorization: Bearer hidden");
        assert!(!value.contains("secret"));
        assert!(!value.contains("hidden"));
        assert!(value.contains("<redacted>"));
    }

    #[test]
    fn role_matrix_rejects_developer_governance_mutation() {
        let mut governance = policy(Role::Developer);
        governance.repository = Repository::Governance;
        assert_eq!(
            authorize(
                &governance,
                Repository::Governance,
                "refs/heads/task/T024",
                Operation::Commit,
                &["src/lib.rs".into()],
                Some("x")
            ),
            Err(PolicyError::WrongRepository)
        );
    }
}
