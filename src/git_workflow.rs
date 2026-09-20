//! Trusted-local Git workflow policy.
//!
//! This module records collaboration controls only. Git itself runs in the
//! trusted ACP/full-access deployment; these checks are not an OS sandbox.

use std::collections::BTreeSet;
use std::collections::{HashMap, VecDeque};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

pub const MAX_PATHS: usize = 256;
pub const MAX_PATH_BYTES: usize = 16 * 1024;
pub const MAX_COMMIT_MESSAGE_BYTES: usize = 8 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024;
pub const MAX_RECEIPT_LINE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Role {
    Developer,
    Coordinator,
    Reviewer,
    Observer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Repository {
    Code,
    Governance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    #[error("wrong repository")]
    WrongRepository,
    #[error("wrong ref")]
    WrongRef,
    #[error("observer mutation")]
    ObserverMutation,
    #[error("operation denied")]
    OperationDenied,
    #[error("invalid path")]
    InvalidPath,
    #[error("path not allowed")]
    PathNotAllowed,
    #[error("too many paths")]
    TooManyPaths,
    #[error("paths too large")]
    PathsTooLarge,
    #[error("message too large")]
    MessageTooLarge,
    #[error("empty add")]
    EmptyAdd,
    #[error("wrong remote")]
    WrongRemote,
    #[error("missing commit authorization")]
    MissingCommitAuthorization,
    #[error("result mismatch")]
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowReceipt {
    pub task_id: String,
    pub role: Role,
    pub repository: Repository,
    pub ref_name: String,
    pub paths: Vec<String>,
    pub remote: String,
    pub base_commit: String,
    pub result_commit: String,
    pub parent_commit: String,
    pub operation: Operation,
    pub review_owner: String,
    pub expires_at_unix: i64,
    pub expected_old: String,
    pub expected_new: String,
    pub observed: Option<String>,
    pub readback: Readback,
    pub output: String,
    pub recorded_at_unix: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum ReceiptError {
    #[error("invalid receipt: {0}")]
    Invalid(#[from] PolicyError),
    #[error("receipt persistence failed")]
    Io(#[source] std::io::Error),
    #[error("receipt encoding failed")]
    Json(#[source] serde_json::Error),
}

pub struct ReceiptStore {
    path: PathBuf,
}

static RECEIPT_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

impl ReceiptStore {
    pub fn open(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        let normalized = if path.exists() {
            std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
        } else if path.is_absolute() {
            path.parent()
                .and_then(|p| std::fs::canonicalize(p).ok())
                .map(|p| p.join(path.file_name().unwrap_or_default()))
                .unwrap_or_else(|| path.to_path_buf())
        } else {
            let absolute = std::env::current_dir().unwrap_or_default().join(path);
            absolute
                .parent()
                .and_then(|p| std::fs::canonicalize(p).ok())
                .map(|p| p.join(absolute.file_name().unwrap_or_default()))
                .unwrap_or(absolute)
        };
        Self { path: normalized }
    }

    pub fn append(
        &self,
        allowlist: &Allowlist,
        receipt: &WorkflowReceipt,
    ) -> Result<(), ReceiptError> {
        let paths: Vec<String> = receipt
            .paths
            .iter()
            .map(|p| normalize_path(p))
            .collect::<Result<_, _>>()?;
        authorize(
            allowlist,
            receipt.repository,
            &receipt.ref_name,
            receipt.operation,
            &paths,
            None,
        )?;
        if receipt.role != allowlist.role
            || receipt.task_id.is_empty()
            || receipt.remote != allowlist.remote
            || receipt.base_commit.is_empty()
            || receipt.result_commit.is_empty()
            || receipt.parent_commit != receipt.base_commit
            || receipt.expected_old != receipt.base_commit
            || receipt.expected_new != receipt.result_commit
            || receipt.review_owner.is_empty()
            || receipt.expires_at_unix <= chrono::Utc::now().timestamp()
            || (receipt.role == Role::Reviewer && !paths.iter().all(|p| p.starts_with("reviews/")))
        {
            return Err(ReceiptError::Invalid(PolicyError::ResultMismatch));
        }
        let mut safe = receipt.clone();
        safe.paths = paths;
        safe.readback = classify_readback(
            &receipt.expected_old,
            receipt.observed.as_deref(),
            &receipt.expected_new,
        );
        safe.output = redact_output(receipt.output.as_bytes());
        if safe.output.contains("<bounded-error>") {
            safe.output = "<bounded-error>".into();
        }
        if safe.output.len() > MAX_OUTPUT_BYTES {
            safe.output.truncate(MAX_OUTPUT_BYTES);
        }
        let encoded = serde_json::to_string(&safe).map_err(ReceiptError::Json)?;
        if encoded.len() > 4 * 1024 {
            return Err(ReceiptError::Invalid(PolicyError::MessageTooLarge));
        }
        let locks = RECEIPT_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let lock = locks
            .lock()
            .expect("receipt lock map")
            .entry(self.path.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().expect("receipt append mutex");
        let mut existing = self.load()?;
        let cutoff = chrono::Utc::now().timestamp() - 30 * 24 * 60 * 60;
        existing.retain(|item| item.recorded_at_unix >= cutoff);
        if existing.len() >= 10_000 {
            existing.drain(..existing.len() - 9_999);
        }
        let temp = self
            .path
            .with_extension(format!("jsonl.tmp.{}", uuid::Uuid::now_v7()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .map_err(ReceiptError::Io)?;
        for item in existing {
            let line = serde_json::to_string(&item).map_err(ReceiptError::Json)?;
            if line.len() > MAX_RECEIPT_LINE_BYTES {
                let _ = std::fs::remove_file(&temp);
                return Err(ReceiptError::Invalid(PolicyError::MessageTooLarge));
            }
            writeln!(file, "{line}").map_err(ReceiptError::Io)?;
        }
        if encoded.len() > MAX_RECEIPT_LINE_BYTES {
            let _ = std::fs::remove_file(&temp);
            return Err(ReceiptError::Invalid(PolicyError::MessageTooLarge));
        }
        writeln!(file, "{encoded}").map_err(ReceiptError::Io)?;
        file.sync_all().map_err(ReceiptError::Io)?;
        std::fs::rename(&temp, &self.path).map_err(ReceiptError::Io)?;
        if let Some(parent) = self.path.parent() {
            std::fs::File::open(parent)
                .map_err(ReceiptError::Io)?
                .sync_all()
                .map_err(ReceiptError::Io)?;
        }
        Ok(())
    }

    pub fn load(&self) -> Result<Vec<WorkflowReceipt>, ReceiptError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = std::fs::File::open(&self.path).map_err(ReceiptError::Io)?;
        let mut receipts = VecDeque::with_capacity(10_001);
        let mut lines = BufReader::new(file).lines().peekable();
        while let Some(line) = lines.next() {
            let line = line.map_err(ReceiptError::Io)?;
            if line.len() > MAX_RECEIPT_LINE_BYTES {
                return Err(ReceiptError::Invalid(PolicyError::MessageTooLarge));
            }
            match serde_json::from_str(&line) {
                Ok(receipt) => {
                    receipts.push_back(receipt);
                    if receipts.len() > 10_000 {
                        receipts.pop_front();
                    }
                }
                Err(error) if lines.peek().is_none() => {
                    let _ = error;
                    break;
                }
                Err(error) => return Err(ReceiptError::Json(error)),
            }
        }
        let cutoff = chrono::Utc::now().timestamp() - 30 * 24 * 60 * 60;
        receipts.retain(|receipt: &WorkflowReceipt| receipt.recorded_at_unix >= cutoff);
        Ok(receipts.into_iter().collect())
    }
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
    if std::str::from_utf8(output).is_err() {
        return "<bounded-error>".into();
    }
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
    if rendered.contains("PRIVATE") || rendered.contains("BEGIN ") {
        return "<bounded-error>".into();
    }
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

    #[test]
    fn receipt_store_redacts_and_survives_reload() {
        let path =
            std::env::temp_dir().join(format!("guigu-receipt-{}.jsonl", uuid::Uuid::now_v7()));
        let store = ReceiptStore::open(&path);
        let receipt = WorkflowReceipt {
            task_id: "T024".into(),
            role: Role::Developer,
            repository: Repository::Code,
            ref_name: "refs/heads/task/T024".into(),
            paths: vec!["src/lib.rs".into()],
            remote: "origin".into(),
            base_commit: "base".into(),
            result_commit: "result".into(),
            parent_commit: "base".into(),
            operation: Operation::Commit,
            review_owner: "coordinator".into(),
            expires_at_unix: chrono::Utc::now().timestamp() + 3600,
            expected_old: "base".into(),
            expected_new: "result".into(),
            observed: Some("result".into()),
            readback: Readback::Confirmed,
            output: "token=secret".into(),
            recorded_at_unix: chrono::Utc::now().timestamp(),
        };
        store
            .append(&policy(Role::Developer), &receipt)
            .expect("receipt");
        let loaded = store.load().expect("reload");
        assert_eq!(loaded.len(), 1);
        assert!(!loaded[0].output.contains("secret"));
        let _ = std::fs::remove_file(path);
    }
}
