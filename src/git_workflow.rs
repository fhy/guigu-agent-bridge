//! Trusted-local Git workflow policy.
//!
//! This module records collaboration controls only. Git itself runs in the
//! trusted ACP/full-access deployment; these checks are not an OS sandbox.

use std::collections::BTreeSet;
use std::collections::{HashMap, VecDeque};
use std::fs::OpenOptions;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use uuid::Uuid;

pub const MAX_PATHS: usize = 256;
pub const MAX_PATH_BYTES: usize = 16 * 1024;
pub const MAX_COMMIT_MESSAGE_BYTES: usize = 8 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024;
pub const MAX_RECEIPT_LINE_BYTES: usize = 64 * 1024;

struct TempGuard {
    path: PathBuf,
    committed: bool,
}
impl Drop for TempGuard {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn read_bounded_lines(
    path: &Path,
    mut visit: impl FnMut(String, bool) -> Result<bool, ReceiptError>,
) -> Result<(), ReceiptError> {
    let mut reader = BufReader::new(std::fs::File::open(path).map_err(ReceiptError::Io)?);
    let mut current = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        let count = reader.read(&mut byte).map_err(ReceiptError::Io)?;
        if count == 0 {
            if !current.is_empty() {
                let _ = visit(
                    String::from_utf8(current.clone())
                        .map_err(|_| ReceiptError::Invalid(PolicyError::MessageTooLarge))?,
                    false,
                )?;
            }
            break;
        }
        current.push(byte[0]);
        if current.len() > MAX_RECEIPT_LINE_BYTES {
            return Err(ReceiptError::Invalid(PolicyError::MessageTooLarge));
        }
        if byte[0] == b'\n' {
            if !visit(
                String::from_utf8(current[..current.len() - 1].to_vec())
                    .map_err(|_| ReceiptError::Invalid(PolicyError::MessageTooLarge))?,
                true,
            )? {
                break;
            }
            current.clear();
        }
    }
    Ok(())
}

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
    #[error("invalid authorization")]
    InvalidAuthorization,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAuthorization {
    role: Role,
    task_id: String,
    base_commit: String,
    allowed_paths: BTreeSet<String>,
    expected_remote: String,
    expected_ref: String,
    capability_id: String,
    issuer_nonce: String,
}

impl CommitAuthorization {
    pub fn issue(
        allowlist: &Allowlist,
        task_id: impl Into<String>,
        base_commit: impl Into<String>,
    ) -> Result<Self, PolicyError> {
        let task_id = task_id.into();
        let base_commit = base_commit.into();
        validate_task_id(&task_id)?;
        validate_ref(&allowlist.ref_name)?;
        validate_oid(&base_commit)?;
        if allowlist.remote.is_empty() || allowlist.path_prefixes.is_empty() {
            return Err(PolicyError::InvalidAuthorization);
        }
        let issuer_nonce = Uuid::now_v7().to_string();
        Ok(Self {
            role: allowlist.role,
            task_id: task_id.clone(),
            base_commit: base_commit.clone(),
            allowed_paths: allowlist.path_prefixes.clone(),
            expected_remote: allowlist.remote.clone(),
            expected_ref: allowlist.ref_name.clone(),
            capability_id: format!("cap-{}", Uuid::now_v7()),
            issuer_nonce,
        })
    }
}

pub fn authorization_id(auth: &CommitAuthorization, result_commit: &str) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        auth.capability_id,
        auth.task_id,
        auth.expected_ref,
        auth.base_commit,
        result_commit,
        auth.expected_remote
    )
}

fn validate_task_id(value: &str) -> Result<(), PolicyError> {
    if value.len() < 2 || !value.starts_with('T') || !value[1..].chars().all(|c| c.is_ascii_digit())
    {
        return Err(PolicyError::InvalidAuthorization);
    }
    Ok(())
}

fn validate_ref(value: &str) -> Result<(), PolicyError> {
    let valid = value.starts_with("refs/heads/task/")
        && value[16..].split('/').all(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        });
    if !valid {
        return Err(PolicyError::WrongRef);
    }
    Ok(())
}

fn validate_oid(value: &str) -> Result<(), PolicyError> {
    if !matches!(value.len(), 40 | 64) || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(PolicyError::ResultMismatch);
    }
    Ok(())
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
    pub staged_paths: Vec<String>,
    pub result_paths: Vec<String>,
    pub authorization_id: String,
    pub capability_id: String,
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
    #[error("receipt commit uncertain after rename")]
    Uncertain,
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
        let _ = allowlist;
        let _ = receipt;
        Err(ReceiptError::Invalid(
            PolicyError::MissingCommitAuthorization,
        ))
    }

    pub fn append_authorized(
        &self,
        allowlist: &Allowlist,
        authorization: &CommitAuthorization,
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
        if authorization.role != allowlist.role
            || receipt.role != authorization.role
            || validate_task_id(&receipt.task_id).is_err()
            || validate_ref(&receipt.ref_name).is_err()
            || receipt.authorization_id.is_empty()
            || receipt.capability_id.is_empty()
            || receipt.remote != allowlist.remote
            || validate_oid(&receipt.base_commit).is_err()
            || validate_oid(&receipt.result_commit).is_err()
            || receipt.parent_commit != receipt.base_commit
            || receipt.expected_old != receipt.base_commit
            || receipt.expected_new != receipt.result_commit
            || receipt.review_owner.is_empty()
            || receipt.expires_at_unix <= chrono::Utc::now().timestamp()
            || (receipt.role == Role::Reviewer && !paths.iter().all(|p| p.starts_with("reviews/")))
        {
            return Err(ReceiptError::Invalid(PolicyError::ResultMismatch));
        }
        if authorization.issuer_nonce.is_empty()
            || authorization.capability_id.is_empty()
            || receipt.task_id != authorization.task_id
            || receipt.role != authorization.role
        {
            return Err(ReceiptError::Invalid(PolicyError::InvalidAuthorization));
        }
        let expected_auth = authorization_id(authorization, &receipt.result_commit);
        if receipt.authorization_id != expected_auth {
            return Err(ReceiptError::Invalid(PolicyError::ResultMismatch));
        }
        if receipt.task_id != authorization.task_id
            || receipt.ref_name != authorization.expected_ref
            || receipt.base_commit != authorization.base_commit
            || receipt.remote != authorization.expected_remote
            || receipt.capability_id != authorization.capability_id
        {
            return Err(ReceiptError::Invalid(PolicyError::ResultMismatch));
        }
        check_paths_against_prefixes(
            &receipt.staged_paths,
            &authorization
                .allowed_paths
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
        )?;
        check_paths_against_prefixes(
            &receipt.result_paths,
            &authorization
                .allowed_paths
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
        )?;
        check_staged_paths(&receipt.staged_paths, &paths)?;
        check_staged_paths(&receipt.result_paths, &paths)?;
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
        let mut guard = TempGuard {
            path: temp.clone(),
            committed: false,
        };
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
        guard.committed = true;
        if let Some(parent) = self.path.parent() {
            std::fs::File::open(parent)
                .map_err(ReceiptError::Io)?
                .sync_all()
                .map_err(|_| ReceiptError::Uncertain)?;
        }
        Ok(())
    }

    pub fn load(&self) -> Result<Vec<WorkflowReceipt>, ReceiptError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let mut receipts = VecDeque::with_capacity(10_001);
        let mut index = 0_usize;
        read_bounded_lines(&self.path, |line, terminated| {
            match serde_json::from_str(&line) {
                Ok(receipt) => {
                    receipts.push_back(receipt);
                    if receipts.len() > 10_000 {
                        receipts.pop_front();
                    }
                }
                Err(error) if !terminated && index > 0 => {
                    let _ = error;
                    return Ok(false);
                }
                Err(error) => return Err(ReceiptError::Json(error)),
            }
            index += 1;
            Ok(true)
        })?;
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

fn check_paths_against_prefixes(paths: &[String], prefixes: &[String]) -> Result<(), PolicyError> {
    let prefixes: Vec<String> = prefixes
        .iter()
        .map(|path| normalize_path(path))
        .collect::<Result<_, _>>()?;
    for path in paths {
        let normalized = normalize_path(path)?;
        if !prefixes
            .iter()
            .any(|prefix| normalized == *prefix || normalized.starts_with(&format!("{prefix}/")))
        {
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
    if rendered.trim().is_empty() {
        String::new()
    } else {
        "<bounded-error>".into()
    }
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
        assert!(value == "<bounded-error>" || value.contains("<redacted>"));
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
    fn authorization_issue_rejects_bad_task_ref_and_oid() {
        let policy = policy(Role::Developer);
        assert!(CommitAuthorization::issue(&policy, "task-T024", "base").is_err());
        let mut bad_ref = policy.clone();
        bad_ref.ref_name = "refs/heads/main".into();
        assert!(
            CommitAuthorization::issue(
                &bad_ref,
                "T024",
                "0123456789abcdef0123456789abcdef01234567"
            )
            .is_err()
        );
        assert!(CommitAuthorization::issue(&policy, "T024", "not-an-oid").is_err());
    }

    #[test]
    fn forged_authorization_without_issuer_nonce_is_rejected() {
        let policy = policy(Role::Developer);
        let mut auth =
            CommitAuthorization::issue(&policy, "T024", "0123456789abcdef0123456789abcdef01234567")
                .expect("authorization");
        auth.issuer_nonce.clear();
        assert!(auth.issuer_nonce.is_empty());
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
            staged_paths: vec!["src/lib.rs".into()],
            result_paths: vec!["src/lib.rs".into()],
            authorization_id: String::new(),
            capability_id: String::new(),
            remote: "origin".into(),
            base_commit: "0123456789abcdef0123456789abcdef01234567".into(),
            result_commit: "fedcba9876543210fedcba9876543210fedcba98".into(),
            parent_commit: "0123456789abcdef0123456789abcdef01234567".into(),
            operation: Operation::Commit,
            review_owner: "coordinator".into(),
            expires_at_unix: chrono::Utc::now().timestamp() + 3600,
            expected_old: "0123456789abcdef0123456789abcdef01234567".into(),
            expected_new: "fedcba9876543210fedcba9876543210fedcba98".into(),
            observed: Some("fedcba9876543210fedcba9876543210fedcba98".into()),
            readback: Readback::Confirmed,
            output: "token=secret".into(),
            recorded_at_unix: chrono::Utc::now().timestamp(),
        };
        let auth = CommitAuthorization::issue(
            &policy(Role::Developer),
            "T024",
            "0123456789abcdef0123456789abcdef01234567",
        )
        .expect("authorization");
        let mut receipt = receipt;
        receipt.capability_id = auth.capability_id.clone();
        receipt.authorization_id = authorization_id(&auth, &receipt.result_commit);
        store
            .append_authorized(&policy(Role::Developer), &auth, &receipt)
            .expect("receipt");
        let loaded = store.load().expect("reload");
        assert_eq!(loaded.len(), 1);
        assert!(!loaded[0].output.contains("secret"));
        let _ = std::fs::remove_file(path);
    }
}
