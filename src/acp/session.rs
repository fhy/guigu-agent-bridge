//! Session persistence: the `sessions` table and its store.
//!
//! Sessions are durable because a restarted bridge must be able to `session/resume`
//! work it already began instead of starting a fresh conversation with the agent.
//!
//! # Why this is not a `Repository` method
//!
//! The [`Repository`](crate::storage::Repository) trait is frozen: T009 implemented
//! its 22 methods and nothing may add a 23rd. Session storage therefore lives in
//! this standalone type, which holds a **clone of the same [`SqlitePool`]** — the
//! pool is a shared, internally synchronised handle, so a
//! [`SqliteSessionStore`] and a [`SqliteRepository`](crate::storage::SqliteRepository)
//! use one set of connections, one `busy_timeout` and one WAL configuration, with
//! no second connection setup and no new method on the frozen trait.
//!
//! The table arrives through `migrations/0002_sessions.sql`, an **appended**
//! migration: [`migrate`](crate::storage::migrate) embeds the migration directory at
//! compile time, so it is applied automatically and an already-migrated database
//! simply gains the new version (unlike an in-place edit of `0001`, an append does
//! not trip the checksum guard).
//!
//! # Invariants
//!
//! 1. **At most one live session per `(endpoint, conversation, cwd)`** — a partial
//!    unique index over rows whose state is not `closed`. That is what makes
//!    "resume the live session" a deterministic choice with no second candidate.
//! 2. **Closed rows do not participate in that uniqueness** — a session that ended
//!    stays as history and does not block a later one.
//! 3. **`(session_id, endpoint_id)` is the primary key** — the agent's session id is
//!    opaque and only unique within its own backend.
//! 4. **Timestamps are fixed-width UTC nanoseconds** (the T008 codec), so their
//!    lexical order is their chronological order.
//! 5. **`endpoint_id` and `conversation_id` are foreign keys**, so a session can
//!    only be recorded for a known endpoint and conversation. The assembly order
//!    (migrate → [`sync_agents`](crate::storage::sync_agents) → adapter) is what
//!    satisfies that precondition; violating it is an explicit
//!    [`StorageError::IntegrityViolation`], never a silent row.
//!
//! # Duplicate handling
//!
//! [`SqliteSessionStore::upsert_session`] follows the same policy as T009's
//! repository: a row for the same `(session_id, endpoint_id)` is refreshed in
//! place, but only while it still describes the same conversation and working
//! directory — a session id rebound to different work is reported as
//! [`StorageError::IntegrityViolation`] rather than silently rewritten.

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};

use crate::models::{ConversationId, EndpointId};
use crate::storage::StorageError;
use crate::storage::codec::{decode_id, decode_timestamp, encode_id, encode_timestamp};

/// The largest session id the adapter accepts.
///
/// Session ids are opaque backend strings; the bound keeps one hostile id from
/// becoming a large frame, row, or error message.
pub const MAX_SESSION_ID_BYTES: usize = 256;

/// Where a session is in its life cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// The session exists and is not currently running a prompt.
    Ready,
    /// A prompt turn is in flight.
    Prompting,
    /// The session is finished; the row is kept as history.
    Closed,
}

impl SessionState {
    /// The stored text value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Prompting => "prompting",
            Self::Closed => "closed",
        }
    }

    fn from_text(text: &str, field: &'static str) -> Result<Self, StorageError> {
        match text {
            "ready" => Ok(Self::Ready),
            "prompting" => Ok(Self::Prompting),
            "closed" => Ok(Self::Closed),
            _ => Err(StorageError::Malformed {
                field,
                detail: "unknown session state".to_owned(),
            }),
        }
    }
}

/// One stored session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSession {
    session_id: String,
    endpoint_id: EndpointId,
    conversation_id: ConversationId,
    cwd: String,
    additional_directories: String,
    compatibility_version: u16,
    compatibility_hash: String,
    backend_id: String,
    protocol_version: u16,
    state: SessionState,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl StoredSession {
    /// Record a session the agent just accepted.
    ///
    /// # Errors
    ///
    /// [`StorageError::Malformed`] when the session id is empty or longer than
    /// [`MAX_SESSION_ID_BYTES`]; the adapter validates the same bound before it
    /// gets here and reports it as a protocol problem.
    pub fn new(
        session_id: impl Into<String>,
        endpoint_id: EndpointId,
        conversation_id: ConversationId,
        cwd: impl Into<String>,
        backend_id: impl Into<String>,
        at: DateTime<Utc>,
    ) -> Result<Self, StorageError> {
        Self::new_with_workspaces(
            session_id,
            endpoint_id,
            conversation_id,
            cwd,
            &[],
            backend_id,
            at,
        )
    }

    pub fn new_with_workspaces(
        session_id: impl Into<String>,
        endpoint_id: EndpointId,
        conversation_id: ConversationId,
        cwd: impl Into<String>,
        additional_directories: &[String],
        backend_id: impl Into<String>,
        at: DateTime<Utc>,
    ) -> Result<Self, StorageError> {
        let session_id = session_id.into();
        if session_id.is_empty() || session_id.len() > MAX_SESSION_ID_BYTES {
            return Err(StorageError::Malformed {
                field: "sessions.session_id",
                detail: "session id is empty or too long".to_owned(),
            });
        }
        let cwd = cwd.into();
        let backend_id = backend_id.into();
        let compatibility_hash = compatibility_hash(
            &backend_id,
            crate::acp::PROTOCOL_VERSION,
            &cwd,
            additional_directories,
        );
        Ok(Self {
            session_id,
            endpoint_id,
            conversation_id,
            cwd,
            additional_directories: serde_json::to_string(additional_directories).map_err(
                |_| StorageError::Malformed {
                    field: "sessions.additional_directories",
                    detail: "invalid workspace identity".to_owned(),
                },
            )?,
            compatibility_version: 1,
            compatibility_hash,
            backend_id,
            protocol_version: crate::acp::PROTOCOL_VERSION,
            state: SessionState::Ready,
            created_at: at,
            updated_at: at,
        })
    }

    /// The agent's session identity.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The endpoint (agent) this session belongs to.
    pub fn endpoint_id(&self) -> EndpointId {
        self.endpoint_id
    }

    /// The conversation this session serves.
    pub fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// The working directory the session was created with.
    pub fn cwd(&self) -> &str {
        &self.cwd
    }

    pub fn additional_directories(&self) -> &str {
        &self.additional_directories
    }

    /// The compatibility identity of the backend (ADR-001).
    pub fn backend_id(&self) -> &str {
        &self.backend_id
    }

    /// The protocol version negotiated with that backend.
    pub fn protocol_version(&self) -> u16 {
        self.protocol_version
    }

    /// The session's current state.
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// When the row was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// When the row was last refreshed.
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

/// Durable session storage over a shared pool.
#[derive(Debug, Clone)]
pub struct SqliteSessionStore {
    pool: SqlitePool,
    owner: Option<crate::storage::BusinessStore>,
}

impl SqliteSessionStore {
    /// Wrap a pool that has been connected and migrated.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool, owner: None }
    }

    pub fn new_owner(owner: crate::storage::BusinessStore) -> Self {
        Self {
            pool: SqlitePool::connect_lazy("sqlite::memory:").expect("owner facade placeholder"),
            owner: Some(owner),
        }
    }

    /// The live session for this endpoint, conversation and working directory.
    ///
    /// Returns at most one row: the schema enforces it.
    ///
    /// # Errors
    ///
    /// [`StorageError`] from the query, or when a stored value cannot be decoded.
    pub async fn live_session(
        &self,
        endpoint: EndpointId,
        conversation: ConversationId,
        cwd: &str,
    ) -> Result<Option<StoredSession>, StorageError> {
        let row = sqlx::query(SELECT_LIVE_SESSION_LEGACY)
            .bind(encode_id(endpoint))
            .bind(encode_id(conversation))
            .bind(cwd)
            .fetch_optional(&self.pool)
            .await
            .map_err(StorageError::from)?;
        row.as_ref().map(session_from_row).transpose()
    }

    pub async fn live_session_with_workspaces(
        &self,
        endpoint: EndpointId,
        conversation: ConversationId,
        cwd: &str,
        additional_directories: &[String],
        backend_id: &str,
    ) -> Result<Option<StoredSession>, StorageError> {
        let hash = compatibility_hash(
            backend_id,
            crate::acp::PROTOCOL_VERSION,
            cwd,
            additional_directories,
        );
        let row = sqlx::query(SELECT_LIVE_SESSION)
            .bind(encode_id(endpoint))
            .bind(encode_id(conversation))
            .bind(cwd)
            .bind(serde_json::to_string(additional_directories).map_err(|_| {
                StorageError::Malformed {
                    field: "sessions.additional_directories",
                    detail: "invalid workspace identity".to_owned(),
                }
            })?)
            .bind(hash)
            .bind(backend_id)
            .bind(i64::from(crate::acp::PROTOCOL_VERSION))
            .fetch_optional(&self.pool)
            .await
            .map_err(StorageError::from)?;
        row.as_ref().map(session_from_row).transpose()
    }

    /// Insert a session, or refresh the row for the same session id.
    ///
    /// # Errors
    ///
    /// [`StorageError::IntegrityViolation`] when that session id is already bound
    /// to a different conversation or working directory, or when the referenced
    /// endpoint/conversation does not exist (the migration's foreign keys).
    pub async fn upsert_session(&self, session: &StoredSession) -> Result<(), StorageError> {
        let result = sqlx::query(UPSERT_SESSION)
            .bind(&session.session_id)
            .bind(encode_id(session.endpoint_id))
            .bind(encode_id(session.conversation_id))
            .bind(&session.cwd)
            .bind(&session.additional_directories)
            .bind(i64::from(session.compatibility_version))
            .bind(&session.compatibility_hash)
            .bind(&session.backend_id)
            .bind(i64::from(session.protocol_version))
            .bind(session.state.as_str())
            .bind(encode_timestamp(&session.created_at))
            .bind(encode_timestamp(&session.updated_at))
            .execute(&self.pool)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        Err(StorageError::IntegrityViolation {
            detail: "sessions.session_id is bound to another conversation or cwd".to_owned(),
        })
    }

    /// Move a session to `state`.
    ///
    /// # Errors
    ///
    /// [`StorageError::NotFound`] when the session is unknown, or
    /// [`StorageError::Duplicate`] when moving it back to a live state would
    /// create a second live session for the same conversation and directory.
    pub async fn set_state(
        &self,
        session_id: &str,
        endpoint: EndpointId,
        state: SessionState,
        at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(UPDATE_SESSION_STATE)
            .bind(state.as_str())
            .bind(encode_timestamp(&at))
            .bind(session_id)
            .bind(encode_id(endpoint))
            .execute(&self.pool)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        Err(StorageError::NotFound {
            entity: "session",
            id: session_id.to_owned(),
        })
    }
}

const SELECT_LIVE_SESSION: &str = "\
    SELECT session_id, endpoint_id, conversation_id, cwd, additional_directories, compatibility_version, compatibility_hash, backend_id, protocol_version, \
           state, created_at, updated_at \
    FROM sessions \
    WHERE endpoint_id = ? AND conversation_id = ? AND cwd = ? AND additional_directories = ? AND state <> 'closed' \
      AND ((compatibility_version = 1 AND compatibility_hash = ?) \
        OR (compatibility_version = 0 AND additional_directories = '[]' AND backend_id = ? AND protocol_version = ?))";
const SELECT_LIVE_SESSION_LEGACY: &str = "\
    SELECT session_id, endpoint_id, conversation_id, cwd, additional_directories, compatibility_version, compatibility_hash, backend_id, protocol_version, \
           state, created_at, updated_at FROM sessions \
    WHERE endpoint_id = ? AND conversation_id = ? AND cwd = ? AND state <> 'closed'";

/// `created_at` is deliberately not refreshed: the first acceptance of a session
/// is a fact, while its state and last-touch time are not.
const UPSERT_SESSION: &str = "\
    INSERT INTO sessions (session_id, endpoint_id, conversation_id, cwd, additional_directories, compatibility_version, compatibility_hash, backend_id, \
                          protocol_version, state, created_at, updated_at) \
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
    ON CONFLICT (session_id, endpoint_id) DO UPDATE SET \
        backend_id = excluded.backend_id, \
        protocol_version = excluded.protocol_version, \
        compatibility_version = excluded.compatibility_version, \
        compatibility_hash = excluded.compatibility_hash, \
        state = excluded.state, \
        updated_at = excluded.updated_at \
    WHERE sessions.conversation_id = excluded.conversation_id AND sessions.cwd = excluded.cwd AND sessions.additional_directories = excluded.additional_directories";

const UPDATE_SESSION_STATE: &str =
    "UPDATE sessions SET state = ?, updated_at = ? WHERE session_id = ? AND endpoint_id = ?";

fn session_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<StoredSession, StorageError> {
    let text = |column: &'static str| -> Result<String, StorageError> {
        row.try_get::<String, _>(column).map_err(StorageError::from)
    };
    let version = row
        .try_get::<i64, _>("protocol_version")
        .map_err(StorageError::from)?;
    let version: u16 = u16::try_from(version).map_err(|_| StorageError::Malformed {
        field: "sessions.protocol_version",
        detail: "value does not fit in u16".to_owned(),
    })?;

    Ok(StoredSession {
        session_id: text("session_id")?,
        endpoint_id: decode_id(&text("endpoint_id")?, "sessions.endpoint_id")?,
        conversation_id: decode_id(&text("conversation_id")?, "sessions.conversation_id")?,
        cwd: text("cwd")?,
        additional_directories: text("additional_directories")?,
        compatibility_version: u16::try_from(
            row.try_get::<i64, _>("compatibility_version")
                .map_err(StorageError::from)?,
        )
        .map_err(|_| StorageError::Malformed {
            field: "sessions.compatibility_version",
            detail: "value does not fit in u16".to_owned(),
        })?,
        compatibility_hash: text("compatibility_hash")?,
        backend_id: text("backend_id")?,
        protocol_version: version,
        state: SessionState::from_text(&text("state")?, "sessions.state")?,
        created_at: decode_timestamp(&text("created_at")?, "sessions.created_at")?,
        updated_at: decode_timestamp(&text("updated_at")?, "sessions.updated_at")?,
    })
}

fn compatibility_hash(backend_id: &str, protocol: u16, cwd: &str, roots: &[String]) -> String {
    let mut digest = Sha256::new();
    for value in [
        backend_id.as_bytes(),
        &protocol.to_be_bytes(),
        cwd.as_bytes(),
    ]
    .into_iter()
    .chain(roots.iter().map(String::as_bytes))
    {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_id_is_bounded() {
        let at: DateTime<Utc> = "2026-09-16T10:00:00Z".parse().expect("timestamp");
        let endpoint = EndpointId::generate();
        let conversation = ConversationId::generate();

        assert!(StoredSession::new("s1", endpoint, conversation, "/tmp", "b/1", at).is_ok());
        assert!(StoredSession::new("", endpoint, conversation, "/tmp", "b/1", at).is_err());
        assert!(
            StoredSession::new(
                "x".repeat(MAX_SESSION_ID_BYTES + 1),
                endpoint,
                conversation,
                "/tmp",
                "b/1",
                at
            )
            .is_err()
        );
    }

    #[test]
    fn a_new_session_starts_ready() {
        let at: DateTime<Utc> = "2026-09-16T10:00:00Z".parse().expect("timestamp");
        let session = StoredSession::new(
            "s1",
            EndpointId::generate(),
            ConversationId::generate(),
            "/tmp",
            "b/1",
            at,
        )
        .expect("session");
        assert_eq!(session.state(), SessionState::Ready);
        assert_eq!(session.protocol_version(), crate::acp::PROTOCOL_VERSION);
        assert_eq!(session.created_at(), at);
        assert_eq!(session.updated_at(), at);
    }

    #[test]
    fn every_session_state_renders_its_stored_value() {
        for (state, text) in [
            (SessionState::Ready, "ready"),
            (SessionState::Prompting, "prompting"),
            (SessionState::Closed, "closed"),
        ] {
            assert_eq!(state.as_str(), text);
            assert_eq!(
                SessionState::from_text(text, "sessions.state").expect("state"),
                state
            );
        }
        assert!(SessionState::from_text("paused", "sessions.state").is_err());
    }
}
