-- T014 migration 0002: ACP sessions.
--
-- Appended, never edited (ADR-002): `migrate` embeds the directory at compile time
-- and records a checksum per version, so a database that already ran 0001 simply
-- gains version 2. Sessions are durable because a restarted bridge must be able to
-- `session/resume` the work it already began instead of starting a fresh
-- conversation with the agent.
--
-- Conventions follow 0001: STRICT tables, TEXT + CHECK for enum-like columns,
-- INTEGER for counters, fixed-width UTC nanosecond RFC 3339 timestamps (so lexical
-- order is chronological order), and no PRAGMA statements (they belong to the
-- connection options in `storage::connect`).
--
-- Foreign keys to `agents` and `conversations` mean the assembly order must be
-- migrate -> sync_agents -> adapter: a session can only be recorded for an endpoint
-- the configuration snapshot already holds.

CREATE TABLE sessions (
    session_id       TEXT    NOT NULL,   -- opaque id issued by the agent
    endpoint_id      TEXT    NOT NULL REFERENCES agents (endpoint_id),
    conversation_id  TEXT    NOT NULL REFERENCES conversations (conversation_id),
    cwd              TEXT    NOT NULL,
    backend_id       TEXT    NOT NULL,   -- ADR-001: agent/name@version
    protocol_version INTEGER NOT NULL CHECK (protocol_version >= 1),
    state            TEXT    NOT NULL CHECK (state IN ('ready', 'prompting', 'closed')),
    created_at       TEXT    NOT NULL,
    updated_at       TEXT    NOT NULL,
    -- The agent's session id is only unique within its own backend.
    PRIMARY KEY (session_id, endpoint_id)
) STRICT;

-- At most one live session per (endpoint, conversation, cwd): this is what makes
-- "resume the live session" a deterministic choice with no second candidate.
-- Closed rows are excluded, so a finished session stays as history without
-- blocking a later one (the same partial-index technique 0001 uses for external
-- conversation references).
CREATE UNIQUE INDEX sessions_live
    ON sessions (endpoint_id, conversation_id, cwd)
    WHERE state <> 'closed';

CREATE INDEX sessions_conversation ON sessions (conversation_id);
