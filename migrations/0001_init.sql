-- T008 migration 0001: initial event-store schema.
--
-- IMMUTABILITY (ADR-002). This file is embedded into the binary at compile time
-- and its checksum is recorded in `_sqlx_migrations`. Once released it MUST NOT
-- be edited or reordered: sqlx re-runs a migration whose file content no longer
-- matches the recorded checksum and fails with `VersionMismatch`, and every
-- schema change must therefore be appended as `0002_*.sql`, `0003_*.sql`, ...
--
-- Conventions (design gate D1-D13, analysis §4):
--   * every table is STRICT (SQLite >= 3.37), so only INT/INTEGER/REAL/TEXT/BLOB/ANY
--     are legal column types: booleans are INTEGER constrained to 0/1, enums are
--     TEXT constrained to their snake_case values, `u64`/`u32` are INTEGER;
--   * every JSON column carries `CHECK (json_valid(...))`;
--   * no PRAGMA statement appears here: `PRAGMA foreign_keys` is a no-op inside a
--     transaction and `journal_mode` is a persistent database property, both set
--     per connection by `storage::connect`;
--   * write order required by the foreign keys (documented on `storage`):
--     agents -> conversations -> messages -> tasks -> task_events/deliveries.

-- agents: snapshot of the endpoints declared in configuration. The primary key is
-- the ADR-003 deterministic UUIDv5 identity; `agent_id` keeps the derivation input
-- (UUIDv5 is not reversible) and is the key the startup sync upserts on.
CREATE TABLE agents (
    endpoint_id       TEXT    NOT NULL PRIMARY KEY,
    agent_id          TEXT    NOT NULL UNIQUE,
    transport         TEXT    NOT NULL CHECK (transport IN ('acp', 'matrix', 'http')),
    enabled           INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    address_json      TEXT,
    capabilities_json TEXT    NOT NULL,
    -- NULL address = declared but not addressable (matrix/http in v1). When an
    -- address is present its JSON tag must match the declared transport.
    CHECK (address_json IS NULL OR (json_valid(address_json) AND CASE transport
        WHEN 'acp'    THEN json_extract(address_json, '$.acp')    IS NOT NULL
        WHEN 'matrix' THEN json_extract(address_json, '$.matrix') IS NOT NULL
        ELSE               json_extract(address_json, '$.http')   IS NOT NULL END)),
    CHECK (json_valid(capabilities_json) AND json_type(capabilities_json) = 'array')
) STRICT;

-- conversations: a persistent context, optionally linked to an external room /
-- thread / session. Participants stay a JSON array: only JSON preserves both
-- element order and duplicates.
CREATE TABLE conversations (
    conversation_id   TEXT NOT NULL PRIMARY KEY,
    transport         TEXT CHECK (transport IS NULL OR transport IN ('acp', 'matrix', 'http')),
    external_id       TEXT,
    thread_ref        TEXT,
    participants_json TEXT NOT NULL,
    -- An external reference is all-or-nothing, and a thread ref requires one.
    -- The three CHECKs below therefore admit exactly three row shapes:
    --   local             (transport, external_id, thread_ref all NULL),
    --   external, no thread (both set, thread_ref NULL),
    --   external, threaded  (all three set).
    CHECK ((transport IS NULL) = (external_id IS NULL)),
    CHECK (transport IS NOT NULL OR thread_ref IS NULL),
    CHECK (json_valid(participants_json) AND json_type(participants_json) = 'array')
) STRICT;

-- An external reference must resolve to AT MOST ONE conversation, because
-- `Repository::conversation_by_external_ref` returns `Option<Conversation>`.
-- Without this, a replayed or concurrent first message would create a second
-- conversation for the same room/thread and T011 could no longer decide which
-- one owns later messages.
--
-- Two UNIQUE *partial* indexes are required; a single three-column UNIQUE
-- constraint cannot express the rule, because SQLite considers NULL values
-- distinct, so rows with `thread_ref IS NULL` would never collide.
--   * non-threaded: one conversation per (transport, external_id);
--   * threaded: one per (transport, external_id, thread_ref), which keeps a
--     room distinct from each of its threads.
-- The `transport IS NOT NULL` guard keeps local conversations (all three
-- columns NULL) out of the constraint entirely: many of them must coexist, and
-- an external reference is never partially NULL (see the table CHECKs).
CREATE UNIQUE INDEX conversations_external_ref_unthreaded
    ON conversations (transport, external_id)
    WHERE thread_ref IS NULL AND transport IS NOT NULL;

CREATE UNIQUE INDEX conversations_external_ref_threaded
    ON conversations (transport, external_id, thread_ref)
    WHERE thread_ref IS NOT NULL;

-- messages: one sender, one recipient (no broadcast; see INC-001).
CREATE TABLE messages (
    message_id      TEXT NOT NULL PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations (conversation_id),
    sender          TEXT NOT NULL,
    recipient       TEXT NOT NULL,
    body            TEXT NOT NULL,
    reply_to        TEXT REFERENCES messages (message_id),
    metadata_json   TEXT NOT NULL,
    CHECK (json_valid(metadata_json))
) STRICT;

CREATE INDEX messages_conversation ON messages (conversation_id, message_id);
CREATE INDEX messages_reply_to     ON messages (reply_to);

-- tasks: immutable task rows. There is deliberately NO `status` column - the
-- current status is derived from the latest `task_events` row (event sourcing).
CREATE TABLE tasks (
    task_id         TEXT    NOT NULL PRIMARY KEY,
    root_task_id    TEXT    NOT NULL REFERENCES tasks (task_id),
    parent_task_id  TEXT             REFERENCES tasks (task_id),
    from_agent      TEXT    NOT NULL,
    to_agent        TEXT    NOT NULL,
    conversation_id TEXT    NOT NULL REFERENCES conversations (conversation_id),
    reply_to        TEXT             REFERENCES messages (message_id),
    text            TEXT    NOT NULL,
    priority        INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 10),
    depth           INTEGER NOT NULL CHECK (depth >= 0),
    hops            INTEGER NOT NULL CHECK (hops >= 0),
    deadline        TEXT,
    version         INTEGER NOT NULL CHECK (version >= 0)
) STRICT;

CREATE INDEX tasks_root         ON tasks (root_task_id);
CREATE INDEX tasks_parent       ON tasks (parent_task_id);
CREATE INDEX tasks_conversation ON tasks (conversation_id);
CREATE INDEX tasks_reply_to     ON tasks (reply_to);

-- task_events: the append-only, immutable event log and the single source of
-- truth. `event_id` deduplicates globally; `(task_id, seq)` pins per-task order
-- and also serves as the index for the `task_id` foreign key.
CREATE TABLE task_events (
    event_id  TEXT    NOT NULL PRIMARY KEY,
    task_id   TEXT    NOT NULL REFERENCES tasks (task_id),
    seq       INTEGER NOT NULL CHECK (seq >= 1),
    status    TEXT    NOT NULL CHECK (status IN
                  ('queued', 'dispatched', 'running', 'completed', 'failed', 'timed_out', 'cancelled')),
    timestamp TEXT    NOT NULL,
    payload   TEXT    NOT NULL,
    UNIQUE (task_id, seq),
    CHECK (json_valid(payload))
) STRICT;

-- deliveries: dispatch and acknowledgement recorded separately (ARCHITECTURE §6);
-- no terminal-outcome column, because the outcome already lives in `task_events`.
CREATE TABLE deliveries (
    delivery_id        TEXT    NOT NULL PRIMARY KEY,
    task_id            TEXT    NOT NULL REFERENCES tasks (task_id),
    attempt            INTEGER NOT NULL CHECK (attempt >= 1),
    target_endpoint_id TEXT    NOT NULL REFERENCES agents (endpoint_id),
    dispatched_at      TEXT    NOT NULL,
    acknowledged_at    TEXT,
    UNIQUE (task_id, attempt),
    CHECK (acknowledged_at IS NULL OR acknowledged_at >= dispatched_at)
) STRICT;

CREATE INDEX deliveries_target ON deliveries (target_endpoint_id);
