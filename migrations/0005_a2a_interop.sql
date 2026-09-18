-- T019: adapter-owned A2A v0.3 identity, content and fenced cleanup facts.
CREATE TABLE a2a_exchanges (
    exchange_id TEXT PRIMARY KEY,
    peer_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('inbound','outbound')),
    request_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    external_task_id TEXT,
    external_context_id TEXT,
    internal_task_id TEXT REFERENCES tasks(task_id),
    state TEXT NOT NULL CHECK (state IN ('reserved','submitted','working','completed','failed','canceled','rejected','acceptance_unknown','recovery_needed')),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    content_bytes INTEGER NOT NULL CHECK (content_bytes >= 0),
    terminal_at TEXT,
    cleanup_state TEXT NOT NULL CHECK (cleanup_state IN ('pending','claimed','cleaned')),
    cleanup_owner TEXT REFERENCES runtime_instances(instance_token),
    cleanup_revision INTEGER NOT NULL CHECK (cleanup_revision >= 0),
    cleanup_claimed_at TEXT,
    content_cleaned_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(peer_id, direction, request_id),
    UNIQUE(peer_id, direction, external_task_id)
) STRICT;

CREATE TABLE a2a_messages (
    message_id TEXT PRIMARY KEY,
    exchange_id TEXT NOT NULL REFERENCES a2a_exchanges(exchange_id),
    external_message_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('user','agent')),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    created_at TEXT NOT NULL,
    UNIQUE(exchange_id, external_message_id),
    UNIQUE(exchange_id, ordinal)
) STRICT;

CREATE TABLE a2a_message_parts (
    message_id TEXT NOT NULL REFERENCES a2a_messages(message_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    kind TEXT NOT NULL CHECK (kind IN ('text','data','file_inline')),
    mime_type TEXT,
    text_value TEXT,
    json_value TEXT,
    blob_value BLOB,
    content_hash TEXT NOT NULL,
    PRIMARY KEY(message_id, ordinal)
) STRICT;

CREATE TABLE a2a_artifacts (
    artifact_id TEXT PRIMARY KEY,
    exchange_id TEXT NOT NULL REFERENCES a2a_exchanges(exchange_id),
    external_artifact_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    name TEXT,
    description TEXT,
    UNIQUE(exchange_id, external_artifact_id),
    UNIQUE(exchange_id, ordinal)
) STRICT;

CREATE TABLE a2a_artifact_parts (
    artifact_id TEXT NOT NULL REFERENCES a2a_artifacts(artifact_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    kind TEXT NOT NULL CHECK (kind IN ('text','data','file_inline')),
    mime_type TEXT,
    text_value TEXT,
    json_value TEXT,
    blob_value BLOB,
    content_hash TEXT NOT NULL,
    PRIMARY KEY(artifact_id, ordinal)
) STRICT;

CREATE INDEX a2a_exchange_cleanup
    ON a2a_exchanges(cleanup_state, terminal_at, created_at);
