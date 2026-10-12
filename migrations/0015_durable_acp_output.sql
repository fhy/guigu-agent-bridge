-- T051: durable ACP output sets, segments, and process-wide quota authority.
CREATE TABLE acp_output_quota (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    reserved_bytes INTEGER NOT NULL CHECK (reserved_bytes >= 0),
    landed_bytes INTEGER NOT NULL CHECK (landed_bytes >= 0),
    revision INTEGER NOT NULL CHECK (revision >= 0)
) STRICT;
INSERT INTO acp_output_quota(singleton,reserved_bytes,landed_bytes,revision)
VALUES(1,0,0,0);

CREATE TABLE acp_output_sets (
    set_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    turn INTEGER NOT NULL CHECK (turn >= 1),
    runtime_fence TEXT NOT NULL,
    room_id TEXT NOT NULL,
    thread_root TEXT,
    reply_event_id TEXT,
    render_version TEXT NOT NULL,
    total_bytes INTEGER,
    total_hash TEXT,
    segment_count INTEGER,
    acknowledged_cursor INTEGER NOT NULL DEFAULT -1 CHECK (acknowledged_cursor >= -1),
    state TEXT NOT NULL CHECK (state IN ('assembling','prepared','sealed','delivering','delivered','partial','abandoned','cleaning','cleaned','quarantined')),
    claim_owner TEXT,
    claim_revision INTEGER NOT NULL DEFAULT 0 CHECK (claim_revision >= 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    failure_class TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(task_id,attempt,turn),
    CHECK ((state='assembling' AND total_bytes IS NULL AND total_hash IS NULL AND segment_count IS NULL)
        OR (state<>'assembling' AND total_bytes IS NOT NULL AND total_hash IS NOT NULL AND segment_count IS NOT NULL)),
    CHECK (failure_class IS NULL OR failure_class IN ('permanent','attempts_exhausted','corrupt'))
) STRICT;

CREATE TABLE acp_output_segments (
    set_id TEXT NOT NULL REFERENCES acp_output_sets(set_id),
    segment_index INTEGER NOT NULL CHECK (segment_index >= 0),
    byte_start INTEGER NOT NULL CHECK (byte_start >= 0),
    byte_end INTEGER NOT NULL CHECK (byte_end >= byte_start),
    byte_length INTEGER NOT NULL CHECK (byte_length >= 0 AND byte_length = byte_end - byte_start),
    content_hash TEXT NOT NULL,
    relative_name TEXT NOT NULL,
    stable_txn_id TEXT,
    state TEXT NOT NULL CHECK (state IN ('reserved','landed','cleaned','quarantined')),
    reservation_owner TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(set_id,segment_index),
    UNIQUE(set_id,relative_name),
    UNIQUE(stable_txn_id)
) STRICT;

CREATE TABLE acp_output_delivery_segments (
    set_id TEXT NOT NULL REFERENCES acp_output_sets(set_id),
    delivery_index INTEGER NOT NULL CHECK (delivery_index >= 0),
    spool_index INTEGER NOT NULL,
    byte_start INTEGER NOT NULL CHECK (byte_start >= 0),
    byte_end INTEGER NOT NULL CHECK (byte_end >= byte_start),
    content_hash TEXT NOT NULL,
    stable_txn_id TEXT NOT NULL UNIQUE,
    PRIMARY KEY(set_id,delivery_index),
    FOREIGN KEY(set_id,spool_index) REFERENCES acp_output_segments(set_id,segment_index)
) STRICT;

CREATE INDEX acp_output_sets_drain
    ON acp_output_sets(state,updated_at,created_at);
CREATE INDEX acp_output_segments_state
    ON acp_output_segments(state,set_id,segment_index);
CREATE INDEX acp_output_delivery_order
    ON acp_output_delivery_segments(set_id,delivery_index);
