CREATE TABLE agent_queue_counters (
    target_endpoint_id TEXT NOT NULL PRIMARY KEY REFERENCES agents(endpoint_id),
    next_sequence INTEGER NOT NULL CHECK (next_sequence >= 1),
    capacity INTEGER NOT NULL CHECK (capacity >= 1),
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE agent_work_queue (
    queue_id TEXT NOT NULL PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
    envelope_id TEXT,
    target_endpoint_id TEXT NOT NULL REFERENCES agents(endpoint_id),
    room_id TEXT NOT NULL,
    thread_root TEXT,
    sender_endpoint_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    body_hash TEXT NOT NULL,
    lane TEXT NOT NULL CHECK (lane IN ('ordinary','control')),
    state TEXT NOT NULL CHECK (state IN ('queued','claimed','running','paused','completed','expired','superseded','recovery_needed')),
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    expires_at TEXT,
    claimed_at TEXT,
    send_started INTEGER NOT NULL DEFAULT 0 CHECK (send_started IN (0,1)),
    runtime_owner TEXT,
    owner_fence INTEGER,
    reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (target_endpoint_id, idempotency_key),
    UNIQUE (target_endpoint_id, sequence)
) STRICT;

CREATE INDEX agent_work_queue_ready ON agent_work_queue(target_endpoint_id, lane, state, sequence);
