-- T018: durable admission, delivery uncertainty, projection replay and runtime epochs.
CREATE TABLE transport_receipts (
    transport TEXT NOT NULL,
    external_event_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    thread_ref TEXT,
    reply_event_id TEXT,
    conversation_id TEXT REFERENCES conversations(conversation_id),
    selected_endpoint_id TEXT REFERENCES agents(endpoint_id),
    task_id TEXT REFERENCES tasks(task_id),
    source_task_id TEXT REFERENCES tasks(task_id),
    retry_task_id TEXT REFERENCES tasks(task_id),
    receipt_kind TEXT NOT NULL CHECK (receipt_kind IN ('ordinary','admin')),
    route_kind TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('reserved','admitted','rejected')),
    result_code TEXT NOT NULL,
    admin_actor TEXT,
    command_reply_body TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (transport, external_event_id),
    CHECK (command_reply_body IS NULL OR length(CAST(command_reply_body AS BLOB)) <= 2048)
) STRICT;

CREATE TABLE runtime_instances (
    instance_token TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active','stopping','stopped')),
    process_fingerprint TEXT NOT NULL
) STRICT;
CREATE UNIQUE INDEX runtime_instances_one_active
    ON runtime_instances(state) WHERE state = 'active';

CREATE TABLE task_admissions (
    task_id TEXT PRIMARY KEY REFERENCES tasks(task_id),
    state TEXT NOT NULL CHECK (state IN ('ready','enqueued','dispatching','running','terminal','recovery_needed')),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    runtime_instance TEXT REFERENCES runtime_instances(instance_token),
    reply_room TEXT,
    reply_thread_root TEXT,
    reply_event_id TEXT,
    monitor_room TEXT,
    monitor_generation INTEGER,
    render_version TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE delivery_dispositions (
    delivery_id TEXT PRIMARY KEY REFERENCES deliveries(delivery_id),
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    state TEXT NOT NULL CHECK (state IN ('prepared','acknowledged','outcome_unknown','terminal')),
    session_id TEXT,
    child_fingerprint TEXT,
    reap_status TEXT,
    reap_at TEXT,
    reason_code TEXT,
    UNIQUE(task_id, attempt)
) STRICT;

CREATE TABLE projection_outbox (
    source_kind TEXT NOT NULL CHECK (source_kind IN ('task_event','transport_receipt')),
    source_id TEXT NOT NULL,
    projection TEXT NOT NULL CHECK (projection IN ('terminal_reply','observer','command_reply')),
    transport TEXT,
    stable_txn_id TEXT NOT NULL UNIQUE,
    room_id TEXT NOT NULL,
    thread_root TEXT,
    reply_event_id TEXT,
    monitor_generation INTEGER,
    render_version TEXT NOT NULL,
    body TEXT NOT NULL,
    body_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','sending','sent','abandoned')),
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    claim_owner TEXT,
    claim_revision INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (source_kind, source_id, projection),
    CHECK (length(CAST(body AS BLOB)) <= 2048)
) STRICT;

CREATE INDEX task_admissions_state ON task_admissions(state, updated_at);
CREATE INDEX delivery_dispositions_task ON delivery_dispositions(task_id, attempt);
CREATE INDEX projection_outbox_pending ON projection_outbox(state, updated_at);
