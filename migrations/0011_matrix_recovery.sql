-- R080: Bridge-owned cutover, batch coverage, and component fencing.
CREATE TABLE matrix_recovery_control (
    control_id INTEGER PRIMARY KEY CHECK (control_id = 1),
    cutover_operation_id TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('baseline', 'active', 'refused')),
    watermark TEXT,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE matrix_recovery_batches (
    batch_id TEXT PRIMARY KEY,
    request_token TEXT,
    response_token TEXT NOT NULL,
    response_digest TEXT NOT NULL,
    expected_events_json TEXT NOT NULL,
    covered_events_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'processing', 'terminal', 'unknown', 'refused')),
    refusal_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE matrix_recovery_receipts (
    account_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    cutover_operation_id TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'processing', 'dispositioned', 'unknown', 'refused')),
    batch_id TEXT NOT NULL REFERENCES matrix_recovery_batches(batch_id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (account_id, room_id, event_id, cutover_operation_id)
) STRICT;

CREATE TABLE matrix_component_leases (
    component TEXT PRIMARY KEY,
    owner_token TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    state TEXT NOT NULL CHECK (state IN ('active', 'expired', 'unknown')),
    expires_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;
