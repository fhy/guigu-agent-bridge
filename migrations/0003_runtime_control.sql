-- T016 runtime control: fenced execution leases and durable continuation turns.
CREATE TABLE execution_leases (
    resource_key TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    owner_token TEXT NOT NULL,
    fence INTEGER NOT NULL CHECK (fence >= 1),
    state TEXT NOT NULL CHECK (state IN ('active', 'released', 'recovery_needed')),
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
) STRICT;

CREATE INDEX execution_leases_state_expires ON execution_leases(state, expires_at);

CREATE TABLE task_continuations (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES tasks(task_id),
    resource_key TEXT NOT NULL REFERENCES execution_leases(resource_key),
    delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
    lease_fence INTEGER NOT NULL CHECK (lease_fence >= 1),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    state TEXT NOT NULL CHECK (state IN ('ready', 'in_flight', 'recovery_needed', 'blocked', 'terminal')),
    next_turn INTEGER NOT NULL CHECK (next_turn >= 1),
    completed_turns INTEGER NOT NULL CHECK (completed_turns >= 0),
    consecutive_no_progress INTEGER NOT NULL CHECK (consecutive_no_progress >= 0),
    next_prompt TEXT NOT NULL,
    started_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    last_progress_at TEXT NOT NULL,
    observed_output_bytes INTEGER NOT NULL CHECK (observed_output_bytes >= 0)
) STRICT;

CREATE INDEX task_continuations_state ON task_continuations(state);
