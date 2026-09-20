CREATE TABLE workflow_envelopes (
    transport TEXT NOT NULL,
    external_event_id TEXT NOT NULL,
    sender_endpoint_id TEXT NOT NULL,
    target_endpoint_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    schema TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    outcome TEXT,
    body_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (transport, external_event_id),
    UNIQUE (sender_endpoint_id, idempotency_key),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

CREATE INDEX workflow_envelopes_task_correlation
    ON workflow_envelopes(task_id, correlation_id, created_at);
