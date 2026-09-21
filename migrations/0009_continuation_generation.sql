-- T023 additive continuation facts: runtime generation and response receipts.
ALTER TABLE task_continuations ADD COLUMN runtime_generation TEXT NOT NULL DEFAULT 'generation.v1:strict';
ALTER TABLE task_continuations ADD COLUMN runtime_generation_version INTEGER NOT NULL DEFAULT 1;

CREATE TABLE continuation_response_receipts (
    idempotency_key TEXT NOT NULL PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
    resource_key TEXT NOT NULL,
    lease_fence INTEGER NOT NULL CHECK (lease_fence >= 1),
    continuation_revision INTEGER NOT NULL CHECK (continuation_revision >= 1),
    response_hash TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('compatibility_end_turn','structured','protocol_failure')),
    output_bytes INTEGER NOT NULL CHECK (output_bytes >= 0),
    runtime_generation TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

CREATE INDEX continuation_receipts_task ON continuation_response_receipts(task_id, continuation_revision);
