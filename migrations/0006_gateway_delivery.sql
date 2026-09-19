CREATE TABLE gateway_envelopes(
 envelope_id TEXT PRIMARY KEY NOT NULL, version TEXT NOT NULL, direction TEXT NOT NULL,
 peer_id TEXT NOT NULL, sender_user_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 sender_endpoint TEXT NOT NULL, recipient_endpoint TEXT NOT NULL, conversation_id TEXT NOT NULL,
 correlation_id TEXT NOT NULL, internal_task_id TEXT REFERENCES tasks(task_id), kind TEXT NOT NULL, canonical_json BLOB NOT NULL,
 payload_sha256 TEXT NOT NULL, created_at TEXT NOT NULL, deadline TEXT,
 route_generation INTEGER NOT NULL, state TEXT NOT NULL, owner_runtime TEXT,
 owner_revision INTEGER NOT NULL DEFAULT 0, cleanup_owner TEXT, cleanup_revision INTEGER NOT NULL DEFAULT 0,
 cleanup_claimed_at TEXT, terminal_at TEXT, retained_bytes INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX gateway_envelopes_idem ON gateway_envelopes(peer_id,sender_user_id,direction,idempotency_key);
CREATE TABLE gateway_deliveries(
 envelope_id TEXT NOT NULL REFERENCES gateway_envelopes(envelope_id), direction TEXT NOT NULL,
 transport TEXT NOT NULL, phase TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
 txn_id TEXT, event_id TEXT, room_id TEXT NOT NULL, thread_root TEXT, last_error TEXT,
 next_attempt_at TEXT, owner_runtime TEXT, owner_revision INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(envelope_id,direction,transport)
);
CREATE TABLE gateway_artifacts(
 artifact_id TEXT PRIMARY KEY NOT NULL, envelope_id TEXT NOT NULL REFERENCES gateway_envelopes(envelope_id),
 media_type TEXT NOT NULL, byte_len INTEGER NOT NULL, sha256 TEXT NOT NULL,
 inline_bytes BLOB NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, terminal_at TEXT
);
CREATE INDEX gateway_deliveries_due ON gateway_deliveries(phase,next_attempt_at);
CREATE INDEX gateway_envelopes_retention ON gateway_envelopes(state,terminal_at);
