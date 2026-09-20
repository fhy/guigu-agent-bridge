CREATE TABLE gateway_envelopes(
 envelope_id TEXT PRIMARY KEY NOT NULL, version TEXT NOT NULL,
 direction TEXT NOT NULL CHECK(direction IN ('inbound','outbound')),
 peer_id TEXT NOT NULL, sender_user_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 sender_endpoint TEXT NOT NULL, recipient_endpoint TEXT NOT NULL, conversation_id TEXT NOT NULL,
 correlation_id TEXT NOT NULL, internal_task_id TEXT REFERENCES tasks(task_id), kind TEXT NOT NULL, canonical_json BLOB NOT NULL,
 payload_sha256 TEXT NOT NULL, created_at TEXT NOT NULL, deadline TEXT,
 route_generation INTEGER NOT NULL CHECK(route_generation>=0),
 state TEXT NOT NULL CHECK(state IN ('received','acked','accepted','running','terminal','cancel_requested','stale','recovery_needed')),
 owner_runtime TEXT, owner_revision INTEGER NOT NULL DEFAULT 0 CHECK(owner_revision>=0),
 cleanup_owner TEXT, cleanup_revision INTEGER NOT NULL DEFAULT 0 CHECK(cleanup_revision>=0),
 cleanup_claimed_at TEXT, terminal_at TEXT,
 retained_bytes INTEGER NOT NULL DEFAULT 0 CHECK(retained_bytes>=0),
 CHECK(length(canonical_json)<=524288), CHECK(length(payload_sha256)=64)
);
CREATE UNIQUE INDEX gateway_envelopes_idem ON gateway_envelopes(peer_id,sender_user_id,direction,idempotency_key);
CREATE TABLE gateway_deliveries(
 envelope_id TEXT NOT NULL REFERENCES gateway_envelopes(envelope_id) ON DELETE CASCADE,
 direction TEXT NOT NULL CHECK(direction IN ('inbound','outbound')),
 transport TEXT NOT NULL CHECK(transport='matrix'),
 phase TEXT NOT NULL CHECK(phase IN ('pending','send_unknown','transport_acked','task_accepted','terminal','stale','recovery_needed')),
 attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt BETWEEN 0 AND 2),
 txn_id TEXT, event_id TEXT, room_id TEXT NOT NULL, thread_root TEXT, last_error TEXT,
 next_attempt_at TEXT, owner_runtime TEXT,
 owner_revision INTEGER NOT NULL DEFAULT 0 CHECK(owner_revision>=0),
 PRIMARY KEY(envelope_id,direction,transport),
 CHECK((direction='inbound' AND txn_id IS NULL AND event_id IS NOT NULL) OR (direction='outbound' AND txn_id IS NOT NULL)),
 CHECK(direction='inbound' OR event_id IS NULL OR phase IN ('transport_acked','task_accepted','terminal','stale')),
 CHECK(direction='inbound' OR phase IN ('pending','send_unknown','recovery_needed') OR event_id IS NOT NULL),
 CHECK(last_error IS NULL OR length(last_error)<=512)
);
CREATE TABLE gateway_artifacts(
 artifact_id TEXT PRIMARY KEY NOT NULL,
 envelope_id TEXT NOT NULL REFERENCES gateway_envelopes(envelope_id) ON DELETE CASCADE,
 media_type TEXT NOT NULL, byte_len INTEGER NOT NULL CHECK(byte_len BETWEEN 0 AND 262144),
 sha256 TEXT NOT NULL CHECK(length(sha256)=64),
 inline_bytes BLOB NOT NULL CHECK(length(inline_bytes) BETWEEN 0 AND 262144),
 state TEXT NOT NULL CHECK(state IN ('live','terminal','stale')),
 created_at TEXT NOT NULL, terminal_at TEXT, CHECK(byte_len=length(inline_bytes))
);
CREATE INDEX gateway_deliveries_due ON gateway_deliveries(phase,next_attempt_at);
CREATE INDEX gateway_deliveries_owner ON gateway_deliveries(owner_runtime,owner_revision);
CREATE INDEX gateway_envelopes_retention ON gateway_envelopes(state,terminal_at);
CREATE INDEX gateway_artifacts_retention ON gateway_artifacts(state,terminal_at);
