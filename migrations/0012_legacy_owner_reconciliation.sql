CREATE TABLE legacy_owner_reconciliation_receipts (
    operation_id TEXT PRIMARY KEY NOT NULL,
    manifest_digest TEXT NOT NULL UNIQUE,
    pre_owner_state_digest TEXT NOT NULL,
    post_owner_state_digest TEXT NOT NULL,
    receipt_digest TEXT NOT NULL,
    receipt_sibling_digest TEXT NOT NULL,
    commit_heartbeat TEXT NOT NULL,
    result TEXT NOT NULL CHECK (result = 'recovered')
) STRICT;
