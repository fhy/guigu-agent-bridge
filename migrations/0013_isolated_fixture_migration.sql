CREATE TABLE isolated_fixture_migration_receipts (
    operation_id TEXT PRIMARY KEY NOT NULL,
    manifest_digest TEXT NOT NULL UNIQUE,
    pre_state_digest TEXT NOT NULL,
    post_state_digest TEXT NOT NULL,
    active_owner_digest TEXT NOT NULL,
    stopping_owner_digest TEXT NOT NULL,
    continuation_digest TEXT NOT NULL,
    receipt_digest TEXT NOT NULL,
    sibling_receipt_digest TEXT NOT NULL,
    commit_timestamp TEXT NOT NULL,
    result TEXT NOT NULL CHECK (result = 'migrated')
) STRICT;
