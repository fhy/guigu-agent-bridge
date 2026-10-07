CREATE TABLE isolated_owner_binding_remediation_witness (
    singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
    contract_version INTEGER NOT NULL CHECK (contract_version = 1),
    operation_id TEXT NOT NULL UNIQUE CHECK (length(operation_id) = 36),
    backup_snapshot_sha256 TEXT NOT NULL CHECK (
        length(backup_snapshot_sha256) = 64
        AND backup_snapshot_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    backup_source_set_sha256 TEXT NOT NULL CHECK (
        length(backup_source_set_sha256) = 64
        AND backup_source_set_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    migration_version INTEGER NOT NULL CHECK (migration_version = 14),
    migration_checksum_digest TEXT NOT NULL CHECK (
        length(migration_checksum_digest) = 64
        AND migration_checksum_digest NOT GLOB '*[^0-9a-f]*'
    ),
    retained_pre0014_business_digest TEXT NOT NULL CHECK (
        length(retained_pre0014_business_digest) = 64
        AND retained_pre0014_business_digest NOT GLOB '*[^0-9a-f]*'
    ),
    migrated_pre_repair_business_digest TEXT NOT NULL CHECK (
        length(migrated_pre_repair_business_digest) = 64
        AND migrated_pre_repair_business_digest NOT GLOB '*[^0-9a-f]*'
    ),
    witness_digest TEXT NOT NULL UNIQUE CHECK (
        length(witness_digest) = 64
        AND witness_digest NOT GLOB '*[^0-9a-f]*'
    )
) STRICT;

CREATE TABLE isolated_owner_binding_remediation_receipts (
    operation_id TEXT PRIMARY KEY NOT NULL CHECK (length(operation_id) = 36),
    contract_version INTEGER NOT NULL CHECK (contract_version = 1),
    backup_snapshot_sha256 TEXT NOT NULL CHECK (
        length(backup_snapshot_sha256) = 64
        AND backup_snapshot_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    backup_source_set_sha256 TEXT NOT NULL CHECK (
        length(backup_source_set_sha256) = 64
        AND backup_source_set_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    pre_state_digest TEXT NOT NULL CHECK (length(pre_state_digest) = 64),
    post_state_digest TEXT NOT NULL CHECK (length(post_state_digest) = 64),
    active_owner_digest TEXT NOT NULL CHECK (length(active_owner_digest) = 64),
    stopping_owner_digest TEXT NOT NULL CHECK (length(stopping_owner_digest) = 64),
    continuation_digest TEXT NOT NULL CHECK (length(continuation_digest) = 64),
    lease_before_digest TEXT NOT NULL CHECK (length(lease_before_digest) = 64),
    lease_after_digest TEXT NOT NULL CHECK (length(lease_after_digest) = 64),
    admission_before_digest TEXT NOT NULL CHECK (length(admission_before_digest) = 64),
    admission_after_digest TEXT NOT NULL CHECK (length(admission_after_digest) = 64),
    sibling_receipt_digest TEXT NOT NULL CHECK (length(sibling_receipt_digest) = 64),
    receipt_digest TEXT NOT NULL UNIQUE CHECK (length(receipt_digest) = 64),
    commit_timestamp TEXT NOT NULL CHECK (length(commit_timestamp) = 30),
    result TEXT NOT NULL CHECK (result = 'remediated'),
    UNIQUE (backup_snapshot_sha256, backup_source_set_sha256)
) STRICT;
