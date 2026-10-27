CREATE TABLE matrix_recovery_compatibility (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    decision TEXT NOT NULL CHECK (decision IN ('native', 'held')),
    source_schema_version INTEGER NOT NULL,
    source_state_digest TEXT NOT NULL,
    legacy_operation_id TEXT,
    legacy_phase TEXT,
    legacy_baseline TEXT,
    legacy_baseline_digest TEXT,
    legacy_watermark TEXT,
    legacy_revision INTEGER,
    legacy_binding TEXT,
    legacy_archive_digest TEXT,
    native_operation_id TEXT,
    reason_code TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK ((decision = 'native' AND native_operation_id IS NOT NULL AND length(trim(native_operation_id)) > 0)
        OR (decision = 'held' AND native_operation_id IS NULL))
) STRICT;
