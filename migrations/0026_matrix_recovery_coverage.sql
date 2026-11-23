-- Partial coverage is never a terminal batch or a checkpoint.
CREATE TABLE matrix_recovery_coverage (
    coverage_id INTEGER PRIMARY KEY CHECK (coverage_id = 1),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    owner_token TEXT NOT NULL REFERENCES runtime_instances(instance_token),
    document TEXT NOT NULL CHECK (json_valid(document))
);
