-- T096/T097: producer-owned runtime identity evidence.  Legacy rows without a
-- matching evidence row remain unsupported and fail closed.
CREATE TABLE runtime_owner_evidence (
    instance_token TEXT NOT NULL REFERENCES runtime_instances(instance_token),
    runtime_generation INTEGER NOT NULL CHECK (runtime_generation >= 1),
    evidence_revision INTEGER NOT NULL CHECK (evidence_revision >= 1),
    state TEXT NOT NULL CHECK (state IN ('observing','live','scan_pending','unknown','changed','reaped')),
    pid INTEGER NOT NULL CHECK (pid > 0),
    proc_starttime INTEGER NOT NULL CHECK (proc_starttime >= 0),
    process_fingerprint TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    boottime_ns INTEGER NOT NULL CHECK (boottime_ns >= 0),
    captured_at TEXT NOT NULL,
    barrier_epoch INTEGER NOT NULL CHECK (barrier_epoch >= 0),
    barrier_state TEXT NOT NULL CHECK (barrier_state IN ('open','closing','closed_joined')),
    in_flight_spawns INTEGER NOT NULL CHECK (in_flight_spawns >= 0),
    child_scan_state TEXT NOT NULL CHECK (child_scan_state IN ('complete_zero_escaped','complete_with_listed_members','overflow','inaccessible','raced_changed')),
    child_snapshot TEXT NOT NULL,
    is_current INTEGER NOT NULL CHECK (is_current IN (0,1)),
    PRIMARY KEY (instance_token, runtime_generation, evidence_revision)
) STRICT;
CREATE UNIQUE INDEX runtime_owner_evidence_current
    ON runtime_owner_evidence(instance_token, runtime_generation) WHERE is_current = 1;
