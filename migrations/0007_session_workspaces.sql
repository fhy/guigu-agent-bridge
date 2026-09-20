ALTER TABLE sessions ADD COLUMN additional_directories TEXT NOT NULL DEFAULT '[]';
ALTER TABLE sessions ADD COLUMN compatibility_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN compatibility_hash TEXT NOT NULL DEFAULT '';

CREATE TABLE workspace_claims (
    claim_id TEXT NOT NULL PRIMARY KEY,
    runtime_owner TEXT NOT NULL,
    task_id TEXT NOT NULL,
    canonical_path TEXT NOT NULL,
    path_components_json TEXT NOT NULL,
    owner_fence INTEGER NOT NULL CHECK (owner_fence >= 1),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    state TEXT NOT NULL CHECK (state IN ('active', 'recovery_needed')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;
CREATE INDEX workspace_claims_active ON workspace_claims (state, runtime_owner, task_id);
