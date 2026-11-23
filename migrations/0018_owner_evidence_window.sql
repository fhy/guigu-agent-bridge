ALTER TABLE runtime_owner_evidence ADD COLUMN max_age_seconds INTEGER NOT NULL DEFAULT 30 CHECK (max_age_seconds BETWEEN 1 AND 300);
CREATE UNIQUE INDEX runtime_owner_evidence_one_current ON runtime_owner_evidence(instance_token) WHERE is_current = 1;
