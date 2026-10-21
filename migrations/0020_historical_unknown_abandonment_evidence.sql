ALTER TABLE historical_unknown_abandonments ADD COLUMN session_id_digest TEXT;
ALTER TABLE historical_unknown_abandonments ADD COLUMN before_state_digest TEXT;
ALTER TABLE historical_unknown_abandonments ADD COLUMN post_state_digest TEXT;
ALTER TABLE historical_unknown_abandonments ADD COLUMN abandonment_kind TEXT NOT NULL DEFAULT 'manual_abandonment' CHECK (abandonment_kind IN ('manual_abandonment','historical_unknown_abandoned'));
ALTER TABLE historical_unknown_abandonments ADD COLUMN termination_proven INTEGER NOT NULL DEFAULT 0 CHECK (termination_proven=0);
