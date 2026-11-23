CREATE TABLE matrix_cutover_required (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    operation_id TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE matrix_cutover (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    operation_id TEXT NOT NULL UNIQUE,
    binding TEXT NOT NULL,
    archive_digest TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('pending', 'active')),
    baseline TEXT,
    baseline_digest TEXT,
    watermark TEXT,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    CHECK ((phase = 'pending' AND baseline IS NULL AND baseline_digest IS NULL AND watermark IS NULL)
        OR (phase = 'active' AND length(baseline) > 0 AND length(baseline_digest) = 64 AND length(watermark) > 0))
) STRICT;

CREATE TABLE matrix_cutover_dispositions (
    operation_id TEXT NOT NULL REFERENCES matrix_cutover(operation_id),
    room_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('processing', 'complete', 'manual_required')),
    PRIMARY KEY (operation_id, room_id, event_id)
) STRICT;
