CREATE TABLE offline_recovery_binding (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    protocol INTEGER NOT NULL CHECK (protocol = 2),
    db_generation TEXT NOT NULL UNIQUE,
    canonical_db_digest TEXT NOT NULL,
    root_digest TEXT NOT NULL,
    admin_operation_id TEXT NOT NULL,
    schema_intent_digest TEXT NOT NULL,
    schema_complete_digest TEXT,
    plan_digest TEXT,
    identity_digest TEXT,
    classification_digest TEXT,
    transition_intent_digest TEXT,
    transition_complete_digest TEXT,
    native_operation_id TEXT UNIQUE,
    phase TEXT NOT NULL CHECK (phase IN (
        'schema_pending', 'schema_ready', 'identity_pending', 'identity_ready',
        'transition_pending', 'baseline_ready', 'baseline_inflight',
        'activation_prepared', 'active', 'refused'
    )),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    CHECK (phase IN ('schema_pending', 'refused') OR schema_complete_digest IS NOT NULL),
    CHECK (phase NOT IN ('identity_ready', 'transition_pending', 'baseline_ready',
        'baseline_inflight', 'activation_prepared', 'active') OR identity_digest IS NOT NULL),
    CHECK (phase NOT IN ('transition_pending', 'baseline_ready', 'baseline_inflight',
        'activation_prepared', 'active') OR (
        plan_digest IS NOT NULL AND classification_digest IS NOT NULL
        AND transition_intent_digest IS NOT NULL AND native_operation_id IS NOT NULL
    )),
    CHECK (phase NOT IN ('baseline_ready', 'baseline_inflight', 'activation_prepared', 'active')
        OR transition_complete_digest IS NOT NULL)
) STRICT;

CREATE TABLE runtime_eligibility_bootstrap (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    state TEXT NOT NULL CHECK (state IN ('pending', 'complete', 'admin_reserved')),
    bootstrap_nonce TEXT NOT NULL UNIQUE,
    generation TEXT UNIQUE,
    inventory_digest TEXT,
    CHECK (state != 'complete' OR (
        bootstrap_nonce IS NOT NULL AND generation IS NOT NULL AND inventory_digest IS NOT NULL
    ))
) STRICT;

INSERT INTO runtime_eligibility_bootstrap(singleton, state, bootstrap_nonce)
VALUES (1, 'pending', lower(hex(randomblob(16))));

CREATE TABLE runtime_eligibility_context (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    generation TEXT NOT NULL UNIQUE,
    bootstrap_nonce TEXT NOT NULL UNIQUE,
    provenance_kind TEXT NOT NULL CHECK (provenance_kind IN ('pristine_local', 'validated_native')),
    compatibility_operation_id TEXT,
    compatibility_source_digest TEXT,
    matrix_phase TEXT NOT NULL CHECK (matrix_phase IN ('absent', 'baseline', 'active', 'refused')),
    expected_b_digest TEXT,
    state TEXT NOT NULL CHECK (state IN ('ready', 'held')),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    created_at TEXT NOT NULL,
    bootstrap_inventory_digest TEXT NOT NULL,
    CHECK ((provenance_kind = 'pristine_local' AND compatibility_operation_id IS NULL
        AND compatibility_source_digest IS NULL AND matrix_phase = 'absent'
        AND expected_b_digest IS NULL) OR (provenance_kind = 'validated_native'
        AND compatibility_operation_id IS NOT NULL AND compatibility_source_digest IS NOT NULL)),
    CHECK (matrix_phase != 'active' OR expected_b_digest IS NOT NULL),
    CHECK (matrix_phase NOT IN ('absent', 'baseline') OR expected_b_digest IS NULL)
) STRICT;

CREATE TABLE offline_recovery_rows (
    table_name TEXT NOT NULL,
    canonical_pk TEXT NOT NULL,
    origin_generation TEXT NOT NULL,
    original_revision TEXT NOT NULL,
    original_digest TEXT NOT NULL,
    parent_relations TEXT NOT NULL CHECK (json_valid(parent_relations)),
    disposition TEXT NOT NULL CHECK (disposition IN (
        'eligible_new', 'terminal_evidenced', 'quarantined_no_effect', 'uncertain_hold', 'refused_hold'
    )),
    evidence_digest TEXT NOT NULL,
    context_kind TEXT NOT NULL CHECK (context_kind IN ('admin', 'nonadmin')),
    context_generation TEXT,
    admin_operation_id TEXT,
    plan_digest TEXT,
    matrix_operation_id TEXT,
    PRIMARY KEY (table_name, canonical_pk),
    FOREIGN KEY (context_generation) REFERENCES runtime_eligibility_context(generation)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK ((context_kind = 'admin' AND admin_operation_id IS NOT NULL AND plan_digest IS NOT NULL
        AND context_generation IS NULL) OR (context_kind = 'nonadmin'
        AND context_generation IS NOT NULL AND admin_operation_id IS NULL AND plan_digest IS NULL)),
    CHECK (context_kind != 'nonadmin' OR origin_generation = context_generation)
) STRICT;

CREATE TABLE matrix_activation_intent (
    native_operation_id TEXT PRIMARY KEY,
    plan_digest TEXT NOT NULL,
    identity_digest TEXT NOT NULL,
    attempt_id TEXT NOT NULL UNIQUE,
    component_owner TEXT NOT NULL,
    component_epoch INTEGER NOT NULL CHECK (component_epoch >= 0),
    request_digest TEXT NOT NULL,
    response_digest TEXT,
    response_token TEXT,
    state TEXT NOT NULL CHECK (state IN ('inflight', 'prepared', 'committed', 'refused')),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    created_at TEXT NOT NULL,
    response_at TEXT,
    FOREIGN KEY (native_operation_id) REFERENCES offline_recovery_binding(native_operation_id),
    CHECK (state NOT IN ('prepared', 'committed') OR (
        response_digest IS NOT NULL AND response_token IS NOT NULL AND response_at IS NOT NULL
    )),
    CHECK (state != 'inflight' OR (
        response_digest IS NULL AND response_token IS NULL AND response_at IS NULL
    ))
) STRICT;

CREATE TRIGGER offline_recovery_binding_immutable
BEFORE UPDATE ON offline_recovery_binding
WHEN NEW.singleton != OLD.singleton OR NEW.protocol != OLD.protocol
    OR NEW.db_generation != OLD.db_generation
    OR NEW.canonical_db_digest != OLD.canonical_db_digest
    OR NEW.root_digest != OLD.root_digest
    OR NEW.admin_operation_id != OLD.admin_operation_id
    OR NEW.schema_intent_digest != OLD.schema_intent_digest
    OR NEW.revision != OLD.revision + 1
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-binding-fence');
END;

CREATE TRIGGER offline_recovery_admin_excludes_local
BEFORE INSERT ON offline_recovery_binding
WHEN EXISTS (SELECT 1 FROM runtime_eligibility_context)
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-mixed-authority');
END;

CREATE TRIGGER offline_recovery_local_excludes_admin
BEFORE INSERT ON runtime_eligibility_context
WHEN EXISTS (SELECT 1 FROM offline_recovery_binding)
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-mixed-authority');
END;
