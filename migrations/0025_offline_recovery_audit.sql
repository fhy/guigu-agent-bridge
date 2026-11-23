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
    matrix_enabled INTEGER NOT NULL DEFAULT 0 CHECK (matrix_enabled IN (0, 1)),
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
    parent_relations TEXT NOT NULL CHECK (json_valid(parent_relations)
        AND json_type(parent_relations) = 'array'),
    disposition TEXT NOT NULL CHECK (disposition IN (
        'eligible_new', 'eligible_preserved', 'terminal_evidenced', 'quarantined_no_effect', 'uncertain_hold', 'refused_hold'
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
    CHECK (context_kind != 'nonadmin' OR origin_generation = context_generation),
    CHECK (disposition != 'eligible_preserved' OR context_kind = 'nonadmin')
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

CREATE TABLE offline_recovery_metadata_anchors (
    table_name TEXT NOT NULL CHECK (table_name IN ('agents', 'conversations')),
    canonical_pk TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role = 'metadata_anchor'),
    db_generation TEXT NOT NULL,
    root_digest TEXT NOT NULL,
    admin_operation_id TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    original_revision TEXT NOT NULL,
    original_digest TEXT NOT NULL,
    PRIMARY KEY (table_name, canonical_pk),
    FOREIGN KEY (table_name, canonical_pk)
        REFERENCES offline_recovery_rows(table_name, canonical_pk)
) STRICT;

CREATE TRIGGER offline_recovery_anchor_binding
BEFORE INSERT ON offline_recovery_metadata_anchors
WHEN NOT EXISTS (
    SELECT 1 FROM offline_recovery_binding AS binding
    JOIN offline_recovery_rows AS classified
        ON classified.table_name = NEW.table_name AND classified.canonical_pk = NEW.canonical_pk
    WHERE binding.singleton = 1 AND binding.phase = 'transition_pending'
        AND binding.db_generation = NEW.db_generation AND binding.root_digest = NEW.root_digest
        AND binding.admin_operation_id = NEW.admin_operation_id AND binding.plan_digest = NEW.plan_digest
        AND classified.context_kind = 'admin' AND classified.origin_generation = binding.native_operation_id
        AND classified.matrix_operation_id = binding.native_operation_id
        AND classified.admin_operation_id = NEW.admin_operation_id AND classified.plan_digest = NEW.plan_digest
        AND classified.original_revision = NEW.original_revision AND classified.original_digest = NEW.original_digest
        AND classified.disposition IN ('terminal_evidenced', 'quarantined_no_effect')
        AND NOT EXISTS (SELECT 1 FROM offline_recovery_rows WHERE disposition IN ('uncertain_hold', 'refused_hold'))
) OR EXISTS (SELECT 1 FROM offline_recovery_metadata_anchors
    WHERE table_name = NEW.table_name AND canonical_pk = NEW.canonical_pk)
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-anchor-binding');
END;

CREATE TRIGGER offline_recovery_anchor_no_update
BEFORE UPDATE ON offline_recovery_metadata_anchors
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-anchor-immutable');
END;

CREATE TRIGGER offline_recovery_anchor_no_delete
BEFORE DELETE ON offline_recovery_metadata_anchors
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-anchor-immutable');
END;

CREATE TRIGGER offline_recovery_anchor_agents_update
BEFORE UPDATE ON agents
WHEN EXISTS (SELECT 1 FROM offline_recovery_metadata_anchors
    WHERE table_name = 'agents' AND canonical_pk = json_array(typeof(OLD.endpoint_id) || ':' || hex(CAST(OLD.endpoint_id AS BLOB))))
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-anchor-metadata-write');
END;

CREATE TRIGGER offline_recovery_anchor_agents_delete
BEFORE DELETE ON agents
WHEN EXISTS (SELECT 1 FROM offline_recovery_metadata_anchors
    WHERE table_name = 'agents' AND canonical_pk = json_array(typeof(OLD.endpoint_id) || ':' || hex(CAST(OLD.endpoint_id AS BLOB))))
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-anchor-metadata-write');
END;

CREATE TRIGGER offline_recovery_anchor_conversations_update
BEFORE UPDATE ON conversations
WHEN EXISTS (SELECT 1 FROM offline_recovery_metadata_anchors
    WHERE table_name = 'conversations' AND canonical_pk = json_array(typeof(OLD.conversation_id) || ':' || hex(CAST(OLD.conversation_id AS BLOB))))
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-anchor-metadata-write');
END;

CREATE TRIGGER offline_recovery_anchor_agents_insert
BEFORE INSERT ON agents
WHEN EXISTS (SELECT 1 FROM offline_recovery_metadata_anchors
    WHERE table_name = 'agents' AND canonical_pk = json_array(typeof(NEW.endpoint_id) || ':' || hex(CAST(NEW.endpoint_id AS BLOB))))
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-anchor-metadata-write');
END;

CREATE TRIGGER offline_recovery_anchor_conversations_insert
BEFORE INSERT ON conversations
WHEN EXISTS (SELECT 1 FROM offline_recovery_metadata_anchors
    WHERE table_name = 'conversations' AND canonical_pk = json_array(typeof(NEW.conversation_id) || ':' || hex(CAST(NEW.conversation_id AS BLOB))))
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-anchor-metadata-write');
END;

CREATE TRIGGER offline_recovery_anchor_conversations_delete
BEFORE DELETE ON conversations
WHEN EXISTS (SELECT 1 FROM offline_recovery_metadata_anchors
    WHERE table_name = 'conversations' AND canonical_pk = json_array(typeof(OLD.conversation_id) || ':' || hex(CAST(OLD.conversation_id AS BLOB))))
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-anchor-metadata-write');
END;

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

CREATE TRIGGER offline_recovery_preserved_bootstrap_only
BEFORE INSERT ON offline_recovery_rows
WHEN NEW.disposition = 'eligible_preserved' AND NOT EXISTS (
    SELECT 1 FROM runtime_eligibility_bootstrap AS bootstrap
    JOIN runtime_eligibility_context AS context
        ON context.bootstrap_nonce = bootstrap.bootstrap_nonce
    WHERE bootstrap.singleton = 1 AND bootstrap.state = 'pending'
        AND context.state = 'ready'
        AND context.generation = NEW.context_generation
        AND NEW.context_kind = 'nonadmin'
        AND NEW.origin_generation = context.generation
)
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-preserved-outside-bootstrap');
END;

CREATE TRIGGER offline_recovery_row_origin_immutable
BEFORE UPDATE ON offline_recovery_rows
WHEN NEW.table_name != OLD.table_name OR NEW.canonical_pk != OLD.canonical_pk
    OR NEW.origin_generation != OLD.origin_generation
    OR NEW.context_kind != OLD.context_kind
    OR NEW.context_generation IS NOT OLD.context_generation
    OR NEW.admin_operation_id IS NOT OLD.admin_operation_id
    OR NEW.plan_digest IS NOT OLD.plan_digest
    OR NEW.matrix_operation_id IS NOT OLD.matrix_operation_id
    OR (NEW.disposition IN ('eligible_new', 'eligible_preserved')
        AND NEW.disposition != OLD.disposition)
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-origin-fence');
END;

CREATE TRIGGER offline_recovery_bootstrap_fence
BEFORE UPDATE ON runtime_eligibility_bootstrap
WHEN OLD.state != 'pending' OR NEW.state NOT IN ('complete', 'admin_reserved')
    OR NEW.bootstrap_nonce != OLD.bootstrap_nonce
    OR (NEW.state = 'complete' AND NOT EXISTS (
        SELECT 1 FROM runtime_eligibility_context AS context
        WHERE context.generation = NEW.generation
            AND context.bootstrap_nonce = OLD.bootstrap_nonce
            AND context.bootstrap_inventory_digest = NEW.inventory_digest
            AND context.state = 'ready'
    ))
    OR (NEW.state = 'admin_reserved' AND (
        NEW.generation IS NOT NULL OR NEW.inventory_digest IS NOT NULL
        OR NOT EXISTS (SELECT 1 FROM offline_recovery_binding)
    ))
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-bootstrap-fence');
END;

CREATE TRIGGER offline_recovery_bootstrap_no_delete
BEFORE DELETE ON runtime_eligibility_bootstrap
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-bootstrap-delete');
END;

CREATE TRIGGER offline_recovery_bootstrap_no_replace
BEFORE INSERT ON runtime_eligibility_bootstrap
WHEN EXISTS (SELECT 1 FROM runtime_eligibility_bootstrap)
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-bootstrap-replace');
END;

CREATE TRIGGER offline_recovery_row_no_delete
BEFORE DELETE ON offline_recovery_rows
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-classification-delete');
END;

CREATE TRIGGER offline_recovery_row_no_replace
BEFORE INSERT ON offline_recovery_rows
WHEN EXISTS (SELECT 1 FROM offline_recovery_rows
    WHERE table_name = NEW.table_name AND canonical_pk = NEW.canonical_pk)
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-classification-replace');
END;

CREATE TRIGGER offline_recovery_context_fence
BEFORE UPDATE ON runtime_eligibility_context
WHEN NEW.singleton != OLD.singleton OR NEW.generation != OLD.generation
    OR NEW.bootstrap_nonce != OLD.bootstrap_nonce
    OR NEW.bootstrap_inventory_digest != OLD.bootstrap_inventory_digest
    OR NEW.created_at != OLD.created_at OR NEW.revision != OLD.revision + 1
    OR OLD.state = 'held'
    OR (OLD.provenance_kind = 'validated_native' AND (
        NEW.provenance_kind != OLD.provenance_kind
        OR NEW.compatibility_operation_id IS NOT OLD.compatibility_operation_id
        OR NEW.compatibility_source_digest IS NOT OLD.compatibility_source_digest
    ))
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-context-fence');
END;

CREATE TRIGGER offline_recovery_context_no_delete
BEFORE DELETE ON runtime_eligibility_context
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-context-delete');
END;

CREATE TRIGGER offline_recovery_context_no_replace
BEFORE INSERT ON runtime_eligibility_context
WHEN EXISTS (SELECT 1 FROM runtime_eligibility_context)
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-context-replace');
END;

CREATE TRIGGER offline_recovery_anchor_role_insert
BEFORE INSERT ON offline_recovery_rows
WHEN EXISTS (
    SELECT 1 FROM json_each(NEW.parent_relations) AS parent
    WHERE json_extract(parent.value, '$.role') = 'metadata_anchor' AND (
        NEW.disposition != 'eligible_new' OR NEW.context_kind != 'admin'
        OR NEW.matrix_operation_id IS NULL OR NOT EXISTS (
            SELECT 1 FROM offline_recovery_metadata_anchors AS anchor
            JOIN offline_recovery_binding AS binding ON binding.singleton = 1
            WHERE anchor.table_name = json_extract(parent.value, '$.identity.table')
                AND anchor.canonical_pk = json_extract(parent.value, '$.identity.key')
                AND anchor.original_revision = json_extract(parent.value, '$.revision')
                AND anchor.original_digest = json_extract(parent.value, '$.digest')
                AND binding.native_operation_id = NEW.origin_generation
                AND anchor.admin_operation_id = NEW.admin_operation_id AND anchor.plan_digest = NEW.plan_digest
                AND binding.phase = 'active' AND binding.root_digest = anchor.root_digest
                AND binding.db_generation = anchor.db_generation
                AND binding.admin_operation_id = anchor.admin_operation_id AND binding.plan_digest = anchor.plan_digest
                AND binding.native_operation_id = NEW.matrix_operation_id
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-anchor-role');
END;

CREATE TRIGGER offline_recovery_parent_roles_immutable
BEFORE UPDATE OF parent_relations ON offline_recovery_rows
WHEN (SELECT json_group_array(json(value)) FROM json_each(NEW.parent_relations)
    WHERE json_extract(value, '$.role') = 'metadata_anchor') IS NOT
    (SELECT json_group_array(json(value)) FROM json_each(OLD.parent_relations)
    WHERE json_extract(value, '$.role') = 'metadata_anchor')
BEGIN
    SELECT RAISE(ABORT, 'offline-recovery-parent-role-immutable');
END;

CREATE TABLE offline_recovery_workspace_deletions (
    table_name TEXT NOT NULL CHECK (table_name = 'workspace_claims'),
    canonical_pk TEXT NOT NULL,
    classification_json TEXT NOT NULL CHECK (json_valid(classification_json)),
    snapshot_json TEXT NOT NULL CHECK (json_valid(snapshot_json)),
    context_json TEXT NOT NULL CHECK (json_valid(context_json)),
    authority_fence_json TEXT NOT NULL CHECK (json_valid(authority_fence_json)),
    receipt_digest TEXT NOT NULL CHECK (length(receipt_digest) = 64),
    runtime_owner TEXT NOT NULL,
    task_id TEXT NOT NULL,
    owner_fence INTEGER NOT NULL CHECK (owner_fence >= 1),
    PRIMARY KEY (table_name, canonical_pk),
    FOREIGN KEY (table_name, canonical_pk) REFERENCES offline_recovery_rows(table_name, canonical_pk)
) STRICT;

CREATE TRIGGER offline_recovery_workspace_deletion_no_update
BEFORE UPDATE ON offline_recovery_workspace_deletions
BEGIN SELECT RAISE(ABORT, 'offline-recovery-deletion-update'); END;

CREATE TRIGGER offline_recovery_workspace_deletion_no_delete
BEFORE DELETE ON offline_recovery_workspace_deletions
BEGIN SELECT RAISE(ABORT, 'offline-recovery-deletion-delete'); END;

CREATE TRIGGER offline_recovery_workspace_deletion_no_replace
BEFORE INSERT ON offline_recovery_workspace_deletions
WHEN EXISTS (SELECT 1 FROM offline_recovery_workspace_deletions
    WHERE table_name=NEW.table_name AND canonical_pk=NEW.canonical_pk)
BEGIN SELECT RAISE(ABORT, 'offline-recovery-deletion-replace'); END;

CREATE TRIGGER offline_recovery_workspace_deletion_requires_live_claim
BEFORE INSERT ON offline_recovery_workspace_deletions
WHEN NOT EXISTS (SELECT 1 FROM workspace_claims
    WHERE json_array(typeof(claim_id)||':'||hex(CAST(claim_id AS BLOB)))=NEW.canonical_pk
    AND runtime_owner=NEW.runtime_owner AND task_id=NEW.task_id
    AND owner_fence=NEW.owner_fence AND state='active')
BEGIN SELECT RAISE(ABORT, 'offline-recovery-deletion-without-claim'); END;

CREATE TRIGGER offline_recovery_workspace_no_resurrection
BEFORE INSERT ON workspace_claims
WHEN EXISTS (SELECT 1 FROM offline_recovery_workspace_deletions
    WHERE canonical_pk=json_array(typeof(NEW.claim_id)||':'||hex(CAST(NEW.claim_id AS BLOB))))
BEGIN SELECT RAISE(ABORT, 'offline-recovery-deletion-resurrection'); END;

CREATE TRIGGER offline_recovery_workspace_no_identity_reuse
BEFORE UPDATE OF claim_id ON workspace_claims
WHEN EXISTS (SELECT 1 FROM offline_recovery_workspace_deletions
    WHERE canonical_pk=json_array(typeof(NEW.claim_id)||':'||hex(CAST(NEW.claim_id AS BLOB))))
BEGIN SELECT RAISE(ABORT, 'offline-recovery-deletion-resurrection'); END;

CREATE TRIGGER offline_recovery_deleted_classification_immutable
BEFORE UPDATE ON offline_recovery_rows
WHEN EXISTS (SELECT 1 FROM offline_recovery_workspace_deletions
    WHERE table_name=OLD.table_name AND canonical_pk=OLD.canonical_pk)
BEGIN SELECT RAISE(ABORT, 'offline-recovery-deleted-classification-update'); END;

CREATE TRIGGER offline_recovery_historical_task_update BEFORE UPDATE ON tasks
WHEN EXISTS (SELECT 1 FROM offline_recovery_rows WHERE table_name='tasks' AND canonical_pk=json_array(typeof(OLD.task_id)||':'||hex(CAST(OLD.task_id AS BLOB))) AND disposition IN ('terminal_evidenced','quarantined_no_effect','uncertain_hold','refused_hold'))
BEGIN SELECT RAISE(ABORT, 'offline-recovery-historical-task'); END;

CREATE TRIGGER offline_recovery_historical_task_delete BEFORE DELETE ON tasks
WHEN EXISTS (SELECT 1 FROM offline_recovery_rows WHERE table_name='tasks' AND canonical_pk=json_array(typeof(OLD.task_id)||':'||hex(CAST(OLD.task_id AS BLOB))) AND disposition IN ('terminal_evidenced','quarantined_no_effect','uncertain_hold','refused_hold'))
BEGIN SELECT RAISE(ABORT, 'offline-recovery-historical-task'); END;

CREATE TRIGGER offline_recovery_historical_task_insert BEFORE INSERT ON tasks
WHEN EXISTS (SELECT 1 FROM offline_recovery_rows WHERE table_name='tasks' AND canonical_pk=json_array(typeof(NEW.task_id)||':'||hex(CAST(NEW.task_id AS BLOB))) AND disposition IN ('terminal_evidenced','quarantined_no_effect','uncertain_hold','refused_hold'))
BEGIN SELECT RAISE(ABORT, 'offline-recovery-historical-task'); END;

CREATE TRIGGER offline_recovery_historical_task_event BEFORE INSERT ON task_events
WHEN EXISTS (SELECT 1 FROM offline_recovery_rows WHERE table_name='tasks' AND canonical_pk=json_array(typeof(NEW.task_id)||':'||hex(CAST(NEW.task_id AS BLOB))) AND disposition IN ('terminal_evidenced','quarantined_no_effect','uncertain_hold','refused_hold'))
BEGIN SELECT RAISE(ABORT, 'offline-recovery-historical-task'); END;
