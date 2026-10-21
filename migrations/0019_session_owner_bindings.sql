-- R059: immutable producer-owned binding between an ACP session and runtime owner.
-- Rows without a qualified final owner evidence remain non-eligible by design.
CREATE TABLE session_owner_bindings (
    binding_id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    session_id_digest TEXT NOT NULL CHECK (length(session_id_digest) = 64),
    endpoint_id TEXT NOT NULL REFERENCES agents(endpoint_id),
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    cwd_digest TEXT NOT NULL CHECK (length(cwd_digest) = 64),
    backend_id TEXT NOT NULL,
    runtime_instance TEXT NOT NULL REFERENCES runtime_instances(instance_token),
    runtime_generation INTEGER NOT NULL CHECK (runtime_generation >= 1),
    resource_key TEXT NOT NULL,
    owner_fence INTEGER NOT NULL CHECK (owner_fence >= 1),
    child_pid INTEGER NOT NULL CHECK (child_pid > 0),
    child_starttime INTEGER NOT NULL CHECK (child_starttime >= 0),
    executable_fingerprint TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    domain_device INTEGER NOT NULL CHECK (domain_device >= 0),
    domain_inode INTEGER NOT NULL CHECK (domain_inode >= 0),
    binding_revision INTEGER NOT NULL CHECK (binding_revision >= 1),
    state TEXT NOT NULL CHECK (state IN ('bound','unknown','reaped')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (task_id, delivery_id, attempt),
    UNIQUE (session_id_digest, endpoint_id, runtime_generation)
) STRICT;
CREATE INDEX session_owner_bindings_runtime ON session_owner_bindings(runtime_instance, runtime_generation, state);
