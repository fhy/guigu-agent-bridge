-- T049: persist A2A as a first-class transport without changing any row shape.
PRAGMA defer_foreign_keys=ON;

CREATE TEMP TABLE __t049_assert_0011 (
    value INTEGER NOT NULL CHECK (value = 1)
) STRICT;

CREATE TABLE __t049_agents_0011 (
    endpoint_id       TEXT    NOT NULL PRIMARY KEY,
    agent_id          TEXT    NOT NULL UNIQUE,
    transport         TEXT    NOT NULL CHECK (transport IN ('acp', 'matrix', 'http', 'a2a')),
    enabled           INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    address_json      TEXT,
    capabilities_json TEXT    NOT NULL,
    CHECK (address_json IS NULL OR (json_valid(address_json) AND CASE transport
        WHEN 'acp'    THEN json_extract(address_json, '$.acp')    IS NOT NULL
        WHEN 'matrix' THEN json_extract(address_json, '$.matrix') IS NOT NULL
        WHEN 'a2a'    THEN json_extract(address_json, '$.a2a')    IS NOT NULL
        ELSE               json_extract(address_json, '$.http')   IS NOT NULL END)),
    CHECK (json_valid(capabilities_json) AND json_type(capabilities_json) = 'array')
) STRICT;

INSERT INTO __t049_agents_0011
    (endpoint_id, agent_id, transport, enabled, address_json, capabilities_json)
SELECT endpoint_id, agent_id, transport, enabled, address_json, capabilities_json
FROM agents;

INSERT INTO __t049_assert_0011(value)
SELECT COUNT(*) = (SELECT COUNT(*) FROM agents)
FROM __t049_agents_0011;

DROP TABLE agents;
ALTER TABLE __t049_agents_0011 RENAME TO agents;

CREATE TABLE __t049_conversations_0011 (
    conversation_id   TEXT NOT NULL PRIMARY KEY,
    transport         TEXT CHECK (transport IS NULL OR transport IN ('acp', 'matrix', 'http', 'a2a')),
    external_id       TEXT,
    thread_ref        TEXT,
    participants_json TEXT NOT NULL,
    CHECK ((transport IS NULL) = (external_id IS NULL)),
    CHECK (transport IS NOT NULL OR thread_ref IS NULL),
    CHECK (json_valid(participants_json) AND json_type(participants_json) = 'array')
) STRICT;

INSERT INTO __t049_conversations_0011
    (conversation_id, transport, external_id, thread_ref, participants_json)
SELECT conversation_id, transport, external_id, thread_ref, participants_json
FROM conversations;

INSERT INTO __t049_assert_0011(value)
SELECT COUNT(*) = (SELECT COUNT(*) FROM conversations)
FROM __t049_conversations_0011;

DROP TABLE conversations;
ALTER TABLE __t049_conversations_0011 RENAME TO conversations;

CREATE UNIQUE INDEX conversations_external_ref_unthreaded
    ON conversations (transport, external_id)
    WHERE thread_ref IS NULL AND transport IS NOT NULL;

CREATE UNIQUE INDEX conversations_external_ref_threaded
    ON conversations (transport, external_id, thread_ref)
    WHERE thread_ref IS NOT NULL;

INSERT INTO __t049_assert_0011(value)
SELECT 0 FROM pragma_foreign_key_check;

DROP TABLE __t049_assert_0011;
PRAGMA defer_foreign_keys=OFF;
