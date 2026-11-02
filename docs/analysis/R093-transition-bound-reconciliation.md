# R093: Same-operation transition-bound reconciliation

Task `R093`, design package for source base `a4850812e0f7867ff471d7169e633c2b1894bad4`.
This is the sole active protocol; prior drafts and R3 questions are superseded.
No production state, retained database, service, grant, credential, SDK, or
external Matrix/ACP operation is used by this document.

## S1. Authority and entry

Protocol 2 is selected by the untrusted format tag in
`offline_production_migration::dispatch_transition`, before the ordinary
protocol-1 guard. It then runs `execute_v2 -> reconciliation::resume_or_start`.
The tag selects a verifier, never authority. Protocol 1 retains its existing
`Evidence::read`, root equality and transition behavior.

Under one existing `RuntimeLock`, pin original input paths and the actual
current executable, perform launcher checks, then read the successor grant
through pinned directory/file handles. The grant is
`/etc/guigu-agent-bridge/recovery-successors/<operation-id>.json`, root-owned
0644 beneath root-owned protected 0755 ancestors, readable by the runtime UID.
Every ancestor is opened relative to a pinned directory FD with `O_NOFOLLOW`;
directories are checked for type, owner and non-writability. The final regular
file has nlink 1; hashing and parsing use the same handle, and pathname,
device/inode and bytes are rechecked before intent publication and each SQL
commit. A publisher uses a protected temporary file, owner/mode setup, file
fsync, `renameat2(RENAME_NOREPLACE)`, and directory fsync. Existing identical
bytes are idempotent; different bytes refuse.

The grant binds version, original OP/root, original provenance and executable
digest, archive/schema-complete/identity digests, successor executable,
package/source/review identities, protocol 2, exact transition entry and
allowed modes. It contains no expiry, command, replacement root or D1
authority. Validate the original root and its immutable Operation, database,
config, manifest and root digest first; the successor can never construct a
replacement original. Then match grant OP/root/provenance/executable,
successor bytes and all plan/action/intent identities. Missing, changed,
wrong-owner, wrong-OP, downgrade or drifted grants refuse before any write.

## S2. Records, digests and marker schema

Plan, intent and completion records are fixed-field, versioned compact UTF-8
JSON; unknown and duplicate fields, floats and implicit nulls are rejected.
Production typed key/cell encoding and its existing hex case are reused.
SHA-256 covers each declared payload except its declared self-digest slots.
Files are `reconciliation-plan-v2.json`, `reconciliation-intent-v2.json` and
`reconciliation-complete-v2.json`, mode 0600, atomically no-replace published
with file and directory fsync. Intent binds OP/root, original schema,
identity/provenance/executable, grant/successor identities, native operation
id, source-state digest, complete action list, schema-25 snapshots and parent
edges, D1/D2 references, expected derivations, classification digest and the
captured native/abandonment times. Resume reuses these values. Completion binds
intent, plan/action, grant, commit and committed/final graph digests.

`source_state_digest` is the production digest of serialized
`(required, cutover, records)` from the three matrix cutover sets. It is not an
archive, config, provenance, executable or work-row digest.

The first business transaction installs exactly this supplemental schema (no
partial-schema acceptance and no migration-ledger or binding CHECK changes):

```sql
CREATE TABLE r093_reconciliation_marker (
 singleton INTEGER PRIMARY KEY CHECK (singleton=1), protocol INTEGER NOT NULL CHECK (protocol=2),
 operation_id TEXT NOT NULL UNIQUE, root_digest TEXT NOT NULL,
 state TEXT NOT NULL CHECK (state IN ('committed','finalized')),
 intent_digest TEXT NOT NULL, grant_digest TEXT NOT NULL, plan_digest TEXT NOT NULL,
 action_digest TEXT NOT NULL, archive_digest TEXT NOT NULL, schema_digest TEXT NOT NULL,
 identity_digest TEXT NOT NULL, committed_graph_digest TEXT NOT NULL,
 committed_binding_revision INTEGER NOT NULL, commit_digest TEXT NOT NULL,
 completion_digest TEXT, final_graph_digest TEXT, final_cas_digest TEXT,
 UNIQUE(operation_id,intent_digest),
 FOREIGN KEY(singleton) REFERENCES offline_recovery_binding(singleton),
 CHECK ((state='committed' AND completion_digest IS NULL AND final_graph_digest IS NULL AND final_cas_digest IS NULL)
     OR (state='finalized' AND completion_digest IS NOT NULL AND final_graph_digest IS NOT NULL AND final_cas_digest IS NOT NULL))
);
CREATE TABLE r093_reconciliation_audit (
 operation_id TEXT NOT NULL, action_id TEXT NOT NULL, action_type TEXT NOT NULL,
 table_name TEXT NOT NULL, canonical_pk TEXT NOT NULL, original_revision TEXT NOT NULL,
 original_digest TEXT NOT NULL, pre_digest TEXT NOT NULL, post_digest TEXT NOT NULL,
 evidence_digest TEXT NOT NULL, PRIMARY KEY(operation_id,action_id),
 FOREIGN KEY(operation_id) REFERENCES r093_reconciliation_marker(operation_id)
   DEFERRABLE INITIALLY DEFERRED
);
```

Expected committed graph excludes its own committed-graph and commit slots;
commit binds that graph and intent/grant/action tuple. Expected final graph
excludes final self-digest slots; final-CAS binds pre/post binding, commit,
completion and final graph. Readback reconstructs these payloads.

## S3. Phase, file and verifier routing

Under the lock inspect `sqlite_schema` and all related objects. Absent schema is
normal for `identity_ready` with no intent or intent-only. Present schema must
match the complete declared columns, keys, CHECKs, FKs, indexes and triggers;
partial or unexpected objects refuse. The routes are:

| State | Route |
|---|---|
| `identity_ready`, no schema, no intent | original archive/schema-25/source digest verification; apply may publish intent; read-only never writes |
| `identity_ready`, matching intent only | re-pin and recompute expected plan; first writer may create schema and apply |
| marker `committed`, `transition_pending`, no completion | immutable archive plus exact committed delta/graph; publish completion |
| marker `committed`, matching completion | revalidate committed graph and completion; final writer CAS |
| marker `finalized`, `baseline_ready` | exact final graph and evidence; idempotent read-only success |
| marker `finalized`, later legitimate runtime phase | frozen reconciliation evidence plus existing phase-specific runtime verifier; no activation authority |
| any missing, extra or drifted combination | refuse; no inferred repair, replay, new OP or archive |

Protocol 2 branches before the old rejection points: original archive
`verify_complete/preserved_exact`, legacy source-state comparison, and
`transition_rows::readback` are respectively routed to original verification,
authorized post-state verification, and complete-delta readback. Protocol 1
continues unchanged. Marker state never skips archive, authorization or full
delta checks.

## S4. Inventory, classifications and parent graph

The pinned reference inventory is 3,660 identities: 3,522 terminal, 7 eligible,
and 131 refused (126 historical owner rows, 3 unknown-chain rows, 2 claims),
digest `9725c3b389c25ad86e24fa13ceeb05da091b7f2e9bc2dad24f88cf4cf1775234`.
Counts are reference only; the plan must contain exact keys, snapshots and
digests. Every original identity is classified, including unchanged terminal
rows; inserted native rows are separately represented.

The single unknown lease/continuation/disposition chain has a separate D1
abandonment decision and exactly three CAS updates, with immutable audit and no
delivery-success assertion. All 126 `runtime_owner_evidence` rows are copied
byte-for-byte, including live/is_current; terminal evidence requires each
composite key, stopped runtime parent with matching generation, and fresh D2
exclusion proof. Active, stopping, missing, wrong-generation or drifted owners
refuse; none is rewritten to reaped. Seven D1 identities are
`quarantined_no_effect`; exactly one agent and two conversations receive
metadata anchors, while delivery/session/transport/ACP rows do not. The two
workspace claims are conditionally deleted only with exact classification and
receipt, released matching lease, task/owner/fence and parent proof.

Each parent edge stores production identity, role, selected revision and digest;
changed parents use post-state while original evidence remains in the action.
Deleted parents use original snapshot plus receipt. The administrative receipt
validator reuses production classification/snapshot/context/fence hashing but
does not call the incompatible non-admin `eligible_preserved` predicate.

## S5. Native expected state and independent delta

Derive expected state from authenticated original schema-25 snapshots and plan
values before observing post-state. Compare every typed key and every cell,
including unchanged cells, parent edges and presence. Native expected writes are:

* held compatibility singleton: exact held row becomes `decision=native`,
  `native_operation_id`, `reason_code=administrative-transition`, all else unchanged;
* absent compatibility singleton: insert singleton, schema version 24, native
  decision/id, `pristine-native`, `created_at=native_at`, and eight legacy fields
  from the legacy cutover tuple (or all NULL);
* empty control table: insert control 1, native operation, baseline phase,
  NULL watermark and restart-hold fields, `updated_at=native_at`.

Require one admin-reserved bootstrap, no runtime eligibility context, empty
native control/batches/receipts/component leases/activation intent, and matching
cutover digest. `native_at` and abandonment time are captured once in intent;
resume never uses observed time. Protocol-2 may factor the native writer, whose
existing `apply` remains `Result<(), Failure>`; protocol 1 is untouched.

## S6. Exact write ledger

One `BEGIN IMMEDIATE` contains schema bootstrap, audit inserts, historical
abandonment audit, three chain CAS updates, native writes, all classifications,
binding CAS to `transition_pending`, three metadata anchors, claim receipts,
claim deletes, graph verification and committed marker insertion. Every write is
plain `INSERT` or exact `UPDATE/DELETE`; zero or multiple affected rows refuse.

The chain predicates are exact task/resource/owner/fence plus
`recovery_needed` for lease and continuation, and exact task/delivery/attempt
plus `outcome_unknown` for disposition; resulting states are released,
terminal, and terminal with reason `historical_unknown_abandoned`. Each affects
exactly one row. Classifications insert one row per complete inventory identity
with the fixed 13-field key/value set and unique table/key, including 126 owner
preservations and seven D1 classifications. Anchors insert exactly three rows
from classification and binding after the binding CAS. Each workspace receipt
is inserted before its exact active claim delete, with classification/snapshot/
context/authority/fence digest and released-lease proof; each receipt and
delete affects one row. Initial binding CAS requires identity_ready, exact
revision/root/schema/identity/old-null tuple and sets transition_pending,
plan/classification/intent/native fields. Marker INSERT is last, with complete
authority and committed graph fields and all three final fields NULL.

The final transaction revalidates all marker, completion, grant, graph and
revision fields, then atomically sets binding `transition_pending -> baseline_ready`
and marker `committed -> finalized` using the exact S2 final UPDATE predicate.
Final graph verification is a result-set check, never a rows-affected claim.

## S7. Lock, close, failure and resume order

Authenticate and exclude; verify original and intent; durably publish intent;
begin the one transaction; revalidate all tuples, inventory, parents, D1/D2;
execute the S6 order; commit. Explicitly close the writer, open a read-only
verifier, validate committed graph, close it, durably publish identical
completion, open a second writer, revalidate and atomically finalize, commit,
close, perform fresh final readback, then release the one lock. No helper takes
a hidden equivalent lock.

Before intent or before BEGIN, no SQL mutation occurs. Transaction failure rolls
back DDL and writes and closes before release. Commit uncertainty never reports
success; resume distinguishes committed versus rolled back. A committed marker
without completion only verifies and publishes completion; completion with
committed performs only final CAS; finalized is read-only idempotence. Completion
or final readback uncertainty retains evidence and retries only after exact
revalidation. Cancellation drains/rolls back/closes live handles; crash recovery
uses the same phase routes. No replay, archive restoration, hold release or
synthetic success is permitted.

## S8. Supersession and evidence boundary

Invalid alternatives removed from this package include intent-before-lock,
successor-as-original authority, binding phases for SQL state, marker-required
before schema, archive-as-source digest, observed timestamps, seven-row-only
classification, live-parent-only joins, admin `eligible_preserved`, prewritten
completion, completion-only SQL, new OP/archive/replay, and owner termination.
Future D1 decisions, retained-state execution, implementation, tests and the
independent design review are separate gates. A1–A7 are covered by the exact
authority, chain CAS, owner/D2, workspace receipt, same-OP resume,
protocol-1/no-replay and successor downgrade rules above.
