# Offline production schema migration

This private administrative command is for a separately reviewed release and
operation runbook. It is not a deployment, restore, historical cleanup, owner
termination proof, or permission to start Observer. The initial supported change
is the unchanged embedded migrations 19 and 20 on a valid migration-18 database.
The existing helper's dirty/zero-checkout-identity rejection is unchanged.

```text
guigu-agent-bridge offline-migrate-production \
  --mode <preflight|apply|resume|verify> \
  --config <canonical-config> --database <canonical-existing-database> \
  --operation-id <operation-id> --manifest-dir <existing-private-directory> \
  --release-provenance <reviewed-private-record> \
  --executable <canonical-running-executable>
```

All seven options occur exactly once. Paths must be canonical and absolute;
database aliases/hardlinks are refused. The manifest directory is initially empty,
mode 0700, and provenance is mode 0600. The config's database must name the same
file (a leading `~/` is expanded). Only database/listener configuration is parsed;
no Matrix/ACP credentials, sessions, transports or runtime workers are loaded.

## Reviewed identity record

The controller supplies and freezes the record's path and checksum in the
operation runbook. The JSON object has exactly these fields:

```text
source_commit             reviewed source SHA-1, 40 nonzero hexadecimal digits
package                   guigu-agent-bridge
version                   exact embedded package version
publication               verified registry/tag reference
review                    independent release/install review reference
package_sha256            verified published crate checksum
executable_sha256         installed and running executable SHA-256
executable                canonical installed executable path
elf_sha256                SHA-256 of the executable's first 64 ELF bytes
migration_set_sha256      digest of the ordered embedded migration contract
launcher_hold             controller-owned maintenance-window evidence
```

The migration contract digest is SHA-256 of compact JSON serializing the ordered
array of `(version, description, checksum-byte-array)` tuples from the embedded
SQLx migrator. SQLx migration checksums are not substituted with file SHA-256.
Release preparation derives this value from the exact reviewed package inputs.
The running executable is opened through `/proc/self/exe`, hashed and bound by
device/inode and stable size/mtime/ctime; an unrelated `--executable` is refused.
Registry packages need no embedded Git commit. Source identity comes only from
reviewed external provenance, never an enclosing checkout or `.cargo_vcs_info`.

`launcher_hold` contains `operation_id`, `controller`, `prior_state`
(`inactive/dead/MainPID=0`), `prior_show_sha256`, canonical `executable` and
`config`, and `manual_launchers_excluded: true`. This is an operator-supplied
record, not a signature or proof against a hostile operator.

## Exclusion and lifetime

Before invoking the command, the controller establishes its serialized launcher
hold, records prior user-unit/ExecStart identity and activation sources, and
excludes manual/old-binary launches. The intended runtime mask is described in
the separately approved operation runbook; this command never masks/unmasks or
starts/stops anything. A conflicting pre-existing hold must not be appropriated.

The command reads `/usr/bin/systemctl --user` state, jobs and reverse dependencies
with bounded subprocess waits. It requires effective runtime masking,
inactive/dead, PID zero and no jobs/triggers/unclassified reverse launchers. It
also checks configured listeners and same-user bridge processes. It rechecks
after acquiring the persistent per-database advisory lock and before mutation.
Unknown state is refusal, not absence. This protects cooperative supported
writers, not arbitrary same-UID/root processes bypassing the operation window.

Normal new-release startup takes the same lock before opening/migrating SQLite,
refuses unresolved operation records, and retains the lock until shutdown closes
the pool. Supported administrative mutation routes take it before their existing
admission/restore guards. Lock order is maintenance, existing guard, SQLite.
The inode is never unlinked for recovery; process exit releases the kernel lock.

## Durable operation protocol

Before backup, a database-parent operation record binds the operation id,
canonical paths, database inode and config/provenance hashes. Publication uses a
private temporary file, file fsync, non-overwriting hard-link publication and
directory fsync. Interrupted publication residue is retained for manual review.

Backup pins a read-only, noncreating WAL snapshot, computes full ledger/schema
and logical-row facts, and uses that same connection for SQLite online backup.
Absent, empty and nonempty WAL are valid. BUSY/LOCKED backup stepping has a finite
deadline. The destination is closed, reopened read-only, integrity/foreign keys,
ledger and row digests verified, then fsynced before `backup_published.json`.
No source checkpoint, journal-mode switch, raw copy, WAL deletion or automatic
restore is performed. WAL/SHM byte equality is not claimed.

After source/provenance revalidation, immutable `migration_intent.json` binds the
original backup and exact migration set **before SQL**. Each migration commits
with its SQLx ledger entry; the pair is not batch-atomic. Reopen verification
checks all original columns/rows, complete ordered ledger/checksums, exact schema,
empty newly added owner bindings and migration-20 column defaults. Only then is
`complete.json` durably published. No owner/death/session/replay state is changed.

| Ledger/evidence | Permitted action |
| --- | --- |
| 18, no record/residue | Explicit apply: record, backup, intent, migrations |
| 18, matching original backup ready | Same-operation resume; publish missing intent |
| 18/19, matching backup + intent | Resume only the remaining embedded migrations |
| 20, matching backup + intent, no completion | Verify/publish completion only; no SQL or second backup |
| 20, matching completion | Verify-only idempotent result |
| 19/20 without original intent/backup | Manual required; no adoption |
| Incomplete backup, orphan bytes, pending record or drift | Manual required; retain all evidence |

Preflight checks read-only SQLite state and creates neither a DB nor operation;
it may acquire/create the persistent lock inode and never authorizes later
mutation from stale results. Verify requires an existing completion and never
creates backup/intent. Resume always names the original operation. No implicit
retry, fresh-operation takeover, downgrade, restore or service start exists.

Exits: 0 success; 2 invalid argv; 4 lock/launcher contention or unknown exclusion;
5 preflight/backup failure; 6 unknown partial state, malformed evidence or drift;
7 migration/postflight failure. On any failure retain the launcher hold and
actual operation artifacts for controller disposition. Main-binary rollback is
not database rollback. Release/installation and subsequent production recovery
remain separate review boundaries.
