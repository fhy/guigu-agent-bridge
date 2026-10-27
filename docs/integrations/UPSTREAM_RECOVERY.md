# Original-SDK recovery and public-schema compatibility

The Bridge uses the unmodified registry `matrix-sdk =0.13.0`. Historical fork
sources remain in Git history; they are not a dependency or package input.

## Upgrade and startup gate

Published migrations 0001–0021 keep their original names and SQL bytes. Recovery
control, batches, receipts and component leases are added at 0022; durable restart
holds at 0023; compatibility decisions at 0024. Never rewrite the SQLx migration
ledger or copy unpublished development checksums over installed versions.

Before shared runtime recovery, queues, ACP, sends or SDK construction, startup
checks `matrix_recovery_compatibility`, in Matrix-enabled and disabled modes.
All nonempty legacy cutovers/dispositions are held. Their baseline, watermark,
binding and receipts are preserved, not adopted as new-protocol proof. Unbound
historical Matrix work also holds shared owners. The retained offline migration,
backup, reconciliation and historical-cleanup CLIs are not an automatic bypass.

Without compatibility provenance, startup inspects only metadata and directory
entries at the configured SDK store root. Nonempty contents, including identity-
only and zero-length files, cause a durable unbound-store hold. This is not a
claim that a token was found. Unsafe/unreadable/indeterminate evidence is not
pristine. The gate does not open a database, read keys or initialize an SDK client.
Missing/empty roots provide no SDK evidence; Bridge evidence must also be absent.
Disabled/no-locator startup searches no default SDK directory.

Pristine disabled startup creates no Matrix control or compatibility record and
can run the non-Matrix paths subject to their existing recovery gates. Pristine
enabled startup atomically creates a native compatibility decision and baseline
control with the same operation identity. Subsequent native startups validate
identity and baseline/NULL-B or active/nonempty-B; ordinary SDK files/progress
are not new unbound evidence. Refused, inconsistent or restart-held state blocks
shared owners even with Matrix disabled. No automatic hold-clearing command is
provided. Holds expose health/reporting without starting application work owners.

## Receive and send ownership

The persistent SDK client anchors the device/crypto store. Initialization and
baseline responses have no application callback and cannot enqueue old events.
An original-SDK notification client shares that crypto identity but has isolated
in-memory room state. The MatrixSync owner requests durable B with `full_state`
to rebuild required state; its first response is retained until ingress is ready.
The outbound sender uses this rebuilt client, not a stale independent room cache.

MatrixSync owns receipt reservation, effect processing, disposition acknowledgement
and checkpoint advancement. Gateway raw classification is pure; its admission
work is explicit under the same receipt owner. Queue/permission/reply processing
must complete its disposition before the batch can advance B. Empty valid batches
can commit their actual token, including an unchanged idle token. Tokens are opaque.
`SqliteSyncTokenStore` loads B but cannot independently save the live checkpoint.

Receipts bind account, room, event, operation and canonical payload; unsigned
metadata is excluded. Batch response identity and complete coverage, the previous
B CAS and a live component owner/epoch fence govern completion. The component
epoch persists across I/O; short SQLite transactions do not contain SDK/channel/
ACP waits. Cancellation leaves B and incomplete durable work for reconciliation.
Uncertain pending/processing/unknown/refused restart work creates a durable shared
hold, rather than being replayed as an unproven new effect.

Limited/gap or unrecoverable encrypted input is fail-closed, never plaintext
fallback. Outbound row claims/revisions fence completion; post-send CAS failure
does not put `sending` work back into the replayable pending set, even when the
abandoned classification write fails. These are local safety semantics, not
production release/installation or external encrypted interoperability evidence.

The legacy `CutoverStore` remains for offline tooling and evidence inspection. It
is not a second active receive/checkpoint owner. Fork snapshot/hydration tests are
replaced by original-SDK explicit B/full-state rebuild and write-isolation tests.
