# v0.2.28 preparation candidate

This local candidate adds a narrow post-send completion repair to published v0.2.27.
It has not been published, installed or validated against live encrypted traffic.

## Change

After a projection send returns a known in-process outcome, ordinary Matrix sync
can temporarily block its durable completion. Completion now retries only the exact
sync-busy refusal for an independent, bounded 30 seconds. Each attempt revalidates
owner, generation, lease/fence, classification and claim CAS; no transaction is held
across network or waiting, and retry never re-enters the network send.

Static redacted failure categories distinguish authority, storage and completion
stages. Hard failures remain visible. Cancellation, timeout, process loss, CAS failure
or unknown send outcome retain unresolved `sending` and forbid resend. This change
cannot establish whether a historical send succeeded, nor repair a retained row
from a later room observation. The historical live failure's exact branch remains
unproven; the controlled sync-contention cause is covered by offline regression.

Pre-send waiting and empty-outbox policy are unchanged. This release does not include
the paused broader wait/health-policy work, historical successor recovery, a new
schema, or an SDK change.

## Compatibility and deployment

The 26 embedded migrations, dependency versions and registry matrix-sdk 0.13.0 are
identical to v0.2.27. Existing schema26 stores require no new migration. Earlier
schema25 upgrade and rollback restrictions remain as documented in
[v0.2.27 notes](RELEASE-0.2.27.md); there is no schema downgrade migration.

Before an authorized deployment, stop the writer and archive the complete matched
configuration/device/database/session/crypto generation plus unresolved-request
evidence. Verify the reviewed executable and pair identity. Do not run two consumers
on one pair, edit live SQL, import retired state or resend old requests.
An existing unresolved `sending` row needs its own approved disposition; a new binary
or normal restart does not prove completion. Rollback requires a compatible reviewed
complete-pair plan and must not erase effects accepted after a snapshot.

## Validation limits

Accepted focused regressions cover completion-only retry, hard refusal and rollback,
cancellation/crash without resend, and real AppRuntime/SDK/ACP loopback lifecycle:
two durable completions across same-store restart, normal stop overlapping an actual
send, and unique sends/replies. Loopback evidence is not live Matrix E2EE acceptance.
Package provenance, release builds and CLI identity are checked separately.

Historical aggregate and expanded test-target lint failures remain disclosed; no
fresh all-tests PASS is claimed. Actual OS reboot remains NOT_RUN. Live acceptance
of these new bytes remains NOT_RUN, and unresolved service sending is preserved.
