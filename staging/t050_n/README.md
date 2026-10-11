# T050-N standalone Linux staging graph

This crate is not a production entrypoint. It does not import the old staging
helper/provisioner dispatch, production constants, or Matrix startup. No live
invocation is authorized by the existence of these executables.

## Implementation analysis (before code)

Normative inputs: independent namespace R2 design (`ec44ebee`) and Controller
revocation decision `c296542` and retained-FD seam decision `c1a75c0`.
Source base: `5876dc88`. All changes are additions
inside this directory, in the exact module/test list approved by R2. Root source,
Cargo files, build script, migrations, old staging graphs and production units are
excluded. No historical runtime metadata is sampled.

Call graph: provisioner -> typed contract -> retained namespace/lock -> descriptor
publication -> journal receipt/reservation -> owned syscall/child supervisor ->
typed manager/readiness -> helper; parent -> granted secret setup before runtime
threads -> root `run_with_shutdown`. Root `run()` is never called. Root startup
loads config, opens/migrates the confined DB and acquires its DB-local admission
guard; its path operations require broker coverage, not a test-only assertion.

Dependencies match root locked versions. This crate has its own Cargo.lock and
shares the approved build target. Build ownership is released before namespace
ownership. The namespace flock is held through reservation, child lifecycle and
final recording; helpers do not reacquire it. Never await a manager/child while
holding the build lock. Journal reservations precede effects; missing acknowledgements
remain unknown, not a retry. Signals, nonzero, early exit and deadlines preserve
the original outcome separately from cleanup. Only retained-descriptor execution,
certified pidfd ownership and complete receipts grant authority.

Publication uses anonymous inodes and descriptor-selected no-replace links, with
file/parent fsync. Publication/recovery cannot recapture current replacements as
original witnesses. Teardown never conditionally unlinks by name: durable revocation
records retain original/replacement identities; every later tuple admission rejects
revocation. A crash before that record is unknown-retained. Authority files remain
private evidence; process/listener/scope retirement and publication-data retention
are different outcomes.

Validation: N01-N31 pure/fake/owned OS-process/private-FS cases only. No real
credentials, service manager mutation, Matrix, native A2A or E2E. Unsupported
kernel boundaries and incomplete trace fail closed. A command-issuance certificate
is not a runtime certificate; each runtime certificate states its exact coverage.

Launcher analysis: `LaunchWitness` borrows original namespace/config/lock FDs and
the current owner PID/start-ticks/image certificate. Admission rechecks them before
fork. The child blocks on a private socket, while its creator captures pidfd/ticks
and journals ownership; `grant` rechecks all witnesses and the retained image before
allowing `execveat(AT_EMPTY_PATH)`. Missing/mismatched FD evidence cannot grant.
Cancel/timeout/drop close the gate and stop/reap this original pidfd only. No root
ACP API, production startup, linker interposition or pathname continuation changes.
The staging adapter is explicit; it does not automatically weaken root Command calls.

## Reviewed-scope adapters and evidence limits

`BoundNamespace` owns the original allocation, authenticated publication, journal,
operation reservation and staging child. Recovery packets are read-only and never
reconstruct that owner. `reserve_staging_child` requires original opaque channel
and provider descriptors; the child receives them as FD3/FD4 only after executable,
namespace and PID witnesses are authenticated. Descriptor duplication does not read,
copy or hash provider contents. Any observed descriptor/channel/admission mismatch
poisons that authority even if the original name or metadata is restored later.

The provisioner provides `--derive`, `--prepare` and read-only `--verify`. The helper
checks a bounded non-secret verification packet, not an A2A success assertion.
There is intentionally no pathname-exec fallback or implicit live command. The
immutable-open syscall fixture has only one-open coverage; it is not a certificate
for the root application's complete runtime or descendants. The explicit launcher
does not interpose on production ACP. Native backend/card/send orchestration and
full runtime syscall coverage have not been executed or certified by these tests.

Focused validation uses the standalone manifest and shared target, Rust 1.94.0,
`--locked --offline`; check/Clippy include all targets, tests are serial. The test
targets are `contract_identity`, `publication_snapshot`, `ownership_process`,
`readiness_manager`, `teardown_recovery`, `nonaccess`, plus namespace Host unit
tests. `teardown_recovery` includes nine ownership fixtures a second time; counts
must disclose that duplication. N20 resides in `publication_snapshot` to keep each
approved new file within structural limits; its named assertions are unchanged.
OS tests use original private synthetic FDs/processes and ephemeral sockets, not
the staging ports or any live service manager. Durable revocation retains declared
private publication/evidence residue; it never claims deletion or blanket residue zero.
