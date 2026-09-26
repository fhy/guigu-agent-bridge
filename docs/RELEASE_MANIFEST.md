# Release Manifest And Deployment Capability Closure

Use this manifest for every release that will be used by a production change.
The release is not deployable until every required capability is present in the
tested candidate binary and in the published artifact.

## Motivating Incident

Version 0.2.4 was published before the independently reviewed T037
pre-acceptance recovery capability was included. During the subsequent T036
validation, the published artifact lacked the recovery path needed for a legacy
pre-acceptance state, so the process correctly stopped before any production
mutation. This incident shows why release provenance must be paired with
capability-by-capability evidence from the exact artifact.

## Release Identity

```text
target_version: v0.2.7
base_version_or_tag: v0.2.6 (454917775de5d19a9297af030f3f5ecb6f75c727)
candidate_commit: 888d1d9709dd110621d6ef76c77efc5999d73b83
annotated_tag: v0.2.7 (a42748dab81f24b1ded2a71ef38a862257659b32)
crate_or_package_checksum: f1d871e83ec8e5db0f3e64ec339f8396cfee45e6439c6d6f512911e0b00759cc
binary_sha256: 701101c71fa0f434fddc996154e8e62a25d5c141fb5b1723cf30b71831320377
rollback_version: v0.2.6 (454917775de5d19a9297af030f3f5ecb6f75c727)
```

The candidate must preserve the complete history of `base_version_or_tag`.
Record the exact commit and artifact checked by the Release/Publication Reviewer.

## Required Capability Closure

List every command, API, migration, configuration field, recovery path, and
operational behavior required by the deployment task. Each row must identify
source provenance, a test, and a check against the candidate binary.

| Capability | Required by task | Source commit/path | Test or gate | Candidate binary check | Result |
|---|---|---|---|---|---|
| CLI/API | `select-preacceptance`, `recover-selected-preacceptance`, `reconcile-delivery`, `recover-runtime` | T038/T039/T040; `src/{readonly_tuple,selected_preacceptance,offline_delivery,offline_recovery}.rs` | Rust 1.94 gates; process probes | v0.2.7 binary fixed redacted output and rejection probes | PASS |
| Migration/schema | Existing production schema and reviewed migrations | T008/T009 reviewed lineage | Locked tests and read-only schema preflight | v0.2.7 package contains reviewed schema code | PASS |
| Configuration | Observer unit and non-secret config path | T017/T040; installed unit is preflight-only | Read-only unit/ExecStart/PID check | Installed binary must hash to v0.2.7 before start | PENDING PREFLIGHT |
| Recovery/rollback | T037 pre-acceptance CAS; T033 terminal reconciliation; T032 stale-owner recovery | T037 `0703952`, T033 `bb03a45`, T032 `392d5a0` | Reviewed SQLite/CAS/rollback matrices | Invoke only through v0.2.7 binary and exact typed identity | PENDING PREFLIGHT |
| Operational behavior | Single Observer readiness and one authorized smoke path | T036 fixed sequence | Fresh snapshot, predicate, readiness and post-stop evidence | No mutation until all preflight rows close | PENDING PREFLIGHT |

No production procedure may depend on a capability that is only present in an
unpublished checkout or a different version.

## Release Verification

- Rust/MSRV gates passed on the exact candidate commit.
- Package manifest and excluded private files were audited.
- Package/build dry-run passed.
- Each required CLI/API was invoked or otherwise verified from the candidate
  binary, including fixed redacted output and rejection behavior.
- Candidate commit, tag object, published checksum, and installed binary hash
  agree with this manifest.
- No credential, local database, crypto store, session, log, or absolute local
  path entered the artifact.

## Production Preflight

Before any mutation, record read-only evidence for:

- current and target versions and binary hashes;
- systemd unit, `ExecStart`, PID, and listener state;
- schema/migration version;
- database and crypto-store snapshot hashes;
- durable-work predicates and the supported recovery command for each state;
- non-secret configuration paths and rollback target.

For this T036 window, the durable `runtime_instances` owner `stopped` predicate
must be verified through the supported read-only path before creating the mutation
snapshot. An inactive systemd unit or free port alone is insufficient.

If a required capability is absent, provenance differs, or a durable state has no
reviewed recovery path, stop before creating a mutation snapshot or changing a
service. Do not combine commands from different releases, use an unpublished
binary as evidence, or bypass the state with manual SQL.

## Execution And Review

Coordinator freezes this manifest and the acceptance boundary. Developer creates
the candidate and evidence. Reviewer independently checks the exact candidate,
artifact, capability closure, and rollback evidence. A named Operator executes
only the closed procedure after separate authorization and reports results for
independent review.

If a new production-required capability is discovered after the manifest is
frozen, stop the release, add the capability and its tests, and create a new
version candidate. Do not append the capability to an already published version.
