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
target_version: v0.2.11 local candidate
base_version_or_tag: v0.2.10 (ddc477cf182db64885f0e18a16587a7b259d3c71)
candidate_commit: PENDING LOCAL REVIEW
annotated_tag: NOT CREATED
crate_or_package_checksum: PENDING LOCAL BUILD
binary_sha256: PENDING LOCAL BUILD
rollback_version: v0.2.10 (ddc477cf182db64885f0e18a16587a7b259d3c71)
```

The candidate must preserve the complete history of `base_version_or_tag`.
Record the exact commit and artifact checked by the Release/Publication Reviewer.

## Required Capability Closure

List every command, API, migration, configuration field, recovery path, and
operational behavior required by the deployment task. Each row must identify
source provenance, a test, and a check against the candidate binary.

| Capability | Required by task | Source commit/path | Test or gate | Candidate binary check | Result |
|---|---|---|---|---|---|
| CLI/API | Existing diagnostics plus `reconcile-orphaned-preacceptance --database PATH`; Matrix pre-start response recovery | T038-T044; reviewed T044 `5e79cae54336cd2f8ede3d5b86566ae289beef23` | Rust 1.94 gates; SQLite/process probes; offline Matrix ownership/cancellation/supervision probes | v0.2.11 candidate fixed redacted CLI output and reviewed initial-response/durable-disposition behavior | PENDING REVIEW |
| Migration/schema | Existing production schema; T043 adds no migration | T008/T009 reviewed lineage | Locked tests and package audit | v0.2.11 candidate contains no migration delta | PENDING REVIEW |
| Configuration | Observer unit and non-secret config path | T017/T040; installed unit is preflight-only | Read-only unit/ExecStart/PID check | Installed binary must hash to the published v0.2.11 artifact after publication and before start | PENDING PREFLIGHT |
| Recovery/rollback | T043 atomic orphan proof plus shared T037 terminal closure; T044 retains pre-start sync response until durable disposition | T044 `5e79cae`, T043 `7fbfdb2`, T037 `0703952` | Reviewed SQLite/CAS/rollback matrices and offline Matrix cursor/supervision matrix | Invoke only through a published v0.2.11 artifact after separate T036 authorization | PENDING PREFLIGHT |
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
