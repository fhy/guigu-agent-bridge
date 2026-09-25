# Release Manifest And Deployment Capability Closure

Use this manifest for every release that will be used by a production change.
The release is not deployable until every required capability is present in the
tested candidate binary and in the published artifact.

## Release Identity

```text
target_version:
base_version_or_tag:
candidate_commit:
annotated_tag:
crate_or_package_checksum:
binary_sha256:
rollback_version:
```

The candidate must preserve the complete history of `base_version_or_tag`.
Record the exact commit and artifact checked by the Release/Publication Reviewer.

## Required Capability Closure

List every command, API, migration, configuration field, recovery path, and
operational behavior required by the deployment task. Each row must identify
source provenance, a test, and a check against the candidate binary.

| Capability | Required by task | Source commit/path | Test or gate | Candidate binary check | Result |
|---|---|---|---|---|---|
| CLI/API | | | | | |
| Migration/schema | | | | | |
| Configuration | | | | | |
| Recovery/rollback | | | | | |
| Operational behavior | | | | | |

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
