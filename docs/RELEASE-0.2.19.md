# v0.2.19 Local Candidate Notes

This local candidate includes reviewed changes through T098. It is not a
published release or authorization to push, tag, install, deploy or restart.

## Capabilities and limits

- Retains historical unknown work without replay, fabricated completion,
  automatic abandonment or history deletion.
- Adds explicit offline `historical-unknown-abandon` dry-run/apply/verify with
  exact selectors, same-database audit, atomic transitions, revision/post-state
  verification, repeat/conflict refusal and rollback protection. This is a local
  operator tool, not production owner proof or authority to operate real data.
- Adds bounded ACP spawn/drain/reap and owner-evidence lifecycle checks.
  Positive qualification requires a trusted exclusive execution domain with
  enforced containment/no migration. None is supplied by this release.
  Production observation refuses positive qualification; test-only modeled
  observations cannot be enabled by configuration.
- Real containment, production historical recovery and Observer readiness
  restoration are NOT RUN/not provided. No healthy production claim is made.

## Schema and configuration

Embedded startup migrations extend the v0.2.18 schema (0001–0015) with 0016
audited historical dispositions, 0017 revisioned owner evidence, and 0018 the
owner-evidence window/current-row constraint. Applied migrations are unchanged.
`bridge.owner_evidence_max_age_seconds` defaults to 30, accepts 1–300 seconds,
and is optional in existing configuration. Invalid values refuse configuration.

## Package identity and rollback

The package includes sources, build script and embedded migrations. Outside a
checkout, source identity remains unavailable and legacy artifact admission
stays disabled; provenance hints are not trusted owner proof.

v0.2.18 is the previous release, not a proven healthy production fallback;
the prior operational incident remains FAILED/CONTAINED. Database downgrade is
unsupported. Any future operational rollback requires separately authorized
consistent pre-upgrade database/configuration/crypto generation and its matching
old executable. No production snapshot is read or created here. Published
checksums, tag and installed executable identity remain pending separate phases.
