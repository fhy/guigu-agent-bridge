# Release Checklist

This order is mandatory. Publication and production are separate authorizations.

1. Complete design, implementation and staging review.
2. Make the final public-tree change, then build the exact Rust 1.94 locked binary and
   package from a clean candidate checkout.
3. Verify the external draft manifest against that checkout, binary and package with
   `verify-release-manifest`. Exact-binary and exact-package rows cannot fall back to a
   different checkout, installed command or `PATH`.
4. Obtain independent candidate, artifact and staging review.
5. The Coordinator freezes and commits the external manifest in governance. Any later
   public-tree, artifact or capability change requires a new version and candidate.
6. Obtain publication authorization, verify tag peel and downloaded package facts,
   then append one `publication` evidence record.
7. Verify candidate and installed binaries together and append one atomic
   `installation` record. A partial installation record is invalid.
8. Create a separate production task naming its Operator and request production
   authorization last.

Append evidence with `verify-release-manifest append-evidence --manifest MANIFEST
--input INPUT --output OUTPUT`. Exact repeats are idempotent; conflicting records fail
without overwrite.
