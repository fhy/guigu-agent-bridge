# v0.2.9 Candidate Notes

v0.2.9 is a local release candidate that adds the reviewed, read-only
`reconcile-lease-owner` eligibility diagnostic from T042. Its output is fixed and
redacted, and it preserves the existing recovery and selector behavior from
v0.2.8.

An observed orphan owner is classified as `non-actionable-orphan`. This candidate
does not repair, replace, delete, or infer an owner and does not authorize any
production mutation or T036 execution.

This document does not claim publication or installation. Publication requires
independent review and separate authorization.
