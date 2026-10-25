# R076 local hydration patch

The two directories are extracted from exact crates.io 0.13.0 archives. Their
SHA-256 identities, upstream Git identity and license are in `PROVENANCE.toml`;
original source license headers and Cargo manifests are retained. The standard
Apache-2.0 license text is included alongside the sources. Registry cache sources
are not modified. Root `[patch.crates-io]` and Cargo.lock select both local crates;
`matrix-sdk` itself remains the unmodified registry 0.13.0 crate.

`hydration.patch` is the complete minimal diff against those exact archives.
To reproduce it, verify both archive hashes, extract into an empty directory,
then compare each extracted crate with the corresponding directory here using
zero-context unified diffs (`diff -U0`, with `a/vendor/` and `b/vendor/` labels).
No generated build files are needed.

The new fallible StateStore capability is forwarded through DynStateStore's
error-erasing wrapper. Unknown providers refuse. The SQLite provider exports the
entire room/state/account-data set, profiles, membership/display-name indices,
receipts and recognized SDK KV data within one read transaction. Event types are
not allowlisted: custom state and account data are retained. Encrypted SQL keys
are verified against identifiers recovered from decoded payloads. Keys whose
semantic identities cannot be recovered (including opaque custom KV/filter or
thread-draft keys) refuse the whole snapshot, never disappear silently. Extra
unmapped or malformed data is an error, not a partially usable clone.

BaseClient seeds a fresh MemoryStore and loads its room/cached-cursor state only
after successful export. All future state writes remain in memory. Authenticated
crypto store and Olm machine sharing are unchanged. Persistent outgoing queues,
media and event-cache data are intentionally not exported or replayed.

CL-R076-04 permits unchanged upstream baseline files to exceed Bridge structure
limits when provenance and this minimal diff are supplied. New snapshot modules,
Bridge code and tests remain subject to normal file/function/test limits.

This patch is local-only. Cargo registry packaging does not propagate a root
`[patch]` section to downstream consumers; it is not evidence of a publishable
patched SDK release. Any later release task must resolve that distribution
boundary explicitly, without silently installing an unpatched registry build.
