# R076 local hydration patch

The three directories are extracted from exact crates.io 0.13.0 archives. Their
SHA-256 identities, upstream Git identity and license are in `PROVENANCE.toml`;
original source license headers and Cargo manifests are retained. The standard
Apache-2.0 license text is included alongside the sources. Registry cache sources
are not modified. Root `[patch.crates-io]` and Cargo.lock select all three local
crates. Direct SDK glue exposes the base client's isolated replay opt-in only.

`hydration.patch` is the complete minimal diff against those exact archives.
To reproduce it, verify all three archive hashes, extract into an empty directory,
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

F1 adds a fallible opt-in available only on successfully hydrated memory clones.
It bypasses duplicate-token suppression, not processing: repeated wire responses
traverse the full SDK pipeline. Ordinary clients still return the SDK's default
processed response on duplicate tokens, and cannot enable the opt-in. Each clone
has its own opt-in flag, shared only by handles of that clone. No cursor reset or
persistent-client behavior change is involved. Bridge enables it before publishing
the active client, deduplicates durable dispositions and paces active batches.

CL-R076-04 permits unchanged upstream baseline files to exceed Bridge structure
limits when provenance and this minimal diff are supplied. New snapshot modules,
Bridge code and tests remain subject to normal file/function/test limits.

R077 distributes these reviewed sources under `guigu-matrix-sdk`,
`guigu-matrix-sdk-base` and `guigu-matrix-sdk-sqlite`, all exact
`0.13.0-guigu.1`. Dependency aliases and library names remain unchanged; relative
path-plus-version inputs are for development only, and normalized registry
manifests retain the renamed exact versions without a root patch. Normal/dev
base edges and the SDK SQLite edge select the same fork base. Crypto/common and
store-encryption remain upstream. Only Bridge's native SQLite graph is certified;
browser/indexeddb, all-features and arbitrary providers are not certified.

`hydration.patch` remains the original R076 source diff with its original SHA;
`distribution.patch` separately records metadata/documentation changes against
the reviewed R076 tree. Each fork archive carries license, upstream provenance,
the reviewed patch and a distribution identity section. Historical upstream
Cargo.toml.orig files are archive evidence, not effective fork manifests.

Local registry rehearsal is not publication or a live-name ownership claim.
The later authorized publisher must verify names/permissions and publish/verify
downloads in base → SQLite → SDK → Bridge order before public cargo installation.
