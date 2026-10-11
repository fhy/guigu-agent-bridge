#!/usr/bin/env bash
# One explicit locked gate. No live operation, service or credential consumption.
set -u
umask 077
: "${T050_BUILD_LOCK:?provide the approved global build lock}"
: "${CARGO_TARGET_DIR:?provide the approved shared target}"
if test -n "$(git status --porcelain)"; then
    echo 'gate refused: source is not exact-clean' >&2
    exit 3
fi
gate_evidence=$(mktemp -d -t t050-n-gate.XXXXXX)
echo "gate_evidence=$gate_evidence"
exec 9>"$T050_BUILD_LOCK"
date -Is >"$gate_evidence/lock-wait.txt"
flock 9
date -Is >"$gate_evidence/lock-acquired.txt"
export CARGO_INCREMENTAL=0 RUSTUP_TOOLCHAIN=1.94.0
unset RUSTFLAGS RUST_TEST_THREADS
git rev-parse HEAD >"$gate_evidence/source.txt"
rustc +1.94.0 -vV >"$gate_evidence/rustc.txt"
cargo +1.94.0 --version >"$gate_evidence/cargo.txt"
df -B1 "$CARGO_TARGET_DIR" >"$gate_evidence/disk.txt"
printf '%s\n' "shared_target=$CARGO_TARGET_DIR" 'cache_cleanup=none' 'incremental=0' >"$gate_evidence/cache.txt"
gate_failed=0
stage() {
    local stage_name=$1
    shift
    "$@" >"$gate_evidence/$stage_name.log" 2>&1
    local stage_exit=$?
    printf '%s\n' "$stage_exit" >"$gate_evidence/$stage_name.exit"
    sync -f "$gate_evidence/$stage_name.exit"
    echo "$stage_name=$stage_exit"
    if test "$stage_exit" -ne 0; then gate_failed=1; fi
}
stage build cargo +1.94.0 build --locked --offline --all-targets --features test-support
stage fmt cargo +1.94.0 fmt --all -- --check
stage check cargo +1.94.0 check --locked --offline --all-targets --features test-support
stage clippy cargo +1.94.0 clippy --locked --offline --all-targets --features test-support -- -D warnings
stage test env \
    "CARGO_BIN_EXE_guigu-agent-bridge=$CARGO_TARGET_DIR/debug/guigu-agent-bridge" \
    "CARGO_BIN_EXE_guigu-t050-proc-proof=$CARGO_TARGET_DIR/debug/guigu-t050-proc-proof" \
    "CARGO_BIN_EXE_provision-isolated-proc-proof-fixture=$CARGO_TARGET_DIR/debug/provision-isolated-proc-proof-fixture" \
    cargo +1.94.0 test --locked --offline --all-targets --features test-support -- --test-threads=1
stage diff git diff --check
sha256sum Cargo.lock \
    "$CARGO_TARGET_DIR/debug/guigu-agent-bridge" \
    "$CARGO_TARGET_DIR/debug/guigu-t050-proc-proof" \
    "$CARGO_TARGET_DIR/debug/provision-isolated-proc-proof-fixture" >"$gate_evidence/hashes.txt"
gate_hash_exit=$?
printf '%s\n' "$gate_hash_exit" >"$gate_evidence/hashes.exit"
if test "$gate_hash_exit" -ne 0; then gate_failed=1; fi
git status --porcelain >"$gate_evidence/final-status.txt"
sync -f "$gate_evidence"
flock -u 9
exec 9>&-
date -Is >"$gate_evidence/lock-released.txt"
sync -f "$gate_evidence"
echo 'lock=released'
exit "$gate_failed"
