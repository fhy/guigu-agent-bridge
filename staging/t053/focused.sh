#!/usr/bin/env bash
set -u
evidence=${1:?private evidence directory required}
exec 9>/home/fhy/guigu-agent-bridge/build.lock
flock 9
export CARGO_TARGET_DIR=/home/fhy/.cache/cargo-target/guigu-agent-bridge
export CARGO_BUILD_JOBS=1
date -Iseconds > "$evidence/lock-acquired"
rustc +1.94.0 -Vv > "$evidence/rustc.txt"
cargo +1.94.0 -V > "$evidence/cargo.txt"
run() {
  local name=$1
  shift
  "$@" > "$evidence/$name.log" 2>&1
  local result=$?
  printf '%s\n' "$result" > "$evidence/$name.exit"
  printf '%s=%s\n' "$name" "$result"
  return "$result"
}
run fmt cargo +1.94.0 fmt --check || exit $?
run check cargo +1.94.0 check --locked --offline --all-targets --features test-support || exit $?
run clippy cargo +1.94.0 clippy --locked --offline --all-targets --features test-support -- -D warnings || exit $?
run durable cargo +1.94.0 test --locked --offline --lib acp::output -- --test-threads=1 || exit $?
run drain cargo +1.94.0 test --locked --offline --lib app::output_drain -- --test-threads=1 || exit $?
run integration cargo +1.94.0 test --locked --offline --features test-support --test t051_migration --test storage_schema --test t049_a2a_migration --test acp_transport --test acp_dispatcher --test app_runtime -- --test-threads=1 || exit $?
date -Iseconds > "$evidence/lock-released"
