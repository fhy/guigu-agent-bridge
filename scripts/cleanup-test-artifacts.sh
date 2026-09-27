#!/usr/bin/env bash
set -euo pipefail

root=${GUI_TEST_ROOT:-"$HOME/.guigu-agent-bridge-tests"}
ttl_days=${GUI_TEST_TTL_DAYS:-7}

if [[ "$root" == *' '* || "$(realpath -m -- "$root")" != "$HOME/.guigu-agent-bridge-tests" ]]; then
    echo "refusing unexpected test root: $root" >&2
    exit 2
fi
[[ "$ttl_days" =~ ^[0-9]+$ ]] || { echo 'TTL must be an integer' >&2; exit 2; }
[[ -d "$root" ]] || exit 0

find "$root" -mindepth 1 -maxdepth 1 -type d -mtime "+$ttl_days" -print -exec rm -rf -- {} +
