#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
guigu_root=${GUIGU_ROOT:-"$HOME/guigu"}

printf '%s\n' 'Rust build cache report'
for project in "$repo_root" "$guigu_root"; do
    if [[ -f "$project/Cargo.toml" ]]; then
        target=$(cd "$project" && cargo metadata --no-deps --format-version 1 --manifest-path Cargo.toml \
            | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
        printf 'project=%s\ntarget=%s\n' "$project" "$target"
        du -sh -- "$target" 2>/dev/null || true
    fi
done

printf '%s\n' 'sccache:'
sccache --show-stats 2>/dev/null || true
printf '%s\n' 'filesystem:'
df -h "$HOME"
