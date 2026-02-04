#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[apps/ts] bun run typecheck:all"
( cd "${repo_root}/apps/ts" && bun run typecheck:all )

echo "[apps/bt] pyright"
"${repo_root}/scripts/bt-run.sh" pyright
