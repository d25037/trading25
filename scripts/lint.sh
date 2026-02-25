#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[dep-direction] Checking dependency direction"
"${repo_root}/scripts/check-dep-direction.sh"

echo "[apps/ts] bun run quality:lint"
( cd "${repo_root}/apps/ts" && bun run quality:lint )

echo "[apps/bt] ruff check ."
"${repo_root}/scripts/bt-run.sh" ruff check .
