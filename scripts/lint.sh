#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[apps/ts] bun run lint"
( cd "${repo_root}/apps/ts" && bun run lint )

echo "[apps/bt] ruff check ."
"${repo_root}/scripts/bt-run.sh" ruff check .
