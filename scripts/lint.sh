#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[apps/ts] bun run lint"
( cd "${repo_root}/apps/ts" && bun run lint )

echo "[apps/bt] uv run ruff check ."
( cd "${repo_root}/apps/bt" && uv run ruff check . )
