#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[apps/ts] bun install"
( cd "${repo_root}/apps/ts" && bun install --frozen-lockfile )

echo "[apps/ts] bun run test"
( cd "${repo_root}/apps/ts" && bun run test )

echo "[apps/bt] uv sync"
( cd "${repo_root}/apps/bt" && uv sync --locked )

echo "[apps/bt] uv run pytest"
( cd "${repo_root}/apps/bt" && uv run pytest )
