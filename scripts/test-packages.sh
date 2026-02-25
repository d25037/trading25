#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${CI_DEPS_READY:-}" ]]; then
  echo "[apps/ts] bun install"
  ( cd "${repo_root}/apps/ts" && bun install --frozen-lockfile )
else
  echo "[apps/ts] bun install skipped (CI_DEPS_READY=1)"
fi

echo "[apps/ts] bun run packages:test"
( cd "${repo_root}/apps/ts" && bun run packages:test )

echo "[apps/bt] pytest tests/unit"
BT_USE_UV=1 "${repo_root}/scripts/bt-run.sh" pytest tests/unit
