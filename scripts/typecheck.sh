#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[apps/ts] bun run quality:typecheck"
( cd "${repo_root}/apps/ts" && bun run quality:typecheck )

echo "[apps/bt] pyright (--project pyproject.toml --pythonpath .venv/bin/python)"
"${repo_root}/scripts/bt-run.sh" pyright --project pyproject.toml --pythonpath .venv/bin/python
