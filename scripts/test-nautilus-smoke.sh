#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[nautilus-smoke] Run bt Nautilus real-runtime smoke"
BT_USE_UV=1 "${repo_root}/scripts/bt-run.sh" pytest \
  -m nautilus_smoke \
  tests/smoke/test_nautilus_runtime_smoke.py

echo "[nautilus-smoke] PASS"
