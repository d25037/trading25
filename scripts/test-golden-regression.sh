#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[golden] Run bt golden dataset regression tests"
BT_USE_UV=1 "${repo_root}/scripts/bt-run.sh" pytest \
  tests/server/test_indicator_golden.py \
  tests/server/test_resample_compatibility.py

echo "[golden] PASS"
