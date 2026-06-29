#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${CI_DEPS_READY:-}" ]]; then
  echo "[coverage] Install deps (apps/ts)"
  (
    cd "${repo_root}/apps/ts"
    bun install --frozen-lockfile
  )

  echo "[coverage] Install deps (apps/bt)"
  (
    cd "${repo_root}/apps/bt"
    UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}" uv sync --locked
  )
else
  echo "[coverage] Dependency install skipped (CI_DEPS_READY=1)"
fi

echo "[coverage] Run TypeScript coverage suites"
(
  cd "${repo_root}/apps/ts"
  bun run workspace:test:coverage
  bun run coverage:check
)

echo "[coverage] Run bt coverage suite"
if [[ -n "${BT_COVERAGE_INPUT_DIR:-}" ]]; then
  bt_coverage_files=()
  while IFS= read -r coverage_file; do
    bt_coverage_files+=("${coverage_file}")
  done < <(find "${BT_COVERAGE_INPUT_DIR}" -type f -name '.coverage*' | sort)
  if [[ ${#bt_coverage_files[@]} -eq 0 ]]; then
    echo "[coverage] ERROR: no bt coverage files found in ${BT_COVERAGE_INPUT_DIR}" >&2
    exit 1
  fi
  echo "[coverage] Combine bt coverage files (${#bt_coverage_files[@]})"
  BT_USE_UV=1 "${repo_root}/scripts/bt-run.sh" coverage erase
  BT_USE_UV=1 "${repo_root}/scripts/bt-run.sh" coverage combine "${bt_coverage_files[@]}"
else
  BT_USE_UV=1 "${repo_root}/scripts/bt-run.sh" coverage run -m pytest tests/
fi
bt_product_coverage_include=(
  "src/application/**/*.py"
  "src/domains/backtest/**/*.py"
  "src/domains/fundamentals/**/*.py"
  "src/domains/lab_agent/**/*.py"
  "src/domains/optimization/**/*.py"
  "src/domains/strategy/**/*.py"
  "src/entrypoints/**/*.py"
  "src/infrastructure/**/*.py"
  "src/shared/**/*.py"
  "src/domains/analytics/fundamental_ranking.py"
  "src/domains/analytics/margin_metrics.py"
  "src/domains/analytics/regression_core.py"
  "src/domains/analytics/screening_evaluator.py"
  "src/domains/analytics/screening_requirements.py"
  "src/domains/analytics/screening_results.py"
  "src/domains/analytics/value_composite_scoring.py"
)
BT_USE_UV=1 "${repo_root}/scripts/bt-run.sh" coverage report \
  --fail-under=70 \
  --include="$(IFS=,; echo "${bt_product_coverage_include[*]}")"

echo "[coverage] PASS"
