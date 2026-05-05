#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

bt_product_analytics_tests=(
  tests/unit/domains/analytics/test_annual_value_composite_selection.py
  tests/unit/domains/analytics/test_cost_structure.py
  tests/unit/domains/analytics/test_readonly_duckdb_support.py
  tests/unit/domains/analytics/test_screening_evaluator.py
  tests/unit/domains/analytics/test_screening_requirements.py
  tests/unit/domains/analytics/test_screening_results.py
  tests/unit/domains/analytics/test_value_composite_scoring.py
)
bt_product_script_tests=(
  tests/unit/scripts/test_audit_skills.py
  tests/unit/scripts/test_check_privacy_leaks.py
  tests/unit/scripts/test_check_research_guardrails.py
  tests/unit/scripts/test_ci_changed_scope.py
  tests/unit/scripts/test_export_openapi_unittest.py
  tests/unit/scripts/test_refresh_skill_references.py
)
bt_server_unit_tests=(
  tests/unit/server
  tests/unit/backtest
  tests/unit/data
  tests/unit/optimization
  tests/unit/strategies
  tests/unit/strategy_config
)
bt_core_unit_tests=(
  tests/unit/agent
  tests/unit/api
  tests/unit/application
  tests/unit/architecture
  tests/unit/cli
  tests/unit/cli_bt
  tests/unit/config
  tests/unit/filters
  tests/unit/models
  tests/unit/shared
  tests/unit/utils
  tests/unit/test_collect_production_smoke_baseline.py
  tests/unit/test_data.py
  tests/unit/test_type_safety.py
  tests/unit/test_validation.py
)

run_bt_unit_shards() {
  local coverage_target="${BT_COVERAGE_DATA_FILE:-}"
  local shard_count="${BT_UNIT_TEST_SHARDS:-1}"

  if [[ "${shard_count}" -le 1 ]]; then
    "${repo_root}/scripts/bt-pytest.sh" \
      "${bt_product_analytics_tests[@]}" \
      "${bt_product_script_tests[@]}" \
      "${bt_server_unit_tests[@]}" \
      "${bt_core_unit_tests[@]}"
    return 0
  fi

  local bt_root="${repo_root}/apps/bt"
  local bt_python="${bt_root}/.venv/bin/python"
  local coverage_dir
  coverage_dir="$(mktemp -d "${TMPDIR:-/tmp}/bt-unit-shards.XXXXXX")"

  BT_SKIP_UV=1 BT_REQUIRE_DEPS=1 "${repo_root}/scripts/bt-env.sh"

  local -a shard_names=("analytics" "server" "core")
  local -a pids=()
  local -a pid_names=()
  local shard_name coverage_file
  local -a shard_args=()

  echo "[apps/bt] sharded unit test mode (BT_UNIT_TEST_SHARDS=${shard_count})"
  for idx in "${!shard_names[@]}"; do
    shard_name="${shard_names[$idx]}"
    coverage_file="${coverage_dir}/.coverage.${shard_name}"
    case "${shard_name}" in
      analytics)
        shard_args=("${bt_product_analytics_tests[@]}" "${bt_product_script_tests[@]}")
        ;;
      server)
        shard_args=("${bt_server_unit_tests[@]}")
        ;;
      core)
        shard_args=("${bt_core_unit_tests[@]}")
        ;;
      *)
        echo "[apps/bt] unknown shard name: ${shard_name}" >&2
        return 2
        ;;
    esac

    (
      export BT_PYTEST_SKIP_ENV=1
      export BT_COVERAGE_DATA_FILE="${coverage_file}"
      echo "[apps/bt] shard ${shard_name} -> ${shard_args[*]}"
      "${repo_root}/scripts/bt-pytest.sh" "${shard_args[@]}"
    ) &
    pids+=("$!")
    pid_names+=("${shard_name}")
  done

  local status=0
  for idx in "${!pids[@]}"; do
    if ! wait "${pids[$idx]}"; then
      echo "[apps/bt] shard failed: ${pid_names[$idx]}" >&2
      status=1
    fi
  done

  if [[ "${status}" -ne 0 ]]; then
    return "${status}"
  fi

  if [[ -n "${coverage_target}" ]]; then
    if [[ "${coverage_target}" = /* ]]; then
      rm -f "${coverage_target}"
    else
      rm -f "${bt_root}/${coverage_target}"
    fi
    (
      cd "${bt_root}"
      "${bt_python}" -m coverage combine --data-file="${coverage_target}" "${coverage_dir}"/.coverage.*
    )
  fi
}

if [[ -z "${CI_DEPS_READY:-}" ]]; then
  echo "[apps/ts] bun install"
  ( cd "${repo_root}/apps/ts" && bun install --frozen-lockfile )
else
  echo "[apps/ts] bun install skipped (CI_DEPS_READY=1)"
fi

echo "[apps/ts] bun run packages:test"
( cd "${repo_root}/apps/ts" && bun run packages:test )

run_bt_unit_shards
