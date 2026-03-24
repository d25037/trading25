#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/bt-pytest.sh <test-path> [<test-path> ...]" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bt_root="${repo_root}/apps/bt"
bt_python="${repo_root}/apps/bt/.venv/bin/python"
test_targets=("$@")
pytest_args=("${test_targets[@]}")

if [[ -n "${BT_PYTEST_MARKEXPR:-}" ]]; then
  pytest_args+=("-m" "${BT_PYTEST_MARKEXPR}")
elif [[ "${BT_PYTEST_FAST:-0}" == "1" ]]; then
  pytest_args+=("-m" "not slow")
fi

if [[ -n "${BT_PYTEST_DURATIONS:-}" ]]; then
  pytest_args+=("--durations=${BT_PYTEST_DURATIONS}")
fi

if [[ -n "${BT_PYTEST_DURATIONS_MIN:-}" ]]; then
  pytest_args+=("--durations-min=${BT_PYTEST_DURATIONS_MIN}")
fi

BT_SKIP_UV=1 BT_REQUIRE_DEPS=1 "${repo_root}/scripts/bt-env.sh"

log_command() {
  printf "[apps/bt] "
  printf "%q " "$@"
  printf "\n"
}

if [[ -n "${BT_COVERAGE_DATA_FILE:-}" ]]; then
  log_command coverage run "--data-file=${BT_COVERAGE_DATA_FILE}" -m pytest "${pytest_args[@]}"
  (
    cd "${bt_root}"
    "${bt_python}" -m coverage run \
      --data-file="${BT_COVERAGE_DATA_FILE}" \
      -m pytest \
      "${pytest_args[@]}"
  )
else
  log_command pytest "${pytest_args[@]}"
  (
    cd "${bt_root}"
    "${bt_python}" -m pytest "${pytest_args[@]}"
  )
fi
