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

if [[ -n "${BT_PYTEST_SKIP_ENV:-}" ]]; then
  if [[ ! -x "${bt_python}" ]]; then
    echo "[apps/bt] BT_PYTEST_SKIP_ENV=1 requires an existing project venv." >&2
    exit 1
  fi

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
  exit 0
fi

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

bt_env_vars=(BT_SKIP_UV=1 BT_REQUIRE_DEPS=1)
needs_uv_bootstrap=false
if [[ ! -x "${bt_python}" ]]; then
  needs_uv_bootstrap=true
elif ! "${bt_python}" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)' >/dev/null 2>&1; then
  needs_uv_bootstrap=true
fi

if ${needs_uv_bootstrap} && ! command -v uv >/dev/null 2>&1; then
  echo "[apps/bt] Python 3.12+ venv is required and uv is unavailable to bootstrap it." >&2
  exit 1
fi

if ${needs_uv_bootstrap}; then
  # Clean or stale checkouts should bootstrap the project-managed Python env via uv.
  bt_env_vars=(BT_USE_UV=1 BT_REQUIRE_DEPS=1)
fi

env "${bt_env_vars[@]}" "${repo_root}/scripts/bt-env.sh"

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
