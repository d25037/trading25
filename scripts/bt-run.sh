#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: bt-run.sh <command> [args...]" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bt_root="${repo_root}/apps/bt"

uv_cache_dir="${UV_CACHE_DIR:-/tmp/uv-cache}"
use_uv=false
if [[ -n "${BT_USE_UV:-}" || -n "${CI:-}" || -n "${CI_DEPS_READY:-}" ]]; then
  use_uv=true
fi

if ${use_uv} && [[ -z "${BT_FORCE_VENV:-}" ]] && command -v uv >/dev/null 2>&1; then
  set +e
  ( cd "${bt_root}" && UV_CACHE_DIR="${uv_cache_dir}" uv run "$@" )
  status=$?
  set -e
  if [[ ${status} -eq 0 ]]; then
    exit 0
  fi
  echo "[apps/bt] uv run failed (exit ${status})." >&2
  if [[ -n "${CI:-}" || -n "${CI_DEPS_READY:-}" ]]; then
    exit "${status}"
  fi
  echo "[apps/bt] falling back to venv + pip." >&2
fi

require_deps=""
case "$1" in
  pyright|pytest|mypy)
    require_deps="1"
    ;;
esac

if [[ -n "${require_deps}" ]]; then
  BT_SKIP_UV=1 BT_REQUIRE_DEPS=1 "${repo_root}/scripts/bt-env.sh"
else
  BT_SKIP_UV=1 "${repo_root}/scripts/bt-env.sh"
fi

( cd "${bt_root}" && ".venv/bin/$1" "${@:2}" )
