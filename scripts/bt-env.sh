#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bt_root="${repo_root}/apps/bt"

uv_cache_dir="${UV_CACHE_DIR:-/tmp/uv-cache}"
use_uv=false
if [[ -n "${BT_USE_UV:-}" || -n "${CI:-}" || -n "${CI_DEPS_READY:-}" ]]; then
  use_uv=true
fi

if ${use_uv} && [[ -z "${BT_SKIP_UV:-}" ]] && command -v uv >/dev/null 2>&1; then
  echo "[apps/bt] uv sync"
  set +e
  ( cd "${bt_root}" && UV_CACHE_DIR="${uv_cache_dir}" uv sync --locked )
  status=$?
  set -e
  if [[ ${status} -eq 0 ]]; then
    exit 0
  fi
  echo "[apps/bt] uv sync failed (exit ${status})." >&2
  if [[ -n "${CI:-}" || -n "${CI_DEPS_READY:-}" ]]; then
    exit "${status}"
  fi
  echo "[apps/bt] falling back to venv + pip." >&2
fi

venv_path="${bt_root}/.venv"
python_bin="${PYTHON_BIN:-python3}"

if [[ ! -x "${venv_path}/bin/python" ]]; then
  echo "[apps/bt] create venv"
  "${python_bin}" -m venv "${venv_path}"
fi

if [[ ! -x "${venv_path}/bin/pip" ]]; then
  echo "[apps/bt] ensurepip"
  "${venv_path}/bin/python" -m ensurepip --upgrade
fi

required_bins=(ruff pyright pytest)
missing_bins=()
for bin in "${required_bins[@]}"; do
  if [[ ! -x "${venv_path}/bin/${bin}" ]]; then
    missing_bins+=("${bin}")
  fi
done

if [[ -n "${BT_REQUIRE_DEPS:-}" ]]; then
  set +e
  "${venv_path}/bin/python" - <<'PY'
import importlib.util
import sys

modules = [
    "fastapi",
    "httpx",
    "loguru",
    "numpy",
    "optuna",
    "pandas",
    "pydantic",
    "rich",
    "ruamel.yaml",
    "scipy",
    "sklearn",
    "sse_starlette",
    "typer",
    "uvicorn",
    "vectorbt",
    "watchdog",
]

missing = [m for m in modules if importlib.util.find_spec(m) is None]
if missing:
    print("[apps/bt] missing runtime deps:", ", ".join(missing))
    sys.exit(1)
PY
  deps_status=$?
  set -e
  if [[ ${deps_status} -eq 0 && -z "${BT_FORCE_INSTALL:-}" ]]; then
    echo "[apps/bt] runtime deps already installed; skipping pip install"
    exit 0
  fi
elif [[ ${#missing_bins[@]} -eq 0 && -z "${BT_FORCE_INSTALL:-}" ]]; then
  echo "[apps/bt] venv already has required tools; skipping pip install"
  exit 0
fi

pip_cache_dir="${PIP_CACHE_DIR:-/tmp/pip-cache}"

echo "[apps/bt] pip install -e ."
PIP_CACHE_DIR="${pip_cache_dir}" "${venv_path}/bin/python" -m pip install -e "${bt_root}"

echo "[apps/bt] pip install -r requirements-dev.txt"
PIP_CACHE_DIR="${pip_cache_dir}" "${venv_path}/bin/python" -m pip install -r "${bt_root}/requirements-dev.txt"
