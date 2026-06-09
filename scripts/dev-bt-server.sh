#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config_file="${TRADING25_CONFIG_FILE:-${HOME}/.config/trading25/config.env}"
jquants_auth_mode="missing"
env_jquants_api_key="${JQUANTS_API_KEY:-}"

if [[ -f "${config_file}" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "${config_file}"
  set +a
fi

if [[ -z "${env_jquants_api_key}" && -n "${JQUANTS_API_KEY:-}" ]]; then
  echo "Do not set JQUANTS_API_KEY in ${config_file}; store it in macOS Keychain or pass a temporary environment override." >&2
  exit 1
fi
if [[ -n "${env_jquants_api_key}" ]]; then
  export JQUANTS_API_KEY="${env_jquants_api_key}"
fi

jquants_keychain_service="${TRADING25_JQUANTS_API_KEY_KEYCHAIN_SERVICE:-trading25-jquants-api-key}"
jquants_keychain_account="${TRADING25_JQUANTS_API_KEY_KEYCHAIN_ACCOUNT:-trading25}"

if [[ -n "${JQUANTS_API_KEY:-}" ]]; then
  jquants_auth_mode="env"
else
  if ! command -v security >/dev/null 2>&1; then
    echo "macOS security command is required to read JQUANTS_API_KEY from Keychain." >&2
    exit 1
  fi

  if [[ -n "${jquants_keychain_account}" ]]; then
    if ! JQUANTS_API_KEY="$(security find-generic-password -s "${jquants_keychain_service}" -a "${jquants_keychain_account}" -w 2>/dev/null)"; then
      echo "Could not read JQUANTS_API_KEY from Keychain service '${jquants_keychain_service}' account '${jquants_keychain_account}'." >&2
      exit 1
    fi
  elif ! JQUANTS_API_KEY="$(security find-generic-password -s "${jquants_keychain_service}" -w 2>/dev/null)"; then
    echo "Could not read JQUANTS_API_KEY from Keychain service '${jquants_keychain_service}'." >&2
    exit 1
  fi

  if [[ -z "${JQUANTS_API_KEY}" ]]; then
    echo "Keychain item '${jquants_keychain_service}' returned an empty JQUANTS_API_KEY." >&2
    exit 1
  fi

  export JQUANTS_API_KEY
  jquants_auth_mode="keychain"
fi

port="${BT_PORT:-3002}"
export TRADING25_FORCE_COLOR="${TRADING25_FORCE_COLOR:-1}"

if [[ "${TRADING25_DRY_RUN:-}" == "1" ]]; then
  printf 'J-Quants auth mode: %s\n' "${jquants_auth_mode}"
  printf 'TRADING25_FORCE_COLOR=%s uv run --project apps/bt bt server --port %s\n' "${TRADING25_FORCE_COLOR}" "${port}"
  exit 0
fi

cd "${repo_root}"
exec uv run --project apps/bt bt server --port "${port}"
