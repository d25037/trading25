#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config_file="${TRADING25_CONFIG_FILE:-${HOME}/.config/trading25/config.env}"
secrets_file="${TRADING25_SECRETS_FILE:-${HOME}/.config/trading25/secrets.env}"
op_auth_mode="desktop-app-or-existing-op-session"

if [[ -f "${config_file}" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "${config_file}"
  set +a
fi

service_account_keychain_service="${TRADING25_OP_SERVICE_ACCOUNT_TOKEN_KEYCHAIN_SERVICE:-}"
service_account_keychain_account="${TRADING25_OP_SERVICE_ACCOUNT_TOKEN_KEYCHAIN_ACCOUNT:-${USER:-}}"

if [[ ! -e "${secrets_file}" ]]; then
  echo "Missing secrets env file: ${secrets_file}" >&2
  echo "Create it with op:// references, for example JQUANTS_API_KEY=op://... ." >&2
  exit 1
fi

if [[ -n "${OP_SERVICE_ACCOUNT_TOKEN:-}" ]]; then
  op_auth_mode="service-account-env"
elif [[ -n "${service_account_keychain_service}" ]]; then
  if ! command -v security >/dev/null 2>&1; then
    echo "macOS security command is required to read OP_SERVICE_ACCOUNT_TOKEN from Keychain." >&2
    exit 1
  fi

  if [[ -n "${service_account_keychain_account}" ]]; then
    if ! OP_SERVICE_ACCOUNT_TOKEN="$(security find-generic-password -s "${service_account_keychain_service}" -a "${service_account_keychain_account}" -w 2>/dev/null)"; then
      echo "Could not read OP_SERVICE_ACCOUNT_TOKEN from Keychain service '${service_account_keychain_service}' account '${service_account_keychain_account}'." >&2
      exit 1
    fi
  elif ! OP_SERVICE_ACCOUNT_TOKEN="$(security find-generic-password -s "${service_account_keychain_service}" -w 2>/dev/null)"; then
    echo "Could not read OP_SERVICE_ACCOUNT_TOKEN from Keychain service '${service_account_keychain_service}'." >&2
    exit 1
  fi

  if [[ -z "${OP_SERVICE_ACCOUNT_TOKEN}" ]]; then
    echo "Keychain item '${service_account_keychain_service}' returned an empty OP_SERVICE_ACCOUNT_TOKEN." >&2
    exit 1
  fi

  export OP_SERVICE_ACCOUNT_TOKEN
  op_auth_mode="service-account-keychain"
fi

port="${BT_PORT:-3002}"
export TRADING25_FORCE_COLOR="${TRADING25_FORCE_COLOR:-1}"

if [[ "${TRADING25_DRY_RUN:-}" == "1" ]]; then
  printf '1Password auth mode: %s\n' "${op_auth_mode}"
  printf 'TRADING25_FORCE_COLOR=%s op run --env-file %s -- uv run --project apps/bt bt server --port %s\n' "${TRADING25_FORCE_COLOR}" "${secrets_file}" "${port}"
  exit 0
fi

cd "${repo_root}"
exec op run --env-file "${secrets_file}" -- uv run --project apps/bt bt server --port "${port}"
