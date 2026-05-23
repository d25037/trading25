#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config_file="${TRADING25_CONFIG_FILE:-${HOME}/.config/trading25/config.env}"
secrets_file="${TRADING25_SECRETS_FILE:-${HOME}/.config/trading25/secrets.env}"

if [[ -f "${config_file}" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "${config_file}"
  set +a
fi

if [[ ! -e "${secrets_file}" ]]; then
  echo "Missing secrets env file: ${secrets_file}" >&2
  echo "Create it with op:// references, for example JQUANTS_API_KEY=op://... ." >&2
  exit 1
fi

port="${BT_PORT:-3002}"

if [[ "${TRADING25_DRY_RUN:-}" == "1" ]]; then
  printf 'op run --env-file %s -- uv run --project apps/bt bt server --port %s\n' "${secrets_file}" "${port}"
  exit 0
fi

cd "${repo_root}"
exec op run --env-file "${secrets_file}" -- uv run --project apps/bt bt server --port "${port}"
