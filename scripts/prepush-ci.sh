#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
uv_cache_dir="${UV_CACHE_DIR:-/tmp/uv-cache}"

include_security=false
include_web_e2e=false
skip_install=false

usage() {
  cat <<'EOF'
Usage: scripts/prepush-ci.sh [--full] [--security] [--web-e2e] [--skip-install]

Run local checks before push using the same repo scripts that CI uses.

Options:
  --full         Include security audits and web E2E smoke.
  --security     Include dependency audit and secret scan.
  --web-e2e      Include Playwright smoke with bt server startup.
  --skip-install Assume deps are already prepared and skip shared install.
  --help         Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --full)
      include_security=true
      include_web_e2e=true
      ;;
    --security)
      include_security=true
      ;;
    --web-e2e)
      include_web_e2e=true
      ;;
    --skip-install)
      skip_install=true
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

coverage_dir="$(mktemp -d "/tmp/trading25-prepush-coverage.XXXXXX")"
bt_server_pid=""
secret_scan_dir=""

cleanup() {
  if [[ -n "${bt_server_pid}" ]] && kill -0 "${bt_server_pid}" >/dev/null 2>&1; then
    kill "${bt_server_pid}" >/dev/null 2>&1 || true
    wait "${bt_server_pid}" 2>/dev/null || true
  fi
  if [[ -n "${secret_scan_dir}" && -d "${secret_scan_dir}" ]]; then
    rm -rf "${secret_scan_dir}"
  fi
  rm -rf "${coverage_dir}"
}
trap cleanup EXIT

run_step() {
  local label="$1"
  shift
  echo
  echo "==> [${label}]"
  "$@"
}

ensure_command() {
  local command_name="$1"
  local purpose="$2"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "[prepush-ci] Missing required command '${command_name}' for ${purpose}." >&2
    exit 2
  fi
}

install_shared_deps() {
  run_step "deps:apps/ts" bash -lc "cd \"${repo_root}/apps/ts\" && bun install --frozen-lockfile"
  run_step "deps:apps/bt" bash -lc "cd \"${repo_root}/apps/bt\" && UV_CACHE_DIR=\"${uv_cache_dir}\" uv sync --locked"
}

run_core_suite() {
  run_step "quality:audit-skills" python3 "${repo_root}/scripts/skills/audit_skills.py" --strict-legacy
  run_step "quality:privacy-leak-check" python3 "${repo_root}/scripts/check-privacy-leaks.py"
  run_step "quality:research-guardrails" python3 "${repo_root}/scripts/check-research-guardrails.py"
  run_step "quality:lint" "${repo_root}/scripts/lint.sh"
  run_step "quality:typecheck" "${repo_root}/scripts/typecheck.sh"
  run_step "contract-tests" "${repo_root}/scripts/check-contract-sync.sh"
  run_step "golden-dataset-regression" "${repo_root}/scripts/test-golden-regression.sh"
  run_step \
    "package-unit-tests" \
    env \
    CI_DEPS_READY=1 \
    BT_COVERAGE_DATA_FILE="${coverage_dir}/.coverage.unit" \
    "${repo_root}/scripts/test-packages.sh"
  run_step \
    "app-integration-tests" \
    env \
    CI_DEPS_READY=1 \
    BT_COVERAGE_DATA_FILE="${coverage_dir}/.coverage.app" \
    "${repo_root}/scripts/test-apps.sh"
  run_step \
    "coverage-gate" \
    env \
    CI_DEPS_READY=1 \
    BT_COVERAGE_INPUT_DIR="${coverage_dir}" \
    "${repo_root}/scripts/coverage-gate.sh"
}

run_security_suite() {
  ensure_command docker "secret-scan"

  run_step "dependency-audit:bun" bash -lc "cd \"${repo_root}/apps/ts\" && bun audit --audit-level=moderate"
  run_step \
    "dependency-audit:pip" \
    bash -lc "cd \"${repo_root}/apps/bt\" && UV_CACHE_DIR=\"${uv_cache_dir}\" uv run --locked --with pip-audit pip-audit --ignore-vuln CVE-2026-4539"
  secret_scan_dir="$(mktemp -d "/tmp/trading25-prepush-gitleaks.XXXXXX")"
  git -C "${repo_root}" archive --format=tar HEAD | tar -xf - -C "${secret_scan_dir}"
  run_step \
    "secret-scan" \
    docker run --rm -v "${secret_scan_dir}:/repo:ro" ghcr.io/gitleaks/gitleaks:v8.25.1 \
      detect --source="/repo" --no-git --redact --verbose
}

start_bt_server() {
  local log_path="/tmp/trading25-prepush-bt-server.log"
  (
    cd "${repo_root}/apps/bt"
    UV_CACHE_DIR="${uv_cache_dir}" uv run bt server --port 3002 >"${log_path}" 2>&1
  ) &
  bt_server_pid=$!

  for _ in {1..60}; do
    if curl -fsS http://127.0.0.1:3002/api/health >/dev/null; then
      return 0
    fi
    sleep 1
  done

  echo "[prepush-ci] bt server failed to start for web E2E." >&2
  cat "${log_path}" >&2 || true
  exit 1
}

run_web_e2e_suite() {
  ensure_command curl "web-e2e health check"

  run_step "web-e2e:install-browser" bash -lc "cd \"${repo_root}/apps/ts/packages/web\" && bunx --bun playwright install chromium"
  run_step "web-e2e:start-bt-server" start_bt_server
  run_step "web-e2e:smoke" bash -lc "cd \"${repo_root}/apps/ts\" && bun run --filter @trading25/web e2e:smoke"
}

main() {
  ensure_command bun "apps/ts checks"
  ensure_command uv "apps/bt checks"
  ensure_command python3 "skill audit"

  if ! ${skip_install}; then
    install_shared_deps
  fi

  run_core_suite

  if ${include_security}; then
    run_security_suite
  fi

  if ${include_web_e2e}; then
    run_web_e2e_suite
  fi

  echo
  echo "[prepush-ci] PASS"
}

main
