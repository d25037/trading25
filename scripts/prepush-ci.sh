#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
uv_cache_dir="${UV_CACHE_DIR:-/tmp/uv-cache}"

include_security=false
include_web_e2e=false
include_research=false
skip_install=false
force_full=false

usage() {
  cat <<'EOF'
Usage: scripts/prepush-ci.sh [--full] [--research] [--security] [--web-e2e] [--skip-install]

Run local checks before push using the same changed-file tiers that CI uses.
By default, changed files are compared against PREPUSH_BASE_REF or origin/main.

Options:
  --full         Force all tiers, and include research, security audits, and web E2E smoke.
  --research     Force research checks even when changed-file scope is not research.
  --security     Include dependency audit and secret scan.
  --web-e2e      Include Playwright smoke with bt server startup.
  --skip-install Assume deps are already prepared and skip shared install.
  --help         Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --full)
      force_full=true
      include_research=true
      include_security=true
      include_web_e2e=true
      ;;
    --research)
      include_research=true
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
changed_files_path=""
product_ci=false
research_ci=false
contracts_ci=false
security_ci=false
docs_only=false

cleanup() {
  if [[ -n "${bt_server_pid}" ]] && kill -0 "${bt_server_pid}" >/dev/null 2>&1; then
    kill "${bt_server_pid}" >/dev/null 2>&1 || true
    wait "${bt_server_pid}" 2>/dev/null || true
  fi
  if [[ -n "${secret_scan_dir}" && -d "${secret_scan_dir}" ]]; then
    rm -rf "${secret_scan_dir}"
  fi
  if [[ -n "${changed_files_path}" && -f "${changed_files_path}" ]]; then
    rm -f "${changed_files_path}"
  fi
  rm -rf "${coverage_dir}"
}
trap cleanup EXIT

run_step() {
  local label="$1"
  shift
  echo
  echo "==> [${label}]"
  local started_at="${SECONDS}"
  set +e
  "$@"
  local status=$?
  set -e
  local elapsed=$((SECONDS - started_at))
  if [[ "${status}" -eq 0 ]]; then
    echo "==> [${label}] done in ${elapsed}s"
  else
    echo "==> [${label}] failed in ${elapsed}s" >&2
  fi
  return "${status}"
}

ensure_command() {
  local command_name="$1"
  local purpose="$2"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "[prepush-ci] Missing required command '${command_name}' for ${purpose}." >&2
    exit 2
  fi
}

install_ts_deps() {
  run_step "deps:apps/ts" bash -lc "cd \"${repo_root}/apps/ts\" && bun install --frozen-lockfile"
}

install_bt_deps() {
  run_step "deps:apps/bt" bash -lc "cd \"${repo_root}/apps/bt\" && UV_CACHE_DIR=\"${uv_cache_dir}\" uv sync --locked"
}

install_deps_for_scope() {
  if ${skip_install}; then
    return
  fi

  if ${product_ci} || ${contracts_ci} || ${security_ci} || ${include_security} || ${include_web_e2e}; then
    install_ts_deps
  fi
  if ${product_ci} || ${research_ci} || ${contracts_ci} || ${security_ci} || ${include_security} || ${include_web_e2e}; then
    install_bt_deps
  fi
}

collect_changed_files() {
  changed_files_path="$(mktemp "/tmp/trading25-prepush-changed-files.XXXXXX")"

  if ${force_full}; then
    git -C "${repo_root}" ls-files >"${changed_files_path}"
    return
  fi

  local base_ref="${PREPUSH_BASE_REF:-origin/main}"
  local base_commit=""
  if git -C "${repo_root}" rev-parse --verify "${base_ref}^{commit}" >/dev/null 2>&1; then
    base_commit="$(git -C "${repo_root}" merge-base "${base_ref}" HEAD || true)"
  fi

  {
    if [[ -n "${base_commit}" ]]; then
      git -C "${repo_root}" diff --name-only "${base_commit}" HEAD
    else
      git -C "${repo_root}" diff --name-only HEAD
    fi
    git -C "${repo_root}" diff --name-only
    git -C "${repo_root}" diff --cached --name-only
    git -C "${repo_root}" ls-files --others --exclude-standard
  } | sort -u >"${changed_files_path}"
}

classify_scope() {
  local line
  while IFS= read -r line; do
    case "${line}" in
      product_ci=true) product_ci=true ;;
      product_ci=false) product_ci=false ;;
      research_ci=true) research_ci=true ;;
      research_ci=false) research_ci=false ;;
      contracts_ci=true) contracts_ci=true ;;
      contracts_ci=false) contracts_ci=false ;;
      security_ci=true) security_ci=true ;;
      security_ci=false) security_ci=false ;;
      docs_only=true) docs_only=true ;;
      docs_only=false) docs_only=false ;;
    esac
  done < <(python3 "${repo_root}/scripts/ci/changed-scope.py" <"${changed_files_path}")

  echo "[prepush-ci] scope: product=${product_ci} research=${research_ci} contracts=${contracts_ci} security=${security_ci} docs_only=${docs_only}"
}

ensure_commands_for_scope() {
  ensure_command git "changed-file detection"
  ensure_command python3 "scope classification"
  ensure_command uv "maintainability snapshot check"

  if ${product_ci} || ${contracts_ci} || ${security_ci} || ${include_security} || ${include_web_e2e}; then
    ensure_command bun "apps/ts checks"
  fi
  if ${product_ci} || ${research_ci} || ${contracts_ci} || ${security_ci} || ${include_security} || ${include_web_e2e}; then
    ensure_command uv "apps/bt checks"
  fi
}

run_maintainability_guardrail() {
  run_step "quality:maintainability-snapshot" uv run --project "${repo_root}/apps/bt" python "${repo_root}/scripts/maintainability_snapshot.py" --root "${repo_root}" --json-out "${repo_root}/docs/maintainability-snapshot-latest.json" --md-out "${repo_root}/docs/maintainability-snapshot-latest.md" --check
}

run_repo_guardrails() {
  run_step "quality:audit-skills" python3 "${repo_root}/scripts/skills/audit_skills.py" --strict-legacy
  run_step "quality:privacy-leak-check" python3 "${repo_root}/scripts/check-privacy-leaks.py"
}

run_quality_suite() {
  run_step "quality:lint" "${repo_root}/scripts/lint.sh"
  run_step "quality:typecheck" "${repo_root}/scripts/typecheck.sh"
}

run_contract_suite() {
  run_step "contract-tests" "${repo_root}/scripts/check-contract-sync.sh"
}

run_product_test_suite() {
  run_step "golden-dataset-regression" "${repo_root}/scripts/test-golden-regression.sh"
  run_step \
    "package-unit-tests" \
    env \
    CI_DEPS_READY=1 \
    SKIP_TS_TESTS=1 \
    BT_UNIT_TEST_SHARDS="${BT_UNIT_TEST_SHARDS:-3}" \
    BT_COVERAGE_DATA_FILE="${coverage_dir}/.coverage.unit" \
    "${repo_root}/scripts/test-packages.sh"
  run_step \
    "app-integration-tests" \
    env \
    CI_DEPS_READY=1 \
    SKIP_TS_TESTS=1 \
    BT_COVERAGE_DATA_FILE="${coverage_dir}/.coverage.app" \
    "${repo_root}/scripts/test-apps.sh"
  run_step \
    "coverage-gate" \
    env \
    CI_DEPS_READY=1 \
    BT_COVERAGE_INPUT_DIR="${coverage_dir}" \
    "${repo_root}/scripts/coverage-gate.sh"
}

collect_fast_research_tests() {
  python3 "${repo_root}/scripts/ci/test_targets.py" --group bt-fast-research
}

collect_mapped_research_tests() {
  python3 "${repo_root}/scripts/ci/research-test-targets.py" <"${changed_files_path}"
}

run_research_suite() {
  local -a fast_research_tests=()
  local -a mapped_research_tests=()
  local test_path

  while IFS= read -r test_path; do
    fast_research_tests+=("${test_path}")
  done < <(collect_fast_research_tests)
  while IFS= read -r test_path; do
    mapped_research_tests+=("${test_path}")
  done < <(collect_mapped_research_tests)

  run_step "quality:research-guardrails" python3 "${repo_root}/scripts/check-research-guardrails.py"
  run_step \
    "bt-research-tests:fast" \
    env \
    BT_PYTEST_FAST=1 \
    "${repo_root}/scripts/bt-pytest.sh" "${fast_research_tests[@]}"

  if [[ "${#mapped_research_tests[@]}" -gt 0 ]]; then
    run_step \
      "bt-research-tests:mapped-local" \
      env \
      BT_PYTEST_FAST=1 \
      "${repo_root}/scripts/bt-pytest.sh" "${mapped_research_tests[@]}"
  else
    echo "[prepush-ci] no changed experiment-level research pytest targets."
  fi
}

run_security_suite() {
  ensure_command docker "secret-scan"

  run_step "dependency-audit:bun" bash -lc "cd \"${repo_root}/apps/ts\" && bun audit --audit-level=moderate"
  run_step \
    "dependency-audit:pip" \
    bash -lc "cd \"${repo_root}/apps/bt\" && UV_CACHE_DIR=\"${uv_cache_dir}\" uv run --locked --with pip-audit pip-audit --ignore-vuln CVE-2026-4539 --ignore-vuln CVE-2026-3219"
  secret_scan_dir="$(mktemp -d "/tmp/trading25-prepush-gitleaks.XXXXXX")"
  git -C "${repo_root}" archive --format=tar HEAD | tar -xf - -C "${secret_scan_dir}"
  run_step \
    "secret-scan" \
    docker run --rm -v "${secret_scan_dir}:/repo:ro" ghcr.io/gitleaks/gitleaks:v8.25.1 \
      detect --source="/repo" --config="/repo/.gitleaks.toml" --no-git --redact --verbose
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
  ensure_command git "changed-file detection"
  ensure_command python3 "scope classification"

  collect_changed_files
  classify_scope
  ensure_commands_for_scope
  run_maintainability_guardrail

  if ${docs_only} && ! ${include_security} && ! ${include_web_e2e}; then
    echo "[prepush-ci] docs-only change; no local CI tiers selected."
    echo
    echo "[prepush-ci] PASS"
    return
  fi

  install_deps_for_scope

  run_repo_guardrails

  if ${product_ci}; then
    run_quality_suite
    run_product_test_suite
  fi

  if ${contracts_ci}; then
    run_contract_suite
  fi

  if ${research_ci} || ${include_research}; then
    run_research_suite
  fi

  if ${security_ci} || ${include_security}; then
    run_security_suite
  fi

  if ${include_web_e2e}; then
    run_web_e2e_suite
  fi

  echo
  echo "[prepush-ci] PASS"
}

main
