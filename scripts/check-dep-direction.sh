#!/usr/bin/env bash
# check-dep-direction.sh — Detect ts→bt dependency direction violations
#
# Enforces ADR-001 Pattern A: bt→ts allowed, ts→bt must be in allowlist.
# Archived package `apps/ts/packages/api` is excluded from scanning.
# Exit 0 if all references are allowed, exit 1 on violations or stale entries.
#
# Compatible with bash 3.2+ (macOS default).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
allowlist_file="${repo_root}/scripts/dep-direction-allowlist.txt"
search_dir="${repo_root}/apps/ts/packages"

# Patterns that indicate ts→bt dependency
combined_pattern='localhost:3002|127\.0\.0\.1:3002|BT_API_URL|/bt/api/|BacktestClient|@trading25/shared/clients/backtest|@trading25/clients-ts/backtest|bt-api-types|backtest/generated'

# ── Load allowlist (strip comments and blank lines) ─────
allowlist_clean=$(sed -e 's/#.*$//' -e '/^[[:space:]]*$/d' "$allowlist_file" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

# ── Find ts→bt references ──────────────────────────────
matched_files=$(
  grep -rlE "$combined_pattern" "$search_dir" \
    --include='*.ts' --include='*.tsx' \
    --exclude-dir=node_modules --exclude-dir=dist --exclude-dir=api \
    2>/dev/null || true
)

violations=0
matched_allowed_file=$(mktemp)
trap 'rm -f "$matched_allowed_file"' EXIT

while IFS= read -r abs_path; do
  [[ -z "$abs_path" ]] && continue
  rel_path="${abs_path#"${repo_root}/"}"

  if echo "$allowlist_clean" | grep -qxF "$rel_path"; then
    echo "$rel_path" >> "$matched_allowed_file"
  else
    if (( violations == 0 )); then
      echo "ERROR: ts→bt dependency direction violations found:"
    fi
    echo "  ${rel_path}"
    violations=$((violations + 1))
  fi
done <<< "$matched_files"

if (( violations > 0 )); then
  echo ""
  echo "Add these files to scripts/dep-direction-allowlist.txt with a PR comment,"
  echo "or remove the bt reference from the file."
  exit 1
fi

# ── Staleness check ─────────────────────────────────────
stale=0
allowlist_count=0

while IFS= read -r entry; do
  [[ -z "$entry" ]] && continue
  allowlist_count=$((allowlist_count + 1))
  abs_entry="${repo_root}/${entry}"

  if [[ ! -f "$abs_entry" ]]; then
    if (( stale == 0 )); then
      echo "ERROR: Stale entries in allowlist (file does not exist):"
    fi
    echo "  ${entry}"
    stale=$((stale + 1))
  elif ! grep -qxF "$entry" "$matched_allowed_file"; then
    if (( stale == 0 )); then
      echo "ERROR: Stale entries in allowlist (no pattern match):"
    fi
    echo "  ${entry}"
    stale=$((stale + 1))
  fi
done <<< "$allowlist_clean"

if (( stale > 0 )); then
  echo ""
  echo "Remove these entries from scripts/dep-direction-allowlist.txt."
  exit 1
fi

echo "dep-direction: OK (${allowlist_count} allowed, 0 violations, 0 stale)"
exit 0
