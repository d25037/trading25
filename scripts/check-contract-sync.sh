#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bt_root="${repo_root}/apps/bt"
snapshot_path="${repo_root}/apps/ts/packages/contracts/openapi/bt-openapi.json"

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/bt-contract-sync.XXXXXX")"
trap 'rm -rf "${tmp_dir}"' EXIT

tmp_openapi="${tmp_dir}/bt-openapi.json"
tmp_openapi_norm="${tmp_dir}/bt-openapi-normalized.json"
tmp_snapshot_norm="${tmp_dir}/bt-openapi-snapshot-normalized.json"

echo "[contract] Export OpenAPI from bt source"
(
  cd "${bt_root}"
  BT_ENABLE_RESEARCH_API=1 \
    UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}" \
    uv run python scripts/export_openapi.py --output "${tmp_openapi}"
)

normalize_json() {
  local source_file="$1"
  local output_file="$2"
  python3 - "$source_file" "$output_file" <<'PY'
import json
import sys
from pathlib import Path

source = Path(sys.argv[1])
output = Path(sys.argv[2])
with source.open() as f:
    data = json.load(f)
with output.open("w") as f:
    json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
    f.write("\n")
PY
}

normalize_json "${tmp_openapi}" "${tmp_openapi_norm}"
normalize_json "${snapshot_path}" "${tmp_snapshot_norm}"

if ! cmp -s "${tmp_openapi_norm}" "${tmp_snapshot_norm}"; then
  echo "[contract] OpenAPI snapshot is stale. Run: bun run --filter @trading25/contracts bt:sync" >&2
  diff -u "${tmp_snapshot_norm}" "${tmp_openapi_norm}" || true
  exit 1
fi

echo "[contract] Check generated TypeScript types against committed snapshot"
if ! (
  cd "${repo_root}/apps/ts"
  bun run --filter @trading25/contracts bt:generate-types -- --check
); then
  echo "[contract] Generated types are not up to date. Run: bun run --filter @trading25/contracts bt:sync" >&2
  exit 1
fi

echo "[contract] Check for handwritten TypeScript wire DTO duplicates"
python3 "${repo_root}/scripts/check-ts-wire-contracts.py" \
  --openapi "${snapshot_path}" \
  --contracts "${repo_root}/apps/ts/packages/contracts/src/types/api-response-types.ts" \
    "${repo_root}/apps/ts/packages/contracts/src/types/api-types.ts" \
  --api-clients "${repo_root}/apps/ts/packages/api-clients/src/analytics/types.ts" \
    "${repo_root}/apps/ts/packages/api-clients/src/backtest/types.ts" \
    "${repo_root}/apps/ts/packages/api-clients/src/backtest/fundamentals-types.ts"

if [[ -n "${OPENAPI_BASE_SNAPSHOT:-}" ]]; then
  echo "[contract] Check OpenAPI backward compatibility"
  python3 "${repo_root}/scripts/openapi_compat.py" \
    --base "${OPENAPI_BASE_SNAPSHOT}" \
    --candidate "${tmp_openapi}" \
    --approvals "${repo_root}/contracts/openapi-breaking-approvals.json" \
    --today "${OPENAPI_COMPAT_TODAY:-$(date -u +%F)}"
fi

echo "[contract] PASS"
