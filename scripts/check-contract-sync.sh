#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bt_root="${repo_root}/apps/bt"
snapshot_path="${repo_root}/apps/ts/packages/shared/openapi/bt-openapi.json"
generated_types_path="${repo_root}/apps/ts/packages/shared/src/clients/backtest/generated/bt-api-types.ts"

tmp_openapi="$(mktemp "/tmp/bt-openapi-generated.XXXXXX.json")"
tmp_openapi_norm="$(mktemp "/tmp/bt-openapi-generated-normalized.XXXXXX.json")"
tmp_snapshot_norm="$(mktemp "/tmp/bt-openapi-snapshot-normalized.XXXXXX.json")"
trap 'rm -f "${tmp_openapi}" "${tmp_openapi_norm}" "${tmp_snapshot_norm}"' EXIT

echo "[contract] Export OpenAPI from bt source"
(
  cd "${bt_root}"
  UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}" uv run python scripts/export_openapi.py --output "${tmp_openapi}"
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
  echo "[contract] OpenAPI snapshot is stale. Run: bun run --filter @trading25/shared bt:sync" >&2
  diff -u "${tmp_snapshot_norm}" "${tmp_openapi_norm}" || true
  exit 1
fi

echo "[contract] Verify OpenAPI compatibility against frozen baseline"
python3 "${repo_root}/scripts/verify-openapi-compat.py" --fastapi-file "${tmp_openapi}"

echo "[contract] Regenerate TypeScript types from committed snapshot"
(
  cd "${repo_root}/apps/ts"
  bun run --filter @trading25/shared bt:generate-types
)

if ! git -C "${repo_root}" diff --exit-code -- "${generated_types_path}" > /dev/null; then
  echo "[contract] Generated types are not up to date. Run: bun run --filter @trading25/shared bt:sync" >&2
  git -C "${repo_root}" --no-pager diff -- "${generated_types_path}" || true
  exit 1
fi

echo "[contract] PASS"
