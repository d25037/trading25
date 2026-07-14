# Market v4 Cutover Task 7 Report

## Status

Complete. Dataset Info now exposes required event-time lineage from the manifest
validated when `DatasetSnapshotReader` is constructed:

- `schemaVersion: 3`
- `sourceMarketSchemaVersion: 4`
- `stockPriceAdjustmentMode: local_projection_v2_event_time`

The API schema and both generated and handwritten TypeScript contracts use
required literal fields. No nullable compatibility fields, manifest filename
change, or contract-major change was introduced. The Dataset Info dialog renders
all three values.

## TDD evidence

- Backend RED: `test_dataset_service.py` failed because `DatasetSnapshot` had no
  lineage fields and accepted a payload without them.
- Backend GREEN: the focused service suite passed after mapping the validated
  reader manifest into required response fields.
- UI RED: the dialog test failed on missing `Payload schema 3`, `Market schema 4`,
  and adjustment-mode text.
- UI GREEN: the focused hook/dialog suite passed after the lineage rendering was
  added.

## Verification

- `uv run --directory apps/bt pytest tests/unit/server/test_dataset_service.py -q`
  - 10 passed
- `uv run --directory apps/bt ruff check src/entrypoints/http/schemas/dataset.py src/application/services/dataset_service.py src/infrastructure/db/market/dataset_snapshot_reader.py`
  - passed
- `uv run --directory apps/bt pyright src/entrypoints/http/schemas/dataset.py src/application/services/dataset_service.py src/infrastructure/db/market/dataset_snapshot_reader.py`
  - 0 errors, 0 warnings
- `bun run --cwd apps/ts --filter @trading25/contracts bt:check`
  - passed
- `bun run --cwd apps/ts --filter @trading25/web test -- src/hooks/useDataset.test.tsx src/components/Backtest/DatasetInfoDialog.test.tsx`
  - 20 passed
- `bun run --cwd apps/ts quality:typecheck`
  - passed, including dependency audit
- Additional dataset regression suite:
  `test_dataset_snapshot_reader.py`, `test_dataset_resolver.py`, and
  `test_routes_dataset.py`
  - 66 passed

## Concern

`python3 scripts/skills/refresh_skill_references.py --check` reports the existing
`.codex/skills/ts-api-endpoints/references/openapi-paths.md` as stale. Task 7
changes a response schema but no API path, so that unrelated generated reference
was not modified. The untracked `.codex/config.toml` was preserved unchanged and
excluded from the commit.
