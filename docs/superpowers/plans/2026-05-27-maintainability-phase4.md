# Maintainability Phase 4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce Data Plane change risk by separating market DuckDB schema/setup and dataset snapshot copy orchestration from oversized service modules.

**Architecture:** Keep public `MarketDb` and dataset build APIs unchanged. Extract schema constants/helpers and dataset copy stages into focused internal modules or functions that are covered by existing DB and dataset builder tests.

**Tech Stack:** Python 3.12, DuckDB, pytest, ruff, pyright, existing maintainability snapshot tooling.

---

## Phase 4 Scope

Phase 4 is intentionally not a broad repo cleanup. It targets the two highest-value Data Plane maintenance risks that still show up in the latest snapshot:

- `apps/bt/src/infrastructure/db/market/market_db.py`
- `apps/bt/src/application/services/dataset_builder_service.py`

Do not widen into research runner files just to reduce count buckets. Those files need a separate research-workflow phase if they become operationally painful.

## Numeric Targets

Starting point is `docs/maintainability-snapshot-latest.md` after Phase 3:

| metric | phase 3 actual | phase 4 target |
| --- | ---: | ---: |
| top hotspot file score | 7,115 | <= 6,500 |
| `market_db.py` hotspot score | 7,115 | <= 6,500 |
| `dataset_builder_service.py` max block code lines | 347 | <= 260 |
| `dataset_builder_service.py` branch score | 184 | <= 150 |
| functions/blocks branch score >= 50 | 3 | <= 2 |

File-count buckets are secondary in this phase. A split that improves Data Plane boundaries is acceptable even if `files >= 1000` is unchanged.

## Actual Results

Completed on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | phase 3 actual | phase 4 actual | phase 4 target |
| --- | ---: | ---: | ---: |
| repo top hotspot file score | 7,115 | 7,056 | <= 6,500 |
| `market_db.py` hotspot score | 7,115 | 6,430 | <= 6,500 |
| `dataset_builder_service.py` max block code lines | 347 | 178 | <= 260 |
| `dataset_builder_service.py` branch score | 184 | 120 | <= 150 |
| functions/blocks branch score >= 50 | 3 | 2 | <= 2 |

The Data Plane-specific goals were met. The repo-wide top hotspot target was not forced because after `market_db.py` dropped, the top file became `sync_strategies.py`; reducing it below 6,500 requires a sync-orchestration phase rather than more market schema or dataset-builder extraction.

## Tasks

### Task 1: Characterize Baseline

**Files:**

- Read: `docs/maintainability-snapshot-latest.md`
- Test: `apps/bt/tests/unit/server/test_dataset_builder_service.py`
- Test: `apps/bt/tests/unit/server/test_dataset_builder_service_branches.py`
- Test: `apps/bt/tests/unit/server/db/test_market_db.py`
- Test: `apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py`

- [x] **Step 1: Run focused baseline tests**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/server/test_dataset_builder_service.py \
  apps/bt/tests/unit/server/test_dataset_builder_service_branches.py \
  apps/bt/tests/unit/server/db/test_market_db.py \
  apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py -q
```

Expected: tests pass before edits.

### Task 2: Split Market DB Schema Setup

**Files:**

- Create: `apps/bt/src/infrastructure/db/market/market_schema.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Test: `apps/bt/tests/unit/server/db/test_market_db.py`
- Test: `apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py`

- [x] **Step 1: Move schema constants and setup helpers**

Move schema-version constants, table lists, column lists, and `ensure_schema` helper logic into `market_schema.py`. Keep `MarketDb.ensure_schema()` as the public method that delegates into the helper.

- [x] **Step 2: Preserve public imports**

Keep `METADATA_KEYS`, `LOCAL_STOCK_PRICE_ADJUSTMENT_MODE`, `MARKET_SCHEMA_VERSION`, and `INCOMPATIBLE_MARKET_SCHEMA_VERSION` importable from `market_db.py` for existing callers.

- [x] **Step 3: Verify DB behavior**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/server/db/test_market_db.py \
  apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py -q
```

Expected: tests pass and schema-version behavior remains unchanged.

### Task 3: Split Dataset Copy Stages

**Files:**

- Create: `apps/bt/src/application/services/dataset_builder_copy_stages.py`
- Modify: `apps/bt/src/application/services/dataset_builder_service.py`
- Test: `apps/bt/tests/unit/server/test_dataset_builder_service.py`
- Test: `apps/bt/tests/unit/server/test_dataset_builder_service_branches.py`

- [x] **Step 1: Extract stock-data copy stage**

Move the batch stock-data copy loop out of `_build_dataset()` into an async helper that returns processed count and warning inputs.

- [x] **Step 2: Extract optional copy stages**

Move TOPIX, indices, statements, and margin copy branches into async helpers. Keep cancellation checks and progress messages byte-for-byte compatible where practical.

- [x] **Step 3: Verify dataset behavior**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/server/test_dataset_builder_service.py \
  apps/bt/tests/unit/server/test_dataset_builder_service_branches.py -q
```

Expected: tests pass and `_build_dataset` drops below the Phase 4 max-block target.

### Task 4: Re-measure and Record

**Files:**

- Modify: `docs/maintainability-snapshot-latest.json`
- Modify: `docs/maintainability-snapshot-latest.md`
- Modify: `docs/superpowers/plans/2026-05-27-maintainability-refactor-targets.md`

- [x] **Step 1: Regenerate maintainability snapshot**

```bash
python3 scripts/maintainability_snapshot.py \
  --json-out docs/maintainability-snapshot-latest.json \
  --md-out docs/maintainability-snapshot-latest.md
```

Expected: command exits 0 and Phase 4 target rows are measurable.

- [x] **Step 2: Run quality gates**

```bash
uv run --project apps/bt ruff check \
  apps/bt/src/infrastructure/db/market/market_db.py \
  apps/bt/src/infrastructure/db/market/market_schema.py \
  apps/bt/src/application/services/dataset_builder_service.py \
  apps/bt/src/application/services/dataset_builder_copy_stages.py
uv run --project apps/bt pyright \
  apps/bt/src/infrastructure/db/market/market_db.py \
  apps/bt/src/infrastructure/db/market/market_schema.py \
  apps/bt/src/application/services/dataset_builder_service.py \
  apps/bt/src/application/services/dataset_builder_copy_stages.py
```

Expected: both commands pass.
