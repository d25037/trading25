# Maintainability Phase 5 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce market sync orchestration risk without changing sync behavior, fetch policy, progress semantics, or public API payloads.

**Architecture:** Keep `sync_strategies.py` as the public compatibility module for existing tests/imports. Move stock-master sync execution and index-master fallback helper responsibilities into focused internal modules, then re-export the existing helper names from `sync_strategies.py`.

**Tech Stack:** Python 3.12, FastAPI service layer, DuckDB market sync helpers, pytest, ruff, pyright, maintainability snapshot tooling.

---

## Phase 5 Scope

Phase 5 is a sync-orchestration maintainability slice. It does not change what sync fetches, how bulk/rest is selected, or which metadata keys are written.

Primary targets:

- `apps/bt/src/application/services/sync_strategies.py`
- stock-master daily sync helper currently embedded in `sync_strategies.py`
- index-master placeholder backfill helper currently embedded in `sync_strategies.py`

Out of scope:

- Changing initial/incremental/repair mode resolution.
- Changing bulk/rest planner policy.
- Changing J-Quants endpoint params.
- Changing `LAST_SYNC_DATE`, `LAST_STOCKS_REFRESH`, failed-date, or fundamentals metadata semantics.
- Moving research runner files just to improve repo-wide counts.

## Numeric Targets

Starting point is `docs/maintainability-snapshot-latest.md` after Phase 4:

| metric | phase 4 actual | phase 5 target |
| --- | ---: | ---: |
| `sync_strategies.py` hotspot score | 7,056 | <= 6,200 |
| repo top hotspot file score | 7,056 | <= 6,500 |
| `_sync_daily_stock_master` code lines | 244 | moved out of `sync_strategies.py` |
| `IncrementalSyncStrategy.execute` code lines | 376 | unchanged or lower; do not force risky split |
| functions/blocks branch score >= 50 | 2 | <= 2 |

## Actual Results

Completed on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | phase 4 actual | phase 5 actual | phase 5 target |
| --- | ---: | ---: | ---: |
| `sync_strategies.py` hotspot score | 7,056 | 5,784 | <= 6,200 |
| repo top hotspot file score | 7,056 | 6,938 | <= 6,500 |
| `_sync_daily_stock_master` code lines in `sync_strategies.py` | 244 | moved out | moved out |
| `IncrementalSyncStrategy.execute` code lines | 376 | 376 | unchanged or lower |
| functions/blocks branch score >= 50 | 2 | 2 | <= 2 |

The sync-specific target was met without splitting `IncrementalSyncStrategy.execute`. The repo-wide top hotspot target was not forced because the top file moved to `annual_first_open_last_close_fundamental_panel.py`; that belongs to a research-runner phase, not this sync-orchestration phase.

## Tasks

### Task 1: Characterize Sync Baseline

**Files:**

- Test: `apps/bt/tests/unit/server/services/test_sync_strategies.py`
- Test: `apps/bt/tests/unit/server/test_routes_db_sync.py`

- [x] **Step 1: Run focused baseline**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/server/services/test_sync_strategies.py \
  apps/bt/tests/unit/server/test_routes_db_sync.py -q
```

Expected: tests pass before edits.

### Task 2: Extract Stock-Master Daily Sync

**Files:**

- Create: `apps/bt/src/application/services/sync_stock_master.py`
- Modify: `apps/bt/src/application/services/sync_strategies.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_strategies.py`

- [x] **Step 1: Move `_sync_daily_stock_master` behavior**

Move the implementation into `sync_stock_master.py`. Keep the `_sync_daily_stock_master` name importable from `sync_strategies.py` by re-exporting the helper.

- [x] **Step 2: Preserve behavior**

Do not change progress stage names, bulk planner arguments, fallback refusal threshold, returned dict keys, metadata writes, or cancelled result semantics.

- [x] **Step 3: Verify focused sync helper tests**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/server/services/test_sync_strategies.py -q
```

Expected: tests pass.

### Task 3: Extract Index-Master Fallback Helpers

**Files:**

- Create: `apps/bt/src/application/services/sync_index_master_backfill.py`
- Modify: `apps/bt/src/application/services/sync_strategies.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_strategies.py`

- [x] **Step 1: Move fallback helper behavior**

Move `_build_fallback_index_master_rows` and `_upsert_indices_rows_with_master_backfill` into `sync_index_master_backfill.py`. Re-export `_build_fallback_index_master_rows` from `sync_strategies.py` for existing tests/imports.

- [x] **Step 2: Preserve publish path**

Keep placeholder master row creation, master upsert, `known_master_codes` mutation, warning log, and `sync_publish_helpers._publish_indices_rows` behavior unchanged.

- [x] **Step 3: Verify focused sync tests**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/server/services/test_sync_strategies.py -q
```

Expected: tests pass.

### Task 4: Re-measure and Validate

**Files:**

- Modify: `docs/maintainability-snapshot-latest.json`
- Modify: `docs/maintainability-snapshot-latest.md`
- Modify: `docs/superpowers/plans/2026-05-27-maintainability-refactor-targets.md`

- [x] **Step 1: Regenerate snapshot**

```bash
python3 scripts/maintainability_snapshot.py \
  --json-out docs/maintainability-snapshot-latest.json \
  --md-out docs/maintainability-snapshot-latest.md
```

Expected: `sync_strategies.py` falls below the Phase 5 hotspot target or the miss is documented with a sync-specific reason.

- [x] **Step 2: Run validation gates**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/server/services/test_sync_strategies.py \
  apps/bt/tests/unit/server/test_routes_db_sync.py -q
uv run --project apps/bt ruff check \
  apps/bt/src/application/services/sync_strategies.py \
  apps/bt/src/application/services/sync_stock_master.py \
  apps/bt/src/application/services/sync_index_master_backfill.py
uv run --project apps/bt pyright \
  apps/bt/src/application/services/sync_strategies.py \
  apps/bt/src/application/services/sync_stock_master.py \
  apps/bt/src/application/services/sync_index_master_backfill.py
```

Expected: all pass.
