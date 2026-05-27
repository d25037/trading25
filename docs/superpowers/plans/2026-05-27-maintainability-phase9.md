# Maintainability Phase 9 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Continue high-value spaghetti-code reduction across the remaining expensive hotspots: `sync_strategies.py`, `calculator.py`, `StrategyEditor.tsx`, `SymbolWorkbenchPage.tsx`, and the top research runner giant function.

**Architecture:** Execute this as a sequence of behavior-preserving slices, not a broad rewrite. Start with the current top hotspot, `topix100_sma_ratio_rank_future_close_lightgbm._run_walkforward_research`, because it is both the repo top file and top function hotspot and has focused synthetic tests. Defer `sync_strategies.py`, `calculator.py`, `StrategyEditor.tsx`, and `SymbolWorkbenchPage.tsx` to subsequent slices under the same goal.

**Tech Stack:** Python 3.12, pandas, LightGBM fake ranker tests, pytest, ruff, pyright, maintainability snapshot tooling.

---

## Scope Order

Phase 9 starts the requested multi-target cleanup with the current measured top hotspot:

1. `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py`
2. `apps/bt/src/application/services/sync_strategies.py`
3. `apps/bt/src/domains/fundamentals/calculator.py`
4. `apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx`
5. `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`

The full user goal remains active after this phase unless all five areas have been addressed and verified.

## Phase 9 Targets

Starting point is `docs/maintainability-snapshot-latest.md` after Phase 8.

| metric | phase 8 actual | phase 9 target |
| --- | ---: | ---: |
| repo top hotspot file score | 5,926 | <= 5,800 |
| `topix100_sma_ratio_rank_future_close_lightgbm.py` hotspot score | 5,926 | <= 5,500 |
| `_run_walkforward_research` code lines | 407 | <= 180 |
| `topix100_sma_ratio_rank_future_close_lightgbm.py` code lines | 1,857 | <= 1,750 |
| functions/blocks >= 180 effective code lines | 47 | <= 46 |

If the file-level score remains high because other helper functions are still large, this phase still succeeds if the giant walk-forward function is decomposed and tests prove behavior preservation.

## Actual Results

Completed first Phase 9 slice on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | phase 8 actual | phase 9 actual | phase 9 target |
| --- | ---: | ---: | ---: |
| repo top hotspot file score | 5,926 | 5,784 | <= 5,800 |
| `topix100_sma_ratio_rank_future_close_lightgbm.py` hotspot score | 5,926 | 5,376 | <= 5,500 |
| `_run_walkforward_research` code lines | 407 | 84 | <= 180 |
| `topix100_sma_ratio_rank_future_close_lightgbm.py` code lines | 1,857 | 2,027 | <= 1,750 |
| functions/blocks >= 180 effective code lines | 47 | 46 | <= 46 |

The giant-function and hotspot targets were met. The file code-line target was not met because this first safe slice kept extracted helpers in the same module to avoid a circular import or broad walk-forward module migration; the next research-runner cleanup can move coherent walk-forward helpers into a dedicated module if reducing physical file size becomes the priority.

## Sync Continuation Results

Completed the first `sync_strategies.py` continuation slice on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | before sync slice | after sync slice |
| --- | ---: | ---: |
| repo top hotspot file score | 5,784 | 5,758 |
| `sync_strategies.py` hotspot score | 5,784 | 5,638 |
| `IncrementalSyncStrategy.execute` code lines | 376 | 169 |
| functions/blocks >= 180 effective code lines | 46 | 45 |

The incremental sync indices and margin stages were extracted behind existing helper boundaries. Bulk/rest decision behavior, fallback logging, index publish, metadata update, and sync result shape were preserved.

## Calculator Continuation Results

Completed the first `calculator.py` continuation slice on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | before calculator slice | after calculator slice |
| --- | ---: | ---: |
| `calculator.py` hotspot score | 5,681 | 4,982 |
| `calculator.py` total lines | 1,336 | 1,189 |
| `calculator.py` code lines | 1,214 | 1,073 |
| `calculator.py` branch score | 241 | 210 |

Daily valuation helpers were extracted into `apps/bt/src/domains/fundamentals/daily_valuation.py` while keeping `FundamentalsCalculator` private method names as compatibility facades for existing tests and callers.

## Strategy Editor Continuation Results

Completed the `StrategyEditor.tsx` continuation slice on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | before TS slice | after TS slice |
| --- | ---: | ---: |
| `StrategyEditor.tsx` hotspot score | 5,155 | 2,338 |
| `StrategyEditor.tsx` total lines | 1,716 | 854 |
| `StrategyEditor.tsx` code lines | not recorded | 459 |
| `StrategyEditor.tsx` max block code lines | 301 | 126 |

Signal editing was moved into `StrategyEditorSignals.tsx`, and shared-config field rendering/option assembly was moved into `StrategyEditorSharedConfig.tsx`. The main component now wires data/query state to the dialog instead of owning signal and shared-config orchestration.

## Symbol Workbench Continuation Results

Completed the `SymbolWorkbenchPage.tsx` continuation slice on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | before TS slice | after TS slice |
| --- | ---: | ---: |
| `SymbolWorkbenchPage.tsx` hotspot score | 5,074 | 1,464 |
| `SymbolWorkbenchPage.tsx` total lines | 1,486 | 449 |
| `SymbolWorkbenchPage.tsx` code lines | not recorded | 250 |
| `SymbolWorkbenchPage.tsx` max block code lines | not recorded | 89 |

Header/provenance display was moved into `SymbolWorkbenchHeader.tsx`, and ordered panel rendering/mobile panel selection was moved into `SymbolWorkbenchPanels.tsx`. The page now owns route state, data fetching, refresh, and top-level empty/loading/error branching.

## Initial Sync Continuation Results

Completed the second `sync_strategies.py` continuation slice on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | before initial slice | after initial slice |
| --- | ---: | ---: |
| `sync_strategies.py` hotspot score | 5,638 | 5,430 |
| `InitialSyncStrategy.execute` code lines | 288 | 142 |
| `sync_strategies.py` max function code lines | 288 | 169 |

Initial TOPIX, daily stock master, fundamentals, stock-data bulk/rest, margin, and metadata finalize stages were extracted into private helpers. `InitialSyncStrategy.execute()` now preserves the same stage order and aggregate `SyncResult` shape while owning only stage sequencing and counters.

## Current Top Research Runner Continuation Results

Completed the current top research-runner slice on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | before current runner slice | after current runner slice |
| --- | ---: | ---: |
| `topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py` max function code lines | 396 | 172 |
| `run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research` code lines | 396 | 151 |
| `topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py` hotspot score | not recorded | 3,753 |
| functions/blocks >= 180 effective code lines | 44 | 42 |

Source-frame preparation, candidate simulation, committee member mapping, and walk-forward assembly were extracted into private dataclass-backed helpers. The runner function now coordinates parameter resolution, frame construction, and result assembly without changing bundle table names or runner-facing API.

## Tasks

### Task 1: Characterize Baseline

**Files:**

- Test: `apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py`
- Test: `apps/bt/tests/unit/scripts/test_run_topix100_sma_ratio_rank_future_close_lightgbm.py`

- [x] **Step 1: Inspect current snapshot and tests**

Confirmed current top hotspot and focused test coverage:

- file score: `topix100_sma_ratio_rank_future_close_lightgbm.py` = `5,926`
- top function: `_run_walkforward_research` = `407` code lines

### Task 2: Split Walk-Forward Orchestration

**Files:**

- Modify: `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py`

- [x] **Step 1: Add private result/context dataclasses**

Add private dataclasses near the existing result dataclasses:

- `_WalkforwardBaselineSplitContext`
- `_WalkforwardHorizonResult`
- `_WalkforwardFrameCollections`

- [x] **Step 2: Extract split baseline setup**

Move the baseline train analysis, composite candidate selection, lookup construction, and scheduled date count logic out of `_run_walkforward_research` into `_build_walkforward_baseline_split_context()`.

- [x] **Step 3: Extract horizon execution**

Move the per-horizon training/test frame filtering, coverage record creation, LightGBM fit/predict, feature-importance frame, and baseline test panel construction into `_run_walkforward_horizon()`.

- [x] **Step 4: Extract final assembly**

Move split coverage sorting, ranked frame validation, analysis generation, selected baseline frame sorting, and `Topix100SmaRatioLightgbmWalkforwardResearchResult` construction into `_build_walkforward_result_from_collections()`.

### Task 3: Verify Phase 9 Slice

- [x] Run focused tests:

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py \
  apps/bt/tests/unit/scripts/test_run_topix100_sma_ratio_rank_future_close_lightgbm.py -q
```

- [x] Run static checks:

```bash
uv run --project apps/bt ruff check \
  apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py \
  apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py \
  apps/bt/tests/unit/scripts/test_run_topix100_sma_ratio_rank_future_close_lightgbm.py

uv run --project apps/bt pyright \
  apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py
```

- [x] Re-run maintainability snapshot and update this plan with actual results.

### Task 4: Split Incremental Sync Stages

**Files:**

- Modify: `apps/bt/src/application/services/sync_strategies.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_strategies.py`

- [x] **Step 1: Extract incremental indices stage**

Move catalog seeding, index target resolution, bulk/rest planning, REST fallback loops, execution logging, and index publishing from `IncrementalSyncStrategy.execute()` into `_sync_incremental_indices_stage()` and `_sync_incremental_indices_rest()`.

- [x] **Step 2: Extract incremental margin stage**

Move listed-market margin target selection, trading frontier resolution, skipped-market count calculation, and `_sync_margin_data()` delegation into `_sync_incremental_margin_stage()`.

- [x] **Step 3: Verify sync behavior**

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_sync_strategies.py -q
uv run --project apps/bt ruff check apps/bt/src/application/services/sync_strategies.py apps/bt/tests/unit/server/services/test_sync_strategies.py
uv run --project apps/bt pyright apps/bt/src/application/services/sync_strategies.py
```

### Task 5: Split Fundamentals Daily Valuation

**Files:**

- Create: `apps/bt/src/domains/fundamentals/daily_valuation.py`
- Modify: `apps/bt/src/domains/fundamentals/calculator.py`
- Test: `apps/bt/tests/server/services/test_fundamentals_service.py`

- [x] **Step 1: Move daily valuation helpers**

Move daily valuation calculation, FY valuation source resolution, forward EPS/FOP resolution, and applicable FY lookup into `daily_valuation.py`.

- [x] **Step 2: Preserve calculator compatibility**

Keep these `FundamentalsCalculator` private method names as delegating facades:

- `_calculate_daily_valuation`
- `_get_applicable_fy_data`
- `_has_valid_valuation_metrics`
- `_resolve_forward_eps_for_daily_valuation`
- `_resolve_forward_operating_profit_for_daily_valuation`
- `_find_applicable_fy`

- [x] **Step 3: Verify focused fundamentals behavior**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/server/services/test_fundamentals_service.py::TestDailyValuation \
  apps/bt/tests/server/services/test_fundamentals_service.py::TestCalculateDailyValuation -q

uv run --project apps/bt ruff check \
  apps/bt/src/domains/fundamentals/calculator.py \
  apps/bt/src/domains/fundamentals/daily_valuation.py

uv run --project apps/bt pyright \
  apps/bt/src/domains/fundamentals/calculator.py \
  apps/bt/src/domains/fundamentals/daily_valuation.py
```

Note: running the full `test_fundamentals_service.py` file still reports two pre-existing `DailyValuationRequiredError` failures in `TestComputeFundamentals`; the focused daily valuation tests covering this extraction pass.

### Task 6: Split Strategy Editor Signal And Shared Config Orchestration

**Files:**

- Modify: `apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx`
- Create: `apps/ts/packages/web/src/components/Backtest/StrategyEditorSignals.tsx`
- Create: `apps/ts/packages/web/src/components/Backtest/StrategyEditorSharedConfig.tsx`
- Test: `apps/ts/packages/web/src/components/Backtest/StrategyEditor.test.tsx`

- [x] **Step 1: Move signal editor orchestration**

Move regular/fundamental signal option construction, add/update/remove callbacks, and signal card rendering into `StrategyEditorSignals.tsx`.

- [x] **Step 2: Move shared-config field rendering**

Move stock-code mode controls, reference select option assembly, dataset snapshot resolution, and shared field rendering into `StrategyEditorSharedConfig.tsx`.

- [x] **Step 3: Verify Strategy Editor behavior**

```bash
bun run --cwd apps/ts --filter @trading25/web test StrategyEditor.test.tsx --run
bun run --cwd apps/ts --filter @trading25/web typecheck
bunx biome check \
  packages/web/src/components/Backtest/StrategyEditor.tsx \
  packages/web/src/components/Backtest/StrategyEditorSignals.tsx \
  packages/web/src/components/Backtest/StrategyEditorSharedConfig.tsx
```

### Task 7: Split Symbol Workbench Header And Panel Rendering

**Files:**

- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`
- Create: `apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx`
- Create: `apps/ts/packages/web/src/pages/SymbolWorkbenchPanels.tsx`
- Test: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx`

- [x] **Step 1: Move header/provenance display**

Move market metadata formatting, liquidity strip, external links, refresh banner, and `ChartHeader` into `SymbolWorkbenchHeader.tsx`.

- [x] **Step 2: Move ordered panel rendering**

Move panel visibility resolution, sub-chart panel rendering, mobile panel selector, and primary chart rendering into `SymbolWorkbenchPanels.tsx`.

- [x] **Step 3: Verify Symbol Workbench behavior**

```bash
bun run --cwd apps/ts --filter @trading25/web test SymbolWorkbenchPage.test.tsx --run
bun run --cwd apps/ts --filter @trading25/web typecheck
bunx biome check \
  packages/web/src/pages/SymbolWorkbenchPage.tsx \
  packages/web/src/pages/SymbolWorkbenchHeader.tsx \
  packages/web/src/pages/SymbolWorkbenchPanels.tsx
```

### Task 8: Split Initial Sync Orchestration

**Files:**

- Modify: `apps/bt/src/application/services/sync_strategies.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_strategies.py`

- [x] **Step 1: Extract initial TOPIX/master/fundamentals stages**

Move TOPIX ingestion, daily stock master sync, and initial listed-market fundamentals sync into private helpers.

- [x] **Step 2: Extract initial stock-data and margin stages**

Move initial stock-data bulk/rest execution, REST failure accumulation, stock-data indexing, and initial margin frontier/target resolution into private helpers.

- [x] **Step 3: Extract initial metadata finalize**

Move `INIT_COMPLETED`, `LAST_SYNC_DATE`, and `FAILED_DATES` metadata writes into `_finalize_initial_sync_metadata()`.

- [x] **Step 4: Verify initial sync behavior**

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_sync_strategies.py -q
uv run --project apps/bt ruff check apps/bt/src/application/services/sync_strategies.py apps/bt/tests/unit/server/services/test_sync_strategies.py
uv run --project apps/bt pyright apps/bt/src/application/services/sync_strategies.py
```

### Task 9: Split Current Top Research Runner

**Files:**

- Modify: `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`
- Test: `apps/bt/tests/unit/scripts/test_run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`

- [x] **Step 1: Extract source-frame preparation**

Move DB reads, date filtering, breadth frame preparation, signal-base construction, and baseline metric construction into `_build_committee_overlay_source_frames()`.

- [x] **Step 2: Extract candidate simulation**

Move single candidate simulation, committee construction, metric rows, comparison frames, and member-map rows into `_build_committee_overlay_candidate_frames()` plus member helpers.

- [x] **Step 3: Extract walk-forward assembly**

Move rank-stability, walk-forward top1 outputs, selection frequency, and space summary construction into `_build_committee_overlay_walkforward_frames()`.

- [x] **Step 4: Verify research runner behavior**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py \
  apps/bt/tests/unit/scripts/test_run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py -q
uv run --project apps/bt ruff check \
  apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py \
  apps/bt/tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py \
  apps/bt/tests/unit/scripts/test_run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py
uv run --project apps/bt pyright \
  apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py
```

## Stop Rule

Stop this slice after `_run_walkforward_research` is below target and focused validation passes. Do not change LightGBM model parameters, ranking feature definitions, PIT/as-of filters, output table names, runner arguments, or bundle schema in Phase 9.
