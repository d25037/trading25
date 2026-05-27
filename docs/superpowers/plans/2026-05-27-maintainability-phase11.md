# Maintainability Phase 11 Implementation Plan

**Goal:** Complete the next high-value refactor set:

1. Split the remaining 300-line-class research runner, starting with `trend_breadth_overlay`.
2. Continue `market_db.py` writer-boundary separation for stock master rebuild, raw stock-data projection, and metadata/index-master writes.
3. Reduce branch-heavy, high-change SoT code around fundamentals calculation.

**Approach:** Keep public APIs and bundle table names unchanged. Move implementation detail into local helper modules while leaving compatibility facades where tests or application services call private methods.

## Completed Slices

### 1. Trend/Breadth Research Runner

`run_topix_downside_return_standard_deviation_trend_breadth_overlay_research()` now delegates to:

- `_resolve_trend_breadth_params()`
- `_build_trend_breadth_source_frames()`
- `_build_trend_breadth_candidate_frames()`
- `_build_trend_breadth_walkforward_frames()`

The runner-facing function, result dataclass, bundle writer/loader, table names, and script entrypoint remain unchanged.

### 2. Market DB Writer Boundaries

Added:

- `apps/bt/src/infrastructure/db/market/stock_master_writers.py`
- `apps/bt/src/infrastructure/db/market/metadata_writers.py`

Extended:

- `apps/bt/src/infrastructure/db/market/time_series_writers.py`

`MarketDb` now delegates:

- stock master upserts
- stock master interval/latest rebuilds
- raw `stock_data_raw` + projected `stock_data` upsert
- sync metadata writes
- `index_master` upsert

The public `MarketDb` method surface remains unchanged.

### 3. Fundamentals Share Adjustment

Added `apps/bt/src/domains/fundamentals/share_adjustments.py` and moved share-adjusted datapoint construction, baseline share resolution, treasury share resolution, and latest-metrics adjustment logic out of `calculator.py`.

`FundamentalsCalculator` keeps private compatibility facades for existing tests and service callers.

## Snapshot Result

After this phase:

| metric | before | after |
| --- | ---: | ---: |
| top function hotspot: `trend_breadth_overlay` runner | 313 | removed from top 15 |
| functions >= 120 lines | 171 | 170 |
| `calculator.py` top-file hotspot presence | top 15 | removed from top 15 |
| `market_db.py` writer details | mixed in class | delegated to writer modules |

## Verification

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_trend_breadth_overlay.py apps/bt/tests/unit/scripts/test_run_topix_downside_return_standard_deviation_trend_breadth_overlay.py -q
uv run --project apps/bt pytest apps/bt/tests/unit/server/db/test_market_db.py apps/bt/tests/unit/server/services/test_db_validation_service.py apps/bt/tests/unit/server/services/test_db_stats_service.py -q
uv run --project apps/bt pytest apps/bt/tests/server/services/test_fundamentals_service.py::TestComputeAdjustedValue apps/bt/tests/server/services/test_fundamentals_service.py::TestDailyValuation apps/bt/tests/server/services/test_fundamentals_service.py::TestCalculateDailyValuation apps/bt/tests/server/services/test_fundamentals_service.py::TestDividendPerShare -q
uv run --project apps/bt ruff check <affected files>
uv run --project apps/bt pyright <affected files>
python3 scripts/check-research-guardrails.py
python3 -m py_compile <affected python files>
git diff --check
```

## Remaining Candidates

- `topix100_top1_open_to_open_5d_duplicate_policy_analysis.py`
- `backtest_executor_mixin.py`
- `topix100_top1_open_to_open_5d_fixed_committee_overlay.py`
- `sync_stock_master.py`
- `SettingsMarketDbPanels.tsx`

These are worth considering only when they overlap with active feature work or have focused tests available.
