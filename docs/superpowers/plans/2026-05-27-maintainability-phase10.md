# Maintainability Phase 10 Implementation Plan

**Goal:** Continue high-value spaghetti-code reduction after Phase 9, focused on:

1. `db_validation_service.py` validation assembly separation.
2. Remaining top research runner giant function.
3. `market_db.py` reader/write/publish boundary separation.

**Approach:** Use behavior-preserving local splits with focused tests. Avoid broad market DB rewrites because DuckDB query/write semantics are operationally sensitive.

## Starting Hotspots

From `docs/maintainability-snapshot-latest.md` after Phase 9:

| target | starting metric |
| --- | ---: |
| `db_validation_service.py validate_market_db` | 312 code lines |
| `shock_confirmation_vote_overlay` runner | 338 code lines |
| `market_db.py` hotspot score | 5,539 |

## Completed Slices

### 1. Validation Service Split

`validate_market_db()` now delegates to private dataclass-backed helpers:

- `_ValidationBaseSnapshot`
- `_FundamentalsValidationSnapshot`
- `_MarginValidationSnapshot`
- `_Options225ValidationSnapshot`
- `_SourceQualitySnapshot`

Result: `db_validation_service.py validate_market_db` no longer appears in the top function hotspot list.

### 2. Top Research Runner Split

`run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research()` now delegates to:

- `_resolve_vote_overlay_params()`
- `_build_vote_overlay_source_frames()`
- `_build_vote_overlay_candidate_frames()`
- `_build_vote_overlay_walkforward_frames()`

The runner-facing API, result dataclass, bundle table names, and script entrypoint remain unchanged.

### 3. Market DB Reader/Writer Boundary Split

Created `apps/bt/src/infrastructure/db/market/stock_master_queries.py` and moved PIT stock-master / TOPIX-date / index-membership reader query construction behind local helper functions. `MarketDb` keeps the public method surface and delegates to the reader module.

Created `apps/bt/src/infrastructure/db/market/time_series_writers.py` and moved low-risk time-series upsert SQL for:

- `stock_data_minute_raw`
- `topix_data`
- `indices_data`
- `options_225_data`
- `margin_data`

This keeps `MarketDb` as the public boundary while moving query/write details into local modules. Higher-risk `stock_data_raw` projection, stock-master rebuild, metadata, and index-master writes remain in `market_db.py` for a later phase.

## Verification

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_db_validation_service.py -q
uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py apps/bt/tests/unit/scripts/test_run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py -q
uv run --project apps/bt pytest apps/bt/tests/unit/server/db/test_market_db.py apps/bt/tests/unit/server/services/test_db_validation_service.py apps/bt/tests/unit/server/services/test_db_stats_service.py -q
uv run --project apps/bt ruff check apps/bt/src/infrastructure/db/market/market_db.py apps/bt/src/infrastructure/db/market/stock_master_queries.py apps/bt/src/infrastructure/db/market/time_series_writers.py apps/bt/src/application/services/db_validation_service.py apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py
uv run --project apps/bt pyright apps/bt/src/infrastructure/db/market/market_db.py apps/bt/src/infrastructure/db/market/stock_master_queries.py apps/bt/src/infrastructure/db/market/time_series_writers.py apps/bt/src/application/services/db_validation_service.py apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py
```

## Next Slices

- Continue remaining top research runner cleanup with `topix_downside_return_standard_deviation_trend_breadth_overlay.py`.
- Split remaining `market_db.py` writer families next: stock master write/rebuild, raw stock-data projection, metadata/index-master writes.
- Consider extracting validation response assembly further only if `/api/db/validate` changes again.
