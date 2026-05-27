# Maintainability Snapshot 2026-05-27

This is a quantitative baseline for staged spaghetti-code reduction.
The numbers are not quality by themselves; they identify where focused, behavior-preserving refactor slices should start.

## Scope

Measured tracked source under:

- `apps/bt/src/`
- `apps/bt/scripts/`
- `apps/ts/packages/api-clients/src/`
- `apps/ts/packages/utils/src/`
- `apps/ts/packages/web/src/`
- `scripts/`

## Summary

| metric | value |
| --- | --- |
| files | 905 |
| functions/blocks | 8856 |
| total lines | 308150 |
| code lines | 245972 |

## Language Split

| language | files | total lines | code lines |
| --- | --- | --- | --- |
| python | 646 | 261476 | 216455 |
| tsx | 137 | 31128 | 16792 |
| typescript | 122 | 15546 | 12725 |

## Baseline To Target

| metric | current | target |
| --- | --- | --- |
| files >= 1000 lines | 85 | 10 |
| files >= 800 lines | 136 | 25 |
| files >= 500 lines | 196 | 75 |
| functions >= 180 lines | 48 | 5 |
| functions >= 120 lines | 169 | 25 |
| functions branch score >= 50 | 5 | 0 |

## Top File Hotspots

| path | lines | code | max block code lines | branch score | nesting | hotspot score |
| --- | --- | --- | --- | --- | --- | --- |
| apps/ts/packages/web/src/pages/SettingsPage.tsx | 1853 | 1067 | 125 | 328 | 5 | 7231 |
| apps/bt/src/infrastructure/db/market/market_db.py | 2802 | 1629 | 59 | 302 | 2 | 7115 |
| apps/bt/src/application/services/sync_strategies.py | 2060 | 1849 | 376 | 233 | 5 | 7056 |
| apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py | 2373 | 1990 | 220 | 246 | 4 | 6938 |
| apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py | 2329 | 2140 | 407 | 192 | 4 | 6677 |
| apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py | 1862 | 1744 | 235 | 223 | 4 | 6323 |
| apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py | 2051 | 1705 | 124 | 245 | 3 | 6322 |
| apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx | 1806 | 1048 | 420 | 208 | 7 | 5987 |
| apps/bt/src/domains/fundamentals/calculator.py | 1336 | 1214 | 98 | 241 | 3 | 5681 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | 1891 | 1657 | 255 | 174 | 6 | 5464 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | 1900 | 1605 | 240 | 176 | 3 | 5328 |
| apps/bt/src/domains/analytics/accumulation_flow_followthrough.py | 2058 | 1834 | 185 | 163 | 4 | 5183 |
| apps/bt/src/application/services/dataset_builder_service.py | 1047 | 891 | 347 | 184 | 6 | 5154 |
| apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx | 1486 | 804 | 89 | 226 | 7 | 5074 |
| apps/bt/src/domains/analytics/annual_value_breakout_periodic_rebalance.py | 1585 | 1507 | 214 | 157 | 7 | 4910 |

## Top Function/Block Hotspots

| path | name | code lines | branch score | nesting |
| --- | --- | --- | --- | --- |
| apps/bt/src/application/services/db_validation_service.py | validate_market_db | 443 | 57 | 2 |
| apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx | export | 420 | 82 | 5 |
| apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py | _run_walkforward_research | 407 | 28 | 4 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py | run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research | 396 | 14 | 4 |
| apps/bt/src/application/services/sync_strategies.py | execute | 376 | 49 | 5 |
| apps/bt/src/application/services/dataset_builder_service.py | _build_dataset | 347 | 64 | 6 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py | run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research | 338 | 20 | 2 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_trend_breadth_overlay.py | run_topix_downside_return_standard_deviation_trend_breadth_overlay_research | 313 | 18 | 2 |
| apps/bt/src/application/services/sync_strategies.py | execute | 288 | 30 | 5 |
| apps/bt/src/domains/analytics/topix100_top1_open_to_open_5d_duplicate_policy_analysis.py | run_topix100_top1_open_to_open_5d_duplicate_policy_analysis | 287 | 17 | 2 |
| apps/bt/src/domains/strategy/core/mixins/backtest_executor_mixin.py | run_multi_backtest | 285 | 46 | 4 |
| apps/bt/src/domains/analytics/topix100_top1_open_to_open_5d_fixed_committee_overlay.py | run_topix100_top1_open_to_open_5d_fixed_committee_overlay_research | 257 | 19 | 2 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | run_volume_ratio_future_return_regime_research | 255 | 29 | 3 |
| apps/bt/src/domains/analytics/nt_ratio_change_topix_close_stock_overnight_distribution.py | run_nt_ratio_change_topix_close_stock_overnight_distribution | 245 | 8 | 2 |
| apps/bt/src/application/services/sync_strategies.py | _sync_daily_stock_master | 244 | 32 | 5 |

## Interpretation Rules

- Reduce large orchestrators by extracting responsibility-specific helpers only when tests can characterize the existing behavior.
- Do not treat low reference count as dead code without proving current runtime/API/workflow reachability.
- Prefer lowering per-file and per-function concentration over raw total LOC reduction; module splits can temporarily increase family LOC.
- Re-run this script after every cleanup slice and compare the baseline-to-target table.

## Notes

- Python function metrics use ast.FunctionDef spans and AST branch nodes.
- Python function line counts are effective code lines; multi-line SQL, Markdown, and string payloads are not counted as executable code.
- TSX function and code-line metrics count logic-bearing lines and avoid JSX-only layout inflation.
- TypeScript/TSX function metrics are heuristic because this script has no TypeScript parser dependency.
- Generated contracts, test files, and docs are excluded; the scope is maintainable production/tool source.
