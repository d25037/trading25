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
| files | 1063 |
| functions/blocks | 9352 |
| total lines | 342436 |
| code lines | 304286 |

## Language Split

| language | files | total lines | code lines |
| --- | --- | --- | --- |
| python | 640 | 260978 | 233054 |
| tsx | 258 | 58210 | 51878 |
| typescript | 165 | 23248 | 19354 |

## Baseline To Target

| metric | current | target |
| --- | --- | --- |
| files >= 1000 lines | 88 | 10 |
| files >= 800 lines | 142 | 25 |
| files >= 500 lines | 209 | 75 |
| functions >= 180 lines | 57 | 5 |
| functions >= 120 lines | 198 | 25 |
| functions branch score >= 50 | 7 | 0 |

## Top File Hotspots

| path | lines | code | max block code lines | branch score | nesting | hotspot score |
| --- | --- | --- | --- | --- | --- | --- |
| apps/bt/src/application/services/sync_strategies.py | 2629 | 2379 | 516 | 300 | 5 | 9212 |
| apps/ts/packages/web/src/pages/SettingsPage.tsx | 1853 | 1719 | 240 | 361 | 5 | 8822 |
| apps/bt/src/infrastructure/db/market/market_db.py | 2802 | 2665 | 59 | 302 | 2 | 8151 |
| apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx | 1806 | 1690 | 632 | 218 | 7 | 7445 |
| apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py | 2373 | 2223 | 220 | 246 | 4 | 7171 |
| apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py | 2329 | 2146 | 407 | 192 | 4 | 6683 |
| apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py | 2051 | 1899 | 124 | 245 | 3 | 6516 |
| apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx | 1486 | 1381 | 164 | 260 | 7 | 6488 |
| apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py | 1862 | 1744 | 235 | 223 | 4 | 6323 |
| apps/bt/src/domains/fundamentals/calculator.py | 1336 | 1214 | 98 | 241 | 3 | 5681 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | 1891 | 1752 | 255 | 174 | 6 | 5559 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | 1900 | 1762 | 240 | 176 | 3 | 5485 |
| apps/bt/src/domains/analytics/accumulation_flow_followthrough.py | 2058 | 1927 | 185 | 163 | 4 | 5276 |
| apps/bt/src/application/services/dataset_builder_service.py | 1047 | 926 | 347 | 184 | 6 | 5189 |
| apps/bt/src/domains/analytics/annual_value_breakout_periodic_rebalance.py | 1585 | 1507 | 214 | 157 | 7 | 4910 |

## Top Function/Block Hotspots

| path | name | code lines | branch score | nesting |
| --- | --- | --- | --- | --- |
| apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx | export | 632 | 82 | 5 |
| apps/bt/src/application/services/sync_strategies.py | execute | 516 | 62 | 5 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py | run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research | 453 | 24 | 4 |
| apps/bt/src/application/services/db_validation_service.py | validate_market_db | 443 | 57 | 2 |
| apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py | _run_walkforward_research | 407 | 28 | 4 |
| apps/ts/packages/web/src/components/Backtest/DefaultConfigEditor.tsx | export | 379 | 63 | 7 |
| apps/bt/src/application/services/dataset_builder_service.py | _build_dataset | 347 | 64 | 6 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py | run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research | 338 | 20 | 2 |
| apps/ts/packages/web/src/components/Chart/ChartPresetSelector.tsx | export | 324 | 24 | 4 |
| apps/ts/packages/web/src/components/Chart/ChartControls.tsx | const | 324 | 11 | 6 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_trend_breadth_overlay.py | run_topix_downside_return_standard_deviation_trend_breadth_overlay_research | 313 | 18 | 2 |
| apps/bt/src/application/services/sync_strategies.py | _sync_fundamentals_incremental | 311 | 41 | 5 |
| apps/ts/packages/web/src/components/Chart/StockChart.tsx | export | 309 | 63 | 6 |
| apps/bt/src/application/services/sync_strategies.py | execute | 288 | 30 | 5 |
| apps/bt/src/domains/analytics/topix100_top1_open_to_open_5d_duplicate_policy_analysis.py | run_topix100_top1_open_to_open_5d_duplicate_policy_analysis | 287 | 17 | 2 |

## Interpretation Rules

- Reduce large orchestrators by extracting responsibility-specific helpers only when tests can characterize the existing behavior.
- Do not treat low reference count as dead code without proving current runtime/API/workflow reachability.
- Prefer lowering per-file and per-function concentration over raw total LOC reduction; module splits can temporarily increase family LOC.
- Re-run this script after every cleanup slice and compare the baseline-to-target table.

## Notes

- Python function metrics use ast.FunctionDef spans and AST branch nodes.
- Python function line counts are effective code lines; multi-line SQL, Markdown, and string payloads are not counted as executable code.
- TypeScript/TSX function metrics are heuristic because this script has no TypeScript parser dependency.
- Generated contracts and docs are excluded; the scope is maintainable production/tool source.
