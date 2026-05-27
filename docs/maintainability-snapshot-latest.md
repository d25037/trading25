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
| files | 921 |
| functions/blocks | 8939 |
| total lines | 309859 |
| code lines | 247518 |

## Language Split

| language | files | total lines | code lines |
| --- | --- | --- | --- |
| python | 660 | 262989 | 217806 |
| tsx | 138 | 31081 | 16766 |
| typescript | 123 | 15789 | 12946 |

## Baseline To Target

| metric | current | target |
| --- | --- | --- |
| files >= 1000 lines | 84 | 10 |
| files >= 800 lines | 137 | 25 |
| files >= 500 lines | 199 | 75 |
| functions >= 180 lines | 47 | 5 |
| functions >= 120 lines | 170 | 25 |
| functions branch score >= 50 | 2 | 0 |

## Top File Hotspots

| path | lines | code | max block code lines | branch score | nesting | hotspot score |
| --- | --- | --- | --- | --- | --- | --- |
| apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py | 2023 | 1857 | 407 | 166 | 4 | 5926 |
| apps/bt/src/application/services/sync_strategies.py | 1757 | 1567 | 376 | 178 | 5 | 5784 |
| apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py | 1909 | 1573 | 124 | 221 | 3 | 5758 |
| apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py | 2051 | 1686 | 220 | 197 | 3 | 5727 |
| apps/bt/src/domains/fundamentals/calculator.py | 1336 | 1214 | 98 | 241 | 3 | 5681 |
| apps/bt/src/infrastructure/db/market/market_db.py | 1710 | 1205 | 46 | 238 | 2 | 5539 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | 1891 | 1657 | 255 | 174 | 6 | 5464 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | 1900 | 1605 | 240 | 176 | 3 | 5328 |
| apps/bt/src/domains/analytics/accumulation_flow_followthrough.py | 2058 | 1834 | 185 | 163 | 4 | 5183 |
| apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx | 1716 | 987 | 301 | 185 | 7 | 5155 |
| apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx | 1486 | 804 | 89 | 226 | 7 | 5074 |
| apps/bt/src/domains/analytics/annual_value_breakout_periodic_rebalance.py | 1585 | 1507 | 214 | 157 | 7 | 4910 |
| apps/bt/src/domains/analytics/standard_missing_forecast_cfo_non_positive_deep_dive.py | 1534 | 1403 | 205 | 160 | 5 | 4783 |
| apps/bt/src/domains/analytics/pre_earnings_eps120_proxy.py | 1826 | 1604 | 178 | 150 | 4 | 4698 |
| apps/bt/src/domains/analytics/topix100_streak_353_signal_score_lightgbm.py | 2028 | 1848 | 173 | 137 | 3 | 4668 |

## Top Function/Block Hotspots

| path | name | code lines | branch score | nesting |
| --- | --- | --- | --- | --- |
| apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py | _run_walkforward_research | 407 | 28 | 4 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py | run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research | 396 | 14 | 4 |
| apps/bt/src/application/services/sync_strategies.py | execute | 376 | 49 | 5 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py | run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research | 338 | 20 | 2 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_trend_breadth_overlay.py | run_topix_downside_return_standard_deviation_trend_breadth_overlay_research | 313 | 18 | 2 |
| apps/bt/src/application/services/db_validation_service.py | validate_market_db | 312 | 11 | 2 |
| apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx | export | 301 | 48 | 4 |
| apps/bt/src/application/services/sync_strategies.py | execute | 288 | 30 | 5 |
| apps/bt/src/domains/analytics/topix100_top1_open_to_open_5d_duplicate_policy_analysis.py | run_topix100_top1_open_to_open_5d_duplicate_policy_analysis | 287 | 17 | 2 |
| apps/bt/src/domains/strategy/core/mixins/backtest_executor_mixin.py | run_multi_backtest | 285 | 46 | 4 |
| apps/bt/src/domains/analytics/topix100_top1_open_to_open_5d_fixed_committee_overlay.py | run_topix100_top1_open_to_open_5d_fixed_committee_overlay_research | 257 | 19 | 2 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | run_volume_ratio_future_return_regime_research | 255 | 29 | 3 |
| apps/bt/src/domains/analytics/nt_ratio_change_topix_close_stock_overnight_distribution.py | run_nt_ratio_change_topix_close_stock_overnight_distribution | 245 | 8 | 2 |
| apps/bt/src/application/services/sync_stock_master.py | sync_daily_stock_master | 244 | 32 | 5 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | _build_event_ledger | 240 | 29 | 3 |

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
