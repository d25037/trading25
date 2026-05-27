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
| files | 926 |
| functions/blocks | 9008 |
| total lines | 310903 |
| code lines | 248567 |

## Language Split

| language | files | total lines | code lines |
| --- | --- | --- | --- |
| python | 661 | 263903 | 218737 |
| tsx | 142 | 31211 | 16884 |
| typescript | 123 | 15789 | 12946 |

## Baseline To Target

| metric | current | target |
| --- | --- | --- |
| files >= 1000 lines | 83 | 10 |
| files >= 800 lines | 136 | 25 |
| files >= 500 lines | 200 | 75 |
| functions >= 180 lines | 40 | 5 |
| functions >= 120 lines | 171 | 25 |
| functions branch score >= 50 | 2 | 0 |

## Top File Hotspots

| path | lines | code | max block code lines | branch score | nesting | hotspot score |
| --- | --- | --- | --- | --- | --- | --- |
| apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py | 1909 | 1573 | 124 | 221 | 3 | 5758 |
| apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py | 2051 | 1686 | 220 | 197 | 3 | 5727 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | 1891 | 1657 | 255 | 174 | 6 | 5464 |
| apps/bt/src/application/services/sync_strategies.py | 1955 | 1762 | 169 | 182 | 5 | 5430 |
| apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py | 2207 | 2027 | 149 | 169 | 4 | 5376 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | 1900 | 1605 | 240 | 176 | 3 | 5328 |
| apps/bt/src/domains/analytics/accumulation_flow_followthrough.py | 2058 | 1834 | 185 | 163 | 4 | 5183 |
| apps/bt/src/domains/fundamentals/calculator.py | 1189 | 1073 | 98 | 210 | 3 | 4982 |
| apps/bt/src/domains/analytics/annual_value_breakout_periodic_rebalance.py | 1585 | 1507 | 214 | 157 | 7 | 4910 |
| apps/bt/src/domains/analytics/standard_missing_forecast_cfo_non_positive_deep_dive.py | 1534 | 1403 | 205 | 160 | 5 | 4783 |
| apps/bt/src/domains/analytics/pre_earnings_eps120_proxy.py | 1826 | 1604 | 178 | 150 | 4 | 4698 |
| apps/bt/src/domains/analytics/topix100_streak_353_signal_score_lightgbm.py | 2028 | 1848 | 173 | 137 | 3 | 4668 |
| apps/bt/src/domains/analytics/standard_negative_eps_right_tail_decomposition.py | 1737 | 1427 | 232 | 143 | 3 | 4532 |
| apps/bt/src/domains/analytics/annual_value_periodic_rebalance.py | 1385 | 1311 | 182 | 150 | 6 | 4467 |
| apps/ts/packages/web/src/pages/SettingsMarketDbPanels.tsx | 810 | 567 | 88 | 207 | 5 | 4442 |

## Top Function/Block Hotspots

| path | name | code lines | branch score | nesting |
| --- | --- | --- | --- | --- |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_trend_breadth_overlay.py | run_topix_downside_return_standard_deviation_trend_breadth_overlay_research | 313 | 18 | 2 |
| apps/bt/src/domains/analytics/topix100_top1_open_to_open_5d_duplicate_policy_analysis.py | run_topix100_top1_open_to_open_5d_duplicate_policy_analysis | 287 | 17 | 2 |
| apps/bt/src/domains/strategy/core/mixins/backtest_executor_mixin.py | run_multi_backtest | 285 | 46 | 4 |
| apps/bt/src/domains/analytics/topix100_top1_open_to_open_5d_fixed_committee_overlay.py | run_topix100_top1_open_to_open_5d_fixed_committee_overlay_research | 257 | 19 | 2 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | run_volume_ratio_future_return_regime_research | 255 | 29 | 3 |
| apps/bt/src/domains/analytics/nt_ratio_change_topix_close_stock_overnight_distribution.py | run_nt_ratio_change_topix_close_stock_overnight_distribution | 245 | 8 | 2 |
| apps/bt/src/application/services/sync_stock_master.py | sync_daily_stock_master | 244 | 32 | 5 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | _build_event_ledger | 240 | 29 | 3 |
| apps/bt/src/domains/analytics/topix100_streak_353_next_session_intraday_lightgbm.py | _score_topix100_streak_353_next_session_intraday_lightgbm_snapshot | 240 | 24 | 2 |
| apps/bt/src/domains/analytics/standard_negative_eps_right_tail_decomposition.py | _build_event_ledger | 232 | 26 | 3 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_family_committee_walkforward.py | run_topix_downside_return_standard_deviation_family_committee_walkforward_research | 221 | 13 | 2 |
| apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py | _build_event_ledger | 220 | 28 | 2 |
| apps/bt/src/domains/analytics/topix_streak_multi_timeframe_mode.py | _build_pair_score_df | 219 | 30 | 4 |
| apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py | _build_feature_values | 218 | 29 | 2 |
| apps/bt/src/domains/analytics/annual_value_breakout_periodic_rebalance.py | run_annual_value_breakout_periodic_rebalance | 214 | 18 | 1 |

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
