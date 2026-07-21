# Maintainability Snapshot

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
| files | 1048 |
| functions/blocks | 10404 |
| total lines | 359248 |
| code lines | 284263 |

## Language Split

| language | files | total lines | code lines |
| --- | --- | --- | --- |
| python | 779 | 309817 | 252134 |
| tsx | 138 | 32172 | 17614 |
| typescript | 131 | 17259 | 14515 |

## Baseline To Target

| metric | current | target |
| --- | --- | --- |
| files >= 1000 lines | 89 | 10 |
| files >= 800 lines | 147 | 25 |
| files >= 500 lines | 236 | 75 |
| functions >= 180 lines | 57 | 5 |
| functions >= 120 lines | 201 | 25 |
| functions branch score >= 50 | 5 | 0 |

## Top File Hotspots

| path | lines | code | max block code lines | branch score | nesting | hotspot score |
| --- | --- | --- | --- | --- | --- | --- |
| apps/bt/src/domains/analytics/ranking_technical_fit_score_shape_evidence.py | 3885 | 3375 | 217 | 321 | 8 | 9764 |
| apps/bt/src/infrastructure/db/market/time_series_store.py | 2679 | 2118 | 280 | 334 | 4 | 8830 |
| apps/bt/src/domains/analytics/daily_ranking_research_base.py | 1916 | 1549 | 208 | 218 | 2 | 5907 |
| apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py | 2056 | 1687 | 220 | 197 | 3 | 5728 |
| apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py | 1874 | 1558 | 120 | 220 | 3 | 5713 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | 1898 | 1657 | 255 | 174 | 6 | 5464 |
| scripts/openapi_compat.py | 1078 | 989 | 236 | 214 | 6 | 5459 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | 1899 | 1590 | 240 | 176 | 3 | 5313 |
| apps/bt/src/domains/analytics/accumulation_flow_followthrough.py | 2109 | 1829 | 191 | 165 | 4 | 5232 |
| apps/bt/src/domains/analytics/ranking_fixed_return_priority_evidence.py | 1921 | 1790 | 221 | 156 | 6 | 5171 |
| apps/bt/src/domains/analytics/annual_value_breakout_periodic_rebalance.py | 1562 | 1486 | 214 | 156 | 7 | 4871 |
| apps/bt/src/domains/analytics/standard_missing_forecast_cfo_non_positive_deep_dive.py | 1508 | 1378 | 205 | 160 | 5 | 4758 |
| apps/bt/src/domains/analytics/pre_earnings_eps120_proxy.py | 1828 | 1606 | 178 | 150 | 4 | 4700 |
| apps/bt/src/domains/analytics/standard_negative_eps_right_tail_decomposition.py | 1736 | 1411 | 232 | 143 | 3 | 4516 |
| apps/bt/src/domains/analytics/annual_value_periodic_rebalance.py | 1371 | 1299 | 182 | 150 | 6 | 4455 |

## Top Function/Block Hotspots

| path | name | code lines | branch score | nesting |
| --- | --- | --- | --- | --- |
| apps/bt/src/application/services/ranking_service.py | get_rankings | 307 | 22 | 2 |
| apps/bt/src/infrastructure/db/market/time_series_store.py | _publish_stock_data_locked | 280 | 61 | 3 |
| apps/ts/packages/web/src/components/Chart/StockChart.tsx | export | 268 | 78 | 6 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | run_volume_ratio_future_return_regime_research | 255 | 29 | 3 |
| apps/bt/src/application/services/dataset_builder_service.py | _build_dataset_from_pinned_source | 255 | 19 | 3 |
| apps/bt/src/application/services/sync_service.py | start_sync | 254 | 38 | 3 |
| apps/bt/src/application/services/stock_refresh_service.py | refresh_stocks | 251 | 49 | 5 |
| apps/bt/src/infrastructure/db/market/time_series_store.py | replace_stock_provider_window | 248 | 39 | 4 |
| apps/bt/src/domains/analytics/nt_ratio_change_topix_close_stock_overnight_distribution.py | run_nt_ratio_change_topix_close_stock_overnight_distribution | 245 | 8 | 2 |
| apps/bt/src/application/services/sync_strategies.py | execute | 244 | 18 | 2 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | _build_event_ledger | 240 | 29 | 3 |
| scripts/openapi_compat.py | _compare_schema | 236 | 53 | 3 |
| apps/bt/src/domains/analytics/standard_negative_eps_right_tail_decomposition.py | _build_event_ledger | 232 | 26 | 3 |
| apps/bt/src/domains/analytics/ranking_sma5_atr_deviation_evidence.py | run_ranking_sma5_atr_deviation_evidence_research | 231 | 7 | 1 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_family_committee_walkforward.py | run_topix_downside_return_standard_deviation_family_committee_walkforward_research | 221 | 13 | 2 |

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
