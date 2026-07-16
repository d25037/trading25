# Maintainability Snapshot 2026-07-16

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
| files | 1002 |
| functions/blocks | 9959 |
| total lines | 341280 |
| code lines | 267533 |

## Language Split

| language | files | total lines | code lines |
| --- | --- | --- | --- |
| python | 736 | 291581 | 235192 |
| tsx | 136 | 31597 | 17294 |
| typescript | 130 | 18102 | 15047 |

## Baseline To Target

| metric | current | target |
| --- | --- | --- |
| files >= 1000 lines | 86 | 10 |
| files >= 800 lines | 139 | 25 |
| files >= 500 lines | 220 | 75 |
| functions >= 180 lines | 48 | 5 |
| functions >= 120 lines | 176 | 25 |
| functions branch score >= 50 | 5 | 0 |

## Top File Hotspots

| path | lines | code | max block code lines | branch score | nesting | hotspot score |
| --- | --- | --- | --- | --- | --- | --- |
| apps/bt/src/application/services/market_v4_cutover.py | 9571 | 9201 | 449 | 1541 | 8 | 38246 |
| apps/bt/src/infrastructure/db/market/time_series_store.py | 2001 | 1509 | 148 | 229 | 4 | 5935 |
| apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py | 2056 | 1687 | 220 | 197 | 3 | 5728 |
| apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py | 1874 | 1558 | 120 | 220 | 3 | 5713 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | 1898 | 1657 | 255 | 174 | 6 | 5464 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | 1899 | 1590 | 240 | 176 | 3 | 5313 |
| apps/bt/src/domains/analytics/accumulation_flow_followthrough.py | 2109 | 1829 | 191 | 165 | 4 | 5232 |
| apps/bt/src/domains/analytics/annual_value_breakout_periodic_rebalance.py | 1562 | 1486 | 214 | 156 | 7 | 4871 |
| apps/bt/src/infrastructure/db/market/valuation_writers.py | 1012 | 824 | 337 | 176 | 4 | 4863 |
| apps/bt/src/domains/analytics/standard_missing_forecast_cfo_non_positive_deep_dive.py | 1508 | 1378 | 205 | 160 | 5 | 4758 |
| apps/bt/src/domains/analytics/pre_earnings_eps120_proxy.py | 1828 | 1606 | 178 | 150 | 4 | 4700 |
| apps/bt/src/domains/analytics/standard_negative_eps_right_tail_decomposition.py | 1736 | 1411 | 232 | 143 | 3 | 4516 |
| apps/bt/src/domains/analytics/annual_value_periodic_rebalance.py | 1371 | 1299 | 182 | 150 | 6 | 4455 |
| apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py | 1270 | 1192 | 180 | 157 | 4 | 4418 |
| apps/bt/src/domains/analytics/recent_return_threshold_forward_response.py | 2012 | 1487 | 182 | 133 | 5 | 4312 |

## Top Function/Block Hotspots

| path | name | code lines | branch score | nesting |
| --- | --- | --- | --- | --- |
| apps/bt/src/application/services/market_v4_cutover.py | _cutover_under_lease | 449 | 45 | 5 |
| apps/bt/src/application/services/market_v4_cutover.py | _rollback_retained_promotion | 393 | 64 | 4 |
| apps/bt/src/infrastructure/db/market/valuation_writers.py | publish_adjusted_basis_materialization | 337 | 77 | 4 |
| apps/bt/src/application/services/market_v4_cutover.py | _promote_retained_under_leases_unchecked | 309 | 29 | 4 |
| apps/bt/src/application/services/market_v4_cutover.py | _rehearse_retained_under_lease | 280 | 47 | 4 |
| apps/bt/src/application/services/market_v4_cutover.py | _retained_promotion_report_contract_valid | 265 | 26 | 1 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | run_volume_ratio_future_return_regime_research | 255 | 29 | 3 |
| apps/bt/src/application/services/ranking_service.py | get_rankings | 246 | 20 | 2 |
| apps/bt/src/application/services/market_v4_cutover.py | _recover_retained_promotion | 245 | 37 | 8 |
| apps/bt/src/application/services/sync_service.py | start_sync | 245 | 34 | 3 |
| apps/bt/src/domains/analytics/nt_ratio_change_topix_close_stock_overnight_distribution.py | run_nt_ratio_change_topix_close_stock_overnight_distribution | 245 | 8 | 2 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | _build_event_ledger | 240 | 29 | 3 |
| apps/ts/packages/web/src/components/Chart/StockChart.tsx | export | 236 | 68 | 6 |
| apps/bt/src/application/services/dataset_builder_service.py | _build_dataset_from_pinned_source | 233 | 16 | 3 |
| apps/bt/src/domains/analytics/standard_negative_eps_right_tail_decomposition.py | _build_event_ledger | 232 | 26 | 3 |

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
