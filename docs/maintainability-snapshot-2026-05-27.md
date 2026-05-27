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
| files | 1062 |
| functions/blocks | 9311 |
| total lines | 342418 |
| code lines | 304243 |

## Language Split

| language | files | total lines | code lines |
| --- | --- | --- | --- |
| python | 639 | 261014 | 233087 |
| tsx | 258 | 58031 | 51702 |
| typescript | 165 | 23373 | 19454 |

## Baseline To Target

| metric | current | target |
| --- | --- | --- |
| files >= 1000 lines | 94 | 10 |
| files >= 800 lines | 142 | 25 |
| files >= 500 lines | 208 | 75 |
| functions >= 180 lines | 90 | 5 |
| functions >= 120 lines | 265 | 25 |
| functions branch score >= 50 | 8 | 0 |

## Top File Hotspots

| path | lines | code | max block | branch score | nesting | hotspot score |
| --- | --- | --- | --- | --- | --- | --- |
| apps/bt/src/application/services/sync_strategies.py | 3148 | 2859 | 571 | 362 | 5 | 10973 |
| apps/bt/src/infrastructure/db/market/market_db.py | 2802 | 2665 | 377 | 302 | 2 | 9042 |
| apps/ts/packages/web/src/pages/SettingsPage.tsx | 1853 | 1719 | 240 | 361 | 5 | 8822 |
| apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx | 1627 | 1514 | 955 | 214 | 7 | 8166 |
| apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx | 1486 | 1381 | 664 | 260 | 7 | 7988 |
| apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py | 2373 | 2223 | 226 | 246 | 4 | 7189 |
| apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py | 2329 | 2146 | 427 | 192 | 4 | 6743 |
| apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py | 2051 | 1899 | 173 | 245 | 3 | 6663 |
| apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py | 1862 | 1744 | 235 | 223 | 4 | 6323 |
| apps/bt/src/domains/fundamentals/calculator.py | 1336 | 1214 | 99 | 241 | 3 | 5684 |
| apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py | 1891 | 1752 | 270 | 174 | 6 | 5604 |
| apps/bt/src/domains/analytics/fy_eps_sign_next_fy_return.py | 1900 | 1762 | 252 | 176 | 3 | 5521 |
| apps/bt/src/domains/analytics/recent_return_threshold_forward_response.py | 2010 | 1904 | 376 | 133 | 5 | 5311 |
| apps/bt/src/application/services/dataset_builder_service.py | 1047 | 926 | 379 | 184 | 6 | 5285 |
| apps/bt/src/domains/analytics/accumulation_flow_followthrough.py | 2058 | 1927 | 188 | 163 | 4 | 5285 |

## Top Function/Block Hotspots

| path | name | lines | branch score | nesting |
| --- | --- | --- | --- | --- |
| apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx | export | 955 | 126 | 7 |
| apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx | ChartHeader | 664 | 116 | 7 |
| apps/bt/src/application/services/sync_strategies.py | execute | 571 | 62 | 5 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py | run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research | 463 | 24 | 4 |
| apps/bt/src/application/services/db_validation_service.py | validate_market_db | 459 | 57 | 2 |
| apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py | _run_walkforward_research | 427 | 28 | 4 |
| apps/bt/src/application/services/sync_strategies.py | _sync_margin_data | 409 | 46 | 4 |
| apps/bt/src/application/services/dataset_builder_service.py | _build_dataset | 379 | 64 | 6 |
| apps/ts/packages/web/src/components/Backtest/DefaultConfigEditor.tsx | export | 379 | 63 | 7 |
| apps/bt/src/infrastructure/db/market/market_db.py | ensure_schema | 377 | 1 | 0 |
| apps/bt/src/domains/analytics/recent_return_threshold_forward_response.py | _create_observation_panel | 376 | 19 | 1 |
| apps/bt/src/domains/strategy/core/mixins/backtest_executor_mixin.py | run_multi_backtest | 373 | 46 | 4 |
| apps/bt/src/infrastructure/db/market/market_db.py | upsert_daily_valuation_from_adjusted_metrics | 371 | 12 | 2 |
| apps/bt/src/domains/analytics/new_high_momentum_research.py | _create_panel_tables | 349 | 20 | 1 |
| apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py | run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research | 347 | 20 | 2 |

## Interpretation Rules

- Reduce large orchestrators by extracting responsibility-specific helpers only when tests can characterize the existing behavior.
- Do not treat low reference count as dead code without proving current runtime/API/workflow reachability.
- Prefer lowering per-file and per-function concentration over raw total LOC reduction; module splits can temporarily increase family LOC.
- Re-run this script after every cleanup slice and compare the baseline-to-target table.

## Notes

- Python function metrics use ast.FunctionDef spans and AST branch nodes.
- TypeScript/TSX function metrics are heuristic because this script has no TypeScript parser dependency.
- Generated contracts and docs are excluded; the scope is maintainable production/tool source.
