# Annual Forward PER Regime Decomposition

先行研究
[`annual-first-open-last-close-fundamental-panel`](../annual-first-open-last-close-fundamental-panel/README.md),
[`annual-fundamental-confounder-analysis`](../annual-fundamental-confounder-analysis/README.md),
[`annual-value-composite-selection`](../annual-value-composite-selection/README.md)
を土台に、`low forward PER` の中身を
`positive low forward PER` と `non-positive forward PER` に分解して検証する研究。

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/annual_forward_per_regime_decomposition.py`
- Runner:
  - `apps/bt/scripts/research/run_annual_forward_per_regime_decomposition.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`
  - `summary.json`

## Design

- Input: annual first-open/last-close fundamental panel bundle.
- Forward PER decomposition:
  - `non_positive`: `forward PER <= 0`
  - `positive_low`: `forward PER > 0` の universe 内で低い群
  - `positive_other`: `forward PER > 0` の残り
  - `missing_or_nonfinite`: 欠損・非finite
- Event-level views:
  - regime coverage
  - regime return summary
  - `low PBR` / `low PBR + small cap` 内の conditional regime summary
  - fixed-effect OLS with `positive low forward PER` と
    `non-positive forward PER` の同時投入
- Portfolio lens:
  - walk-forward `low PBR + small cap`
  - walk-forward `low PBR + small cap + full low forward PER`
  - walk-forward `low PBR + small cap + positive low forward PER`
  - walk-forward `low PBR + small cap + non-positive forward PER`
  - walk-forward decomposed four-factor
  - top `5% / 10%`, liquidity `none / ADV60 >= 10mn JPY`

## Outputs

- `prepared_panel_df`
- `regime_coverage_df`
- `regime_return_summary_df`
- `conditional_regime_summary_df`
- `panel_regression_df`
- `selected_event_df`
- `selection_mix_df`
- `portfolio_daily_df`
- `portfolio_summary_df`
- `portfolio_regime_contribution_df`

## Current Findings

Baseline result: [`baseline-2026-04-24.md`](./baseline-2026-04-24.md)

- `standard` では `positive low forward PER` が event-level で最も強く、
  `non-positive forward PER` は secondary positive contributor として残った。
- Portfolio lens では、`top 10% / none` は decomposed walk-forward が最良、
  `ADV60 >= 10mn` では decomposed walk-forward が `full low forward PER`
  と同等以上だった。

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_forward_per_regime_decomposition.py \
  --output-root /tmp/trading25-research
```

出力先:

`/tmp/trading25-research/market-behavior/annual-forward-per-regime-decomposition/<run_id>/`

## Caveats

- Market split still uses the current `stocks` snapshot from the upstream
  annual panel.
- `positive_low` は各 `year x current market` の positive-forward-PER universe
  内 percentile で定義する。
- This is still a long-only yearly rebalance research view. Costs, slippage,
  order-size caps, and borrowability remain outside this runner.
