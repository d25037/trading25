# Annual Forward PER Regime Decomposition

先行研究
[`annual-first-open-last-close-fundamental-panel`](../annual-first-open-last-close-fundamental-panel/README.md),
[`annual-fundamental-confounder-analysis`](../annual-fundamental-confounder-analysis/README.md),
[`annual-value-composite-selection`](../annual-value-composite-selection/README.md)
を土台に、`low forward PER` の中身を
`positive low forward PER` と `non-positive forward PER` に分解して検証する研究。

## Published Readout

### Decision

低 `forward PER` は v3 rerun でも有効だが、単純な positive-only 分解より、`low PBR + small cap` の base score に `full/decomposed forward PER` を補助的に足す扱いが良い。`non-positive forward PER` は捨てるだけではなく、secondary contributor として残る。

### Why This Research Was Run

low `forward PER` の効果が、健全な `forward PER > 0` の割安性なのか、赤字・非正分母銘柄の反発なのかを切り分け、portfolio selection でどちらを残すべきか確認した。

### Data Scope / PIT Assumptions

入力は v3 parent bundle `/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260429_212200_e60eacef/`。期間は `2017-2025`、analysis events は `32,264`、finite forward PER events は `26,059`。upstream は entry-date `stock_master_daily` と entry-date as-of fundamentals を使う。

### Main Findings

#### 結論

| Scope | Regime | Events | Mean return | Annual mean | Year t |
| --- | --- | ---: | ---: | ---: | ---: |
| `all` | `positive_low` | `4,873` | `20.65%` | `20.66%` | `2.86` |
| `all` | `non_positive` | `1,646` | `9.73%` | `9.71%` | `1.48` |
| `standard` | `positive_low` | `1,635` | `22.71%` | `21.99%` | `2.64` |
| `standard` | `non_positive` | `604` | `13.37%` | `12.53%` | `1.82` |

#### 結論

| Market | Score | Liquidity | Top | Events | CAGR | Sharpe | MaxDD |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `standard` | `decomposed_forward_per_walkforward` | none | `10%` | `775` | `37.44%` | `2.20` | `-30.83%` |
| `standard` | `full_forward_per_walkforward` | none | `10%` | `775` | `37.05%` | `2.15` | `-31.42%` |
| `standard` | `base_pbr_size_walkforward` | none | `10%` | `828` | `36.79%` | `2.15` | `-32.73%` |
| `standard` | `decomposed_forward_per_walkforward` | `adv10m` | `10%` | `377` | `33.27%` | `1.62` | `-38.88%` |

### Interpretation

Event-level では `positive_low` が明確に強いが、portfolio lens では `non_positive` も secondary contributor として残る。したがって、`forward PER <= 0` を機械的に全排除するより、positive-ratio practical run と decomposition を並べて扱う方が安全。

### Production Implication

Production candidate では、低 `forward PER` を単独主役にせず、低 `PBR + small cap` の composite に対する補助ファクターとして扱う。Positive-only gate は品質管理として使えるが、alpha source の一部を落とす可能性がある。

### Caveats

This is still an annual equal-weight research portfolio. Costs, slippage, order-size caps, and borrowability remain outside this runner. `non_positive` は distress と turnaround が混ざるため、別途 negative denominator の内訳研究が必要。

### Source Artifacts

- v3 bundle: `/tmp/trading25-research/market-behavior/annual-forward-per-regime-decomposition/20260429_212554_e60eacef/`
- Domain: `apps/bt/src/domains/analytics/annual_forward_per_regime_decomposition.py`
- Runner: `apps/bt/scripts/research/run_annual_forward_per_regime_decomposition.py`

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

- Market split uses the upstream annual panel's entry-date `stock_master_daily`
  membership in the v3 rerun.
- `positive_low` は各 `year x entry-date market` の positive-forward-PER universe
  内 percentile で定義する。
- This is still a long-only yearly rebalance research view. Costs, slippage,
  order-size caps, and borrowability remain outside this runner.
