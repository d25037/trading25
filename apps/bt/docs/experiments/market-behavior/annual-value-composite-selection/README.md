# Annual Value Composite Selection

先行研究
[`annual-first-open-last-close-fundamental-panel`](../annual-first-open-last-close-fundamental-panel/README.md)
と
[`annual-fundamental-confounder-analysis`](../annual-fundamental-confounder-analysis/README.md)
を土台に、低 `PBR` + 小型 + 低 `forward PER` を銘柄採点へ落とす研究。

## Published Readout

### Decision

v3 PIT stock-master rerun でも、実用候補は `standard` の低 `PBR` + 小型 + 低 `forward PER` composite。`ADV60` floor は alpha score へ混ぜず、capacity / execution diagnostic として別管理する。

### Why This Research Was Run

confounder analysis で残った低 `PBR`、小型、低 `forward PER` を、年次リバランスの portfolio lens に落としたときに安定しているか、また positive-ratio filtering と liquidity floor でどれだけ劣化するかを確認した。

### Data Scope / PIT Assumptions

入力は v3 parent bundle `/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260429_212200_e60eacef/`。default は `32,264` scored events、positive-ratio run は `21,532` scored events。価格curveは parent bundle の `db_path` から selected event codes のみを読み直す。

### Main Findings

#### 結論

| Run | Market | Score | Liquidity | Top | Events | CAGR | Sharpe | MaxDD |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| default | `standard` | `fixed_55_25_20` | none | `10%` | `775` | `39.05%` | `2.22` | `-31.93%` |
| default | `standard` | `bucket_sum` | none | `10%` | `775` | `37.78%` | `2.20` | `-31.49%` |
| default | `standard` | `walkforward_regression_weight` | none | `10%` | `775` | `37.05%` | `2.15` | `-31.42%` |
| positive ratios | `standard` | `walkforward_regression_weight` | none | `10%` | `722` | `35.75%` | `2.14` | `-31.05%` |
| positive ratios | `standard` | `fixed_55_25_20` | none | `10%` | `722` | `35.84%` | `2.13` | `-30.78%` |

#### 結論

| Liquidity read | Effect |
| --- | --- |
| `ADV60 >= 10mn` | return/risk metrics fall materially versus no floor |
| rank score | keep alpha ranking independent from ADV |
| capacity | use ADV as position sizing / exclusion diagnostic |

### Interpretation

The strongest practical surface remains `standard` top decile. The fixed simple score is competitive with walk-forward regression, so a small, explainable composite is preferable to overfitting weights. Positive-ratio filtering lowers but does not remove the edge.

### Production Implication

Use `standard` value composite as a ranking candidate, not as a standalone production strategy. Keep liquidity, cost, turnover, and order-size caps outside the score and evaluate them in a separate execution layer.

### Caveats

The portfolio lens is annual open-to-close equal-weight and does not include costs, slippage, capacity, borrowability, or live turnover. The very strong `pbr_required_equal_weight` rows are small event-count branches and should not be promoted without a separate robustness check.

### Source Artifacts

- Default bundle: `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260429_212756_e60eacef/`
- Positive-ratio bundle: `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260429_212748_e60eacef/`
- Domain: `apps/bt/src/domains/analytics/annual_value_composite_selection.py`
- Runner: `apps/bt/scripts/research/run_annual_value_composite_selection.py`

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/annual_value_composite_selection.py`
- Runner:
  - `apps/bt/scripts/research/run_annual_value_composite_selection.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`
  - `summary.json`

## Design

- Input: annual first-open/last-close fundamental panel bundle.
- Core score:
  - `low_pbr_score`
  - `small_market_cap_score`
  - `low_forward_per_score`
- Score construction:
  - 各 score は `year x entry-date market` 内 percentile。
  - 高いほど「低PBR・小型・低forward PER」方向が強い。
  - `equal_weight`: 3 score の単純平均。
  - `bucket_sum`: 3 score の preferred-direction Q1-Q5 bucket 平均。
  - `pbr_required_equal_weight`: 低PBR bucket Q5 を必須にした単純平均。
  - `walkforward_regression_weight`: 過去年だけで core score のOLS係数を推定し、
    正の係数だけを正規化して翌年の重みに使う。
- Selection:
  - `all` / `prime` / `standard` / `growth` の market scope ごと。
  - 年ごとに top `5% / 10% / 15% / 20%` を選定。
  - liquidity/capacity floor として `ADV60 >= 10mn/30mn JPY`、
    `market cap >= 10bn JPY`、およびその組み合わせを比較する。
- Portfolio lens:
  - 選定銘柄を大発会 `Open` で等ウェイト保有し、大納会 `Close` まで日次で評価。
  - 年またぎの active position がない前提で、同一 signal family の全期間曲線を作る。

## Outputs

- `scored_panel_df`: realized event panel with core scores and composite scores.
- `walkforward_weight_df`: yearly prior-data-only OLS weights.
- `selected_event_df`: score method / liquidity scenario / top fraction ごとの採用銘柄。
- `selection_summary_df`: annual cohort return summary.
- `portfolio_daily_df`: equal-weight daily portfolio curve.
- `portfolio_summary_df`: total return, CAGR, Sharpe, Sortino, Calmar, maxDD.

## Current Findings

Baseline results:

- [`baseline-2026-04-23.md`](./baseline-2026-04-23.md)
- [`baseline-2026-04-24.md`](./baseline-2026-04-24.md)
  (`PBR > 0` and `forward PER > 0` practical rerun)

The practical rerun keeps the same main read: `standard` is still the cleanest
market, small cap stays first, low `PBR` stays second, and low `forward PER`
still adds value once obviously distressed negative-ratio names are excluded.
The deployable score should not include an `ADV60` floor: the capacity checks
show that even `ADV60 >= 10mn JPY` materially lowers return, so liquidity should
remain a side diagnostic rather than part of the rank score.

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_value_composite_selection.py \
  --output-root /tmp/trading25-research
```

Practical rerun:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_value_composite_selection.py \
  --output-root /tmp/trading25-research \
  --require-positive-pbr-and-forward-per
```

出力先:

`/tmp/trading25-research/market-behavior/annual-value-composite-selection/<run_id>/`

## Caveats

- Market split uses the upstream annual panel's entry-date `stock_master_daily`
  membership in the v3 rerun.
- The upstream annual panel handles per-share metrics with a common share
  baseline, so post-FY split/reverse-split bugs in EPS/BPS/forward EPS are
  inherited as adjusted diagnostics rather than raw ratios.
- This is a long-only yearly rebalance research view. Costs, slippage,
  borrowability, order-size caps, and liquidity impact are still outside this
  runner.
