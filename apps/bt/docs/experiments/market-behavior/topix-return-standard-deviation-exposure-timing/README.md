# TOPIX Return Standard Deviation Exposure Timing

TOPIX long-only book に対して、return standard deviation が拡大したときに exposure を落とし、低下したら 100% に戻す rule-based baseline 研究です。

## Published Readout

### Decision

通常の return standard deviation overlay は最終候補にはしない。探索上は max drawdown を大きく削る候補もあるが、discovery leader は validation CAGR をかなり犠牲にし、best validation Sharpe 候補も selection bias が強い。後続では downside return standard deviation に絞った refined search を優先する。

### Why This Research Was Run

TOPIX100 streak 研究では銘柄選択 edge が見えたが、drawdown が重かった。そこで、個別株選択の前段または overlay として、TOPIX の volatility expansion 時に exposure を減らすだけで long book の Sharpe / max drawdown が改善するかを確認した。この研究は downside stddev 研究の baseline でもある。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。common signal window は `2016-06-01 -> 2026-04-09`、realized comparison window は `2016-06-02 -> 2026-04-10`。signal は date `X` close で判定し、rebalance は `X+1 open`。overnight `X close -> X+1 open` は旧 exposure、intraday `X+1 open -> X+1 close` は新 exposure を使う。grid は std windows `5/10/20/40`、mean windows `1/3/5`、high thresholds `0.15/0.20/0.25/0.30`、low thresholds `0.10/0.15/0.20/0.25`、reduced exposure `0/0.25/0.50/0.75`、valid combinations `624`。

### Main Findings

#### baseline validation は地合いが強く、単純に exposure を落とすと CAGR を失いやすい。

| Split | CAGR | Sharpe | Sortino | Max drawdown | Positive rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| discovery | `+6.10%` | `0.43` | `0.61` | `-35.31%` | `52.7%` |
| validation | `+23.90%` | `1.12` | `1.57` | `-23.97%` | `56.5%` |
| full | `+11.15%` | `0.67` | `0.94` | `-35.31%` | `53.8%` |

#### discovery leaders は drawdown を削るが、validation CAGR の犠牲が大きい。

| Selection | Params | Validation CAGR | Sharpe | Max drawdown | CAGR diff | Sharpe diff | DD improvement |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| best discovery Sharpe | `std10 mean1 high0.15 low0.10 reduced0.25` | `+11.95%` | `1.12` | `-15.23%` | `-11.95pp` | `+0.01` | `+8.74pp` |
| best discovery CAGR | `std5 mean1 high0.30 low0.25 reduced0.75` | `+23.13%` | `1.18` | `-20.92%` | `-0.78pp` | `+0.06` | `+3.05pp` |
| best discovery DD improvement | `std20 mean1 high0.15 low0.10 reduced0` | `+5.24%` | `0.87` | `-7.65%` | `-18.66pp` | `-0.25` | `+16.32pp` |

#### validation 後知恵では良い候補もあるが、これをそのまま採用しない。

| Candidate | Validation CAGR | Sharpe | Max drawdown | Avg exposure | Reduced state |
| --- | ---: | ---: | ---: | ---: | ---: |
| `std20_mean5_hi0p15_lo0p15_reduced0p25` | `+19.30%` | `1.60` | `-11.00%` | `66.9%` | `44.2%` |
| `std20_mean5_hi0p15_lo0p15_reduced0` | `+17.43%` | `1.59` | `-8.70%` | `55.8%` | `44.2%` |
| `std5_mean3_hi0p25_lo0p2_reduced0` | `+22.55%` | `1.48` | `-14.52%` | `86.8%` | `13.2%` |

### Interpretation

return stddev は volatility expansion を捉えるが、上昇局面でも exposure を削るため、強い validation 地合いでは機会損失が大きい。drawdown を削るだけなら有効だが、Sharpe / CAGR / drawdown のバランスで見ると signal が粗い。downside-specific volatility の方が目的に合う可能性が高い。

### Production Implication

この通常 stddev overlay は production 候補にしない。後続の `topix-downside-return-standard-deviation-exposure-timing` を優先し、さらに trend / breadth / shock confirmation を組み合わせた committee overlay へ進める。通常 stddev は比較 baseline として残す。

### Caveats

rule search は `624` combinations で multiple testing がある。selection は discovery 基準だが、validation leader の後知恵は採用しない。TOPIX long-only の exposure overlay であり、TOPIX100 stock selection への適用は別途検証が必要。cost、slippage、tax、cash return は未評価。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix_return_standard_deviation_exposure_timing.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_return_standard_deviation_exposure_timing.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-return-standard-deviation-exposure-timing/20260413_topix_return_stddev_exposure_baseline`
- Tables: `results.duckdb`

## Purpose

- TOPIX long-only exposure を volatility expansion 時に落とすだけで risk-adjusted return が改善するかを見る。
- downside stddev overlay の baseline にする。
- 実行 timing を `X close signal -> X+1 open rebalance` に固定する。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_return_standard_deviation_exposure_timing.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_return_standard_deviation_exposure_timing.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix-return-standard-deviation-exposure-timing/20260413_topix_return_stddev_exposure_baseline`

## Current Read

- 通常 stddev overlay は比較 baseline に留める。
- drawdown は削れるが CAGR loss が大きい。
- 後続は downside stddev に絞る。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_return_standard_deviation_exposure_timing.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- downside stddev なら上昇局面の機会損失を減らせるか。
- trend / breadth confirmation を加えると false de-risk が減るか。
- TOPIX100 streak strategy の risk overlay に転用できるか。
