# TOPIX Downside Return Standard Deviation Exposure Timing

TOPIX long-only book に対して、downside return standard deviation が拡大したときだけ exposure を落とす rule-based refined overlay 研究です。

## Published Readout

### Decision

downside return standard deviation overlay は TOPIX exposure control の candidate として継続する。discovery leader は validation CAGR を犠牲にしつつ max drawdown を `-23.97% -> -13.12%` へ削り、Sharpe も `1.14 -> 1.28` に改善した。通常 stddev より目的に合うが、単独 rule ではなく後続の trend / breadth / shock confirmation committee で false de-risk を減らす。

### Why This Research Was Run

先行研究 `topix-return-standard-deviation-exposure-timing` では、通常 stddev が drawdown を削る一方で、上昇局面の exposure も落として CAGR を失いやすかった。この研究では downside return standard deviation に絞り、下落リスク拡大時だけ exposure を減らす設計にすることで、Sharpe と drawdown のバランスが改善するかを確認した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。common signal window は `2016-04-06 -> 2026-04-09`、realized comparison window は `2016-04-07 -> 2026-04-10`。signal は date `X` close、rebalance は `X+1 open`。overnight は旧 exposure、intraday は新 exposure。grid は downside std windows `3/4/5/6/7`、mean windows `1/2`、high thresholds `0.22/0.24/0.25/0.26/0.28`、low thresholds `0.05/0.10/0.15/0.20/0.22/0.24/0.25`、reduced exposure `0/0.10/0.25`、valid combinations `960`。

### Main Findings

#### downside refined は baseline より drawdown と Sharpe を改善するが、CAGR は落ちる。

| Selection | Params | Validation CAGR | Sharpe | Sortino | Max drawdown | CAGR diff | Sharpe diff | DD improvement |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 100% TOPIX | `+24.18%` | `1.14` | `1.60` | `-23.97%` | n/a | n/a | n/a |
| best discovery Sharpe/CAGR | `downside_std3 mean1 high0.25 low0.05 reduced0` | `+19.50%` | `1.28` | `1.84` | `-13.12%` | `-4.68pp` | `+0.14` | `+10.85pp` |
| best discovery DD improvement | `downside_std4 mean1 high0.24 low0.05 reduced0.1` | `+21.02%` | `1.39` | `2.01` | `-14.80%` | `-3.16pp` | `+0.25` | `+9.17pp` |

#### validation 後知恵ではより良い候補もあるが、committee で確認してから昇格する。

| Candidate | Validation CAGR | Sharpe | Sortino | Max drawdown | Avg exposure | Reduced state |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `std5_mean2_hi0p22_lo0p22_reduced0` | `+28.98%` | `1.73` | `2.57` | `-10.46%` | `92.9%` | `7.1%` |
| `std5_mean1_hi0p25_lo0p1_reduced0` | `+27.61%` | `1.72` | `2.55` | `-8.70%` | `90.6%` | `9.4%` |
| `std5_mean2_hi0p24_lo0p22_reduced0` | `+28.91%` | `1.72` | `2.56` | `-10.31%` | `93.5%` | `6.5%` |

#### baseline validation が強い地合いでも、downside signal は通常 stddev より機会損失が小さい。

| Strategy | Validation CAGR | Sharpe | Max drawdown | Avg exposure | Reduced state |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100% TOPIX baseline | `+24.18%` | `1.14` | `-23.97%` | `100.0%` | `0.0%` |
| downside best discovery | `+19.50%` | `1.28` | `-13.12%` | `90.2%` | `9.8%` |
| normal stddev best discovery Sharpe | `+11.95%` | `1.12` | `-15.23%` | `54.9%` | `60.1%` |

### Interpretation

downside stddev は、通常 stddev よりも「不要な de-risk」を減らしながら drawdown を削る。best discovery candidate は validation CAGR を落とすが、平均 exposure が `90%` 前後で、risk-off する日が少ないため、単純な volatility overlay より使いやすい。とはいえ validation 上位候補は後知恵なので、単独で採用せず confirmation layer が必要。

### Production Implication

candidate として継続する。次段では downside stddev を core risk trigger とし、trend / breadth / shock confirmation を加えた committee overlay で、false de-risk と missed rebound を減らす。TOPIX100 streak strategy に使う場合も、まず TOPIX exposure overlay として独立に評価してから個別株 selection に重ねる。

### Caveats

grid は `960` combinations で multiple testing がある。selection は discovery leader を尊重し、validation 後知恵 candidate は昇格させない。cash return、cost、slippage、tax、rebalance friction は未評価。TOPIX long-only の overlay であり、個別株 strategy の drawdown 改善は別途検証が必要。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_exposure_timing.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_exposure_timing.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-exposure-timing/20260413_topix_downside_return_stddev_exposure_refined`
- Tables: `results.duckdb`

## Purpose

- 通常 stddev overlay の機会損失を減らす。
- downside risk expansion に絞った TOPIX exposure control を評価する。
- 後続の trend / breadth / shock confirmation committee overlay の core trigger を決める。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_exposure_timing.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_exposure_timing.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-exposure-timing/20260413_topix_downside_return_stddev_exposure_refined`

## Current Read

- downside stddev overlay は candidate として継続。
- 単独 rule ではなく confirmation committee へ進める。
- 通常 stddev より exposure を落とす日が少なく、目的に合う。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_downside_return_standard_deviation_exposure_timing.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- trend / breadth / shock confirmation を足すと false de-risk は減るか。
- TOPIX100 streak candidate の overlay として drawdown を削れるか。
- validation 後知恵で良い `std5` 周辺は walk-forward でも残るか。
