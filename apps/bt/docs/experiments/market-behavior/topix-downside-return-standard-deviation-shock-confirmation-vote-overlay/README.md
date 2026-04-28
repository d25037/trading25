# TOPIX Downside Return Standard Deviation Shock Confirmation Vote Overlay

downside stddev stress family、TOPIX trend family、TOPIX100 breadth family を個別 rule ではなく vote threshold として扱う overlay 研究です。

## Published Readout

### Decision

この run は **future leak により invalidated**。family-level vote の発想は残してよいが、breadth universe が current TOPIX100 membership proxy なので、validation Sharpe win rate `55%` や平均 Sharpe excess `+0.20` は candidate evidence として使わない。PIT-safe breadth universe で再実行するまで production / ranking 判断から除外する。

### Why This Research Was Run

先行する trend breadth overlay では、個別 trend rule / breadth rule の exact selection が不安定だった。この研究では individual rule pick ではなく、trend family と breadth family の vote threshold に抽象化し、family-level confirmation の方が fold 間で安定するかを確認した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。stress windows は `5`、stress means は `1/2`、stress high thresholds は `0.24/0.25`、stress low thresholds は `0.20/0.22`、reduced exposure は `0/0.10`。trend family は `close_below_sma20`、`sma20_below_sma60`、`drawdown_63d_le_neg0p05`、`return_10d_le_neg0p03`。breadth family は `topix100_above_sma20_le_0p40`、`topix100_positive_5d_le_0p40`、`topix100_at_20d_low_ge_0p20`。trend vote thresholds は `1/2/3/4`、breadth vote thresholds は `1/2/3`、confirmation modes は `stress_and_trend_and_breadth`、`stress_and_trend_or_breadth`、`two_of_three_vote`。candidate count は `576`、walk-forward fold count は `11`。

PIT 上の blocker は TOPIX100 breadth universe。historical constituents ではなく current market DB の proxy を過去日に固定しているため、universe-level future leak がある。

### Main Findings

#### vote overlay も TOPIX100 membership leak により invalidated。

| Item | 旧 readout の見え方 | 現在の扱い |
| --- | --- | --- |
| Best candidate validation Sharpe | `1.16` vs baseline `1.08` | contaminated universe のため invalid |
| Best candidate validation CAGR | `+22.68%` vs baseline `+23.08%` | contaminated universe のため invalid |
| Walk-forward Sharpe win rate | `55%` | contaminated universe のため invalid |
| Candidate status | family-level confirmation candidate | candidate から除外し、PIT-safe rerun 待ち |

#### 旧 best discovery は Sharpe を少し上げたが、CAGR と drawdown では baseline を明確に超えない。

| Lens | Value |
| --- | ---: |
| Candidate | `std5_mean1_hi0p25_lo0p22_reduced0_trendvotes4_breadthvotes1_mode_stress_and_trend_and_breadth` |
| Discovery Sharpe | `0.70` |
| Validation Sharpe | `1.16` |
| Validation CAGR | `+22.68%` |
| Validation max drawdown | `-23.97%` |
| Baseline validation Sharpe | `1.08` |
| Baseline validation CAGR | `+23.08%` |
| Baseline validation max drawdown | `-23.97%` |

#### leak を除いても exact vote threshold の rank stability は弱い。

| Metric | Value |
| --- | ---: |
| Fold count | `11` |
| Mean fold Spearman Sharpe | `0.10` |
| Median fold Spearman Sharpe | `0.16` |
| Mean top 10 overlap | `0.00` |
| Mean top 20 overlap | `0.10` |
| Mean top 50 overlap | `0.12` |
| Mean validation Sharpe excess | `+0.20` |
| Mean validation CAGR excess | `+2.83pp` |
| Mean validation max drawdown improvement | `+5.68pp` |

### Interpretation

family-level vote は、個別 rule を選ぶより抽象度が高く、PIT-safe に作り直す価値はある。ただし、この run は breadth universe が contaminated なので performance は読まない。さらに top overlap が低く、vote threshold を exact に最適化する形も危ない。再実行時は family concept と threshold stability を分けて見る必要がある。

### Production Implication

production / ranking / screening には使わない。PIT-safe TOPIX100 breadth または historical constituents を使った再実行で、vote family が downside stddev overlay の false de-risk を本当に減らすかを確認する。現時点で残せるのは「個別 rule ではなく family vote に抽象化する」という設計案だけ。

### Caveats

これは通常の caveat ではなく **P0 invalidation**。current TOPIX100 membership proxy を過去に固定した future leak がある。cost、slippage、tax、cash return、capacity、multiple testing は未評価だが、まず universe contamination の解消が必要。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-shock-confirmation-vote-overlay/20260413_topix_shock_confirmation_vote_overlay`
- Tables: `results.duckdb`

## Purpose

- trend / breadth confirmation を exact rule selection ではなく family vote として扱う。
- vote threshold が fold 間で安定するかを見る。
- 後続の committee overlay の候補空間を絞る。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-shock-confirmation-vote-overlay/20260413_topix_shock_confirmation_vote_overlay`

## Current Read

- future leak により invalidated。
- family vote の設計案だけを残し、performance 数値は使わない。
- PIT-safe breadth universe で完全に再実行する。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py \
  --run-id 20260413_topix_shock_confirmation_vote_overlay
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

