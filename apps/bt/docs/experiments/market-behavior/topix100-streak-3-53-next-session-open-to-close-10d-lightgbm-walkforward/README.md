# TOPIX100 Streak 3/53 Next-Session Open-to-Close 10D LightGBM Walk-Forward

TOPIX streak `short=3 / long=53` context を使い、TOPIX100 個別株を X+1 open で買って X+10 close で評価する long-only walk-forward LightGBM 研究です。

## Published Readout

### Decision

この run は **future leak により invalidated**。LightGBM Top 3 raw `+1.36%`、TOPIX excess `+0.83%`、drawdown `-96.69%` という旧 headline は、current TOPIX100 membership を過去日に固定した contaminated universe 上の数値なので、longer-hold sensitivity としても採用しない。

### Why This Research Was Run

先行する `topix100-streak-3-53-next-session-open-to-close-5d-lightgbm-walkforward` では、TOPIX streak 3/53 context と LightGBM が 5d swing の long-only selection で baseline を上回った。この研究では hold を X+10 close まで伸ばし、rebound edge がより大きくなるのか、それとも drawdown と overlap risk が増えすぎるのかを確認した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。analysis range は `2016-07-27 -> 2026-03-19`。target は `X+1 open -> X+10 close return`。walk-forward は train `756` / test `126` / step `126`、split count は `12`、purge signal dates は `0`。top-k は `1` / `3` / `5` / `10` / `20`。primary benchmark は TOPIX、secondary benchmark は equal-weight TOPIX100 universe。

ただし、TOPIX100 universe は PIT-safe ではない。current `stocks.scale_category` の Core30 / Large70 を過去全期間へ固定しており、signal date 時点で知らない将来の membership / survivorship 情報を含む。10d hold overlap より重大な leak であり、walk-forward split や purge では取り除けない。

### Main Findings

#### future membership leak があるため、10d の強い headline は採用不可。

| Item | 旧 readout の見え方 | 現在の扱い |
| --- | --- | --- |
| Top 3 raw | LightGBM `+1.3615%` | contaminated universe のため invalid |
| Top 3 TOPIX excess | LightGBM `+0.8283%` | contaminated universe のため invalid |
| 10d sensitivity | 5d より return は大きいが drawdown が重い | sensitivity としても使わない |
| Production blocker | drawdown / overlap / capacity | まず future membership leak が P0 blocker |

#### LightGBM は全 Top-K で baseline を上回るが、lift は Top-K が広がるほど縮む。

| Top-K | Baseline raw | LightGBM raw | Raw lift | Baseline vs TOPIX | LightGBM vs TOPIX | TOPIX lift | LightGBM hit vs TOPIX |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `1` | `+0.9095%` | `+1.6110%` | `+0.7015%` | `+0.3763%` | `+1.0778%` | `+0.7015%` | `53.3%` |
| `3` | `+0.9475%` | `+1.3615%` | `+0.4139%` | `+0.4144%` | `+0.8283%` | `+0.4139%` | `55.6%` |
| `5` | `+0.9949%` | `+1.2114%` | `+0.2166%` | `+0.4617%` | `+0.6782%` | `+0.2166%` | `55.0%` |
| `10` | `+0.9167%` | `+1.0800%` | `+0.1633%` | `+0.3836%` | `+0.5469%` | `+0.1633%` | `57.6%` |
| `20` | `+0.9379%` | `+0.9574%` | `+0.0194%` | `+0.4048%` | `+0.4242%` | `+0.0194%` | `61.4%` |

#### Top 3 の return / Sharpe は強いが、drawdown は production には重すぎる。

| Model | Series | Avg daily | Sharpe | Max drawdown | Positive rate |
| --- | --- | ---: | ---: | ---: | ---: |
| baseline | long | `+0.9475%` | `2.95` | `-97.65%` | `57.1%` |
| LightGBM | long | `+1.3615%` | `3.60` | `-96.69%` | `59.2%` |
| LightGBM | excess vs TOPIX | `+0.8283%` | `3.04` | `-72.90%` | `55.6%` |
| LightGBM | excess vs universe | `+0.6494%` | `2.45` | `-80.37%` | `53.8%` |

#### Feature importance は 5d swing と同じく `price_vs_sma_50_gap` が先頭。

| Feature | Importance share |
| --- | ---: |
| `price_vs_sma_50_gap` | `15.88%` |
| `volume_sma_5_20` | `13.51%` |
| `recent_return_5d` | `10.46%` |
| `range_pct` | `10.09%` |
| `recent_return_3d` | `9.21%` |
| `decile` | `7.74%` |

### Interpretation

旧解釈は破棄する。以前は「10d は return が強いが drawdown が重い sensitivity」と読んでいたが、current TOPIX100 universe を過去に固定しているため、return の強さが signal なのか future membership bias なのか判別できない。hold 長の比較も PIT-safe rerun まで保留する。

### Production Implication

production / ranking / screening には使わない。10d は exit diagnostic としても一旦破棄し、date-effective TOPIX100 membership または PIT-safe 代替 universe で再構築した後にだけ、5d vs 10d の hold sensitivity を読み直す。

### Caveats

これは通常の caveat ではなく **P0 invalidation**。fees、slippage、capacity、overlap、sector concentration、drawdown より先に、current TOPIX100 membership を過去に固定した future leak がある。`purge_signal_dates=0` は 10d overlap policy を解決していないだけでなく、universe-level leak には無関係。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix100_streak_353_next_session_open_to_close_10d_lightgbm_walkforward.py`
- Domain logic: `apps/bt/src/domains/analytics/topix100_streak_353_next_session_open_to_close_10d_lightgbm_walkforward.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix100-streak-3-53-next-session-open-to-close-10d-lightgbm-walkforward/20260411_swing10d_wf126`
- Tables: `results.duckdb`

## Purpose

- 5d swing で見えた TOPIX100 streak 3/53 edge が 10d hold でも残るかを見る。
- long-only raw / TOPIX excess / universe excess を walk-forward OOS で比較する。
- hold extension の return と drawdown の tradeoff を確認する。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_streak_353_next_session_open_to_close_10d_lightgbm_walkforward.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_streak_353_next_session_open_to_close_10d_lightgbm_walkforward.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix100-streak-3-53-next-session-open-to-close-10d-lightgbm-walkforward/20260411_swing10d_wf126`

## Current Read

- future leak により invalidated。
- 10d の headline return / Sharpe / drawdown は sensitivity としても使わない。
- PIT-safe universe で再実行するまで、5d との hold 比較は保留する。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt --group research python \
  apps/bt/scripts/research/run_topix100_streak_353_next_session_open_to_close_10d_lightgbm_walkforward.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- date-effective TOPIX100 membership で 10d return は残るか。
- PIT-safe rerun 後に 5d と 10d のどちらが risk-adjusted に良いか。
- 再実行までは continuation / exit diagnostic として使わない。
