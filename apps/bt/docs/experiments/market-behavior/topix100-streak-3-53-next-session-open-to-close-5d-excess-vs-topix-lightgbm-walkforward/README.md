# TOPIX100 Streak 3/53 Next-Session Open-to-Close 5D Excess-vs-TOPIX LightGBM Walk-Forward

TOPIX streak `short=3 / long=53` context を使い、TOPIX100 個別株の X+1 open -> X+5 close excess return vs TOPIX を直接 target にする long-only walk-forward LightGBM 研究です。

## Published Readout

### Decision

この run は **future leak により invalidated**。5d excess-vs-TOPIX target は旧 readout では主 candidate だったが、TOPIX100 universe を current `stocks.scale_category` から作って過去日に固定していたため、Top 3 LightGBM `+0.43%` vs baseline `+0.18%`、Sharpe `2.33`、max drawdown `-44.55%` は production evidence として使わない。

### Why This Research Was Run

先行研究 `topix100-streak-3-53-next-session-open-to-close-5d-lightgbm-walkforward` は raw return target で TOPIX excess も改善したが、beta-loaded rebound を拾っている可能性が残った。この研究では train target を `stock 5d return - TOPIX 5d return` に変え、TOPIX を直接超える銘柄選択が OOS で残るかを確認した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。analysis range は `2016-07-27 -> 2026-03-27`。target は `stock(X+1 open -> X+5 close) - TOPIX(X+1 open -> X+5 close)`。walk-forward は train `756` / test `126` / step `126`、split count は `12`、purge signal dates は `0`。top-k は `1` / `3` / `5` / `10` / `20`。評価 table は raw realized PnL を使い、training target は TOPIX excess のまま。

ただし、TOPIX100 universe は PIT-safe ではない。`stocks` table の現在/事後確定 `scale_category`（Core30 / Large70）を使って過去全期間の銘柄集合を作っており、signal date 時点で利用できない future membership / survivorship 情報が入っている。TOPIX excess target にしても、この universe-level leak は消えない。

### Main Findings

#### future membership leak があるため、旧主 candidate は撤回する。

| Item | 旧 readout の見え方 | 現在の扱い |
| --- | --- | --- |
| Primary KPI | Top 3 TOPIX excess `+0.4294%` | contaminated universe のため invalid |
| Baseline comparison | LightGBM が baseline を `+0.2484%` 上回る | contaminated universe のため invalid |
| Drawdown improvement | TOPIX excess drawdown `-44.55%` まで改善 | leak が先に blocker。risk改善根拠にしない |
| SoT candidate | TOPIX100 streak 系の主 candidate | candidate 撤回。PIT-safe rerun まで停止 |

#### Top 3 では LightGBM が raw / TOPIX excess / universe excess の全てで baseline を上回った。

| Top-K | Baseline raw | LightGBM raw | Raw lift | Baseline vs TOPIX | LightGBM vs TOPIX | TOPIX lift | LightGBM hit vs TOPIX |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `1` | `+0.4630%` | `+0.8165%` | `+0.3534%` | `+0.2105%` | `+0.5639%` | `+0.3534%` | `51.3%` |
| `3` | `+0.4334%` | `+0.6819%` | `+0.2484%` | `+0.1809%` | `+0.4294%` | `+0.2484%` | `55.8%` |
| `5` | `+0.4269%` | `+0.5966%` | `+0.1697%` | `+0.1744%` | `+0.3441%` | `+0.1697%` | `55.8%` |
| `10` | `+0.4789%` | `+0.5344%` | `+0.0555%` | `+0.2264%` | `+0.2819%` | `+0.0555%` | `56.7%` |
| `20` | `+0.4455%` | `+0.4434%` | `-0.0021%` | `+0.1930%` | `+0.1908%` | `-0.0021%` | `56.3%` |

#### raw 5d より drawdown profile は改善し、TOPIX excess の max drawdown は `-44.55%` まで下がった。

| Model | Series | Avg daily | Sharpe | Max drawdown | Positive rate |
| --- | --- | ---: | ---: | ---: | ---: |
| baseline | excess vs TOPIX | `+0.1809%` | `1.22` | `-48.73%` | `49.5%` |
| LightGBM | excess vs TOPIX | `+0.4294%` | `2.33` | `-44.55%` | `55.8%` |
| LightGBM | excess vs universe | `+0.3480%` | `1.94` | `-53.03%` | `54.2%` |
| LightGBM | long | `+0.6819%` | `2.61` | `-82.46%` | `59.1%` |

#### Feature importance は raw target と違い、`volume_sma_5_20` が先頭に出た。

| Feature | Importance share |
| --- | ---: |
| `volume_sma_5_20` | `13.24%` |
| `price_vs_sma_50_gap` | `12.78%` |
| `range_pct` | `11.67%` |
| `recent_return_5d` | `11.33%` |
| `recent_return_3d` | `9.77%` |
| `intraday_return` | `9.59%` |

### Interpretation

旧解釈は破棄する。以前は「TOPIX excess を直接 target にしても lift が残るため beta rebound だけではない」と読んでいたが、current TOPIX100 membership を過去へ固定しているため、その lift が stock selection なのか future membership bias なのか判別できない。`volume_sma_5_20` の feature importance も候補根拠にしない。

### Production Implication

production / ranking / screening には使わない。この研究を TOPIX100 streak 系の主 candidate とする判断は撤回する。次段は overlap / capacity / risk cap ではなく、date-effective TOPIX100 membership または PIT-safe 代替 universe で feature panel、benchmark universe、TOPIX excess target を作り直すこと。

### Caveats

これは通常の caveat ではなく **P0 invalidation**。fees、slippage、capacity、position overlap、concentration、raw drawdown より先に、current TOPIX100 membership を過去に固定した future leak がある。TOPIX excess target、walk-forward、`purge_signal_dates=0` は、この universe-level leak を解消しない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix100_streak_353_next_session_open_to_close_5d_excess_vs_topix_lightgbm_walkforward.py`
- Domain logic: `apps/bt/src/domains/analytics/topix100_streak_353_next_session_open_to_close_5d_excess_vs_topix_lightgbm_walkforward.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix100-streak-3-53-next-session-open-to-close-5d-excess-vs-topix-lightgbm-walkforward/20260413_swing5d_excess_topix_wf126_fixeval`
- Tables: `results.duckdb`

## Purpose

- TOPIX100 streak 3/53 swing score を TOPIX excess target に寄せる。
- raw beta-loaded return ではなく、TOPIX を直接超える stock selection を OOS で確認する。
- 後続の duplicate policy / committee overlay / production-like risk control の baseline にする。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_streak_353_next_session_open_to_close_5d_excess_vs_topix_lightgbm_walkforward.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_streak_353_next_session_open_to_close_5d_excess_vs_topix_lightgbm_walkforward.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix100-streak-3-53-next-session-open-to-close-5d-excess-vs-topix-lightgbm-walkforward/20260413_swing5d_excess_topix_wf126_fixeval`

## Current Read

- future leak により invalidated。
- TOPIX excess target を主 candidate とする判断は撤回する。
- PIT-safe universe で再実行するまで、Top 3/5 や drawdown 改善は使わない。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt --group research python \
  apps/bt/scripts/research/run_topix100_streak_353_next_session_open_to_close_5d_excess_vs_topix_lightgbm_walkforward.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- date-effective TOPIX100 membership で excess-vs-TOPIX lift は残るか。
- PIT-safe 代替 universe でも TOPIX excess target は有効か。
- 再実行後に初めて duplicate / capacity / risk control を検討する。
