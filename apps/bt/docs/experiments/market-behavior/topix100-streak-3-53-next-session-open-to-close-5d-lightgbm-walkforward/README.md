# TOPIX100 Streak 3/53 Next-Session Open-to-Close 5D LightGBM Walk-Forward

TOPIX streak `short=3 / long=53` context を使い、TOPIX100 個別株を X+1 open で買って X+5 close で評価する long-only walk-forward LightGBM 研究です。

## Published Readout

### Decision

この run は **future leak により invalidated**。Top 3 の LightGBM lift や `+0.30%` 改善は、TOPIX100 universe を current `stocks.scale_category` から作って過去日に固定した contaminated result なので、candidate として扱わない。5d swing long-only の仮説は残してよいが、この readout の performance 数値は production / ranking 判断に使わない。

### Why This Research Was Run

直前の intraday walk-forward では、LightGBM が baseline を上回らず、short edge も弱かった。一方、先行する TOPIX streak 研究は bearish/rebound context を示していた。この研究では target を `X+1 open -> X+5 close` に伸ばし、long-only で TOPIX と TOPIX100 universe を超える銘柄選択ができるかを確認した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。analysis range は `2016-07-27 -> 2026-04-03`。target は `X+1 open -> X+5 close return`。walk-forward は train `756` / test `126` / step `126`、split count は `12`、purge signal dates は `0`。top-k は `1` / `3` / `5` / `10` / `20`。primary benchmark は TOPIX、secondary benchmark は equal-weight TOPIX100 universe。

ただし、この universe は PIT-safe ではない。TOPIX100 を `stocks` table の現在/事後確定 `scale_category`（Core30 / Large70）で抽出し、その構成を全過去日に適用している。これは signal date 時点で知らない将来の index membership / survivorship を feature panel と benchmark universe に混ぜる leak であり、walk-forward split では修正できない。

### Main Findings

#### future membership leak があるため、旧 headline は candidate evidence ではない。

| Item | 旧 readout の見え方 | 現在の扱い |
| --- | --- | --- |
| Top 3 raw lift | LightGBM が baseline を `+0.2959%` 上回る | contaminated universe のため invalid |
| Top 3 TOPIX excess lift | LightGBM が `+0.2959%` 上回る | contaminated universe のため invalid |
| Drawdown | `-79.85%` が主な production blocker | leak が先に blocker。drawdown 議論は二次的 |
| Candidate status | 5d swing long-only を候補として継続 | candidate から除外し、PIT-safe rerun 待ち |

#### Top 3 では LightGBM が raw / TOPIX excess / universe excess の全てで baseline を上回った。

| Top-K | Baseline raw | LightGBM raw | Raw lift | Baseline vs TOPIX | LightGBM vs TOPIX | TOPIX lift | LightGBM hit vs TOPIX |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `1` | `+0.5016%` | `+0.9064%` | `+0.4048%` | `+0.2491%` | `+0.6539%` | `+0.4048%` | `52.1%` |
| `3` | `+0.4270%` | `+0.7229%` | `+0.2959%` | `+0.1745%` | `+0.4704%` | `+0.2959%` | `55.5%` |
| `5` | `+0.4354%` | `+0.6342%` | `+0.1988%` | `+0.1829%` | `+0.3817%` | `+0.1988%` | `55.8%` |
| `10` | `+0.4656%` | `+0.4710%` | `+0.0054%` | `+0.2131%` | `+0.2185%` | `+0.0054%` | `56.7%` |
| `20` | `+0.4373%` | `+0.4027%` | `-0.0346%` | `+0.1848%` | `+0.1502%` | `-0.0346%` | `55.8%` |

#### Top 3 の portfolio stats は強いが、drawdown が production には重い。

| Model | Series | Avg daily | Sharpe | Max drawdown | Positive rate |
| --- | --- | ---: | ---: | ---: | ---: |
| baseline | long | `+0.4270%` | `1.99` | `-82.15%` | `56.6%` |
| LightGBM | long | `+0.7229%` | `2.73` | `-79.85%` | `58.1%` |
| LightGBM | excess vs TOPIX | `+0.4704%` | `2.45` | `-60.54%` | `55.5%` |
| LightGBM | excess vs universe | `+0.3891%` | `2.09` | `-58.16%` | `54.4%` |

#### Feature importance は intraday と同じく `price_vs_sma_50_gap` が先頭で、SMA50 系列と整合する。

| Feature | Importance share |
| --- | ---: |
| `price_vs_sma_50_gap` | `15.24%` |
| `volume_sma_5_20` | `12.95%` |
| `recent_return_5d` | `10.76%` |
| `range_pct` | `10.22%` |
| `recent_return_3d` | `9.97%` |
| `decile` | `6.90%` |

### Interpretation

旧解釈は破棄する。以前は「5d swing では stock selection lift がある」と読んでいたが、母集団が current TOPIX100 membership で固定されていたため、stock selection の lift と survivorship / future membership bias を分離できない。PIT-safe universe で再実行するまでは、SMA50 系 feature importance も候補根拠にしない。

### Production Implication

production / ranking / screening には使わない。次段は risk control ではなく、まず date-effective TOPIX100 membership または PIT-safe 代替 universe による再構築。再実行後にだけ、5d swing long-only、Top 3/5、excess vs TOPIX を再評価する。

### Caveats

これは通常の caveat ではなく **P0 invalidation**。fees、slippage、capacity、position overlap、sector/market concentration、drawdown より先に、current TOPIX100 membership を過去に固定した future leak がある。`purge_signal_dates=0` は 5d hold overlap だけの話で、universe-level leak を無効化しない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix100_streak_353_next_session_open_to_close_5d_lightgbm_walkforward.py`
- Domain logic: `apps/bt/src/domains/analytics/topix100_streak_353_next_session_open_to_close_5d_lightgbm_walkforward.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix100-streak-3-53-next-session-open-to-close-5d-lightgbm-walkforward/20260411_swing5d_wf126`
- Tables: `results.duckdb`

## Purpose

- intraday ではなく 5d swing target で TOPIX100 stock selection を検証する。
- LightGBM が lookup baseline、TOPIX、TOPIX100 universe を OOS で超えるかを見る。
- 後続の excess-vs-TOPIX / duplicate-policy / committee overlay 系列の baseline にする。

## Scope

- Universe:
  - `TOPIX100`
- Target:
  - `X+1 open -> X+5 close return`
- Walk-forward:
  - train `756`, test `126`, step `126`
- Benchmarks:
  - TOPIX
  - equal-weight TOPIX100 universe

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_streak_353_next_session_open_to_close_5d_lightgbm_walkforward.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_streak_353_next_session_open_to_close_5d_lightgbm_walkforward.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix100-streak-3-53-next-session-open-to-close-5d-lightgbm-walkforward/20260411_swing5d_wf126`

## Current Read

- future leak により invalidated。
- 5d swing long-only は仮説としては残るが、この run は candidate ではない。
- 次の関門は risk control ではなく、PIT-safe universe での完全再実行。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt --group research python \
  apps/bt/scripts/research/run_topix100_streak_353_next_session_open_to_close_5d_lightgbm_walkforward.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- date-effective TOPIX100 membership を使うと Top 3/5 lift は残るか。
- PIT-safe 代替 universe では 5d swing edge は消えるか。
- 再実行後に初めて overlap / capacity / risk cap を検討する。
