# TOPIX100 Streak 3/53 Next-Session Intraday LightGBM Walk-Forward

TOPIX streak `short=3 / long=53` context を使い、TOPIX100 個別株の next-session open-to-close return を LightGBM と lookup baseline で walk-forward 比較する実験です。

## Published Readout

### Decision

この run は **future leak により invalidated**。TOPIX100 universe を signal date 時点の構成銘柄ではなく、`stocks` の現在/事後確定 `scale_category` から作って過去日に適用していたため、survivorship / future membership leak を含む。したがって、walk-forward OOS、baseline 比較、Top/Bottom spread、feature importance のどれも production 判断の証拠として使わない。

### Why This Research Was Run

先行研究 `topix-streak-multi-timeframe-mode` で `short=3 / long=53` の market context が選ばれた。次に、TOPIX100 個別株 selection として、同じ context に `price_vs_sma_50_gap` / `volume_sma_5_20` / recent return / intraday features を足した LightGBM が、train-only lookup baseline を OOS で上回るかを確認した。これは fixed split intraday score の過学習チェックでもある。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。analysis range は `2016-07-27 -> 2026-04-09`。target は `next-session open -> close return`。walk-forward は train `756` / test `126` / step `126`、split count は `12`、purge signal dates は `0`。top-k は `1` / `3` / `5` / `10` / `20`。各 split で baseline を train window から再構築し、LightGBM も同じ train block だけで再学習する。

ただし、universe construction が PIT-safe ではない。`TOPIX100` を current `stocks.scale_category` の Core30 / Large70 で固定し、その現在構成を 2016 年以降の過去日に結合している。これは split 内学習以前の入力 universe に未来情報が入るため、purge や walk-forward では除去できない。

### Main Findings

#### future membership leak があるため、旧 headline は evidence ではない。

| Item | 旧 readout の見え方 | 現在の扱い |
| --- | --- | --- |
| Universe | TOPIX100 個別株の walk-forward OOS | current TOPIX100 構成を過去に固定した contaminated universe |
| Baseline vs LightGBM | baseline が LightGBM より強い | 比較母集団が漏洩しているため invalid |
| Top/Bottom spread | baseline `+0.12%` vs LightGBM `+0.05%` | production 判断に使わない |
| Next step | intraday を捨てて 5d swing へ移る | TOPIX100 streak 系は PIT-safe universe で作り直すまで停止 |

#### OOS の Top/Bottom spread は baseline の方が強く、LightGBM は intraday edge を上積みできなかった。

| Top-K | Baseline long | LightGBM long | Long lift | Baseline spread | LightGBM spread | Spread lift | LightGBM spread hit+ |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `1` | `+0.0804%` | `+0.0610%` | `-0.0194%` | `+0.1467%` | `-0.0349%` | `-0.1816%` | `49.5%` |
| `3` | `+0.1043%` | `+0.0781%` | `-0.0262%` | `+0.1238%` | `+0.0460%` | `-0.0779%` | `52.1%` |
| `5` | `+0.0733%` | `+0.0718%` | `-0.0015%` | `+0.0809%` | `+0.0600%` | `-0.0209%` | `52.4%` |
| `10` | `+0.0509%` | `+0.0379%` | `-0.0129%` | `+0.0517%` | `+0.0400%` | `-0.0117%` | `52.1%` |
| `20` | `+0.0347%` | `+0.0306%` | `-0.0041%` | `+0.0282%` | `+0.0254%` | `-0.0028%` | `51.4%` |

#### Top 3 の execution view でも、LightGBM pair は drawdown が重く、baseline より弱い。

| Model | Series | Avg daily | Sharpe | Max drawdown | Positive rate |
| --- | --- | ---: | ---: | ---: | ---: |
| baseline | long | `+0.1043%` | `1.52` | `-15.52%` | `53.2%` |
| baseline | pair 50/50 | `+0.0619%` | `1.72` | `-6.88%` | `54.5%` |
| LightGBM | long | `+0.0781%` | `0.85` | `-23.99%` | `52.4%` |
| LightGBM | pair 50/50 | `+0.0230%` | `0.47` | `-28.64%` | `52.1%` |
| LightGBM | short edge | `-0.0322%` | `-0.37` | `-71.56%` | `49.2%` |

#### Feature importance は `price_vs_sma_50_gap` が先頭だが、model lift にはつながらなかった。

| Feature | Importance share |
| --- | ---: |
| `price_vs_sma_50_gap` | `13.03%` |
| `volume_sma_5_20` | `12.00%` |
| `range_pct` | `11.95%` |
| `recent_return_5d` | `11.24%` |
| `intraday_return` | `11.00%` |

### Interpretation

旧解釈は破棄する。以前は「intraday target は薄く、5d swing へ移る」と読んでいたが、実際には TOPIX100 universe 自体が事後確定情報を含んでいた。model が弱かったという結論すら、正しい母集団で再検証するまでは採用しない。

### Production Implication

production / ranking / screening には使わない。TOPIX100 streak 3/53 系は、date-effective な index membership または PIT-safe な代替 universe で feature panel を再構築し、すべての headline を再生成するまで停止する。

### Caveats

これは通常の caveat ではなく **P0 invalidation**。open auction slippage、fees、borrow cost、turnover control、capacity 以前に、current TOPIX100 membership を過去に使った future leak がある。`purge_signal_dates=0` や walk-forward 再学習は、この universe-level leak を解消しない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix100_streak_353_next_session_intraday_lightgbm_walkforward.py`
- Domain logic: `apps/bt/src/domains/analytics/topix100_streak_353_next_session_intraday_lightgbm_walkforward.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix100-streak-3-53-next-session-intraday-lightgbm-walkforward/20260410_ptit_wf126`
- Tables: `results.duckdb`

## Purpose

- TOPIX streak 3/53 context を TOPIX100 intraday stock selection に転用する。
- lookup baseline と LightGBM を rolling train/test で OOS 比較する。
- fixed split intraday score が過学習だったかを確認する。

## Scope

- Universe:
  - `TOPIX100`
- Target:
  - `next-session open -> close return`
- Walk-forward:
  - train `756`, test `126`, step `126`
- Top-K:
  - `1`, `3`, `5`, `10`, `20`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix100_streak_353_next_session_intraday_lightgbm_walkforward.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix100_streak_353_next_session_intraday_lightgbm_walkforward.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix100-streak-3-53-next-session-intraday-lightgbm-walkforward/20260410_ptit_wf126`

## Current Read

- future leak により invalidated。
- current TOPIX100 membership を過去日に固定しているため、headline metrics は使わない。
- PIT-safe universe で再実行するまで、TOPIX100 streak 系は production 候補から除外する。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt --group research python \
  apps/bt/scripts/research/run_topix100_streak_353_next_session_intraday_lightgbm_walkforward.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- date-effective TOPIX100 membership を取得できるか。
- 取得できない場合、PIT-safe な代替 universe をどう定義するか。
- PIT-safe rerun 後も streak 3/53 context の stock-selection lift は残るか。
