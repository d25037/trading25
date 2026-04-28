# TOPIX Extreme Mode Mean-Reversion Comparison

TOPIX の daily extreme mode と streak-candle extreme mode を同じ next-open entry / N-day close exit の簡易 backtest で比較し、mean-reversion trigger としてどちらを使うべきかを読む実験です。

## Published Readout

### Decision

この run は **future leak により invalidated**。streak mode を優先する判断は、先行 TOPIX streak の future-derived window selection と、この比較内の selected streak / hold read を引き継いでいる。`long_on_bearish` 10d の headline は、PIT-safe に選ばれた streak parameter の final OOS ではないため、TOPIX mean-reversion trigger として採用しない。

### Why This Research Was Run

前段の daily extreme mode と streak extreme mode は、どちらも segment state を説明しつつ、forward return は bearish 後に高いという mean-reversion 的な形だった。この研究では、同じ common date range と同じ next-open entry / N-day close exit に揃え、normal daily mode と streak mode のどちらを後続の TOPIX100 streak 研究に渡すべきかを判断した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。available range は `2016-03-25 -> 2026-04-03`、common comparison range は `2016-08-30 -> 2026-03-04`。normal candidate windows と streak candidate windows はともに `2..60`、selected normal は `2 days`、selected streak は `3 streaks`。hold days は `1` / `5` / `10` / `20`、validation ratio は `0.30`。entry は signal 後の next open、exit は N-day close の簡易 backtest。

ただし、selected streak window と execution comparison が PIT-safe に分離されていない。streak window / trigger candidate を future return で選び、その同じ broad historical range で long_on_bearish を比較しているため、execution-like な表でも strict OOS evidence ではない。

### Main Findings

#### future-derived streak selection を引き継ぐため、旧 execution headline は採用不可。

| Item | 旧 readout の見え方 | 現在の扱い |
| --- | --- | --- |
| Preferred trigger | streak `long_on_bearish` | selected streak が contaminated なので invalid |
| Best headline | streak 10d mean `+1.70%` / compound `+101.43%` | final OOS evidence ではない |
| Short side | `short_on_bullish` は弱い | 方向性の解釈も PIT-safe rerun まで保留 |
| Downstream use | TOPIX100 streak 3/53 に渡す mode definition | 使用停止 |

#### `long_on_bearish` は streak 10d が最も良く、bearish 後 rebound を実行側に寄せるなら streak を使う。

| Model | Hold | Trades | Mean | Win rate | Compound |
| --- | ---: | ---: | ---: | ---: | ---: |
| normal | `1d` | `298` | `+0.03%` | `52.3%` | `+8.34%` |
| streak | `1d` | `145` | `+0.31%` | `60.0%` | `+56.20%` |
| normal | `10d` | `62` | `+1.15%` | `69.4%` | `+91.77%` |
| streak | `10d` | `45` | `+1.70%` | `73.3%` | `+101.43%` |
| streak | `20d` | `26` | `+2.34%` | `73.1%` | `+76.70%` |

#### `short_on_bullish` は対称な short signal としては弱く、promote しない。

| Model | Hold | Trades | Mean | Win rate | Compound |
| --- | ---: | ---: | ---: | ---: | ---: |
| normal | `1d` | `410` | `-0.02%` | `49.0%` | `-7.54%` |
| streak | `1d` | `199` | `+0.18%` | `63.3%` | `+42.74%` |
| normal | `10d` | `64` | `-1.04%` | `32.8%` | `-50.07%` |
| streak | `10d` | `54` | `-0.40%` | `35.2%` | `-22.19%` |
| streak | `20d` | `30` | `-1.44%` | `33.3%` | `-37.09%` |

#### long/short combined は 1d streak が compound では強いが、主役は bearish-buy 側。

| Strategy | Model | Hold | Trades | Mean | Win rate | Compound |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `long_bear_short_bull` | streak | `1d` | `344` | `+0.24%` | `61.9%` | `+122.96%` |
| `long_bear_short_bull` | normal | `5d` | `141` | `+0.36%` | `55.3%` | `+58.59%` |
| `long_bear_short_bull` | streak | `20d` | `32` | `+0.78%` | `53.1%` | `+24.16%` |

### Interpretation

旧解釈は破棄する。以前は「使える側は bearish 後買いで、streak mode が自然」と読んでいたが、streak parameter が future-derived なので、normal vs streak の優劣や hold day の比較は final OOS で読み直す必要がある。

### Production Implication

production / ranking / screening には使わない。TOPIX regime の direct trading rule にも、TOPIX100 個別株研究の mode definition にも、この run の streak preference を使わない。PIT-safe parameter selection と final OOS comparison を分離して再実行する。

### Caveats

これは通常の caveat ではなく **P0 invalidation**。簡易 backtest、cost、slippage、tax、turnover、short 実装以前に、streak trigger selection が future-derived である。fixed validation split はこの leak を十分に防いでいない。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix_extreme_mode_mean_reversion_comparison.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_extreme_mode_mean_reversion_comparison.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-extreme-mode-mean-reversion-comparison/20260406_100115_8dc36bd0`
- Tables: `results.duckdb`

## Purpose

- normal daily extreme mode と streak extreme mode を同一 execution assumption で比較する。
- bearish 後 mean-reversion が実行候補になるかを確認する。
- 後続の TOPIX100 streak 3/53 研究に渡す mode definition を決める。

## Scope

- Index:
  - TOPIX
- Models:
  - normal daily extreme mode
  - streak-candle extreme mode
- Strategies:
  - `long_on_bearish`
  - `short_on_bullish`
  - `long_bear_short_bull`
- Hold days:
  - `1`, `5`, `10`, `20`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_extreme_mode_mean_reversion_comparison.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_extreme_mode_mean_reversion_comparison.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix-extreme-mode-mean-reversion-comparison/20260406_100115_8dc36bd0`

## Current Read

- future leak により invalidated。
- streak mode を execution trigger の baseline にする判断は撤回する。
- normal vs streak / hold day / directionality は PIT-safe rerun まで保留する。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_extreme_mode_mean_reversion_comparison.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- streak parameter selection と execution comparison を walk-forward で分離できるか。
- final OOS で `long_on_bearish` は残るか。
- PIT-safe に選び直した mode definition を TOPIX100 へ渡せるか。
