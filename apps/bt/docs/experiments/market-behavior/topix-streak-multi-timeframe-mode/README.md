# TOPIX Streak Multi-Timeframe Mode

TOPIX の streak-candle mode を short window と long window で組み合わせ、4-state の forward-return ordering がどれだけ安定するかを比較する実験です。

## Published Readout

### Decision

この run は **future leak により invalidated**。`short=3 / long=53` は full-period pair scan の future return ordering / validation stability を見て選ばれており、その同じ履歴上で標準 context pair として採用している。したがって、3/53 state を TOPIX streak context の標準 pair や TOPIX100 LightGBM feature として使わない。

### Why This Research Was Run

先行研究 `topix-streak-extreme-mode` と `topix-extreme-mode-mean-reversion-comparison` では、single-window streak mode が bearish 後 rebound の実行側候補になった。この研究では、短期 streak trigger に長期 streak regime を重ねることで、forward return の state ordering が安定するか、また `short=3` をそのまま使うべきかを確認した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。available range は `2016-03-25 -> 2026-04-03`、analysis range は `2016-08-30 -> 2026-03-04`。candidate windows は `2..60` streak candles、validation ratio は `0.30`、minimum state observations は `12`。stability horizons は `5` / `10` / `20`。selected pair は short `3` streaks、long `53` streaks。

ただし、pair selection が PIT-safe ではない。future return ordering を含む pair scan で `short=3 / long=53` を選び、同じ historical range で stability と downstream decision を読んでいる。これは validation split があるだけでは不十分で、parameter selection と final evaluation を分離した walk-forward / pure OOS が必要。

### Main Findings

#### future-derived pair selection があるため、3/53 標準化は撤回する。

| Item | 旧 readout の見え方 | 現在の扱い |
| --- | --- | --- |
| Selected pair | `short=3 / long=53` を標準 context pair | future return を見た pair selection のため invalid |
| Ordering | both-bearish が全 horizon で最上位 | selection 後の同一履歴評価なので evidence にしない |
| Downstream use | TOPIX100 stock ranking の market context feature | 使用停止 |
| Next step | TOPIX100 LightGBM へ渡す | PIT-safe pair selection からやり直す |

#### pair scan は `short=3 / long=53` を最上位に選び、best/worst state が全 horizon で固定された。

| Short | Long | Rank consistency | Edge lock | Mean spread | Best 5d | Worst 5d | Best 20d | Worst 20d |
| ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| `3` | `53` | `100.0%` | `100.0%` | `+2.14%` | Long Bearish / Short Bearish `+1.19%` | Long Bullish / Short Bullish `+0.00%` | Long Bearish / Short Bearish `+3.75%` | Long Bullish / Short Bullish `+0.41%` |
| `3` | `54` | `100.0%` | `100.0%` | `+2.12%` | Long Bearish / Short Bearish `+1.19%` | Long Bullish / Short Bullish `+0.00%` | Long Bearish / Short Bearish `+3.75%` | Long Bullish / Short Bullish `+0.44%` |
| `3` | `55` | `100.0%` | `100.0%` | `+2.10%` | Long Bearish / Short Bearish `+1.14%` | Long Bullish / Short Bullish `-0.06%` | Long Bearish / Short Bearish `+3.73%` | Long Bullish / Short Bullish `+0.45%` |

#### validation の 4-state forward ordering は、both-bearish が一貫して最上位だった。

| Horizon | 1st | 2nd | 3rd | 4th |
| ---: | --- | --- | --- | --- |
| `5d` | Long Bearish / Short Bearish `+1.19%` | Long Bearish / Short Bullish `+0.30%` | Long Bullish / Short Bearish `+0.13%` | Long Bullish / Short Bullish `+0.00%` |
| `10d` | Long Bearish / Short Bearish `+2.11%` | Long Bearish / Short Bullish `+0.70%` | Long Bullish / Short Bearish `+0.31%` | Long Bullish / Short Bullish `+0.22%` |
| `20d` | Long Bearish / Short Bearish `+3.75%` | Long Bearish / Short Bullish `+1.90%` | Long Bullish / Short Bearish `+1.69%` | Long Bullish / Short Bullish `+0.41%` |

#### segment view は trend alignment ではなく exhaustion state を示している。

| State | Segment return | Positive segment ratio | Mean candles | Mean days |
| --- | ---: | ---: | ---: | ---: |
| Long Bullish / Short Bullish | `+3.39%` | `100.0%` | `3.5` | `7.8` |
| Long Bullish / Short Bearish | `-1.78%` | `0.0%` | `2.5` | `3.6` |
| Long Bearish / Short Bullish | `+3.17%` | `100.0%` | `3.0` | `6.6` |
| Long Bearish / Short Bearish | `-2.71%` | `0.0%` | `2.5` | `4.7` |

### Interpretation

旧解釈は破棄する。以前は「3/53 は trend-following ではなく exhaustion/rebound hierarchy」と読んでいたが、3/53 の選択自体が future-derived なので、この hierarchy が本当に out-of-sample に残るかは未確認。3/53 を説明変数として固定する判断もしない。

### Production Implication

production / ranking / screening には使わない。後続の TOPIX100 個別株 selection へ `short=3 / long=53` を固定 feature として渡す判断は撤回する。まず pair selection を walk-forward 化し、final OOS で state ordering が残るかを確認する。

### Caveats

これは通常の caveat ではなく **P0 invalidation**。pair は validation stability で選択しているだけでなく、future return ordering を見た selection と readout の判断が同じ historical range に乗っている。TOPIX 単体/個別株転用/entry timing 以前に、future-derived parameter selection を除去する必要がある。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix_streak_multi_timeframe_mode.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_streak_multi_timeframe_mode.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-streak-multi-timeframe-mode/20260406_102304_8dc36bd0`
- Tables: `results.duckdb`

## Purpose

- single-window streak mode に long-window context を重ねる。
- 4-state ordering が 5d/10d/20d で安定する pair を選ぶ。
- TOPIX100 streak LightGBM 系列に渡す market context を決める。

## Scope

- Index:
  - TOPIX
- Selected pair:
  - short `3` streaks
  - long `53` streaks
- Stability horizons:
  - `5`, `10`, `20`
- Validation ratio:
  - `0.30`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_streak_multi_timeframe_mode.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_streak_multi_timeframe_mode.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix-streak-multi-timeframe-mode/20260406_102304_8dc36bd0`

## Current Read

- future leak により invalidated。
- `short=3 / long=53` を標準 context pair にする判断は撤回する。
- PIT-safe rerun まで TOPIX100 stock ranking の market context feature にしない。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_streak_multi_timeframe_mode.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- pair selection を walk-forward 化すると 3/53 は残るか。
- final OOS でも both-bearish rebound hierarchy は残るか。
- PIT-safe に選び直した state を TOPIX100 feature に使えるか。
