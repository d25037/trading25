# TOPIX Extreme Close-to-Close Mode

TOPIX の日次 close-to-close return から、直近 `X` 日で最も大きい絶対値の変化を mode として採用し、bullish / bearish mode の segment と forward return を比較する実験です。

## Published Readout

### Decision

close-to-close extreme mode は「現在の地合い segment」を説明する diagnostic として有用だが、forward timing rule にはしない。選択された `X=2` は validation の segment return では bullish / bearish を分けるが、forward return では bearish mode 後の mean が高く、順張り exposure signal としては逆向きに崩れる。

### Why This Research Was Run

先行する TOPIX regime / shock 系研究では、地合いの状態を短く表す特徴が必要だった。この研究では、日次 close-to-close の最大変化に基づく simple mode が、TOPIX の bullish / bearish segment を分けられるか、さらにそのまま forward exposure timing に使えるかを確認した。後続の `topix-streak-extreme-mode` と multi-timeframe mode の土台になる研究でもある。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。available range は `2016-03-25 -> 2026-04-03`、analysis range は `2016-06-23 -> 2026-03-05`。candidate windows は `2..60`、future horizons は `1` / `5` / `10` / `20`、validation ratio は `0.30`。選択 metric は discovery segment composite score で、selected overall window は `2`、short window は `2`、long window は `41`。

### Main Findings

#### `X=2` は segment state を分けるが、validation の directional accuracy は強くない。

| Split | Bull segment return | Bear segment return | Spread | Directional accuracy | Bull days | Bear days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| discovery | `+0.9181%` | `-0.8739%` | `+1.7920%` | `58.7%` | `3.4` | `2.9` |
| validation | `+1.0215%` | `-0.4919%` | `+1.5135%` | `51.6%` | `3.3` | `2.4` |
| full | `+0.9509%` | `-0.7525%` | `+1.7035%` | `56.5%` | `3.4` | `2.7` |

#### forward return は bearish mode 後の方が高く、順張り timing にはならなかった。

| Horizon | Bull mean | Bear mean | Bull hit+ | Bear hit+ | Bull - Bear |
| --- | ---: | ---: | ---: | ---: | ---: |
| `1d` | `+0.0651%` | `+0.1415%` | `54.9%` | `60.0%` | `-0.0764%` |
| `5d` | `+0.3611%` | `+0.6005%` | `59.8%` | `62.0%` | `-0.2394%` |
| `10d` | `+0.6623%` | `+1.2289%` | `60.7%` | `69.0%` | `-0.5666%` |
| `20d` | `+1.2385%` | `+2.4099%` | `66.1%` | `77.0%` | `-1.1714%` |

#### 4-state では short/long が揃う segment は説明しやすいが、5d forward は別物だった。

| State | Segment return | Positive segment ratio | Mean segment days |
| --- | ---: | ---: | ---: |
| Long Bullish / Short Bullish | `+0.9495%` | `66.2%` | `3.5` |
| Long Bullish / Short Bearish | `-0.2627%` | `22.2%` | `2.2` |
| Long Bearish / Short Bullish | `+0.7697%` | `46.4%` | `2.6` |
| Long Bearish / Short Bearish | `-0.6025%` | `29.8%` | `2.5` |

### Interpretation

close-to-close extreme mode は、今どちらの segment にいるかを説明するには使える。しかし forward return では bearish 後の mean が高く、これは mean reversion や market rebound を拾っている可能性が高い。したがって、この feature をそのまま「bullish mode なら long / bearish mode なら risk-off」と読むと誤る。

### Production Implication

production exposure timing には昇格しない。使うなら market regime annotation、または後続の mean-reversion comparison の入力として扱う。forward timing を狙う場合は、close-to-close の日次 mode ではなく、streak candle 化や downside volatility confirmation のように別の安定化が必要。

### Caveats

validation は fixed split であり、walk-forward ではない。`X=2` はかなり短く、ノイズと反転を拾いやすい。TOPIX 単体の close-to-close 研究で、個別株への転用、execution timing、cost、portfolio overlay の実装制約は未評価。bundle は `git_dirty: true` の run なので、再利用前に current runner で再現確認する。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix_extreme_close_to_close_mode.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_extreme_close_to_close_mode.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-extreme-close-to-close-mode/20260406_100054_8dc36bd0`
- Tables: `results.duckdb`

## Purpose

- TOPIX の日次 close-to-close extreme mode が segment state を分けるかを見る。
- 同じ mode が forward exposure timing に使えるかを検証する。
- 後続の streak mode / multi-timeframe mode / mean-reversion comparison の baseline にする。

## Scope

- Index:
  - TOPIX
- Candidate windows:
  - `2..60`
- Future horizons:
  - `1`, `5`, `10`, `20`
- Validation ratio:
  - `0.30`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_extreme_close_to_close_mode.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_extreme_close_to_close_mode.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix-extreme-close-to-close-mode/20260406_100054_8dc36bd0`

## Current Read

- `X=2` は segment の現在状態を分けるが、forward return は bearish 後の方が高い。
- そのまま順張り exposure timing に使うのではなく、mean-reversion diagnostic として扱う。
- 後続では streak candle 化や multi-timeframe confirmation が必要。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_extreme_close_to_close_mode.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- streak candle 化で segment state と forward timing のズレは改善するか。
- bearish mode 後の高い forward return は mean-reversion rule として利用できるか。
- long/short multi-timeframe state は downside overlay として使えるか。
