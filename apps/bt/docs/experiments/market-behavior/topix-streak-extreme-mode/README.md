# TOPIX Streak Extreme Mode

TOPIX の連続上昇/下落日を streak candle に圧縮し、直近 `X` 本の streak candle で最も大きい絶対値の candle を mode として採用する実験です。

## Published Readout

### Decision

この run は **future leak により invalidated**。`X=3` streaks は full-period discovery / validation の forward return を見て選ばれており、その同じ履歴上で「segment state が強い」「bearish 後 rebound」と読んでいる。window selection が future-derived なので、`X=3` を標準 streak mode とする判断、validation headline、後続 TOPIX100 への転用根拠は使わない。

### Why This Research Was Run

先行研究 `topix-extreme-close-to-close-mode` では日次 `X=2` mode が segment を分けたが、forward return は bearish 後の方が高かった。この研究では、日次 noise を streak candle にまとめることで、mode の状態説明力と forward timing のズレが改善するかを確認した。後続の multi-timeframe mode と TOPIX100 streak LightGBM 系列の土台でもある。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。available range は `2016-03-25 -> 2026-04-03`、analysis range は `2016-08-30 -> 2026-03-04`。candidate windows は `2..60` streak candles、future horizons は `1` / `5` / `10` / `20` calendar days from streak end、validation ratio は `0.30`。selected window は `3` streaks、selection metric は discovery mode-segment composite score。

ただし、window selection 自体が PIT-safe ではない。将来リターンを含む discovery / validation 結果で `X=3` を選び、その同じ historical run の readout で state / rebound を解釈しているため、strict OOS parameter selection になっていない。これは後続の TOPIX100 streak 系にも伝播する future-derived parameter leak として扱う。

### Main Findings

#### future-derived window selection があるため、旧 headline は evidence ではない。

| Item | 旧 readout の見え方 | 現在の扱い |
| --- | --- | --- |
| Selected window | `X=3` streaks を採用 | future return を見た parameter selection のため invalid |
| Validation state split | directional accuracy `100%` | window selection 後の同一履歴評価なので evidence にしない |
| Forward timing | bearish 後 rebound diagnostic | strict OOS rerun まで保留 |
| Downstream use | multi-timeframe / TOPIX100 streak の土台 | downstream feature として使わない |

#### `X=3` streaks は validation でも segment state を非常に強く分けた。

| Split | Bull segment return | Bear segment return | Spread | Directional accuracy | Bull candles | Bear candles | Bull days | Bear days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| discovery | `+3.0178%` | `-2.5160%` | `+5.5338%` | `100.0%` | `3.3` | `2.8` | `7.0` | `5.4` |
| validation | `+3.4961%` | `-2.3690%` | `+5.8651%` | `100.0%` | `3.4` | `2.5` | `7.6` | `4.3` |
| full | `+3.1811%` | `-2.4709%` | `+5.6520%` | `100.0%` | `3.4` | `2.7` | `7.2` | `5.0` |

#### forward return は bearish mode 後が高く、順張りではなく rebound を示した。

| Horizon | Bull mean | Bear mean | Bull hit+ | Bear hit+ | Bull - Bear |
| --- | ---: | ---: | ---: | ---: | ---: |
| `1d` | `-0.2670%` | `+0.4783%` | `35.7%` | `69.9%` | `-0.7453%` |
| `5d` | `+0.1527%` | `+0.7612%` | `54.3%` | `67.1%` | `-0.6085%` |
| `10d` | `+0.4610%` | `+1.3784%` | `58.8%` | `67.1%` | `-0.9174%` |
| `20d` | `+1.1648%` | `+2.9190%` | `63.3%` | `79.5%` | `-1.7542%` |

### Interpretation

旧解釈は破棄する。以前は「streak candle 化で state 説明力が改善し、bearish 後 rebound を拾う」と読んでいたが、`X=3` の選択が future-derived なので、その解釈が本当に out-of-sample に残るかは未確認。state label としても後続に渡さない。

### Production Implication

production / ranking / screening には使わない。TOPIX streak mode は、window selection と validation を分離した walk-forward / pure OOS で再実行するまで、market state label、mean-reversion filter、TOPIX100 feature のどれにも昇格しない。

### Caveats

これは通常の caveat ではなく **P0 invalidation**。fixed split という弱さに加えて、future return を見た window selection が readout の中心判断に入っている。streak candle の entry timing、cost、slippage、portfolio overlay 以前に、parameter-selection leak を除去した rerun が必要。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix_streak_extreme_mode.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_streak_extreme_mode.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-streak-extreme-mode/20260406_100103_8dc36bd0`
- Tables: `results.duckdb`

## Purpose

- 日次 close-to-close mode の noise を streak candle 化で減らす。
- segment state の説明力と forward timing のズレを確認する。
- 後続の multi-timeframe mode / TOPIX100 streak LightGBM 系列の baseline にする。

## Scope

- Index:
  - TOPIX
- Candidate windows:
  - `2..60` streak candles
- Selected window:
  - `3` streaks
- Future horizons:
  - `1`, `5`, `10`, `20`

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_streak_extreme_mode.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_streak_extreme_mode.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix-streak-extreme-mode/20260406_100103_8dc36bd0`

## Current Read

- future leak により invalidated。
- `X=3` streaks の採用判断は future-derived parameter selection を含む。
- PIT-safe rerun まで market state label / rebound diagnostic として使わない。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_streak_extreme_mode.py
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

## Next Questions

- window selection と validation を walk-forward で分離すると `X=3` は残るか。
- pure OOS でも bearish 後 rebound は残るか。
- PIT-safe に選び直した TOPIX streak state を TOPIX100 feature に使えるか。
