# High-Zone Bearish Price Action

高値圏で出る下方向 price action について、`Bearish Engulfing` 単体ではなく、
包み足・大陰線・終値安値寄り・出来高増加を分解して、その後の tradeable return が
Prime / Standard で悪化するかを見る研究。

## Published Readout

### Decision

`strict_bearish_engulfing` 単体は弱気 filter として採用しない。高値圏で strict
包み足が出ても、Prime / Standard の 5d return は同日同市場 high-zone baseline より
むしろ良かった。

採用候補として残すのは `large_red_atr_1_6`、特に `volume_ratio_20d >= 2.0` を伴う
大陰線。これは short signal ではなく、既存 long candidate の短期除外 filter として
扱う。`large_red_atr_1_2` は弱い warning、`large_red_atr_0_8` や
`bearish_outside_day` は単独では採用しない。

### Why This Research Was Run

高値圏での `Bearish Engulfing` は弱気シグナルと言われるが、日本株の日足では
strict な包み足の件数や gap 条件への依存が強い可能性がある。そこで、
包み足そのものと、包んではいないが `body / ATR` が大きい陰線、出来高・売買代金の
増加を同じ event-study ledger で比較する。

### Data Scope / PIT Assumptions

`market.duckdb` の `stock_data` / `topix_data` / `stock_master_daily` を使う。
universe は price date と同日の `stock_master_daily` で Prime / Standard を
PIT 解決する。シグナルは日足確定後にしか分からないため、評価 return は
`next open -> future close` を使い、当日 close 約定を採用しない。

### Main Findings

#### 結論

`Bearish Engulfing` というローソク足名そのものより、`body / prior ATR` が大きく、
かつ volume expansion があるかの方が重要だった。

| Market | Candidate | Volume | Events | 5d Return | 5d Excess | Same-Day High-Zone Lift | 5% Loss Rate |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `Prime` | `high_zone_all` | `all` | `2,181,087` | `0.15%` | `-0.00%` | `0.00%` | `9.6%` |
| `Prime` | `strict_bearish_engulfing` | `all` | `96,461` | `0.28%` | `0.06%` | `0.06%` | `8.9%` |
| `Prime` | `large_red_atr_1_2` | `all` | `51,847` | `0.19%` | `-0.07%` | `-0.03%` | `10.9%` |
| `Prime` | `large_red_atr_1_6` | `all` | `18,599` | `0.10%` | `-0.24%` | `-0.13%` | `12.4%` |
| `Prime` | `large_red_atr_1_6` | `volume_ratio_20d_ge_2` | `8,339` | `0.01%` | `-0.27%` | `-0.14%` | `15.8%` |
| `Standard` | `high_zone_all` | `all` | `1,335,387` | `0.24%` | `0.06%` | `0.00%` | `11.6%` |
| `Standard` | `strict_bearish_engulfing` | `all` | `60,595` | `0.45%` | `0.23%` | `0.19%` | `10.8%` |
| `Standard` | `large_red_atr_1_2` | `all` | `47,174` | `0.24%` | `-0.00%` | `-0.03%` | `12.8%` |
| `Standard` | `large_red_atr_1_6` | `all` | `22,190` | `0.07%` | `-0.18%` | `-0.20%` | `14.4%` |
| `Standard` | `large_red_atr_1_6` | `volume_ratio_20d_ge_2` | `12,316` | `-0.35%` | `-0.57%` | `-0.62%` | `19.2%` |

#### Horizon Stability

| Market | Candidate | Volume | 1d Excess | 3d Excess | 5d Excess | 10d Excess | 20d Excess |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `Prime` | `strict_bearish_engulfing` | `all` | `-0.01%` | `0.01%` | `0.06%` | `0.06%` | `0.13%` |
| `Prime` | `large_red_atr_1_6` | `all` | `-0.12%` | `-0.19%` | `-0.24%` | `-0.23%` | `-0.13%` |
| `Prime` | `large_red_atr_1_6` | `volume_ratio_20d_ge_2` | `-0.18%` | `-0.23%` | `-0.27%` | `-0.23%` | `-0.09%` |
| `Standard` | `strict_bearish_engulfing` | `all` | `-0.07%` | `0.19%` | `0.23%` | `0.32%` | `0.64%` |
| `Standard` | `large_red_atr_1_6` | `all` | `-0.08%` | `-0.16%` | `-0.18%` | `-0.05%` | `0.35%` |
| `Standard` | `large_red_atr_1_6` | `volume_ratio_20d_ge_2` | `-0.24%` | `-0.47%` | `-0.57%` | `-0.50%` | `-0.23%` |

### Interpretation

今回の大きな発見は、`Bearish Engulfing` の名前で語られる弱気性が、このデータでは
確認できなかったこと。Prime の strict 包み足は 5d excess `+0.06%`、Standard は
`+0.23%` で、同日同市場の high-zone baseline 比でもプラスだった。包み足は
「前日陽線を当日陰線で包む」という形状条件が強い一方、下落の強さを十分に測っていない。

一方、`large_red_atr_1_6` は Prime / Standard とも 1d-10d で一貫して悪い。
特に Standard で `volume_ratio_20d >= 2.0` を伴うと、5d excess `-0.57%`、
同日 high-zone baseline lift `-0.62%`、5% loss rate `19.2%` まで悪化した。
これは「高値圏で crowding した後に、出来高を伴って大きく売られた」状態を
包み足より直接に拾っている可能性が高い。

ただし、Prime では効果は Standard より小さい。Prime の volume 2x +
`large_red_atr_1_6` は 5d excess `-0.27%` だが、20d では `-0.09%` まで薄れる。
したがって、これは長期 bearish regime ではなく、短期の避けるべき
price-action shock と読む。

### Production Implication

最初から short signal として採用せず、既存 long candidate の除外 filter として読む。
採用候補は、5d / 10d の TOPIX excess と同日同市場 high-zone baseline 比の両方で
悪化し、Prime / Standard のどちらかで十分な event 数を持つものに限定する。

次の実装候補は以下。

- `high_zone_large_red_atr_1_6_volume_surge`: high-zone かつ `red_body_atr_ratio >= 1.6` かつ `volume_ratio_20d >= 2.0`
- `high_zone_large_red_atr_1_6`: volume 条件なしの軽い警戒 flag
- `strict_bearish_engulfing`: 採用しない。表示するなら diagnostic に留める

### Caveats

日足のみの研究なので intraday の発生順序は見ない。出来高増加は raw volume と
trading value の trailing average 比で見るため、立会中の進行形 volume は扱わない。
同日に event が集中する market shock の影響は、TOPIX excess と同日 high-zone baseline
lift で一次補正する。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/high_zone_bearish_price_action.py`
- Runner: `apps/bt/scripts/research/run_high_zone_bearish_price_action.py`
- Bundle: `/tmp/trading25-research/market-behavior/high-zone-bearish-price-action/20260504_073307_f0ee7bac/`
- Results DB: `/tmp/trading25-research/market-behavior/high-zone-bearish-price-action/20260504_073307_f0ee7bac/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/high-zone-bearish-price-action/20260504_073307_f0ee7bac/summary.md`

## Current Surface

- Output tables:
  - `universe_summary_df`
  - `pattern_summary_df`
  - `top_negative_patterns_df`
  - `sampled_events_df`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_high_zone_bearish_price_action.py \
  --output-root /tmp/trading25-research
```
