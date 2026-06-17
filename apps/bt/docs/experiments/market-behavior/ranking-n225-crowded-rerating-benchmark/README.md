# Ranking N225 Crowded Rerating Benchmark

## Purpose

Daily Ranking Research Base の benchmark を TOPIX と N225 で比較できるようにしたうえで、`crowded_rerating` に絞り、long candidate として使える overlay を再検討する。

Neutral rerating では Deep Value が base anchor として残ったが、crowded rerating は需給と左テールの意味が違うため、別 research として記録する。

## Published Readout

Run: `20260617_n225_crowded_rerating_prime_full_history_v2`

### Source Artifacts

- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-n225-crowded-rerating-benchmark/20260617_n225_crowded_rerating_prime_full_history_v2/`
- Results DB: `results.duckdb`
- Runner summary: `summary.md`

Scope:

- Universe: Prime
- Liquidity regime: `crowded_rerating`
- Period: 2016-09-07 to 2026-06-16
- Observations: 184,349
- Codes: 1,046
- Benchmark coverage: N225 20D coverage 99.05%, sector strength 100.00%, ATR 100.00%

### Main Findings

Crowded rerating は neutral より明確に左テールが重い。`crowded_all` は N225 excess 20D median -1.23%、60D median -3.06% で、severe underperformance rate は 20D 19.55%、60D 34.71% まで上がる。

Deep Value は crowded の中でも有効な anchor だが、単独では neutral ほど安定しない。20D median は +0.38% まで改善する一方、60D median は -0.11% にとどまる。

最も強い候補は `Deep Value + Sector Strong + ATR20 Accel`。20D median +5.78%、win rate 74.70%、severe underperformance 5.42%、60D median +10.03% と、crowded の悪い base rate を大きく上書きする。ただし 166 observations / 32 codes で sample は薄い。

Momentum は crowded では hard filter にしない。`Deep Value + Sector Strong + Momentum` は 20D median +4.46% だが、60D median -4.54% へ崩れ、left tail も残る。standalone の `momentum_20_60_top20` と `sector_strong` は crowded_all より悪い場面が多い。

### Signal Results

| signal | horizon | obs | codes | median N225 excess | win rate | severe underperf | median TOPIX excess | N225 minus TOPIX |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| crowded_all | 20D | 182,602 | 1,041 | -1.23% | 45.28% | 19.55% | -1.11% | -0.12pp |
| deep_value | 20D | 7,753 | 162 | +0.38% | 51.92% | 12.15% | +0.33% | +0.05pp |
| sector_strong | 20D | 31,374 | 329 | -2.19% | 42.65% | 24.00% | -1.98% | -0.21pp |
| atr20_acceleration_ex_overheat | 20D | 21,220 | 314 | -0.94% | 46.23% | 18.38% | -0.94% | +0.00pp |
| momentum_20_60_top20 | 20D | 17,010 | 272 | -2.09% | 44.10% | 26.68% | -2.02% | -0.07pp |
| deep_value_sector_strong | 20D | 1,792 | 89 | +1.04% | 53.74% | 14.84% | +0.82% | +0.22pp |
| deep_value_atr20_acceleration | 20D | 943 | 90 | +2.02% | 59.38% | 5.09% | +1.57% | +0.44pp |
| deep_value_momentum | 20D | 471 | 37 | +4.21% | 60.93% | 18.90% | +4.29% | -0.07pp |
| deep_value_sector_strong_atr20_acceleration | 20D | 166 | 32 | +5.78% | 74.70% | 5.42% | +4.84% | +0.94pp |
| deep_value_sector_strong_momentum | 20D | 330 | 26 | +4.46% | 61.21% | 18.48% | +5.00% | -0.54pp |
| crowded_all | 60D | 179,581 | 1,036 | -3.06% | 43.57% | 34.71% | -2.79% | -0.27pp |
| deep_value | 60D | 7,672 | 159 | -0.11% | 49.67% | 26.79% | +0.23% | -0.34pp |
| sector_strong | 60D | 30,143 | 324 | -3.65% | 42.62% | 36.07% | -3.27% | -0.38pp |
| atr20_acceleration_ex_overheat | 60D | 21,020 | 313 | -3.00% | 43.49% | 33.98% | -2.82% | -0.18pp |
| momentum_20_60_top20 | 60D | 16,656 | 270 | -4.70% | 40.37% | 40.75% | -4.42% | -0.28pp |
| deep_value_sector_strong | 60D | 1,766 | 87 | -1.23% | 46.60% | 29.95% | -0.69% | -0.54pp |
| deep_value_atr20_acceleration | 60D | 939 | 89 | +0.35% | 51.12% | 24.07% | +1.18% | -0.83pp |
| deep_value_momentum | 60D | 471 | 37 | -1.95% | 47.77% | 36.52% | -1.72% | -0.22pp |
| deep_value_sector_strong_atr20_acceleration | 60D | 166 | 32 | +10.03% | 59.64% | 19.88% | +9.61% | +0.42pp |
| deep_value_sector_strong_momentum | 60D | 330 | 26 | -4.54% | 43.33% | 39.39% | -4.25% | -0.29pp |

### Interpretation

Crowded rerating を long に使うなら、`crowded` 自体は positive signal ではなく adverse regime とみなす。base rate が悪いため、long candidate は Deep Value を最低条件に置き、さらに ATR acceleration を要求するのが妥当。

`Sector Strong` は単独では逆効果だが、Deep Value + ATR acceleration と重なった場合だけ高 conviction overlay になる。これは sector flow そのものではなく、割安リセット後に反転の初動が出ているケースを拾っている可能性が高い。

`Momentum` は今回の crowded rerating では neutral rerating と同じ扱いにしない。20D の上振れは見えるが 60D の持続性と severe underperformance が悪く、Deep Value + Sector Strong + ATR acceleration の代替にはならない。

### Production Implication

Daily Ranking の long candidate として `crowded_rerating` を使う場合、crowded は positive label ではなく adverse regime として扱う。候補化するなら `Deep Value + ATR20 Accel` を最低ラインにし、`Deep Value + Sector Strong + ATR20 Accel` は sample-thin な high-conviction overlay として別ラベルにする。

### Caveats

- `Deep Value + Sector Strong + ATR20 Accel` は 166 observations / 32 codes で、headline は強いが sample が薄い。
- N225 coverage は 99.05% で、一部 horizon は benchmark 欠損により除外される。
- 今回は Prime + `crowded_rerating` に限定しており、neutral/stale/stress へ同じ判断を外挿しない。

### Decision

- `crowded_rerating` は neutral とは別 research として維持する。
- Production 候補に近いのは `Deep Value + ATR20 Accel`。
- `Deep Value + Sector Strong + ATR20 Accel` は strongest overlay だが sample-thin のため、label は high-conviction candidate に留める。
- `Momentum` は crowded rerating の必須条件にしない。

## Reproduction

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_ranking_n225_crowded_rerating_benchmark.py \
  --horizons 20,60 \
  --markets prime \
  --liquidity-regimes crowded_rerating \
  --min-observations 100 \
  --run-id 20260617_n225_crowded_rerating_prime_full_history_v2
```
