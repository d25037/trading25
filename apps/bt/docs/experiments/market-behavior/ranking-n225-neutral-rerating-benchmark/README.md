# Ranking N225 Neutral Rerating Benchmark

## Published Readout

### Decision

Daily Ranking Research Base に追加した `N225_UNDERPX` benchmark を使い、Prime の `neutral_rerating` に限定して、既存の long 候補 scaffold が日経平均 excess でも強いかを全区間で検証した。

結論:

- Full-history run の観測期間は `2016-09-07` から `2026-06-16`。`--start-date` は指定せず、local `market.duckdb` の利用可能範囲まで広げた。
- `neutral_rerating` 全体は N225 excess では弱い。20D median `-0.661%`、60D median `-1.888%`。
- `Deep Value` は N225 excess でも改善するが、full-history では 20D median `+0.337%`、60D median `+0.118%` に留まる。2018 / 2020 / 2026 が弱い。
- 20D は `Deep Value + Sector Strong + ATR20 Accel` が最も実務的に強い。20D median N225 excess `+2.475%`、win rate `64.60%`、severe loss `5.76%`。
- 60D は 2024以降 run と違い、Momentum confirmation が full-history では安定しない。`Deep Value + Sector Strong + ATR20 Accel` は60D median `+1.567%`、`Deep Value + Sector Strong + Momentum` は `-0.918%`。
- `Deep Value + Sector Strong + ATR20 Accel + Momentum` は20Dでは良いが、60Dでは N225 excess median `-1.330%` まで落ちる。全部載せを hard filter にしない。
- 日経平均 benchmark は TOPIX benchmark より厳しい。60Dではほぼ全 signal で N225 excess median が TOPIX excess median を下回り、日経平均を超える hurdle として機能する。

### Main Findings

#### 結論: neutral rerating 全体は日経平均に対して弱く、Deep Value が必要

| Signal | Horizon | Obs | Median N225 excess | Win rate | Severe loss | Median TOPIX excess |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Neutral all | 20D | 1,136,734 | -0.661% | 46.07% | 9.48% | -0.500% |
| Deep Value | 20D | 78,060 | +0.337% | 52.02% | 6.48% | +0.368% |
| Sector Strong | 20D | 152,672 | -0.725% | 46.05% | 10.98% | -0.541% |
| ATR20 Accel ex-overheat | 20D | 146,053 | -0.301% | 48.26% | 8.58% | -0.283% |
| Momentum 20/60 top20 | 20D | 113,300 | -0.756% | 46.45% | 14.17% | -0.613% |
| Neutral all | 60D | 1,121,620 | -1.888% | 43.85% | 25.06% | -1.489% |
| Deep Value | 60D | 77,043 | +0.118% | 50.40% | 19.65% | +0.595% |

#### 結論: 20D は Deep Value + Sector Strong + ATR20 Accel が最も素直

20D では `Deep Value + Sector Strong + ATR20 Accel` が N225 excess median `+2.475%`。Momentum も同時に満たす subset は win rate `67.18%`、severe loss `3.85%` だが、観測数は `390` まで減る。

| Signal | Horizon | Obs | Codes | Median N225 excess | Win rate | Severe loss | Bank share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value + Sector Strong | 20D | 15,338 | 391 | +1.486% | 57.85% | 7.03% | 57.11% |
| Deep Value + ATR20 Accel | 20D | 10,389 | 471 | +1.195% | 57.12% | 5.51% | 32.40% |
| Deep Value + Momentum | 20D | 5,426 | 270 | +0.828% | 54.31% | 8.98% | 37.06% |
| Deep Value + Sector Strong + ATR20 Accel | 20D | 2,726 | 227 | +2.475% | 64.60% | 5.76% | 64.34% |
| Deep Value + Sector Strong + Momentum | 20D | 2,363 | 146 | +1.280% | 57.26% | 8.13% | 65.04% |
| Deep Value + Sector Strong + ATR20 Accel + Momentum | 20D | 390 | 73 | +2.519% | 67.18% | 3.85% | 70.00% |

#### 結論: 60D は Momentum ではなく、Deep Value + Sector Strong を中心に読む

2024以降 run では60D Momentum が強かったが、full-history では過去局面で崩れる。60D は `Deep Value + Sector Strong` が median `+1.341%`、ATR20 Accel 併用が `+1.567%`。Momentum 併用は `-0.918%` で、全区間の hard filter にはしない。

| Signal | Horizon | Obs | Codes | Median N225 excess | Win rate | Severe loss | Bank share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value + Sector Strong | 60D | 15,120 | 381 | +1.341% | 53.53% | 19.67% | 57.12% |
| Deep Value + ATR20 Accel | 60D | 10,291 | 465 | +0.076% | 50.24% | 20.79% | 32.57% |
| Deep Value + Momentum | 60D | 5,381 | 269 | -0.269% | 49.32% | 24.88% | 37.24% |
| Deep Value + Sector Strong + ATR20 Accel | 60D | 2,708 | 223 | +1.567% | 53.47% | 20.24% | 64.25% |
| Deep Value + Sector Strong + Momentum | 60D | 2,354 | 146 | -0.918% | 47.45% | 24.89% | 65.00% |
| Deep Value + Sector Strong + ATR20 Accel + Momentum | 60D | 390 | 73 | -1.330% | 47.95% | 24.62% | 70.00% |

#### 結論: 年別では局面差が大きく、Momentum は近年だけ強い

`Deep Value + Sector Strong + Momentum` は 2025 / 2026 では強いが、2021-2023 の60Dで弱い。full-history の production implication では、Momentum は「近年 regime の補助診断」に留める。

| Signal | Horizon | Year | Obs | Median N225 excess | Win rate | Severe loss |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Deep Value | 20D | 2018 | 3,753 | -1.728% | 36.61% | 6.42% |
| Deep Value | 20D | 2020 | 4,865 | -2.991% | 31.24% | 11.72% |
| Deep Value | 20D | 2022 | 9,063 | +2.003% | 62.80% | 2.32% |
| Deep Value | 20D | 2023 | 11,787 | +2.121% | 61.14% | 6.07% |
| Deep Value | 20D | 2026 | 3,224 | -0.417% | 47.70% | 11.32% |
| Deep Value + Sector Strong + ATR20 Accel | 20D | 2021 | 332 | -7.665% | 19.28% | 31.93% |
| Deep Value + Sector Strong + ATR20 Accel | 20D | 2022 | 539 | +4.640% | 80.15% | 0.19% |
| Deep Value + Sector Strong + ATR20 Accel | 20D | 2023 | 519 | +4.676% | 78.03% | 0.96% |
| Deep Value + Sector Strong + ATR20 Accel | 20D | 2025 | 580 | +4.460% | 77.76% | 1.72% |
| Deep Value + Sector Strong + Momentum | 60D | 2021 | 245 | -3.602% | 42.45% | 36.33% |
| Deep Value + Sector Strong + Momentum | 60D | 2022 | 272 | -2.476% | 40.44% | 20.96% |
| Deep Value + Sector Strong + Momentum | 60D | 2023 | 646 | -4.528% | 36.38% | 32.66% |
| Deep Value + Sector Strong + Momentum | 60D | 2025 | 608 | +5.302% | 61.02% | 20.39% |
| Deep Value + Sector Strong + Momentum | 60D | 2026 | 221 | +7.480% | 65.16% | 15.38% |

### Interpretation

N225 excess は TOPIX excess より厳しい benchmark になった。`neutral_rerating` 全体や単独の sector / ATR / momentum は日経平均に対して十分ではなく、Deep Value を土台にする必要がある。

20D の実務読みは、`Deep Value + Sector Strong + ATR20 Accel ex-overheat` を短期 timing confirmation として使うのが自然。60D の full-history 読みは、`Deep Value + Sector Strong` を中心にし、ATR20 Accel は小幅な上乗せ、Momentum は近年 regime の補助診断に留める。

Bank share は良い signal ほど高い。`Deep Value + Sector Strong + ATR20 Accel` は 20D bank share `64.34%`、60D `64.25%` で、sector-balanced portfolio lens なしに production hard filter へ昇格しない。

### Production Implication

- Ranking long-side の既存方向、`Deep Value` を主条件にし、`Sector Strong` と technical confirmation を重ねる読みは N225 benchmark でも維持する。
- 20D candidate priority は `Deep Value + Sector Strong + ATR20 Accel ex-overheat` を高く見る。
- 60D / holding continuation は、full-history では `Deep Value + Sector Strong` を中心にし、Momentum を必須条件にしない。
- `ATR20 Accel` と `Momentum` を同時必須にする全部載せは、特に60Dでは採用しない。
- 日経平均 benchmark の導入後も、TOPIX excess を捨てない。N225 excess は大型・日経平均寄り hurdle、TOPIX excess は広範市場 hurdle として併記する。

### Caveats

- Run window は full-history run の local coverage に基づき `2016-09-07` から `2026-06-16` まで。2016 と 2026 は partial year。
- `N225_UNDERPX` は日経225 options の `UnderPx` 由来 synthetic index で、true OHLC index ではない。close-to-close benchmark として使い、intraday range 解析には使わない。
- Sector Strong 併用の良い signal は銀行比率が高い。sector cap / sector-balanced portfolio では未検証。
- 今回は Prime / `neutral_rerating` に限定した。Crowded / Standard / Growth には外挿しない。

### Source Artifacts

- Runner: `uv run --project apps/bt python apps/bt/scripts/research/run_ranking_n225_neutral_rerating_benchmark.py --horizons 20,60 --markets prime --liquidity-regimes neutral_rerating --min-observations 100 --run-id 20260617_n225_neutral_rerating_prime_full_history`
- Bundle: `~/.local/share/trading25/research/market-behavior/ranking-n225-neutral-rerating-benchmark/20260617_n225_neutral_rerating_prime_full_history`
- Results DB: `~/.local/share/trading25/research/market-behavior/ranking-n225-neutral-rerating-benchmark/20260617_n225_neutral_rerating_prime_full_history/results.duckdb`
- Summary: `~/.local/share/trading25/research/market-behavior/ranking-n225-neutral-rerating-benchmark/20260617_n225_neutral_rerating_prime_full_history/summary.md`
- Domain module: `apps/bt/src/domains/analytics/ranking_n225_neutral_rerating_benchmark.py`

