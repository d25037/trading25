# Ranking Long Sector Leadership Horizon Decomposition

## Published Readout

### Decision

Long側の `Momentum Value + Sector Score: Strong` は、現行 `Sector Score` だけで評価するのではなく、`Long Sector Leadership` を別軸として選択・比較できるようにする。長期 sector leadership は ex Banks では改善を示し、all sectors でも 20D は現行と概ね同等、60D は改善する。ただし short側の `Sector Score: Weak` は現行 `Sector Score` が機能しているため置換しない。

したがって完全置換ではなく、Daily Ranking / research runner で sector score family を `Current Sector Score` と `Long Sector Leadership` から選択できる二層化を次の実装候補にする。次の検証は、sector score の式をさらに足すより、sector cap / sector-balanced portfolio lens で「銀行が leader であることを認めつつ、銀行以外でも return source が残るか」を見る。

### Main Findings

#### 結論1: 2022-2026 の ex Banks では長期 leadership が現行 Sector Score を上回る

`Undervalued + 20/60D Momentum` に対して、現行 `Sector Score: Strong` と PIT 長期 sector leadership を比較した。数値は年次 median を observation count で加重した期間集計。

| period | horizon | sector scope | overlay | obs | median raw | median TOPIX excess | win rate | bank share | future top5 share |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022-2026 | 20D | ex Banks | Current Sector Score: Strong | 1,412 | 1.24% | 0.02% | 49.9% | 0.0% | 18.9% |
| 2022-2026 | 20D | ex Banks | Long Hybrid Leadership | 1,257 | 2.65% | 1.62% | 57.6% | 0.0% | 27.8% |
| 2022-2026 | 60D | ex Banks | Current Sector Score: Strong | 1,385 | 5.77% | 1.30% | 52.3% | 0.0% | 18.0% |
| 2022-2026 | 60D | ex Banks | Long Hybrid Leadership | 1,245 | 8.46% | 4.26% | 60.3% | 0.0% | 27.1% |

#### 結論2: all sectors では長期 leadership も銀行集中を解けない

2022-2026 の all sectors では `Long Hybrid Leadership` の成績は強いが、bank share は約75%で、現行 `Sector Score: Strong` と同じ問題を残す。

| period | horizon | overlay | obs | median raw | median TOPIX excess | win rate | bank share | future top5 share |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2022-2026 | 20D | Current Sector Score: Strong | 5,630 | 3.76% | 2.53% | 64.5% | 74.9% | 4.7% |
| 2022-2026 | 20D | Long Hybrid Leadership | 5,053 | 4.42% | 2.40% | 64.4% | 75.1% | 6.9% |
| 2022-2026 | 60D | Current Sector Score: Strong | 5,524 | 4.85% | 1.75% | 54.5% | 74.9% | 4.5% |
| 2022-2026 | 60D | Long Hybrid Leadership | 5,000 | 8.21% | 3.30% | 58.0% | 75.1% | 6.7% |

この結果は、銀行込みで `Long Hybrid Leadership` が現行より劣るという意味ではない。20D TOPIX excess は現行がわずかに上だが、raw return と60Dでは `Long Hybrid Leadership` が上回る。銀行集中は欠陥というより、銀行が実際に leader sector だった局面を正しく拾っている。ただし score を一本化すると short側の現行 `Sector Score: Weak` の有効性まで壊すため、置換ではなく選択可能化が妥当。

#### 結論3: 2016-2021 は 60D では改善するが、20D は不安定

2016-2021 の ex Banks では 60D の長期 leadership が改善する。一方、20D は `Long Hybrid Leadership` が悪く、2020 のコロナ局面で大きく崩れる。短期 timing signal としてはまだ弱い。

| period | horizon | sector scope | overlay | obs | median raw | median TOPIX excess | win rate | bank share | future top5 share |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2016-2021 | 20D | ex Banks | Current Sector Score: Strong | 1,004 | 1.91% | -0.36% | 48.8% | 0.0% | 25.4% |
| 2016-2021 | 20D | ex Banks | Long Hybrid Leadership | 274 | -0.26% | -0.84% | 44.9% | 0.0% | 57.3% |
| 2016-2021 | 60D | ex Banks | Current Sector Score: Strong | 1,004 | 4.37% | 0.78% | 52.2% | 0.0% | 25.4% |
| 2016-2021 | 60D | ex Banks | Long Hybrid Leadership | 274 | 1.99% | 4.27% | 51.5% | 0.0% | 57.3% |
| 2016-2021 | 60D | ex Banks | Current Strong + Long Hybrid Leadership | 89 | 17.38% | 18.76% | 73.0% | 0.0% | 75.3% |

#### 結論4: 年次では 2022/2023/2025 の ex Banks 改善が主な根拠、2024/2026 はまだ銀行依存

| year | horizon | sector scope | Current Sector Score: Strong excess | Long Hybrid Leadership excess | Long Hybrid obs | Long Hybrid bank share |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| 2022 | 20D | ex Banks | -0.08% | 1.09% | 248 | 0.0% |
| 2023 | 20D | ex Banks | 1.19% | 2.04% | 777 | 0.0% |
| 2024 | 20D | ex Banks | -1.06% | -1.72% | 35 | 0.0% |
| 2025 | 20D | ex Banks | -0.80% | 1.49% | 171 | 0.0% |
| 2026 | 20D | ex Banks | -4.06% | -0.26% | 26 | 0.0% |
| 2022 | 60D | ex Banks | 4.81% | 6.62% | 236 | 0.0% |
| 2023 | 60D | ex Banks | 1.25% | 2.85% | 777 | 0.0% |
| 2024 | 60D | ex Banks | -5.23% | -0.96% | 35 | 0.0% |
| 2025 | 60D | ex Banks | 2.26% | 9.74% | 171 | 0.0% |
| 2026 | 60D | ex Banks | -12.46% | -3.84% | 26 | 0.0% |

### Interpretation

長期 sector leadership は、短期の `Sector Score` とは違う情報を持っている。特に ex Banks では、2022-2026 の 20D/60D ともに現行 `Sector Score: Strong` より改善したため、「銀行以外から return を得る」方向の candidate として価値がある。

ただし、all sectors の `Long Hybrid Leadership` は 2025/2026 でほぼ銀行業になる。これは「銀行業が長期 winner として認識され続ける」こと自体を捕まえているだけで、銀行 beta を超える score とは言えない。銀行業を抜かずに採用すると、現行 `Sector Score: Strong` と同じ concentration 問題を再生産する。

2016-2021 では 60D の改善が見える一方、20D は不安定で、2018/2020 のような局面では長期 winner 追随が短期で逆風になる。したがって daily Ranking の long-side confidence overlay に直結するより、holding horizon / rebalance horizon を分ける必要がある。

### Production Implication

現時点で production Ranking の `Sector Score` を完全置換しない。候補として残すのは、次の3つ。

1. `Long Sector Leadership` は long-side research candidate として継続する。
2. `Sector Score` family を `Current Sector Score` / `Long Sector Leadership` から選択可能にする。long側は後者を優先候補、short側は前者を維持する。
3. 採用前に sector cap / sector-balanced portfolio lens を必須にする。とくに銀行業を除外するのではなく、銀行業の自然な強さを認めつつ、1セクター集中で headline return が決まらない構造にする。

short側はこの readout の対象外。既存の `Overvalued + Momentum + Sector Score: Weak` は、現行 `Sector Score` で十分に機能しているという前回結論を維持する。

### Caveats

- 2026 は anchor end date が 2026-05-14 の partial year。
- forward return は daily close-to-close の forward outcome で、portfolio construction、turnover、cost、capacity は未反映。
- `future top5 share` は、過去 discussion で確認した 2016-2026 の事後 sector winner 診断であり、signal には使っていない。
- 長期 leadership は 120/252/504D の過去 sector return rank だけで作り、future sector ranking は使っていない。
- `Long Hybrid Leadership` は sector-level bucket なので、sector内の銘柄選別や position cap は未検証。
- ATR20 Accel の補足診断では、pre-2022 TSE1 (`0101`) を `prime` に正規化しない market SoT bug が見つかった。`recent_return_threshold_forward_response._market_master_cte` を shared `MARKET_CODES_BY_SCOPE` に寄せて修正した後、2018-2021 の `Undervalued + ATR20 Accel + Long Hybrid Leadership` は 20D TOPIX excess -0.17%、60D +2.17% だった。ATR20 Accel は 20D の補助候補だが、60D では Momentum 20/60 の方が強い。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_ranking_long_sector_leadership_horizon_decomposition.py`
- Domain: `apps/bt/src/domains/analytics/ranking_long_sector_leadership_horizon_decomposition.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_ranking_long_sector_leadership_horizon_decomposition.py`
- Bundle: `/tmp/trading25-research/market-behavior/ranking-long-sector-leadership-horizon-decomposition/20260604_ranking_long_sector_leadership_prime_v1/`
- Tables: `annual_overlay_summary_df`, `bank_concentration_df`, `sector_contribution_df`, `leadership_horizon_df`, `current_vs_long_matrix_df`, `future_top5_diagnostic_df`, `overlay_comparison_df`

## Method

Base universe is Prime/TSE1 SoT resolved by `stock_master_daily_exact_date`. The base long condition is:

- `Undervalued`: same Daily Ranking terminology, implemented as PBR percentile <= 20% and forward PER percentile <= 20%.
- `20/60D Momentum`: both 20D and 60D recent return percentiles >= 80%.

Sector overlays are compared as:

- no sector overlay
- current `Sector Score: Strong`
- `Long Index Leadership`: past 120/252/504D sector index TOPIX-excess rank
- `Long Constituent/Breadth Leadership`: past 120/252/504D constituent TOPIX-excess rank and breadth rank
- `Long Hybrid Leadership`: average of index and constituent/breadth leadership
- crosses of current `Sector Score` and `Long Hybrid Leadership`

All long leadership inputs are anchor-date PIT past-return inputs only.
